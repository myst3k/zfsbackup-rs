//! Push parser for `zfs send` streams.
//!
//! We never interpret the data — the stream is opaque to us — but we do walk
//! the record framing so we can:
//!
//! 1. read the BEGIN header (magic, header type, feature flags, to/from GUIDs,
//!    dataset name) before deciding whether to accept a stream;
//! 2. verify the running fletcher4 checksum ZFS embeds in every record, giving
//!    end-to-end integrity without a `zfs receive`;
//! 3. read the END record's checksum, which we store in the manifest and use
//!    to prove a reassembled stream is bit-identical to what the sender saw.
//!
//! Layout follows `dmu_replay_record_t` in OpenZFS `sys/zfs_ioctl.h`: a fixed
//! 312-byte record header (4-byte type, 4-byte payload length, 304-byte
//! union) optionally followed by a payload whose size depends on the type.
//! Compound streams (`zfs send -R` / `-I`) nest sub-streams, each with its own
//! BEGIN…END; we report every sub-stream's END.

use bytes::{Buf, BytesMut};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::types::Guid;
use crate::fletcher::Fletcher4;

pub const RECORD_LEN: usize = 312;
const CKSUM_OFF: usize = RECORD_LEN - 32;
const UNION_OFF: usize = 8;

pub const DMU_BACKUP_MAGIC: u64 = 0x0002_f5ba_cbac;

/// `drr_type` values.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum RecordType {
    Begin = 0,
    Object = 1,
    FreeObjects = 2,
    Write = 3,
    Free = 4,
    End = 5,
    WriteByRef = 6,
    Spill = 7,
    WriteEmbedded = 8,
    ObjectRange = 9,
    Redact = 10,
}

impl RecordType {
    fn from_u32(v: u32) -> Option<Self> {
        Some(match v {
            0 => Self::Begin,
            1 => Self::Object,
            2 => Self::FreeObjects,
            3 => Self::Write,
            4 => Self::Free,
            5 => Self::End,
            6 => Self::WriteByRef,
            7 => Self::Spill,
            8 => Self::WriteEmbedded,
            9 => Self::ObjectRange,
            10 => Self::Redact,
            _ => return None,
        })
    }
}

/// `DMU_BACKUP_FEATURE_*` bits from `drr_versioninfo`.
pub mod feature {
    pub const DEDUP: u64 = 1 << 0;
    pub const DEDUPPROPS: u64 = 1 << 1;
    pub const SA_SPILL: u64 = 1 << 2;
    pub const EMBED_DATA: u64 = 1 << 16;
    pub const LZ4: u64 = 1 << 17;
    // Bit positions verified against OpenZFS 2.2 streams (see deploy/dev/zfs-e2e.sh).
    pub const LARGE_BLOCKS: u64 = 1 << 19;
    pub const RESUMING: u64 = 1 << 20;
    pub const REDACTED: u64 = 1 << 21;
    pub const COMPRESSED: u64 = 1 << 22;
    pub const LARGE_DNODE: u64 = 1 << 23;
    pub const RAW: u64 = 1 << 24;
    pub const ZSTD: u64 = 1 << 25;
    pub const HOLDS: u64 = 1 << 26;
    pub const SWITCH_TO_LARGE_BLOCKS: u64 = 1 << 27;
    pub const LONGNAME: u64 = 1 << 28;

    pub fn names(bits: u64) -> Vec<&'static str> {
        let table = [
            (DEDUP, "dedup"),
            (DEDUPPROPS, "dedupprops"),
            (SA_SPILL, "sa_spill"),
            (EMBED_DATA, "embed_data"),
            (LZ4, "lz4"),
            (LARGE_BLOCKS, "large_blocks"),
            (RESUMING, "resuming"),
            (REDACTED, "redacted"),
            (COMPRESSED, "compressed"),
            (LARGE_DNODE, "large_dnode"),
            (RAW, "raw"),
            (ZSTD, "zstd"),
            (HOLDS, "holds"),
            (SWITCH_TO_LARGE_BLOCKS, "switch_to_large_blocks"),
            (LONGNAME, "longname"),
        ];
        table
            .iter()
            .filter(|(b, _)| bits & b != 0)
            .map(|(_, n)| *n)
            .collect()
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum HeaderType {
    /// A single dataset stream.
    Substream,
    /// `zfs send -R` / `-I`: a package of substreams.
    Compound,
}

/// Decoded `drr_begin`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct BeginHeader {
    pub header_type: HeaderType,
    /// Raw `DMU_BACKUP_FEATURE_*` bits. See [`feature`].
    pub features: u64,
    pub creation_time: u64,
    pub objset_type: u32,
    pub flags: u32,
    pub to_guid: Guid,
    /// Zero for a full stream.
    pub from_guid: Option<Guid>,
    pub to_name: String,
    /// Stream was produced on a machine of the opposite endianness.
    pub byteswapped: bool,
}

impl BeginHeader {
    pub fn is_incremental(&self) -> bool {
        self.from_guid.is_some()
    }
    pub fn is_raw(&self) -> bool {
        self.features & feature::RAW != 0
    }
    pub fn is_compressed(&self) -> bool {
        self.features & (feature::COMPRESSED | feature::LZ4 | feature::ZSTD) != 0
    }
    pub fn has_large_blocks(&self) -> bool {
        self.features & feature::LARGE_BLOCKS != 0
    }
    pub fn feature_names(&self) -> Vec<&'static str> {
        feature::names(self.features)
    }
}

/// Decoded `drr_end`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EndRecord {
    pub checksum: Fletcher4,
    pub to_guid: Guid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    Begin(BeginHeader),
    End(EndRecord),
}

#[derive(Debug, Error)]
pub enum StreamError {
    #[error("bad magic {0:#x}: not a zfs send stream")]
    BadMagic(u64),
    #[error("first record is not DRR_BEGIN")]
    NoBegin,
    #[error("unknown record type {0}")]
    UnknownRecordType(u32),
    #[error("record checksum mismatch at stream offset {offset} (record #{index})")]
    RecordChecksum { offset: u64, index: u64 },
    #[error(
        "END checksum mismatch at stream offset {offset}: stream says {expected:?}, computed {actual:?}"
    )]
    EndChecksum {
        offset: u64,
        expected: Fletcher4,
        actual: Fletcher4,
    },
    #[error("payload of {0} bytes is not 4-byte aligned")]
    UnalignedPayload(u64),
    #[error("stream ended mid-record at offset {0}")]
    Truncated(u64),
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Stats {
    pub records: u64,
    pub bytes: u64,
    pub write_bytes: u64,
    pub writes: u64,
    pub objects: u64,
    pub frees: u64,
}

enum Phase {
    /// Waiting for a full 312-byte header.
    Header,
    /// Skipping `remaining` payload bytes (feeding them to the checksum).
    Payload { remaining: u64 },
}

/// Incremental parser. Feed bytes with [`StreamParser::feed`]; collect events
/// with the returned vector. Cheap: it copies only the header bytes.
pub struct StreamParser {
    buf: BytesMut,
    phase: Phase,
    cksum: Fletcher4,
    swap: bool,
    started: bool,
    depth: u32,
    pub stats: Stats,
    pub begin: Option<BeginHeader>,
    pub ends: Vec<EndRecord>,
    /// Set when the outermost END has been seen; further bytes are an error
    /// for a well-formed stream but we don't enforce that here.
    pub finished: bool,
}

impl Default for StreamParser {
    fn default() -> Self {
        Self::new()
    }
}

impl StreamParser {
    pub fn new() -> Self {
        Self {
            buf: BytesMut::with_capacity(RECORD_LEN * 2),
            phase: Phase::Header,
            cksum: Fletcher4::ZERO,
            swap: false,
            started: false,
            depth: 0,
            stats: Stats::default(),
            begin: None,
            ends: Vec::new(),
            finished: false,
        }
    }

    /// Running checksum so far (what the next END record should carry).
    pub fn checksum(&self) -> Fletcher4 {
        self.cksum
    }

    /// Feed the next slice of the stream. Returns events found in it.
    pub fn feed(&mut self, mut data: &[u8]) -> Result<Vec<Event>, StreamError> {
        let mut events = Vec::new();
        while !data.is_empty() {
            match self.phase {
                Phase::Payload { remaining } => {
                    let take = (remaining.min(data.len() as u64)) as usize;
                    // Payloads are 4-byte aligned in total but the slice we get
                    // may not be; buffer the ragged tail in `buf`.
                    let (chunk, rest) = data.split_at(take);
                    self.checksum_ragged(chunk);
                    self.stats.bytes += take as u64;
                    data = rest;
                    let remaining = remaining - take as u64;
                    self.phase = if remaining == 0 {
                        self.flush_ragged();
                        Phase::Header
                    } else {
                        Phase::Payload { remaining }
                    };
                }
                Phase::Header => {
                    let need = RECORD_LEN - self.buf.len();
                    let take = need.min(data.len());
                    self.buf.extend_from_slice(&data[..take]);
                    data = &data[take..];
                    if self.buf.len() < RECORD_LEN {
                        break;
                    }
                    let hdr: [u8; RECORD_LEN] = self.buf[..RECORD_LEN].try_into().unwrap();
                    self.buf.advance(RECORD_LEN);
                    if let Some(ev) = self.record(&hdr)? {
                        events.push(ev);
                    }
                }
            }
        }
        Ok(events)
    }

    /// Call at EOF. Errors if the stream stopped mid-record.
    pub fn finish(&self) -> Result<(), StreamError> {
        match self.phase {
            Phase::Header if self.buf.is_empty() => Ok(()),
            _ => Err(StreamError::Truncated(self.stats.bytes)),
        }
    }

    // Payload bytes may arrive in slices that aren't multiples of 4; keep up
    // to 3 stray bytes in `buf` until we can form a word.
    fn checksum_ragged(&mut self, chunk: &[u8]) {
        if self.buf.is_empty() && chunk.len().is_multiple_of(4) {
            self.cksum.update(chunk, self.swap);
            return;
        }
        self.buf.extend_from_slice(chunk);
        let aligned = self.buf.len() - self.buf.len() % 4;
        if aligned > 0 {
            let words = self.buf.split_to(aligned);
            self.cksum.update(&words, self.swap);
        }
    }

    fn flush_ragged(&mut self) {
        // A well-formed payload is a multiple of 4; anything left is a bug in
        // the sender, but fletcher on a padded tail is what ZFS would do too.
        if !self.buf.is_empty() {
            let mut tail = self.buf.split().to_vec();
            while !tail.len().is_multiple_of(4) {
                tail.push(0);
            }
            self.cksum.update(&tail, self.swap);
        }
    }

    fn u32(&self, hdr: &[u8], off: usize) -> u32 {
        let a: [u8; 4] = hdr[off..off + 4].try_into().unwrap();
        if self.swap {
            u32::from_be_bytes(a)
        } else {
            u32::from_le_bytes(a)
        }
    }

    fn u64(&self, hdr: &[u8], off: usize) -> u64 {
        let a: [u8; 8] = hdr[off..off + 8].try_into().unwrap();
        if self.swap {
            u64::from_be_bytes(a)
        } else {
            u64::from_le_bytes(a)
        }
    }

    fn record(&mut self, hdr: &[u8; RECORD_LEN]) -> Result<Option<Event>, StreamError> {
        let offset = self.stats.bytes;
        let index = self.stats.records;

        // Endianness is decided by the very first BEGIN's magic.
        if !self.started {
            let magic_le = u64::from_le_bytes(hdr[UNION_OFF..UNION_OFF + 8].try_into().unwrap());
            let magic_be = u64::from_be_bytes(hdr[UNION_OFF..UNION_OFF + 8].try_into().unwrap());
            if magic_le == DMU_BACKUP_MAGIC {
                self.swap = false;
            } else if magic_be == DMU_BACKUP_MAGIC {
                self.swap = true;
            } else {
                return Err(StreamError::BadMagic(magic_le));
            }
            if self.u32(hdr, 0) != RecordType::Begin as u32 {
                return Err(StreamError::NoBegin);
            }
            self.started = true;
        }

        let rtype = self.u32(hdr, 0);
        let rtype = RecordType::from_u32(rtype).ok_or(StreamError::UnknownRecordType(rtype))?;
        let payloadlen = self.u32(hdr, 4) as u64;
        let u = &hdr[UNION_OFF..];

        // Checksum discipline (mirrors dump_record / zstream's read_hdr):
        // the running checksum covers everything before the trailing 32-byte
        // field; that field then carries the running value (except in BEGIN)
        // and is itself folded in afterwards.
        let before = self.cksum;
        self.cksum.update(&hdr[..CKSUM_OFF], self.swap);
        let embedded = Fletcher4::from_stream_bytes(&hdr[CKSUM_OFF..], self.swap);
        if rtype != RecordType::Begin && !embedded.is_zero() && embedded != self.cksum {
            return Err(StreamError::RecordChecksum { offset, index });
        }
        self.cksum.update(&hdr[CKSUM_OFF..], self.swap);

        self.stats.records += 1;
        self.stats.bytes += RECORD_LEN as u64;

        let mut event = None;
        let payload: u64 = match rtype {
            RecordType::Begin => {
                // drr_versioninfo: bits 0-1 header type, bits 2-31 feature
                // flags, bits 32-63 DMU_BACKUP_HEADER_VERSION (verified
                // against real OpenZFS 2.2 streams).
                let versioninfo = self.u64(u, 8);
                let header_type = match versioninfo & 0x3 {
                    2 => HeaderType::Compound,
                    _ => HeaderType::Substream,
                };
                let from = self.u64(u, 40);
                let name_raw = &u[48..48 + 256];
                let name_len = name_raw.iter().position(|&b| b == 0).unwrap_or(256);
                let bh = BeginHeader {
                    header_type,
                    features: (versioninfo >> 2) & 0x3fff_ffff,
                    creation_time: self.u64(u, 16),
                    objset_type: self.u32(u, 24),
                    flags: self.u32(u, 28),
                    to_guid: Guid(self.u64(u, 32)),
                    from_guid: (from != 0).then_some(Guid(from)),
                    to_name: String::from_utf8_lossy(&name_raw[..name_len]).into_owned(),
                    byteswapped: self.swap,
                };
                if self.begin.is_none() {
                    self.begin = Some(bh.clone());
                }
                self.depth += 1;
                event = Some(Event::Begin(bh));
                payloadlen
            }
            RecordType::End => {
                let end = EndRecord {
                    checksum: Fletcher4::from_stream_bytes(&u[0..32], self.swap),
                    to_guid: Guid(self.u64(u, 32)),
                };
                // END carries the checksum of everything before this record.
                // A compound stream's outer END has an all-zero checksum.
                if !end.checksum.is_zero() && end.checksum != before {
                    return Err(StreamError::EndChecksum {
                        offset,
                        expected: end.checksum,
                        actual: before,
                    });
                }
                self.ends.push(end);
                self.depth = self.depth.saturating_sub(1);
                if self.depth == 0 {
                    self.finished = true;
                }
                event = Some(Event::End(end));
                0
            }
            RecordType::Object => {
                self.stats.objects += 1;
                let bonuslen = self.u32(u, 20) as u64;
                let raw_bonuslen = self.u32(u, 28) as u64;
                if raw_bonuslen != 0 {
                    raw_bonuslen
                } else {
                    (bonuslen + 7) & !7
                }
            }
            RecordType::Write => {
                self.stats.writes += 1;
                let logical = self.u64(u, 24);
                let compressed = self.u64(u, 88);
                let n = if compressed != 0 { compressed } else { logical };
                self.stats.write_bytes += n;
                n
            }
            RecordType::Spill => {
                let length = self.u64(u, 8);
                let compressed = self.u64(u, 40);
                if compressed != 0 { compressed } else { length }
            }
            RecordType::WriteEmbedded => {
                let psize = self.u32(u, 44) as u64;
                (psize + 7) & !7
            }
            RecordType::Free | RecordType::FreeObjects => {
                self.stats.frees += 1;
                0
            }
            RecordType::WriteByRef | RecordType::ObjectRange | RecordType::Redact => 0,
        };

        if !payload.is_multiple_of(4) {
            return Err(StreamError::UnalignedPayload(payload));
        }
        self.phase = if payload > 0 {
            Phase::Payload { remaining: payload }
        } else {
            Phase::Header
        };
        Ok(event)
    }
}

pub mod synth {
    //! Builds synthetic but checksum-correct streams. Used by tests and by
    //! the dev tooling (`fake-zfs`) on machines without ZFS.
    use super::*;

    pub struct Builder {
        pub out: Vec<u8>,
        cksum: Fletcher4,
    }

    impl Default for Builder {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Builder {
        pub fn new() -> Self {
            Self {
                out: Vec::new(),
                cksum: Fletcher4::ZERO,
            }
        }

        /// A plausible stream: BEGIN, `bytes` of pseudo-random WRITE
        /// payload in `record`-sized records, a FREE, END.
        pub fn stream(
            to: u64,
            from: u64,
            features: u64,
            name: &str,
            bytes: usize,
            record: usize,
            seed: u64,
        ) -> Vec<u8> {
            let mut b = Builder::new();
            b.begin(to, from, features, name);
            let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut left = bytes;
            let mut buf = vec![0u8; record];
            while left > 0 {
                let n = left.min(record) & !3;
                if n == 0 {
                    break;
                }
                for w in buf[..n].as_chunks_mut::<8>().0 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    *w = x.to_le_bytes();
                }
                b.write(&buf[..n]);
                left -= n;
            }
            b.free();
            b.end(to);
            b.out
        }

        /// Same stream as [`Builder::stream`], written to `w` record by
        /// record so memory stays at one record regardless of `bytes`
        /// (multi-GiB load-test streams). Returns the bytes written.
        #[allow(clippy::too_many_arguments)] // mirrors `stream`, plus the sink
        pub fn stream_to<W: std::io::Write>(
            w: &mut W,
            to: u64,
            from: u64,
            features: u64,
            name: &str,
            bytes: usize,
            record: usize,
            seed: u64,
        ) -> std::io::Result<u64> {
            let mut b = Builder::new();
            let mut total = 0u64;
            let mut flush = |b: &mut Builder, w: &mut W| -> std::io::Result<()> {
                w.write_all(&b.out)?;
                total += b.out.len() as u64;
                b.out.clear();
                Ok(())
            };
            b.begin(to, from, features, name);
            flush(&mut b, w)?;
            let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
            let mut left = bytes;
            let mut buf = vec![0u8; record];
            while left > 0 {
                let n = left.min(record) & !3;
                if n == 0 {
                    break;
                }
                for wd in buf[..n].as_chunks_mut::<8>().0 {
                    x ^= x << 13;
                    x ^= x >> 7;
                    x ^= x << 17;
                    *wd = x.to_le_bytes();
                }
                b.write(&buf[..n]);
                flush(&mut b, w)?;
                left -= n;
            }
            b.free();
            b.end(to);
            flush(&mut b, w)?;
            w.flush()?;
            Ok(total)
        }

        fn record(&mut self, mut hdr: [u8; RECORD_LEN], payload: &[u8]) {
            let is_begin = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) == 0;
            self.cksum.update(&hdr[..CKSUM_OFF], false);
            if !is_begin {
                hdr[CKSUM_OFF..].copy_from_slice(&self.cksum.to_bytes());
            }
            self.cksum.update(&hdr[CKSUM_OFF..], false);
            self.out.extend_from_slice(&hdr);
            self.out.extend_from_slice(payload);
            self.cksum.update(payload, false);
        }

        pub fn begin(&mut self, to: u64, from: u64, features: u64, name: &str) {
            let mut h = [0u8; RECORD_LEN];
            h[0..4].copy_from_slice(&0u32.to_le_bytes());
            let u = &mut h[UNION_OFF..];
            u[0..8].copy_from_slice(&DMU_BACKUP_MAGIC.to_le_bytes());
            u[8..16].copy_from_slice(&((1u64 << 32) | (features << 2) | 1).to_le_bytes());
            u[16..24].copy_from_slice(&1_700_000_000u64.to_le_bytes());
            u[24..28].copy_from_slice(&2u32.to_le_bytes());
            u[32..40].copy_from_slice(&to.to_le_bytes());
            u[40..48].copy_from_slice(&from.to_le_bytes());
            u[48..48 + name.len()].copy_from_slice(name.as_bytes());
            self.record(h, &[]);
        }

        pub fn write(&mut self, data: &[u8]) {
            assert!(data.len().is_multiple_of(4));
            let mut h = [0u8; RECORD_LEN];
            h[0..4].copy_from_slice(&3u32.to_le_bytes());
            let u = &mut h[UNION_OFF..];
            u[24..32].copy_from_slice(&(data.len() as u64).to_le_bytes());
            self.record(h, data);
        }

        pub fn free(&mut self) {
            let mut h = [0u8; RECORD_LEN];
            h[0..4].copy_from_slice(&4u32.to_le_bytes());
            self.record(h, &[]);
        }

        pub fn end(&mut self, to: u64) {
            let mut h = [0u8; RECORD_LEN];
            h[0..4].copy_from_slice(&5u32.to_le_bytes());
            let u = &mut h[UNION_OFF..];
            u[0..32].copy_from_slice(&self.cksum.to_bytes());
            u[32..40].copy_from_slice(&to.to_le_bytes());
            self.record(h, &[]);
        }
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn synth_stream_to_matches_stream() {
        let f = super::feature::LARGE_BLOCKS | super::feature::COMPRESSED;
        let a = super::synth::Builder::stream(7, 0, f, "t/d@s", 3 << 20, 128 << 10, 42);
        let mut b = Vec::new();
        let n = super::synth::Builder::stream_to(&mut b, 7, 0, f, "t/d@s", 3 << 20, 128 << 10, 42)
            .expect("write to vec");
        assert_eq!(n as usize, a.len());
        assert_eq!(a, b);
    }

    use super::synth::Builder;
    use super::*;

    fn sample() -> Vec<u8> {
        let mut b = Builder::new();
        b.begin(
            0xAAAA,
            0x5555,
            feature::RAW | feature::LARGE_BLOCKS,
            "tank/data@s2",
        );
        b.write(&[1u8; 4096]);
        b.free();
        b.write(&[2u8; 512]);
        b.end(0xAAAA);
        b.out
    }

    #[test]
    fn parses_header_and_end() {
        let data = sample();
        let mut p = StreamParser::new();
        let evs = p.feed(&data).unwrap();
        p.finish().unwrap();
        assert_eq!(evs.len(), 2);
        let Event::Begin(h) = &evs[0] else { panic!() };
        assert_eq!(h.to_guid, Guid(0xAAAA));
        assert_eq!(h.from_guid, Some(Guid(0x5555)));
        assert!(h.is_raw() && h.has_large_blocks());
        assert_eq!(h.to_name, "tank/data@s2");
        assert_eq!(h.header_type, HeaderType::Substream);
        assert!(p.finished);
        assert_eq!(p.stats.writes, 2);
        assert_eq!(p.stats.write_bytes, 4608);
        assert_eq!(p.stats.records, 5);
        assert_eq!(p.stats.bytes, data.len() as u64);
    }

    #[test]
    fn byte_at_a_time_feed_matches() {
        let data = sample();
        let mut p = StreamParser::new();
        let mut evs = Vec::new();
        for b in &data {
            evs.extend(p.feed(std::slice::from_ref(b)).unwrap());
        }
        p.finish().unwrap();
        assert_eq!(evs.len(), 2);
        assert!(p.finished);
    }

    #[test]
    fn odd_slices_feed_matches() {
        let data = sample();
        let mut p = StreamParser::new();
        for chunk in data.chunks(7) {
            p.feed(chunk).unwrap();
        }
        p.finish().unwrap();
        assert!(p.finished);
    }

    #[test]
    fn detects_flipped_bit_in_payload() {
        let mut data = sample();
        data[RECORD_LEN + 100] ^= 0x01; // inside first write payload
        let mut p = StreamParser::new();
        let err = p.feed(&data).unwrap_err();
        assert!(matches!(err, StreamError::RecordChecksum { .. }), "{err}");
    }

    #[test]
    fn detects_flipped_bit_before_end() {
        let mut data = sample();
        let last_payload_start = data.len() - RECORD_LEN - 512;
        data[last_payload_start + 3] ^= 0x80;
        let mut p = StreamParser::new();
        let err = p.feed(&data).unwrap_err();
        // Caught by the END record's trailing checksum field (which every
        // non-BEGIN record carries), before the END-specific check runs.
        assert!(
            matches!(
                err,
                StreamError::RecordChecksum { .. } | StreamError::EndChecksum { .. }
            ),
            "{err}"
        );
    }

    #[test]
    fn detects_tampered_end_checksum() {
        let mut data = sample();
        let end_off = data.len() - RECORD_LEN;
        // The END's own drr_checksum field, but keep the trailing field intact
        // by recomputing nothing: the trailing field check happens first on
        // bytes before it, so flip a byte *inside* drr_end.drr_checksum and
        // also fix up nothing — the trailing check fails (RecordChecksum).
        data[end_off + UNION_OFF] ^= 1;
        let mut p = StreamParser::new();
        assert!(p.feed(&data).is_err());
    }

    #[test]
    fn rejects_non_stream() {
        let mut p = StreamParser::new();
        let err = p.feed(&[0u8; RECORD_LEN]).unwrap_err();
        assert!(matches!(err, StreamError::BadMagic(_)));
    }

    #[test]
    fn truncated() {
        let data = sample();
        let mut p = StreamParser::new();
        p.feed(&data[..data.len() - 10]).unwrap();
        assert!(matches!(p.finish(), Err(StreamError::Truncated(_))));
    }

    #[test]
    fn real_versioninfo_words() {
        // drr_versioninfo values captured from OpenZFS 2.2.2 `zfs send`.
        let f = |vi: u64| (vi >> 2) & 0x3fff_ffff;
        assert_eq!(f(0x11), feature::SA_SPILL);
        assert_eq!(
            f(0x0508_0011),
            feature::RAW | feature::COMPRESSED | feature::LZ4 | feature::SA_SPILL
        );
        assert_eq!(f(0x0020_0011), feature::LARGE_BLOCKS | feature::SA_SPILL);
        assert_eq!(
            f(0x0928_0011),
            feature::ZSTD
                | feature::COMPRESSED
                | feature::LARGE_BLOCKS
                | feature::LZ4
                | feature::SA_SPILL
        );
        assert_eq!(f(0x1000_0002), feature::HOLDS);
        assert_eq!(0x1000_0002u64 & 3, 2, "-h streams are compound");
        assert_eq!(
            f(0x000c_0011),
            feature::EMBED_DATA | feature::LZ4 | feature::SA_SPILL
        );
    }

    #[test]
    fn feature_names() {
        assert_eq!(
            feature::names(feature::RAW | feature::COMPRESSED),
            vec!["compressed", "raw"]
        );
    }
}
