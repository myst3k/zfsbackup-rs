//! Incremental fletcher4 exactly as OpenZFS computes it over send streams
//! (`fletcher_4_incremental_native` / `_byteswap`).
//!
//! The stream is treated as a sequence of little-endian 32-bit words (or
//! big-endian for a byte-swapped stream); every buffer fed in must be a
//! multiple of 4 bytes, which ZFS guarantees for record headers and payloads.

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Fletcher4 {
    pub a: u64,
    pub b: u64,
    pub c: u64,
    pub d: u64,
}

impl Fletcher4 {
    pub const ZERO: Fletcher4 = Fletcher4 {
        a: 0,
        b: 0,
        c: 0,
        d: 0,
    };

    pub fn is_zero(&self) -> bool {
        *self == Self::ZERO
    }

    /// Feed bytes in stream order. `swap` selects big-endian word order, used
    /// when the stream was produced on a machine of the opposite endianness.
    pub fn update(&mut self, data: &[u8], swap: bool) {
        debug_assert!(
            data.len().is_multiple_of(4),
            "fletcher4 input must be 4-byte aligned"
        );
        let (mut a, mut b, mut c, mut d) = (self.a, self.b, self.c, self.d);
        for w in data.as_chunks::<4>().0 {
            let word = if swap {
                u32::from_be_bytes(*w)
            } else {
                u32::from_le_bytes(*w)
            } as u64;
            a = a.wrapping_add(word);
            b = b.wrapping_add(a);
            c = c.wrapping_add(b);
            d = d.wrapping_add(c);
        }
        self.a = a;
        self.b = b;
        self.c = c;
        self.d = d;
    }

    /// Parse the 32-byte `zio_cksum_t` as laid out in a stream record.
    pub fn from_stream_bytes(b: &[u8], swap: bool) -> Self {
        let rd = |i: usize| {
            let arr: [u8; 8] = b[i * 8..i * 8 + 8].try_into().unwrap();
            if swap {
                u64::from_be_bytes(arr)
            } else {
                u64::from_le_bytes(arr)
            }
        };
        Fletcher4 {
            a: rd(0),
            b: rd(1),
            c: rd(2),
            d: rd(3),
        }
    }

    /// Read the 32-byte on-stream form back; inverse of `to_bytes`,
    /// used by the roundtrip test.
    #[cfg(test)]
    pub fn from_bytes(b: &[u8; 32]) -> Self {
        let w = |i: usize| u64::from_le_bytes(b[i * 8..i * 8 + 8].try_into().expect("8 bytes"));
        Self {
            a: w(0),
            b: w(1),
            c: w(2),
            d: w(3),
        }
    }

    pub fn to_bytes(self) -> [u8; 32] {
        let mut out = [0u8; 32];
        out[0..8].copy_from_slice(&self.a.to_le_bytes());
        out[8..16].copy_from_slice(&self.b.to_le_bytes());
        out[16..24].copy_from_slice(&self.c.to_le_bytes());
        out[24..32].copy_from_slice(&self.d.to_le_bytes());
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn known_vector() {
        // Words 1,2,3: a=6, b=1+3+6=10, c=1+4+10=15, d=1+5+15=21
        let mut f = Fletcher4::default();
        let mut data = Vec::new();
        for w in [1u32, 2, 3] {
            data.extend_from_slice(&w.to_le_bytes());
        }
        f.update(&data, false);
        assert_eq!(
            f,
            Fletcher4 {
                a: 6,
                b: 10,
                c: 15,
                d: 21
            }
        );
    }

    #[test]
    fn incremental_equals_whole() {
        let data: Vec<u8> = (0..4096u32).flat_map(|i| i.to_le_bytes()).collect();
        let mut whole = Fletcher4::default();
        whole.update(&data, false);
        let mut inc = Fletcher4::default();
        for chunk in data.chunks(100 * 4) {
            inc.update(chunk, false);
        }
        assert_eq!(whole, inc);
    }

    #[test]
    fn bytes_roundtrip() {
        let f = Fletcher4 {
            a: 1,
            b: 2,
            c: 3,
            d: 4,
        };
        assert_eq!(Fletcher4::from_bytes(&f.to_bytes()), f);
    }
}
