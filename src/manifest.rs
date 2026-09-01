//! What lives in the bucket.
//!
//! The bucket is the whole database. Every archived snapshot is a set of
//! chunk objects plus one manifest object; `list`, `receive`, `verify` and
//! `retention` work from manifests alone.
//!
//! Layout under the bucket (or `s3://bucket/prefix`):
//!
//! ```text
//! zb/v1/<dataset_guid>/<snapshot_guid>/manifest.json
//! zb/v1/<dataset_guid>/<snapshot_guid>/chunk-000000  … chunk-NNNNNN
//! zb/v1/pins/<snapshot_guid>
//! ```
//!
//! Keys are GUID-based so dataset renames never orphan anything; names are
//! recorded inside the manifest for humans.

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

use crate::types::{Guid, SendFlags};

pub const FORMAT_VERSION: u32 = 1;

/// One archived `zfs send` stream.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Manifest {
    pub format_version: u32,
    /// `pool/dataset` at backup time.
    pub dataset: String,
    pub dataset_guid: Guid,
    /// `pool/dataset@snap` at backup time.
    pub snapshot: String,
    pub snapshot_guid: Guid,
    /// Incremental base snapshot GUID; `None` for a full stream.
    pub from_guid: Option<Guid>,
    /// `createtxg` of the snapshot: orders snapshots within a dataset.
    pub createtxg: u64,
    #[serde(with = "time::serde::rfc3339")]
    pub created_at: OffsetDateTime,
    pub send_flags: SendFlags,
    /// Total stream bytes.
    pub bytes: u64,
    /// BLAKE3 of the whole stream, hex.
    pub stream_blake3: String,
    /// The ZFS END record's fletcher4 checksum, hex (16 bytes LE ×4 words):
    /// ties the archive to what `zfs send` itself computed.
    pub end_checksum: Option<String>,
    pub chunks: Vec<Chunk>,
}

/// Written before the first chunk of a send and deleted once the manifest
/// commits. It records everything that determines the stream's bytes, so an
/// interrupted run can only resume when the next run would produce the same
/// stream; anything else starts clean.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Pending {
    pub from_guid: Option<Guid>,
    pub send_flags: SendFlags,
    pub chunk_size: u64,
    /// Identifies the run that owns this attempt.
    pub run_id: uuid::Uuid,
    /// Refreshed while the run is uploading. A marker whose lease has not
    /// expired means another send of this snapshot is still alive, and this
    /// one must not touch its chunks.
    #[serde(with = "time::serde::rfc3339")]
    pub refreshed_at: OffsetDateTime,
}

/// How long a marker stays authoritative after its last refresh. Long enough
/// to cover a stalled network call, short enough that a crashed run does not
/// block the next attempt for long.
pub const LEASE: time::Duration = time::Duration::minutes(5);

impl Pending {
    /// Same stream? (Ownership and freshness are separate questions.)
    pub fn same_stream(&self, other: &Pending) -> bool {
        self.from_guid == other.from_guid
            && self.send_flags == other.send_flags
            && self.chunk_size == other.chunk_size
    }

    pub fn lease_live(&self, now: OffsetDateTime) -> bool {
        now - self.refreshed_at < LEASE
    }

    pub fn encode(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec(self)
    }

    pub fn decode(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }
}

/// One chunk object. Chunks are `chunk_size`-sized slices of the raw stream,
/// in order; the last one is short.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Chunk {
    pub seq: u32,
    pub bytes: u64,
    /// BLAKE3 of this chunk, hex — what `verify` recomputes.
    pub blake3: String,
}

impl Manifest {
    pub fn is_full(&self) -> bool {
        self.from_guid.is_none()
    }

    /// Refuse a manifest written by a format this binary does not implement,
    /// rather than interpreting its fields under v1 rules.
    pub fn check_version(&self) -> Result<(), String> {
        if self.format_version == FORMAT_VERSION {
            Ok(())
        } else {
            Err(format!(
                "manifest format version {} (this build understands {FORMAT_VERSION}); upgrade zfsbackup-rs",
                self.format_version
            ))
        }
    }

    pub fn encode(&self) -> serde_json::Result<Vec<u8>> {
        serde_json::to_vec_pretty(self)
    }

    pub fn decode(bytes: &[u8]) -> serde_json::Result<Self> {
        serde_json::from_slice(bytes)
    }
}

/// Bucket keys. `prefix` is the optional path portion of `s3://bucket/prefix`.
pub mod keys {
    use crate::types::Guid;

    fn root(prefix: &str) -> String {
        if prefix.is_empty() {
            "zb/v1".into()
        } else {
            format!("{}/zb/v1", prefix.trim_matches('/'))
        }
    }

    pub fn snapshot_dir(prefix: &str, dataset: Guid, snapshot: Guid) -> String {
        format!("{}/{dataset}/{snapshot}", root(prefix))
    }

    pub fn manifest(prefix: &str, dataset: Guid, snapshot: Guid) -> String {
        format!("{}/manifest.json", snapshot_dir(prefix, dataset, snapshot))
    }

    pub fn chunk(prefix: &str, dataset: Guid, snapshot: Guid, seq: u32) -> String {
        format!("{}/chunk-{seq:06}", snapshot_dir(prefix, dataset, snapshot))
    }

    /// In-progress marker for a send; see [`super::Pending`].
    pub fn pending(prefix: &str, dataset: Guid, snapshot: Guid) -> String {
        format!("{}/pending.json", snapshot_dir(prefix, dataset, snapshot))
    }

    pub fn all_manifests_prefix(prefix: &str) -> String {
        format!("{}/", root(prefix))
    }

    pub fn pin(prefix: &str, snapshot: Guid) -> String {
        format!("{}/pins/{snapshot}", root(prefix))
    }

    pub fn pins_prefix(prefix: &str) -> String {
        format!("{}/pins/", root(prefix))
    }

    /// `true` for keys that are manifests.
    pub fn is_manifest(key: &str) -> bool {
        key.ends_with("/manifest.json")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_shape() {
        let d = Guid(0xabc);
        let s = Guid(0xdef);
        assert_eq!(
            keys::manifest("", d, s),
            "zb/v1/0000000000000abc/0000000000000def/manifest.json"
        );
        assert_eq!(
            keys::chunk("backups", d, s, 7),
            "backups/zb/v1/0000000000000abc/0000000000000def/chunk-000007"
        );
        assert!(keys::is_manifest(&keys::manifest("", d, s)));
        assert!(!keys::is_manifest(&keys::chunk("", d, s, 0)));
    }

    #[test]
    fn manifest_roundtrip() {
        let m = Manifest {
            format_version: FORMAT_VERSION,
            dataset: "tank/data".into(),
            dataset_guid: Guid(1),
            snapshot: "tank/data@s1".into(),
            snapshot_guid: Guid(2),
            from_guid: None,
            createtxg: 42,
            created_at: OffsetDateTime::UNIX_EPOCH,
            send_flags: Default::default(),
            bytes: 10,
            stream_blake3: "aa".into(),
            end_checksum: None,
            chunks: vec![Chunk {
                seq: 0,
                bytes: 10,
                blake3: "bb".into(),
            }],
        };
        let m2 = Manifest::decode(&m.encode().unwrap()).unwrap();
        assert_eq!(m2.snapshot_guid, Guid(2));
        assert!(m2.is_full());
    }
}
