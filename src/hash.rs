//! Hashes attached to every part.
//!
//! BLAKE3 is our integrity hash (fast, keyed by content, what the catalog and
//! manifests store). MD5 exists only because S3 returns it as the `ETag` of a
//! multipart part, which gives a free transport check gateway → object store.

use std::fmt;

use md5::Digest as _;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

macro_rules! hex_newtype {
    ($name:ident, $len:expr) => {
        #[derive(Clone, Copy, PartialEq, Eq, Hash)]
        pub struct $name(pub [u8; $len]);

        impl $name {
            pub fn as_bytes(&self) -> &[u8; $len] {
                &self.0
            }

            pub fn to_hex(&self) -> String {
                hex::encode(self.0)
            }

            pub fn from_hex(s: &str) -> Result<Self, hex::FromHexError> {
                let v = hex::decode(s)?;
                let arr: [u8; $len] = v
                    .try_into()
                    .map_err(|_| hex::FromHexError::InvalidStringLength)?;
                Ok(Self(arr))
            }

            pub fn from_slice(b: &[u8]) -> Option<Self> {
                b.try_into().ok().map(Self)
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.write_str(&self.to_hex())
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}({})", stringify!($name), self.to_hex())
            }
        }

        impl Serialize for $name {
            fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
                if s.is_human_readable() {
                    s.serialize_str(&self.to_hex())
                } else {
                    s.serialize_bytes(&self.0)
                }
            }
        }

        impl<'de> Deserialize<'de> for $name {
            fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
                if d.is_human_readable() {
                    let s = String::deserialize(d)?;
                    Self::from_hex(&s).map_err(serde::de::Error::custom)
                } else {
                    let v = serde_bytes_vec::deserialize(d)?;
                    Self::from_slice(&v)
                        .ok_or_else(|| serde::de::Error::custom("wrong digest length"))
                }
            }
        }
    };
}

hex_newtype!(Blake3, 32);
hex_newtype!(Md5, 16);

/// Minimal bytes deserializer so we don't pull in `serde_bytes` for one use.
mod serde_bytes_vec {
    use serde::de::{Deserializer, Error, SeqAccess, Visitor};
    use std::fmt;

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<Vec<u8>, D::Error> {
        struct V;
        impl<'de> Visitor<'de> for V {
            type Value = Vec<u8>;
            fn expecting(&self, f: &mut fmt::Formatter) -> fmt::Result {
                f.write_str("bytes")
            }
            fn visit_bytes<E: Error>(self, v: &[u8]) -> Result<Vec<u8>, E> {
                Ok(v.to_vec())
            }
            fn visit_byte_buf<E: Error>(self, v: Vec<u8>) -> Result<Vec<u8>, E> {
                Ok(v)
            }
            fn visit_seq<A: SeqAccess<'de>>(self, mut seq: A) -> Result<Vec<u8>, A::Error> {
                let mut out = Vec::with_capacity(seq.size_hint().unwrap_or(0));
                while let Some(b) = seq.next_element::<u8>()? {
                    out.push(b);
                }
                Ok(out)
            }
        }
        d.deserialize_bytes(V)
    }
}

/// Inputs at least this large are hashed on the rayon pool.
const RAYON_THRESHOLD: usize = 256 * 1024;

/// Computes BLAKE3 (always) and optionally MD5 over the same bytes in one
/// pass. MD5 is only needed where the S3 ETag is checked (the gateway); the
/// agent skips it, since MD5 is the slowest thing in its pipeline.
pub struct PartHasher {
    blake3: blake3::Hasher,
    md5: Option<md5::Md5>,
    len: u64,
}

impl Default for PartHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl PartHasher {
    pub fn new() -> Self {
        Self {
            blake3: blake3::Hasher::new(),
            md5: Some(md5::Md5::new()),
            len: 0,
        }
    }

    pub fn blake3_only() -> Self {
        Self {
            blake3: blake3::Hasher::new(),
            md5: None,
            len: 0,
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        // BLAKE3 is a tree hash: large inputs split across cores. Below the
        // threshold the fork/join overhead outweighs the gain.
        if data.len() >= RAYON_THRESHOLD {
            self.blake3.update_rayon(data);
        } else {
            self.blake3.update(data);
        }
        if let Some(m) = &mut self.md5 {
            m.update(data);
        }
        self.len += data.len() as u64;
    }

    pub fn len(&self) -> u64 {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// (BLAKE3, MD5) — MD5 is all-zero when built with `blake3_only`.
    pub fn finalize(self) -> (Blake3, Md5) {
        let b = Blake3(*self.blake3.finalize().as_bytes());
        let m = match self.md5 {
            Some(h) => Md5(h.finalize().into()),
            None => Md5([0; 16]),
        };
        (b, m)
    }
}

/// Whole-stream BLAKE3, fed part by part in sequence order.
#[derive(Default)]
pub struct StreamHasher(blake3::Hasher);

impl StreamHasher {
    pub fn new() -> Self {
        Self::default()
    }
    pub fn update(&mut self, data: &[u8]) {
        if data.len() >= RAYON_THRESHOLD {
            self.0.update_rayon(data);
        } else {
            self.0.update(data);
        }
    }
    pub fn finalize(self) -> Blake3 {
        Blake3(*self.0.finalize().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tee_matches_reference() {
        let data = b"hello snapshift";
        let mut h = PartHasher::new();
        h.update(&data[..5]);
        h.update(&data[5..]);
        let (b, m) = h.finalize();
        assert_eq!(b.0, *blake3::hash(data).as_bytes());
        assert_eq!(m.0, <[u8; 16]>::from(md5::Md5::digest(data)));
    }

    #[test]
    fn hex_roundtrip() {
        let b = Blake3([7u8; 32]);
        assert_eq!(Blake3::from_hex(&b.to_hex()).unwrap(), b);
        let m = Md5([9u8; 16]);
        assert_eq!(Md5::from_hex(&m.to_hex()).unwrap(), m);
    }

    #[test]
    fn cbor_roundtrip_is_binary() {
        let b = Blake3([1u8; 32]);
        let mut buf = Vec::new();
        ciborium::into_writer(&b, &mut buf).unwrap();
        // 32 bytes + 2-byte CBOR header, not 64 hex chars.
        assert_eq!(buf.len(), 34);
        let back: Blake3 = ciborium::from_reader(&buf[..]).unwrap();
        assert_eq!(back, b);
    }
}
