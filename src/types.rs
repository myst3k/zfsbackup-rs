//! Small shared types.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A ZFS object GUID (dataset or snapshot). Stable across renames, which is
/// why every relationship in the manifests is keyed on it and never on a name.
///
/// Serialized as 16 lowercase hex digits so it round-trips through JSON
/// without u64 precision problems.
#[derive(Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Guid(pub u64);

impl fmt::Display for Guid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:016x}", self.0)
    }
}

impl fmt::Debug for Guid {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Guid({:016x})", self.0)
    }
}

impl FromStr for Guid {
    type Err = std::num::ParseIntError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        // Accept both hex (our canonical form) and the decimal `zfs list` prints.
        if s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
            u64::from_str_radix(s, 16).map(Guid)
        } else {
            s.parse::<u64>().map(Guid)
        }
    }
}

impl Serialize for Guid {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Guid {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// Send flags that must stay constant across an incremental chain.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct SendFlags {
    /// `--raw` / `-w`
    pub raw: bool,
    /// `-c`
    pub compressed: bool,
    /// `-L`
    pub large_blocks: bool,
    /// `-e`
    pub embedded: bool,
    /// `-h`
    pub holds: bool,
    /// `-p`
    pub props: bool,
}

impl SendFlags {
    pub fn zfs_args(&self) -> Vec<&'static str> {
        let mut a = Vec::new();
        if self.raw {
            a.push("--raw");
        }
        if self.compressed {
            a.push("-c");
        }
        if self.large_blocks {
            a.push("-L");
        }
        if self.embedded {
            a.push("-e");
        }
        if self.holds {
            a.push("-h");
        }
        if self.props {
            a.push("-p");
        }
        a
    }
}

/// Parse a human size: plain bytes, or a KiB/MiB/GiB/K/M/G suffix.
pub fn parse_size(s: &str) -> Result<u64, String> {
    let t = s.trim();
    let (num, mult) = if let Some(n) = t.strip_suffix("GiB").or_else(|| t.strip_suffix('G')) {
        (n, 1u64 << 30)
    } else if let Some(n) = t.strip_suffix("MiB").or_else(|| t.strip_suffix('M')) {
        (n, 1u64 << 20)
    } else if let Some(n) = t.strip_suffix("KiB").or_else(|| t.strip_suffix('K')) {
        (n, 1u64 << 10)
    } else {
        (t, 1)
    };
    num.trim()
        .parse::<u64>()
        .map(|v| v * mult)
        .map_err(|e| format!("{s:?}: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guid_hex_roundtrip() {
        let g = Guid(0xdead_beef_0000_0001);
        assert_eq!(g.to_string(), "deadbeef00000001");
        assert_eq!("deadbeef00000001".parse::<Guid>().unwrap(), g);
        assert_eq!("12345".parse::<Guid>().unwrap(), Guid(12345));
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("64MiB").unwrap(), 64 << 20);
        assert_eq!(parse_size("5M").unwrap(), 5 << 20);
        assert_eq!(parse_size("123").unwrap(), 123);
        assert!(parse_size("x").is_err());
    }
}
