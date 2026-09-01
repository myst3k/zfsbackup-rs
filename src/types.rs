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

#[derive(Debug, thiserror::Error)]
#[error("{0:?} is not a GUID in 16 hex digits")]
pub struct ParseGuidError(String);

impl Guid {
    /// Parse the decimal form ZFS prints (`zfs list -Hp -o guid`).
    ///
    /// Kept separate from [`FromStr`] on purpose: a decimal GUID of exactly
    /// 16 digits is also a valid hex string, so one parser that guessed
    /// between the two would silently decode ~1 in 2000 GUIDs as a different
    /// number — and could map two distinct snapshots onto one identity.
    pub fn from_zfs(s: &str) -> Result<Self, std::num::ParseIntError> {
        s.trim().parse::<u64>().map(Guid)
    }
}

/// The canonical 16-hex-digit form written by [`fmt::Display`], used in
/// bucket keys and JSON.
impl FromStr for Guid {
    type Err = ParseGuidError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if s.len() == 16 && s.bytes().all(|b| b.is_ascii_hexdigit()) {
            u64::from_str_radix(s, 16)
                .map(Guid)
                .map_err(|_| ParseGuidError(s.to_string()))
        } else {
            Err(ParseGuidError(s.to_string()))
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
    let v: u64 = num.trim().parse().map_err(|e| format!("{s:?}: {e}"))?;
    v.checked_mul(mult)
        .ok_or_else(|| format!("{s:?}: size is larger than u64"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn guid_hex_roundtrip() {
        let g = Guid(0xdead_beef_0000_0001);
        assert_eq!(g.to_string(), "deadbeef00000001");
        assert_eq!("deadbeef00000001".parse::<Guid>().unwrap(), g);
    }

    /// A 16-digit decimal GUID is also a valid hex string. The two forms have
    /// separate parsers so this one can never be read as the other: ZFS's
    /// decimal 1234567890123456 and the hex GUID 1234567890123456 are
    /// different snapshots and must stay different identities.
    #[test]
    fn decimal_and_hex_do_not_alias() {
        let ambiguous = "1234567890123456";
        assert_eq!(Guid::from_zfs(ambiguous).unwrap(), Guid(1234567890123456));
        assert_eq!(ambiguous.parse::<Guid>().unwrap(), Guid(0x1234567890123456));
        assert_ne!(
            Guid::from_zfs(ambiguous).unwrap(),
            ambiguous.parse::<Guid>().unwrap()
        );
    }

    #[test]
    fn zfs_guids_are_decimal() {
        // Real values from `zfs list -Hp -o guid`.
        assert_eq!(
            Guid::from_zfs("1848502438638364855").unwrap(),
            Guid(1848502438638364855)
        );
        // The canonical parser rejects them: they are not the 16-hex form.
        assert!("1848502438638364855".parse::<Guid>().is_err());
    }

    #[test]
    fn sizes() {
        assert_eq!(parse_size("64MiB").unwrap(), 64 << 20);
        assert_eq!(parse_size("5M").unwrap(), 5 << 20);
        assert_eq!(parse_size("123").unwrap(), 123);
        assert!(parse_size("x").is_err());
        // Overflow reports instead of wrapping (release builds have no
        // overflow checks, so this silently became 1 GiB before).
        assert!(parse_size("17179869185G").is_err());
    }
}

/// Is this environment variable set to something a person means as "yes"?
///
/// `ZB_ALLOW_HTTP=1` and `ZB_ALLOW_HTTP=true` are the same intent; rejecting
/// one of them is a papercut, and silently ignoring it would be worse.
pub fn env_enabled(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase()
            .as_str(),
        "1" | "true" | "yes" | "on"
    )
}

#[cfg(test)]
mod env_tests {
    use super::env_enabled;

    #[test]
    fn truthy_spellings() {
        for (v, want) in [
            ("1", true),
            ("true", true),
            ("TRUE", true),
            (" yes ", true),
            ("on", true),
            ("0", false),
            ("false", false),
            ("", false),
            ("maybe", false),
        ] {
            unsafe { std::env::set_var("ZB_TEST_TOGGLE", v) };
            assert_eq!(env_enabled("ZB_TEST_TOGGLE"), want, "{v:?}");
        }
        unsafe { std::env::remove_var("ZB_TEST_TOGGLE") };
        assert!(!env_enabled("ZB_TEST_TOGGLE"));
    }
}
