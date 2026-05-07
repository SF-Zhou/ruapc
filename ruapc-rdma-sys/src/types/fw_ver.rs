//! Firmware version type with serialization support
//!
//! FwVer wraps a 64-byte null-terminated string representing firmware version.

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;

/// Firmware version information
#[repr(transparent)]
#[derive(Clone, Copy)]
pub struct FwVer(pub [u8; 64usize]);

impl std::fmt::Display for FwVer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let len = self.0.iter().position(|&c| c == 0).unwrap_or(64);
        let s = std::str::from_utf8(&self.0[..len]).unwrap_or("<invalid>");
        f.write_str(s)
    }
}

impl std::fmt::Debug for FwVer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Serialize for FwVer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(self.to_string().as_str())
    }
}

impl<'de> Deserialize<'de> for FwVer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        let mut fw_ver = [0u8; 64];
        let bytes = s.as_bytes();
        let len = bytes.len().min(63);
        fw_ver[..len].copy_from_slice(&bytes[..len]);
        Ok(FwVer(fw_ver))
    }
}

impl JsonSchema for FwVer {
    fn schema_name() -> Cow<'static, str> {
        "FwVer".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fw_ver_display() {
        let mut fw_ver = [0u8; 64];
        fw_ver[0..10].copy_from_slice(b"20.28.1042");
        let fw = FwVer(fw_ver);
        assert_eq!(format!("{}", fw), "20.28.1042");
    }

    #[test]
    fn test_fw_ver_empty() {
        let fw = FwVer([0u8; 64]);
        assert_eq!(format!("{}", fw), "");
    }

    #[test]
    fn test_fw_ver_full_length() {
        let mut fw_ver = [0u8; 64];
        for (i, b) in fw_ver[..63].iter_mut().enumerate() {
            *b = b'a' + (i as u8 % 26);
        }
        let fw = FwVer(fw_ver);
        let s = format!("{}", fw);
        assert_eq!(s.len(), 63);
        assert!(s.starts_with("a"));
    }

    #[test]
    fn test_fw_ver_serialize() {
        let mut fw_ver = [0u8; 64];
        fw_ver[0..10].copy_from_slice(b"24.06.1234");
        let fw = FwVer(fw_ver);
        let json = serde_json::to_string(&fw).unwrap();
        assert_eq!(json, "\"24.06.1234\"");
    }

    #[test]
    fn test_fw_ver_deserialize() {
        let json = "\"20.28.1042\"";
        let fw: FwVer = serde_json::from_str(json).unwrap();
        assert_eq!(format!("{}", fw), "20.28.1042");
    }

    #[test]
    fn test_fw_ver_serialize_deserialize_roundtrip() {
        let original_str = "30.12.5678";
        let json = format!("\"{}\"", original_str);
        let fw: FwVer = serde_json::from_str(&json).unwrap();
        assert_eq!(format!("{}", fw), original_str);
    }

    #[test]
    fn test_fw_ver_deserialize_truncates() {
        let json = "\"abcdefghijklmnopqrstuvwxyz1234567890ABCDEFGHIJKLMNOPQRSTUVWXYZ12345678901234567890123\"";
        let fw: FwVer = serde_json::from_str(json).unwrap();
        let s = format!("{}", fw);
        assert_eq!(s.len(), 63);
    }
}
