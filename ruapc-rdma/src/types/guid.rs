//! RDMA Globally Unique Identifier (GUID) type with serialization support
//!
//! The GUID is a 64-bit identifier that uniquely identifies an RDMA device.

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::borrow::Cow;

/// Globally Unique Identifier for RDMA devices
#[repr(transparent)]
#[derive(Clone, Copy, Default, PartialEq, Eq, Hash)]
pub struct Guid(u64);

impl Guid {
    /// Creates a GUID from a u64 value in big-endian (network) byte order.
    /// The provided value is assumed to already be in the internal representation
    /// format and is stored as-is without additional byte-order conversion.
    pub fn from_be(guid: u64) -> Self {
        Self(guid)
    }

    /// Converts GUID from network byte order to host byte order
    fn as_u64(&self) -> u64 {
        u64::from_be(self.0)
    }
}

impl std::fmt::Display for Guid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let guid = self.as_u64();
        write!(
            f,
            "{:04x}:{:04x}:{:04x}:{:04x}",
            (guid >> 48) & 0xFFFF,
            (guid >> 32) & 0xFFFF,
            (guid >> 16) & 0xFFFF,
            guid & 0xFFFF
        )
    }
}

impl std::fmt::Debug for Guid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        std::fmt::Display::fmt(self, f)
    }
}

impl Serialize for Guid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Guid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        let parts: Vec<_> = s.split(':').collect();
        if parts.len() != 4 {
            return Err(D::Error::custom("invalid GUID format"));
        }
        let mut guid: u64 = 0;
        for (i, part) in parts.iter().enumerate() {
            let value = u16::from_str_radix(part, 16)
                .map_err(|_| D::Error::custom("invalid hexadecimal value"))?;
            guid |= (value as u64) << (48 - i * 16);
        }
        Ok(Guid(guid.to_be()))
    }
}

impl JsonSchema for Guid {
    fn schema_name() -> Cow<'static, str> {
        "Guid".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "pattern": "^[0-9a-fA-F]{4}:[0-9a-fA-F]{4}:[0-9a-fA-F]{4}:[0-9a-fA-F]{4}$"
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_guid_display() {
        let guid = Guid::from_be(u64::to_be(0x506b0b03_0039e8a4));
        assert_eq!(format!("{}", guid), "506b:0b03:0039:e8a4");
    }

    #[test]
    fn test_guid_debug() {
        let guid = Guid(u64::to_be(0x12345678_9ABCDEF0));
        let debug_str = format!("{:?}", guid);
        assert_eq!(debug_str, "1234:5678:9abc:def0");
    }

    #[test]
    fn test_guid_serialize() {
        let guid = Guid(u64::to_be(0xAABBCCDD_EEFF1122));
        let json = serde_json::to_string(&guid).unwrap();
        assert_eq!(json, "\"aabb:ccdd:eeff:1122\"");
    }

    #[test]
    fn test_guid_deserialize() {
        let json = "\"506b:0b03:0039:e8a4\"";
        let guid: Guid = serde_json::from_str(json).unwrap();
        assert_eq!(guid.0, u64::to_be(0x506b0b03_0039e8a4));
    }

    #[test]
    fn test_guid_serialize_deserialize_roundtrip() {
        let original = Guid(u64::to_be(0x11112222_33334444));
        let json = serde_json::to_string(&original).unwrap();
        let deserialized: Guid = serde_json::from_str(&json).unwrap();
        assert_eq!(original.0, deserialized.0);
    }

    #[test]
    fn test_guid_deserialize_invalid_format() {
        let json = "\"invalid-guid\"";
        let result: Result<Guid, _> = serde_json::from_str(json);
        assert!(result.is_err());

        let json = "\"506b:0b03:0039\"";
        let result: Result<Guid, _> = serde_json::from_str(json);
        assert!(result.is_err());

        let json = "\"506b:0b03:0039:e8a4:1234\"";
        let result: Result<Guid, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_guid_deserialize_invalid_hex() {
        let json = "\"506b:0g03:0039:e8a4\"";
        let result: Result<Guid, _> = serde_json::from_str(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_guid_case_insensitive() {
        let json = "\"ABCD:EF01:2345:6789\"";
        let guid: Guid = serde_json::from_str(json).unwrap();
        assert_eq!(guid.0, u64::to_be(0xABCD_EF01_2345_6789));

        let json = "\"abcd:ef01:2345:6789\"";
        let guid: Guid = serde_json::from_str(json).unwrap();
        assert_eq!(guid.0, u64::to_be(0xABCD_EF01_2345_6789));
    }
}
