//! RDMA GID (Global Identifier) type with serialization support
//!
//! The GID is a 128-bit identifier used for addressing in RDMA networks.
//! It can be represented as an IPv6 address.

use schemars::{JsonSchema, Schema, SchemaGenerator, json_schema};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::{borrow::Cow, net::Ipv6Addr};

use crate::ibv_gid;

impl PartialEq for ibv_gid {
    fn eq(&self, other: &Self) -> bool {
        self.as_bits() == other.as_bits()
    }
}

impl Eq for ibv_gid {}

impl ibv_gid {
    /// Returns the raw GID bytes
    pub fn as_raw(&self) -> &[u8; 16] {
        unsafe { &self.raw }
    }

    /// Returns the GID as a 128-bit integer
    pub fn as_bits(&self) -> u128 {
        u128::from_be_bytes(unsafe { self.raw })
    }

    /// Returns the GID as an IPv6 address
    pub fn as_ipv6(&self) -> Ipv6Addr {
        Ipv6Addr::from_bits(self.as_bits())
    }

    /// Returns the subnet prefix portion of the GID
    pub fn subnet_prefix(&self) -> u64 {
        u64::from_be(unsafe { self.global.subnet_prefix })
    }

    /// Returns the interface ID portion of the GID
    pub fn interface_id(&self) -> u64 {
        u64::from_be(unsafe { self.global.interface_id })
    }

    /// Checks if the GID is null (zero interface ID)
    pub fn is_null(&self) -> bool {
        self.interface_id() == 0
    }
}

impl std::fmt::Debug for ibv_gid {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_ipv6())
    }
}

impl Serialize for ibv_gid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.as_ipv6().to_string())
    }
}

impl<'de> Deserialize<'de> for ibv_gid {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        use serde::de::Error;
        let s = String::deserialize(deserializer)?;
        let addr = s
            .parse::<Ipv6Addr>()
            .map_err(|_| D::Error::custom("invalid IPv6 address format"))?;
        let bits = addr.to_bits();
        let mut gid = ibv_gid::default();
        gid.global.subnet_prefix = ((bits >> 64) as u64).to_be();
        gid.global.interface_id = (bits as u64).to_be();
        Ok(gid)
    }
}

impl JsonSchema for ibv_gid {
    fn schema_name() -> Cow<'static, str> {
        "GID".into()
    }

    fn json_schema(_generator: &mut SchemaGenerator) -> Schema {
        json_schema!({
            "type": "string",
            "pattern": "^(?:[0-9a-fA-F]{1,4}:){7}[0-9a-fA-F]{1,4}$",
            "description": "IPv6 address format GID"
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: create a GID from subnet prefix and interface ID (host byte order).
    fn make_gid(subnet_prefix: u64, interface_id: u64) -> ibv_gid {
        let mut gid = ibv_gid::default();
        gid.global.subnet_prefix = subnet_prefix.to_be();
        gid.global.interface_id = interface_id.to_be();
        gid
    }

    #[test]
    fn test_gid_null() {
        let gid = ibv_gid::default();
        assert!(gid.is_null());

        let gid = make_gid(0xfe80_0000_0000_0000, 1);
        assert!(!gid.is_null());
    }

    #[test]
    fn test_gid_subnet_interface() {
        let gid = make_gid(0xfe80_0000_0000_0000, 0x0001_0002_0003_0004);
        assert_eq!(gid.subnet_prefix(), 0xfe80_0000_0000_0000);
        assert_eq!(gid.interface_id(), 0x0001_0002_0003_0004);
    }

    #[test]
    fn test_gid_as_ipv6() {
        let gid = make_gid(0xfe80_0000_0000_0000, 1);
        assert_eq!(gid.as_ipv6(), "fe80::1".parse::<Ipv6Addr>().unwrap());
    }

    #[test]
    fn test_gid_as_bits() {
        let gid = make_gid(0xfe80_0000_0000_0000, 1);
        let expected = ((0xfe80_0000_0000_0000u128) << 64) | 1u128;
        assert_eq!(gid.as_bits(), expected);
    }

    #[test]
    fn test_gid_debug() {
        let gid = make_gid(0xfe80_0000_0000_0000, 1);
        let debug = format!("{:?}", gid);
        assert_eq!(debug, "fe80::1");
    }

    #[test]
    fn test_gid_serialize_deserialize() {
        let gid = make_gid(0xfe80_0000_0000_0000, 0x0001_0002_0003_0004);
        let json = serde_json::to_string(&gid).unwrap();
        let deserialized: ibv_gid = serde_json::from_str(&json).unwrap();
        assert_eq!(gid, deserialized);
    }

    #[test]
    fn test_gid_equality() {
        let a = make_gid(0xfe80_0000_0000_0000, 1);
        let b = make_gid(0xfe80_0000_0000_0000, 1);
        let c = make_gid(0xfe80_0000_0000_0000, 2);
        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
