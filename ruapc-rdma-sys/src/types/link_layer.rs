//! RDMA link layer type with serialization support
//!
//! The link layer specifies the physical layer protocol used by the port:
//! - InfiniBand: Native IB protocol
//! - Ethernet: RoCE (RDMA over Converged Ethernet)

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Link layer type for RDMA ports
///
/// Corresponds to the `ibv_link_layer` enum from libibverbs:
/// - IBV_LINK_LAYER_UNSPECIFIED = 0
/// - IBV_LINK_LAYER_INFINIBAND = 1
/// - IBV_LINK_LAYER_ETHERNET = 4
#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq, Hash, Debug, Serialize, Deserialize, JsonSchema)]
pub enum LinkLayer {
    /// Link layer not specified or unknown
    Unspecified = 0,
    /// InfiniBand link layer
    InfiniBand = 1,
    /// Ethernet link layer (used for RoCE)
    Ethernet = 4,
}

impl LinkLayer {
    /// Creates a LinkLayer from a raw u8 value
    ///
    /// Returns `LinkLayer::Unspecified` for unknown values
    pub const fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Unspecified,
            1 => Self::InfiniBand,
            4 => Self::Ethernet,
            _ => Self::Unspecified,
        }
    }

    /// Returns the string representation of this link layer
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Unspecified => "Unspecified",
            Self::InfiniBand => "InfiniBand",
            Self::Ethernet => "Ethernet",
        }
    }

    /// Returns true if this is an InfiniBand link layer
    pub fn is_infiniband(&self) -> bool {
        matches!(self, Self::InfiniBand)
    }

    /// Returns true if this is an Ethernet link layer (RoCE)
    pub fn is_ethernet(&self) -> bool {
        matches!(self, Self::Ethernet)
    }
}

impl From<u8> for LinkLayer {
    fn from(value: u8) -> Self {
        Self::from_u8(value)
    }
}

impl From<LinkLayer> for u8 {
    fn from(layer: LinkLayer) -> Self {
        layer as u8
    }
}

impl std::fmt::Display for LinkLayer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_link_layer_from_u8() {
        assert_eq!(LinkLayer::from_u8(0), LinkLayer::Unspecified);
        assert_eq!(LinkLayer::from_u8(1), LinkLayer::InfiniBand);
        assert_eq!(LinkLayer::from_u8(4), LinkLayer::Ethernet);
        assert_eq!(LinkLayer::from_u8(99), LinkLayer::Unspecified);
    }

    #[test]
    fn test_link_layer_from() {
        assert_eq!(LinkLayer::from(0u8), LinkLayer::Unspecified);
        assert_eq!(LinkLayer::from(1u8), LinkLayer::InfiniBand);
        assert_eq!(LinkLayer::from(4u8), LinkLayer::Ethernet);
    }

    #[test]
    fn test_link_layer_to_u8() {
        assert_eq!(u8::from(LinkLayer::Unspecified), 0);
        assert_eq!(u8::from(LinkLayer::InfiniBand), 1);
        assert_eq!(u8::from(LinkLayer::Ethernet), 4);
    }

    #[test]
    fn test_link_layer_display() {
        assert_eq!(format!("{}", LinkLayer::Unspecified), "Unspecified");
        assert_eq!(format!("{}", LinkLayer::InfiniBand), "InfiniBand");
        assert_eq!(format!("{}", LinkLayer::Ethernet), "Ethernet");
    }

    #[test]
    fn test_link_layer_as_str() {
        assert_eq!(LinkLayer::Unspecified.as_str(), "Unspecified");
        assert_eq!(LinkLayer::InfiniBand.as_str(), "InfiniBand");
        assert_eq!(LinkLayer::Ethernet.as_str(), "Ethernet");
    }

    #[test]
    fn test_link_layer_is_infiniband() {
        assert!(!LinkLayer::Unspecified.is_infiniband());
        assert!(LinkLayer::InfiniBand.is_infiniband());
        assert!(!LinkLayer::Ethernet.is_infiniband());
    }

    #[test]
    fn test_link_layer_is_ethernet() {
        assert!(!LinkLayer::Unspecified.is_ethernet());
        assert!(!LinkLayer::InfiniBand.is_ethernet());
        assert!(LinkLayer::Ethernet.is_ethernet());
    }

    #[test]
    fn test_link_layer_serialize_deserialize() {
        let layer = LinkLayer::InfiniBand;
        let json = serde_json::to_string(&layer).unwrap();
        assert_eq!(json, "\"InfiniBand\"");

        let deserialized: LinkLayer = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, LinkLayer::InfiniBand);
    }
}
