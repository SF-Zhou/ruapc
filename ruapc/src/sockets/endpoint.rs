use std::{fmt, net::SocketAddr, str::FromStr, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Error, ErrorKind};

/// Wire transport used to reach an RPC endpoint.
///
/// All variants exist regardless of enabled Cargo features. Support for a
/// transport is checked when acquiring a connection, not when describing it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, clap::ValueEnum)]
pub enum Transport {
    TCP,
    WS,
    HTTP,
    RDMA,
}

impl Transport {
    #[must_use]
    pub const fn scheme(self) -> &'static str {
        match self {
            Self::TCP => "tcp",
            Self::WS => "ws",
            Self::HTTP => "http",
            Self::RDMA => "rdma",
        }
    }
}

impl fmt::Display for Transport {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.scheme())
    }
}

impl FromStr for Transport {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value.to_ascii_lowercase().as_str() {
            "tcp" => Ok(Self::TCP),
            "ws" => Ok(Self::WS),
            "http" => Ok(Self::HTTP),
            "rdma" => Ok(Self::RDMA),
            _ => Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("unsupported endpoint transport: {value}"),
            )),
        }
    }
}

/// How a server listener interprets accepted TCP streams.
#[derive(Deserialize, Serialize, Debug, Clone, Copy, PartialEq, Eq, clap::ValueEnum)]
pub enum ListenMode {
    TCP,
    WS,
    HTTP,
    UNIFIED,
}

impl fmt::Display for ListenMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(self, f)
    }
}

/// One server reached through a nonempty set of equivalent addresses.
///
/// Addresses are sorted and deduplicated. Their order carries no preference;
/// the transport and complete set identify the endpoint within a client pool.
/// Overlapping but different sets do not share connections. Callers guarantee
/// that every address reaches the same server state, including its RDMA leases.
///
/// The string representation is `transport://address,address`, with brackets
/// around IPv6 addresses, for example `tcp://127.0.0.1:8000,[::1]:8000`.
/// Serde uses this normalized string in JSON and MessagePack. An endpoint can
/// be stored or exchanged without enabling its transport backend, including
/// RDMA; backend availability is checked when acquiring a connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Endpoint {
    transport: Transport,
    addrs: Arc<[SocketAddr]>,
}

impl Endpoint {
    /// Creates an endpoint, rejecting an empty address set.
    pub fn new(
        transport: Transport,
        addrs: impl IntoIterator<Item = SocketAddr>,
    ) -> crate::Result<Self> {
        let mut addrs: Vec<_> = addrs.into_iter().collect();
        addrs.sort_unstable();
        addrs.dedup();
        if addrs.is_empty() {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "an endpoint requires at least one address".into(),
            ));
        }
        Ok(Self {
            transport,
            addrs: addrs.into(),
        })
    }

    /// Creates a TCP endpoint from equivalent server addresses.
    pub fn tcp(addrs: impl IntoIterator<Item = SocketAddr>) -> crate::Result<Self> {
        Self::new(Transport::TCP, addrs)
    }

    #[must_use]
    pub const fn transport(&self) -> Transport {
        self.transport
    }

    #[must_use]
    pub fn addrs(&self) -> &[SocketAddr] {
        &self.addrs
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://", self.transport)?;
        for (index, addr) in self.addrs.iter().enumerate() {
            if index != 0 {
                f.write_str(",")?;
            }
            write!(f, "{addr}")?;
        }
        Ok(())
    }
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (scheme, addresses) = value.split_once("://").ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("endpoint must use transport://address,address: {value}"),
            )
        })?;
        let transport = scheme.parse()?;
        let addrs = addresses
            .split(',')
            .map(|addr| {
                addr.parse().map_err(|err| {
                    Error::new(
                        ErrorKind::InvalidArgument,
                        format!("invalid endpoint address {addr}: {err}"),
                    )
                })
            })
            .collect::<crate::Result<Vec<_>>>()?;
        Self::new(transport, addrs)
    }
}

impl Serialize for Endpoint {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl schemars::JsonSchema for Endpoint {
    fn schema_name() -> std::borrow::Cow<'static, str> {
        "Endpoint".into()
    }

    fn json_schema(generator: &mut schemars::SchemaGenerator) -> schemars::Schema {
        <String as schemars::JsonSchema>::json_schema(generator)
    }
}

impl<'de> Deserialize<'de> for Endpoint {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        String::deserialize(deserializer)?
            .parse()
            .map_err(serde::de::Error::custom)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn endpoint_uri_roundtrip() {
        for value in [
            "tcp://127.0.0.1:8000",
            "ws://127.0.0.1:8001",
            "http://[::1]:8002",
            "tcp://127.0.0.1:8000,127.0.0.2:8000",
            "ws://127.0.0.1:8000,[::1]:8000",
            "http://[::1]:8000,[::2]:8000",
            "rdma://127.0.0.1:8000,[::1]:8000",
        ] {
            let endpoint: Endpoint = value.parse().unwrap();
            assert_eq!(endpoint.to_string(), value);
            let json = serde_json::to_string(&endpoint).unwrap();
            assert_eq!(json, format!("\"{value}\""));
            assert_eq!(serde_json::from_str::<Endpoint>(&json).unwrap(), endpoint);
            let messagepack = rmp_serde::to_vec(&endpoint).unwrap();
            assert_eq!(
                rmp_serde::from_slice::<String>(&messagepack).unwrap(),
                value
            );
            assert_eq!(
                rmp_serde::from_slice::<Endpoint>(&messagepack).unwrap(),
                endpoint
            );
        }
    }

    #[test]
    fn deserialization_normalizes_endpoint_strings_and_rejects_invalid_groups() {
        let value = "rdma://127.0.0.1:8001,127.0.0.1:8000,127.0.0.1:8001";
        let json = serde_json::to_string(value).unwrap();
        let messagepack = rmp_serde::to_vec(value).unwrap();
        let expected = Endpoint::new(
            Transport::RDMA,
            [
                "127.0.0.1:8000".parse().unwrap(),
                "127.0.0.1:8001".parse().unwrap(),
            ],
        )
        .unwrap();
        assert_eq!(serde_json::from_str::<Endpoint>(&json).unwrap(), expected);
        assert_eq!(
            rmp_serde::from_slice::<Endpoint>(&messagepack).unwrap(),
            expected
        );
        assert_eq!(
            serde_json::to_value(expected).unwrap(),
            "rdma://127.0.0.1:8000,127.0.0.1:8001"
        );

        for value in [
            "rdma://",
            "rdma://127.0.0.1:8000,",
            "unknown://127.0.0.1:8000",
        ] {
            assert!(
                serde_json::from_str::<Endpoint>(&serde_json::to_string(value).unwrap()).is_err()
            );
            assert!(rmp_serde::from_slice::<Endpoint>(&rmp_serde::to_vec(value).unwrap()).is_err());
        }
    }

    #[test]
    fn endpoint_rejects_missing_or_unknown_scheme() {
        assert!("127.0.0.1:8000".parse::<Endpoint>().is_err());
        assert!("unified://127.0.0.1:8000".parse::<Endpoint>().is_err());
        for invalid in [
            "tcp://",
            "tcp://127.0.0.1:8000,",
            "tcp://,127.0.0.1:8000",
            "tcp://127.0.0.1:8000,ws://127.0.0.1:8001",
            "http://127.0.0.1:8000/path",
            "tcp://::1:8000",
        ] {
            assert!(invalid.parse::<Endpoint>().is_err(), "{invalid}");
        }
        assert!(Endpoint::tcp([]).is_err());
    }

    #[test]
    fn complete_normalized_set_defines_identity() {
        let a = "127.0.0.1:8000".parse().unwrap();
        let b = "127.0.0.1:8001".parse().unwrap();
        let first = Endpoint::tcp([b, a, b]).unwrap();
        let second = Endpoint::tcp([a, b]).unwrap();
        assert_eq!(first, second);
        assert_eq!(first.to_string(), "tcp://127.0.0.1:8000,127.0.0.1:8001");
        let mut groups = std::collections::HashSet::new();
        groups.insert(first);
        groups.insert(second);
        groups.insert(Endpoint::tcp([a]).unwrap());
        groups.insert(Endpoint::new(Transport::WS, [a, b]).unwrap());
        assert_eq!(groups.len(), 3);
        let schema = schemars::schema_for!(Endpoint);
        assert_eq!(schema.as_value()["type"], "string");
    }
}
