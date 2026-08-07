use std::{fmt, net::SocketAddr, str::FromStr};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Error, ErrorKind};

/// Wire transport used to reach an RPC endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, clap::ValueEnum)]
pub enum Transport {
    TCP,
    WS,
    HTTP,
    #[cfg(feature = "rdma")]
    RDMA,
}

impl Transport {
    #[must_use]
    pub const fn scheme(self) -> &'static str {
        match self {
            Self::TCP => "tcp",
            Self::WS => "ws",
            Self::HTTP => "http",
            #[cfg(feature = "rdma")]
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
            #[cfg(feature = "rdma")]
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

/// A complete outbound RPC destination.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Endpoint {
    transport: Transport,
    addr: SocketAddr,
}

impl Endpoint {
    #[must_use]
    pub const fn new(transport: Transport, addr: SocketAddr) -> Self {
        Self { transport, addr }
    }

    #[must_use]
    pub const fn tcp(addr: SocketAddr) -> Self {
        Self::new(Transport::TCP, addr)
    }

    #[must_use]
    pub const fn transport(self) -> Transport {
        self.transport
    }

    #[must_use]
    pub const fn addr(self) -> SocketAddr {
        self.addr
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://{}", self.transport, self.addr)
    }
}

impl FromStr for Endpoint {
    type Err = Error;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        let (scheme, addr) = value.split_once("://").ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("endpoint must use transport://address: {value}"),
            )
        })?;
        let transport = scheme.parse()?;
        let addr = addr.parse().map_err(|err| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("invalid endpoint address {addr}: {err}"),
            )
        })?;
        Ok(Self::new(transport, addr))
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
        ] {
            let endpoint: Endpoint = value.parse().unwrap();
            assert_eq!(endpoint.to_string(), value);
            let json = serde_json::to_string(&endpoint).unwrap();
            assert_eq!(serde_json::from_str::<Endpoint>(&json).unwrap(), endpoint);
        }
    }

    #[test]
    fn endpoint_rejects_missing_or_unknown_scheme() {
        assert!("127.0.0.1:8000".parse::<Endpoint>().is_err());
        assert!("unified://127.0.0.1:8000".parse::<Endpoint>().is_err());
    }
}
