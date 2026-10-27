use std::{fmt, net::SocketAddr, str::FromStr, sync::Arc};

use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::{Error, ErrorKind, Result};

/// Wire transport used to reach an RPC endpoint.
///
/// All variants exist regardless of enabled Cargo features. Support for a
/// transport is checked when acquiring a connection, not when describing it.
/// RDMA endpoints additionally name a bootstrap transport in their scheme,
/// such as `http+rdma://`; see [`Endpoint::rdma`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, clap::ValueEnum)]
pub enum Transport {
    TCP,
    WS,
    HTTP,
    RDMA,
}

impl Transport {
    /// Canonical name of this transport.
    ///
    /// This is the raw transport, not always an endpoint scheme: an RDMA
    /// endpoint encodes its bootstrap transport as `<bootstrap>+rdma`; see
    /// [`Endpoint::scheme`].
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

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
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

/// Stream transport that can carry RDMA bootstrap RPCs.
///
/// The private type makes an RDMA endpoint with an RDMA bootstrap
/// unrepresentable after parsing or construction.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum RdmaBootstrap {
    Tcp,
    Ws,
    Http,
}

impl RdmaBootstrap {
    const fn transport(self) -> Transport {
        match self {
            Self::Tcp => Transport::TCP,
            Self::Ws => Transport::WS,
            Self::Http => Transport::HTTP,
        }
    }

    const fn scheme(self) -> &'static str {
        self.transport().scheme()
    }
}

impl TryFrom<Transport> for RdmaBootstrap {
    type Error = Error;

    fn try_from(transport: Transport) -> std::result::Result<Self, Self::Error> {
        match transport {
            Transport::TCP => Ok(Self::Tcp),
            Transport::WS => Ok(Self::Ws),
            Transport::HTTP => Ok(Self::Http),
            Transport::RDMA => Err(Error::new(
                ErrorKind::InvalidArgument,
                format!("RDMA bootstrap transport must be tcp, ws or http; got {transport}"),
            )),
        }
    }
}

/// One transport and, for RDMA, the stream transport carrying bootstrap RPCs.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
enum EndpointScheme {
    Stream(Transport),
    Rdma(RdmaBootstrap),
}

impl EndpointScheme {
    fn parse(value: &str) -> Result<Self> {
        match value.split_once('+') {
            Some((bootstrap, transport)) => {
                let transport = transport.parse::<Transport>()?;
                if transport != Transport::RDMA {
                    return Err(Error::new(
                        ErrorKind::InvalidArgument,
                        format!(
                            "endpoint scheme {value} must combine a bootstrap transport with rdma"
                        ),
                    ));
                }
                let bootstrap = RdmaBootstrap::try_from(bootstrap.parse::<Transport>()?)?;
                Ok(Self::Rdma(bootstrap))
            }
            None => match value.parse::<Transport>()? {
                Transport::RDMA => Err(Error::new(
                    ErrorKind::InvalidArgument,
                    "RDMA endpoints must name their bootstrap transport, e.g. tcp+rdma://, ws+rdma:// or http+rdma://"
                        .into(),
                )),
                transport => Ok(Self::Stream(transport)),
            },
        }
    }

    const fn transport(self) -> Transport {
        match self {
            Self::Stream(transport) => transport,
            Self::Rdma(_) => Transport::RDMA,
        }
    }

    const fn bootstrap(self) -> Option<Transport> {
        match self {
            Self::Stream(_) => None,
            Self::Rdma(bootstrap) => Some(bootstrap.transport()),
        }
    }
}

impl fmt::Display for EndpointScheme {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::Stream(transport) => f.write_str(transport.scheme()),
            Self::Rdma(bootstrap) => {
                write!(f, "{}+{}", bootstrap.scheme(), Transport::RDMA.scheme())
            }
        }
    }
}

/// One server reached through one transport and a nonempty set of equivalent
/// addresses.
///
/// Addresses are sorted and deduplicated. Their order carries no preference;
/// the transport, bootstrap transport and complete set identify the endpoint
/// within a client pool. Overlapping but different sets do not share
/// connections. Callers guarantee that every address reaches the same server
/// state, including its RDMA leases.
///
/// RDMA first exchanges discovery and connection metadata over another
/// transport. An RDMA endpoint therefore also names the bootstrap transport:
/// [`Endpoint::rdma`] accepts TCP, WebSocket or HTTP. The string representation
/// always makes it explicit, for example `http+rdma://127.0.0.1:8000`. Other
/// transports use `transport://address,address`, with brackets around IPv6
/// addresses, for example `tcp://127.0.0.1:8000,[::1]:8000`.
///
/// Serde uses this normalized string in JSON and MessagePack. An endpoint can
/// be stored or exchanged without enabling its transport backend, including
/// RDMA; backend availability is checked when acquiring a connection.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct Endpoint {
    scheme: EndpointScheme,
    addrs: Arc<[SocketAddr]>,
}

impl Endpoint {
    /// Creates a non-RDMA endpoint, rejecting an empty address set.
    ///
    /// RDMA endpoints must name the transport that carries bootstrap RPCs;
    /// use [`Endpoint::rdma`] instead.
    pub fn new(transport: Transport, addrs: impl IntoIterator<Item = SocketAddr>) -> Result<Self> {
        if transport == Transport::RDMA {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA endpoints require a bootstrap transport; use Endpoint::rdma with tcp, ws or http"
                    .into(),
            ));
        }
        Self::from_parts(EndpointScheme::Stream(transport), addrs)
    }

    /// Creates a TCP endpoint from equivalent server addresses.
    pub fn tcp(addrs: impl IntoIterator<Item = SocketAddr>) -> Result<Self> {
        Self::new(Transport::TCP, addrs)
    }

    /// Creates an RDMA endpoint whose bootstrap RPCs use `bootstrap`.
    ///
    /// `bootstrap` must be [`Transport::TCP`], [`Transport::WS`] or
    /// [`Transport::HTTP`]. The address group carries bootstrap RPCs and
    /// identifies the peer; the RDMA data plane uses the negotiated device
    /// endpoints. Every address must reach the same server state.
    pub fn rdma(bootstrap: Transport, addrs: impl IntoIterator<Item = SocketAddr>) -> Result<Self> {
        let bootstrap = RdmaBootstrap::try_from(bootstrap)?;
        Self::from_parts(EndpointScheme::Rdma(bootstrap), addrs)
    }

    fn from_parts(
        scheme: EndpointScheme,
        addrs: impl IntoIterator<Item = SocketAddr>,
    ) -> Result<Self> {
        Ok(Self {
            scheme,
            addrs: normalized_addrs(addrs)?,
        })
    }

    #[must_use]
    pub const fn transport(&self) -> Transport {
        self.scheme.transport()
    }

    /// The transport carrying RDMA bootstrap RPCs.
    ///
    /// Returns `None` for every non-RDMA endpoint.
    #[must_use]
    pub const fn bootstrap_transport(&self) -> Option<Transport> {
        self.scheme.bootstrap()
    }

    /// Canonical endpoint scheme, including the RDMA bootstrap prefix.
    ///
    /// Returns `tcp`, `ws`, `http`, `tcp+rdma`, `ws+rdma` or `http+rdma`.
    /// [`Transport::scheme`] returns only the raw transport name.
    #[must_use]
    pub fn scheme(&self) -> String {
        self.scheme.to_string()
    }

    /// The stream endpoint used for RDMA bootstrap RPCs.
    ///
    /// Returns `None` for every non-RDMA endpoint. The result uses the same
    /// normalized address group and the bootstrap transport of this endpoint.
    #[must_use]
    pub fn bootstrap_endpoint(&self) -> Option<Self> {
        match self.scheme {
            EndpointScheme::Rdma(bootstrap) => Some(Self {
                scheme: EndpointScheme::Stream(bootstrap.transport()),
                addrs: self.addrs.clone(),
            }),
            EndpointScheme::Stream(_) => None,
        }
    }

    #[must_use]
    pub fn addrs(&self) -> &[SocketAddr] {
        &self.addrs
    }
}

fn normalized_addrs(addrs: impl IntoIterator<Item = SocketAddr>) -> Result<Arc<[SocketAddr]>> {
    let mut addrs: Vec<_> = addrs.into_iter().collect();
    addrs.sort_unstable();
    addrs.dedup();
    if addrs.is_empty() {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            "an endpoint requires at least one address".into(),
        ));
    }
    Ok(addrs.into())
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}://", self.scheme)?;
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

    fn from_str(value: &str) -> std::result::Result<Self, Self::Err> {
        let (scheme, addresses) = value.split_once("://").ok_or_else(|| {
            Error::new(
                ErrorKind::InvalidArgument,
                format!("endpoint must use transport://address,address: {value}"),
            )
        })?;
        let scheme = EndpointScheme::parse(scheme)?;
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
            .collect::<Result<Vec<_>>>()?;
        Self::from_parts(scheme, addrs)
    }
}

impl Serialize for Endpoint {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
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
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
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
            "tcp+rdma://127.0.0.1:8000,[::1]:8000",
            "ws+rdma://127.0.0.1:8000,[::1]:8000",
            "http+rdma://127.0.0.1:8000,[::1]:8000",
        ] {
            let endpoint: Endpoint = value.parse().unwrap();
            assert_eq!(endpoint.to_string(), value);
            assert_eq!(endpoint.scheme(), value.split_once("://").unwrap().0);
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
        let value = "http+rdma://127.0.0.1:8001,127.0.0.1:8000,127.0.0.1:8001";
        let json = serde_json::to_string(value).unwrap();
        let messagepack = rmp_serde::to_vec(value).unwrap();
        let expected = Endpoint::rdma(
            Transport::HTTP,
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
            "http+rdma://127.0.0.1:8000,127.0.0.1:8001"
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
    fn endpoint_rejects_missing_unknown_or_ambiguous_scheme() {
        assert!("127.0.0.1:8000".parse::<Endpoint>().is_err());
        assert!("unified://127.0.0.1:8000".parse::<Endpoint>().is_err());
        for invalid in [
            "tcp://",
            "tcp://127.0.0.1:8000,",
            "tcp://,127.0.0.1:8000",
            "tcp://127.0.0.1:8000,ws://127.0.0.1:8001",
            "http://127.0.0.1:8000/path",
            "tcp://::1:8000",
            // RDMA must name a bootstrap transport.
            "rdma://127.0.0.1:8000",
            // Only a bootstrap transport plus final `rdma` is accepted.
            "tcp+ws://127.0.0.1:8000",
            "ws+tcp://127.0.0.1:8000",
            "rdma+tcp://127.0.0.1:8000",
            "rdma+rdma://127.0.0.1:8000",
            "tcp+tcp://127.0.0.1:8000",
            "http+rdma+ws://127.0.0.1:8000",
            "tcp+://127.0.0.1:8000",
        ] {
            assert!(invalid.parse::<Endpoint>().is_err(), "{invalid}");
        }
        assert!(Endpoint::tcp([]).is_err());
        assert!(Endpoint::rdma(Transport::TCP, []).is_err());
        assert!(Endpoint::new(Transport::RDMA, ["127.0.0.1:8000".parse().unwrap()]).is_err());
        assert!(Endpoint::rdma(Transport::RDMA, ["127.0.0.1:8000".parse().unwrap()]).is_err());
    }

    #[test]
    fn rdma_bootstrap_transport_is_part_of_endpoint_identity() {
        let a = "127.0.0.1:8000".parse().unwrap();
        let b = "127.0.0.1:8001".parse().unwrap();
        let tcp = Endpoint::rdma(Transport::TCP, [a, b]).unwrap();
        let ws = Endpoint::rdma(Transport::WS, [a]).unwrap();
        let http = Endpoint::rdma(Transport::HTTP, [a, b]).unwrap();
        assert_eq!(tcp.transport(), Transport::RDMA);
        assert_eq!(tcp.bootstrap_transport(), Some(Transport::TCP));
        assert_eq!(ws.bootstrap_transport(), Some(Transport::WS));
        assert_eq!(http.bootstrap_transport(), Some(Transport::HTTP));
        assert_eq!(
            ws.bootstrap_endpoint(),
            Some(Endpoint::new(Transport::WS, [a]).unwrap())
        );
        assert_ne!(tcp, http);
        assert_eq!(tcp.scheme(), "tcp+rdma");
        assert_eq!(http.scheme(), "http+rdma");
        assert_eq!(tcp.to_string(), "tcp+rdma://127.0.0.1:8000,127.0.0.1:8001");
        assert_eq!(
            http.to_string(),
            "http+rdma://127.0.0.1:8000,127.0.0.1:8001"
        );
        assert_eq!(
            tcp.bootstrap_endpoint(),
            Some(Endpoint::tcp([a, b]).unwrap())
        );
        let plain = Endpoint::tcp([a]).unwrap();
        assert_eq!(plain.bootstrap_transport(), None);
        assert_eq!(plain.bootstrap_endpoint(), None);
        assert_eq!(plain.scheme(), "tcp");

        let mut groups = std::collections::HashSet::new();
        groups.insert(tcp);
        groups.insert(http);
        groups.insert(plain);
        assert_eq!(groups.len(), 3);
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
