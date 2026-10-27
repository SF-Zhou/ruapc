use std::{sync::Arc, time::Duration};

use ruapc_bufpool::RemoteBufferInfo;
use ruapc_rdma::{DeviceInfo, Gid, LinkLayer, Port};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::{
    Context, CopyOp, Error, ErrorKind, MsgMeta, Result, Socket,
    core::ContextEndpoint,
    rdma::{self, RdmaSocket, RdmaSocketPoolConfig},
    service,
};

/// Version of the RDMA bootstrap protocol.
pub(crate) const RDMA_BOOTSTRAP_PROTOCOL_VERSION: u32 = 3;

fn port_is_connectable(port: &Port) -> bool {
    port.is_usable() && (!port.port_attr.link_layer.is_ethernet() || !port.gids.is_empty())
}

/// Port information advertised for RDMA connection negotiation.
///
/// Deliberately kept independent of the `ibv_*` structures generated from
/// the ibverbs headers: it carries exactly the fields the peer needs to
/// select a port/GID pair, nothing else. Only usable ports (active, with an
/// InfiniBand or Ethernet link layer) are advertised, so no port state
/// travels on the wire.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct RdmaPortInfo {
    /// Port number (1-based).
    pub port_num: u8,
    /// Link layer type (InfiniBand or Ethernet).
    pub link_layer: LinkLayer,
    /// Usable GIDs of this port (filtered at collection time).
    pub gids: Vec<Gid>,
}

/// RDMA device information for connection negotiation.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct RdmaDeviceInfo {
    /// Device name (e.g., "mlx5_0").
    pub name: String,
    /// Live RDMA connections (outbound + inbound) currently on this device.
    /// Advertised so that clients can prefer less-loaded server NICs.
    pub active_connections: u32,
    /// Server-advertised per-connection RDMA resource limits for this device.
    pub limits: rdma::RdmaConnectionLimits,
    /// Usable ports on this device.
    pub ports: Vec<RdmaPortInfo>,
}

impl RdmaDeviceInfo {
    fn from_device_info(
        info: &DeviceInfo,
        config: &RdmaSocketPoolConfig,
        active_connections: u32,
    ) -> Option<Self> {
        let ports: Vec<_> = info
            .ports
            .iter()
            .filter(|port| port_is_connectable(port))
            .map(|port| RdmaPortInfo {
                port_num: port.port_num,
                link_layer: port.port_attr.link_layer,
                gids: port.gids.clone(),
            })
            .collect();
        if ports.is_empty() {
            return None;
        }
        let max_send_wr = config
            .connection
            .qp
            .max_send_wr
            .min(info.device_attr.max_qp_wr as u32);
        let max_recv_wr = config
            .connection
            .qp
            .max_recv_wr
            .min(info.device_attr.max_qp_wr as u32);
        Some(Self {
            name: info.name.clone(),
            active_connections,
            limits: rdma::RdmaConnectionLimits {
                max_send_wr,
                max_recv_wr,
                recv_queue_len: config.connection.recv_queue_len.min(max_recv_wr),
                max_msg_size: config.connection.max_msg_size,
                max_large_msg_size: config.connection.max_large_msg_size,
            },
            ports,
        })
    }
}

/// Information about available RDMA devices in the system.
///
/// This structure is exchanged between client and server during
/// RDMA connection negotiation. It contains only the minimal
/// information needed to select a compatible device/port/GID pair.
#[derive(Debug, Serialize, Deserialize, JsonSchema, Clone)]
pub struct RdmaPeerAdvertisement {
    /// Bootstrap wire protocol version understood by this peer.
    pub protocol_version: u32,
    /// List of available RDMA devices with connection-relevant info
    pub devices: Vec<RdmaDeviceInfo>,
}

impl RdmaPeerAdvertisement {
    /// Build an advertisement from the full device info list.
    ///
    /// Only usable ports are advertised. GIDs unusable for communication
    /// (RoCE v2 GIDs derived from loopback or IPv6 link-local addresses)
    /// are already filtered out at collection time.
    pub(crate) fn from_devices(
        devices: &[super::RdmaDevice],
        config: &RdmaSocketPoolConfig,
        conn_counts: &[std::sync::atomic::AtomicUsize],
    ) -> Self {
        RdmaPeerAdvertisement {
            protocol_version: RDMA_BOOTSTRAP_PROTOCOL_VERSION,
            devices: devices
                .iter()
                .enumerate()
                .filter_map(|(index, d)| {
                    let info = d.info();
                    let active_connections = conn_counts
                        .get(index)
                        .map(|c| c.load(std::sync::atomic::Ordering::Acquire))
                        .unwrap_or(0)
                        .try_into()
                        .unwrap_or(u32::MAX);
                    RdmaDeviceInfo::from_device_info(&info, config, active_connections)
                })
                .collect(),
        }
    }
}

pub(super) const PROBE_BYTES: usize = std::mem::size_of::<u64>();

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
pub(crate) struct ProbeReply {
    pub(crate) rpc_nonce: u64,
    pub(crate) read_nonce: u64,
}

/// An immutable serialized payload exposed for READ on this connection.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(crate) struct LargePayloadOffer {
    pub(crate) transfer_id: u64,
    pub(crate) payload_len: u64,
    pub(crate) regions: Vec<RemoteBufferInfo>,
    #[schemars(with = "PayloadMetadataSchema")]
    pub(crate) meta: MsgMeta,
}

// Schema-only projection: wire metadata has no dependency on schema generation.
// Keep these field types and defaults aligned with ruapc_wire::MsgMeta.
#[derive(JsonSchema)]
#[allow(dead_code)]
struct PayloadMetadataSchema {
    #[schemars(default)]
    method: String,
    #[schemars(default)]
    flags: u8,
    #[schemars(default)]
    msgid: u64,
    #[schemars(default)]
    read_regions: Vec<RemoteBufferInfo>,
    #[schemars(default)]
    write_regions: Vec<RemoteBufferInfo>,
    #[schemars(default)]
    timeout_ms: u32,
}

/// Identifies a source whose READs have settled, or were never posted.
#[derive(Debug, Clone, Serialize, Deserialize, JsonSchema)]
pub(crate) struct FinishPayloadRequest {
    pub(crate) transfer_id: u64,
}

/// Service interface for RDMA operations.
///
/// Manages RDMA connections, queries device information, checks connection
/// health, and transfers connection-owned serialized payloads. Payload methods
/// require delivery through the established RDMA connection. All methods share
/// the configured handler concurrency limit.
#[service]
pub(crate) trait RdmaService {
    /// Discovers the peer's connectable RDMA devices and limits.
    async fn discover(&self, ctx: &Context, _: &()) -> Result<rdma::RdmaPeerAdvertisement>;

    /// Prepares an RDMA connection with the selected acceptor endpoint.
    async fn prepare_connection(
        &self,
        ctx: &Context,
        request: &rdma::PrepareConnectionRequest,
    ) -> Result<rdma::PrepareConnectionResponse>;

    /// Confirms that the initiator received the endpoint and retained its
    /// local QP. The first confirmation starts the data-plane activation
    /// budget; repeated confirmations do not extend it. The connection
    /// remains leased until a successful receive also proves activation.
    /// The bootstrap service assumes a trusted control plane; the token
    /// correlates lifecycle state but does not authenticate the caller.
    async fn commit_connection(&self, ctx: &Context, lease: &rdma::ConnectionLease) -> Result<()>;

    /// Best-effort idempotent cleanup when the initiator cannot complete the
    /// confirmation exchange.
    async fn cancel_connection(&self, ctx: &Context, lease: &rdma::ConnectionLease) -> Result<()>;

    /// Checks RPC delivery and reads the attached nonce over the same RDMA connection.
    async fn probe(&self, ctx: &Context, nonce: &u64) -> Result<ProbeReply>;

    /// Starts receiving a serialized payload on the incoming RDMA connection.
    async fn offer(&self, ctx: &Context, offer: &LargePayloadOffer) -> Result<()>;

    /// Releases a settled source and reports whether its payload remains valid.
    async fn finish(&self, ctx: &Context, request: &FinishPayloadRequest) -> Result<bool>;
}

/// Default implementation of `RdmaService` for the unit type.
///
/// Connection management delegates to the socket pool; payload transfers and
/// health probes operate on the incoming RDMA socket.
impl RdmaService for () {
    /// Retrieves RDMA device information from the socket pool.
    async fn discover(&self, ctx: &Context, (): &()) -> Result<rdma::RdmaPeerAdvertisement> {
        ctx.state.socket_pool.rdma_peer_advertisement()
    }

    /// Prepares an RDMA connection using the socket pool.
    async fn prepare_connection(
        &self,
        ctx: &Context,
        request: &rdma::PrepareConnectionRequest,
    ) -> Result<rdma::PrepareConnectionResponse> {
        ctx.state
            .socket_pool
            .rdma_prepare_connection(request, &ctx.state)
    }

    async fn commit_connection(&self, ctx: &Context, lease: &rdma::ConnectionLease) -> Result<()> {
        ctx.state.socket_pool.rdma_commit_connection(lease)
    }

    async fn cancel_connection(&self, ctx: &Context, lease: &rdma::ConnectionLease) -> Result<()> {
        ctx.state.socket_pool.rdma_cancel_connection(lease)
    }

    async fn probe(&self, ctx: &Context, nonce: &u64) -> Result<ProbeReply> {
        if !matches!(&ctx.endpoint, ContextEndpoint::Connected(Socket::RDMA(_))) {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA probe requires an established RDMA connection".into(),
            ));
        }
        if ctx.remote_read_space()?.total_len() != PROBE_BYTES as u64 {
            return Err(Error::new(
                ErrorKind::InvalidArgument,
                "RDMA probe requires exactly eight bytes of read space".into(),
            ));
        }
        let mut destination = ctx
            .state
            .buffer_pool
            .allocate(PROBE_BYTES)
            .map_err(|error| Error::new(ErrorKind::InvalidArgument, error.to_string()))?;
        destination.set_len(PROBE_BYTES);
        let destinations = ctx
            .remote_read(&[CopyOp::new(0, 0, PROBE_BYTES as u64)], vec![destination])
            .await?;
        let mut read_bytes = [0; PROBE_BYTES];
        read_bytes.copy_from_slice(&destinations[0]);
        Ok(ProbeReply {
            rpc_nonce: *nonce,
            read_nonce: u64::from_le_bytes(read_bytes),
        })
    }

    async fn offer(&self, ctx: &Context, offer: &LargePayloadOffer) -> Result<()> {
        let socket = payload_socket(ctx)?;
        let began = ctx
            .deadline()
            .and_then(|deadline| {
                deadline.checked_sub(Duration::from_millis(u64::from(ctx.msg_meta.timeout_ms)))
            })
            .ok_or_else(|| Error::kind(ErrorKind::InvalidArgument))?;
        socket.start_large_receive(offer.clone(), &ctx.state, began)
    }

    async fn finish(&self, ctx: &Context, request: &FinishPayloadRequest) -> Result<bool> {
        payload_socket(ctx)?.finish_large(request.transfer_id, &ctx.state)
    }
}

fn payload_socket(ctx: &Context) -> Result<&Arc<RdmaSocket>> {
    match &ctx.endpoint {
        ContextEndpoint::Connected(Socket::RDMA(socket)) => Ok(socket),
        _ => Err(Error::new(
            ErrorKind::InvalidArgument,
            "RDMA payload transfer requires delivery on an established RDMA connection".into(),
        )),
    }
}

pub(crate) fn is_payload_method(method: &str) -> bool {
    matches!(method, "RdmaService/offer" | "RdmaService/finish")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ErrorKind, SocketPoolConfig, rdma::RdmaSocketPoolConfig};

    #[test]
    fn ethernet_advertisement_requires_a_gid() {
        let mut ethernet = Port {
            port_num: 1,
            port_attr: ruapc_rdma::ibv_port_attr {
                state: ruapc_rdma::ibv_port_state::IBV_PORT_ACTIVE,
                link_layer: LinkLayer::Ethernet,
                ..Default::default()
            },
            gids: Vec::new(),
        };
        assert!(!port_is_connectable(&ethernet));
        let config = RdmaSocketPoolConfig::default();
        let ethernet_only = DeviceInfo {
            name: "ethernet-only".into(),
            ports: vec![ethernet.clone()],
            ..Default::default()
        };
        assert!(RdmaDeviceInfo::from_device_info(&ethernet_only, &config, 0).is_none());

        ethernet.gids.push(Gid {
            index: 0,
            gid: ruapc_rdma::ibv_gid::default(),
            gid_type: ruapc_rdma::GidType::RoCEv2,
        });
        assert!(port_is_connectable(&ethernet));
        let ethernet_with_gid = DeviceInfo {
            name: "ethernet-with-gid".into(),
            ports: vec![ethernet],
            ..Default::default()
        };
        assert!(RdmaDeviceInfo::from_device_info(&ethernet_with_gid, &config, 0).is_some());

        let infiniband = Port {
            port_num: 1,
            port_attr: ruapc_rdma::ibv_port_attr {
                state: ruapc_rdma::ibv_port_state::IBV_PORT_ACTIVE,
                link_layer: LinkLayer::InfiniBand,
                ..Default::default()
            },
            gids: Vec::new(),
        };
        assert!(port_is_connectable(&infiniband));
    }

    #[tokio::test]
    async fn test_rdma_bootstrap_discover_returns_connectable_devices() {
        let config = SocketPoolConfig::default();
        let ctx = Context::create(&config).expect("failed to create RDMA context");
        let result = ().discover(&ctx, &()).await;
        assert!(result.is_ok());
        let advertisement = result.unwrap();
        assert_eq!(
            advertisement.protocol_version,
            RDMA_BOOTSTRAP_PROTOCOL_VERSION
        );
        assert!(!advertisement.devices.is_empty());
        assert!(
            advertisement
                .devices
                .iter()
                .all(|device| !device.ports.is_empty())
        );
        let lease = rdma::ConnectionLease {
            attempt_id: u64::MAX,
            accepted_connection_id: u64::MAX,
        };
        let err = ().commit_connection(&ctx, &lease).await.unwrap_err();
        assert_eq!(err.kind, ErrorKind::InvalidArgument);
        ().cancel_connection(&ctx, &lease).await.unwrap();
    }

    #[tokio::test]
    async fn test_rdma_bootstrap_prepare_non_rdma_returns_err() {
        // With a TCP pool, preparation propagates the socket-pool error.
        let ctx = Context::create(&SocketPoolConfig::default()).unwrap();
        let endpoint = rdma::RdmaQpEndpoint {
            qp_num: 0,
            port_num: 1,
            gid_index: 0,
            gid: ruapc_rdma::ibv_gid::default(),
            lid: 0,
            link_layer: ruapc_rdma::LinkLayer::Ethernet,
            active_mtu: ruapc_rdma::ibv_mtu::IBV_MTU_512,
            psn: 0,
            rd_atomic_cap: 1,
        };
        let request = rdma::PrepareConnectionRequest {
            attempt_id: 1,
            endpoint,
            source_device: "test".into(),
            same_connectivity_domain: false,
            target: rdma::DeviceSelection {
                device_name: "missing".into(),
                port_num: 1,
                gid_index: 0,
            },
            limits: rdma::RdmaConnectionLimits {
                max_send_wr: 64,
                max_recv_wr: 64,
                recv_queue_len: 64,
                max_msg_size: 1024 * 1024,
                max_large_msg_size: 0,
            },
            traffic_class: 0,
        };
        let result = ().prepare_connection(&ctx, &request).await;
        assert!(result.is_err());
    }
}

#[cfg(test)]
mod payload_tests {
    use super::*;
    use crate::{Client, Endpoint, Router, Server, SocketPoolConfig, Transport};

    fn offer() -> LargePayloadOffer {
        LargePayloadOffer {
            transfer_id: 1,
            payload_len: 1,
            regions: Vec::new(),
            meta: MsgMeta::default(),
        }
    }

    #[tokio::test]
    async fn payload_methods_reject_unconnected_and_tcp_contexts() {
        let config = SocketPoolConfig {
            rdma: None,
            ..Default::default()
        };
        let context = Context::create(&config).unwrap();
        let finish = FinishPayloadRequest { transfer_id: 1 };
        assert_eq!(
            ().offer(&context, &offer()).await.unwrap_err().kind,
            ErrorKind::InvalidArgument
        );
        assert_eq!(
            ().finish(&context, &finish).await.unwrap_err().kind,
            ErrorKind::InvalidArgument
        );
        let server = Server::create(Router::default(), &config).unwrap();
        let address = server.listen("127.0.0.1:0".parse().unwrap()).await.unwrap();
        let context = context.with_endpoint(Endpoint::new(Transport::TCP, [address]).unwrap());
        for use_msgpack in [true, false] {
            let client = Client {
                use_msgpack,
                ..Default::default()
            };
            for error in [
                client.offer(&context, &offer()).await.unwrap_err(),
                client.finish(&context, &finish).await.unwrap_err(),
            ] {
                assert_eq!(error.kind, ErrorKind::InvalidArgument);
                assert!(error.msg.contains("established RDMA connection"));
            }
        }
        context.state.socket_pool.stop();
        context.state.socket_pool.join().await;
        server.stop();
        server.join().await;
    }
}
