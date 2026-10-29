//! Typed, bidirectional RPC over TCP, WebSocket, HTTP and optional RDMA.
//!
//! Define an async trait with [`service`], implement it, and register the
//! implementation in a [`Router`]. A [`Server`] hosts that router; generated
//! methods on [`Client`] use a [`Context`] and [`Endpoint`] to reach it.
//!
//! ## Configuration
//!
//! [`SocketPoolConfig`] controls listeners, connection resources and handler
//! admission. TCP is the default listener; combine [`ListenMode`] flags with
//! `|` to select protocols on one port, or use [`ListenMode::UNIFIED`] for all
//! three. HTTP routes and WebSocket upgrades are enabled independently.
//! [`Client`] controls connection/response budgets, retries and payload format.
//! Payloads default to MessagePack;
//! setting [`Client::use_msgpack`] to `false` selects JSON.
//!
//! The optional `rdma` feature requires libibverbs development files. With
//! it enabled, the default socket configuration initializes RDMA resources;
//! set its `rdma` field to `None` for stream-only operation.
//!
//! ## Remote memory
//!
//! [`ClientWithBuffers`] attaches owned read sources and write destinations.
//! Handlers use [`Context::remote_read`] and [`Context::remote_write`] with
//! logical [`CopyOp`] ranges. Handlers and clients share the declared service
//! trait and [`BufferReply`] result type, with explicit local destination availability.
//! [`RemoteIoError`] describes recovery after a failed transfer.
//!
//! See the [quick start](https://github.com/SF-Zhou/ruapc#example) for a complete
//! service and the [documentation index](https://github.com/SF-Zhou/ruapc/blob/main/docs/README.md)
//! for architecture, ownership contracts and runnable workloads.

#![forbid(unsafe_code)]
// Transport enums and client call glue use statically dispatched futures.
// Their concrete implementations determine Send; user service traits instead
// receive an explicit `impl Future + Send` contract from `#[service]`.
#![allow(async_fn_in_trait)]

pub use ruapc_macro::service;

mod error;
pub use error::{Error, ErrorKind, RemoteIoError, Result};

mod msg;
pub use msg::{Message, MsgFlags, MsgMeta, Payload};

mod client;
pub use client::{Client, ClientWithBuffers};

mod core;
#[doc(hidden)]
pub use client::{RawCall, ResponseContract};
pub use core::{Context, Listener, MethodSchema, Router, Server, State};
#[doc(hidden)]
pub use core::{catch_handler_panic, spawn_handler};

mod remote_memory;
pub use remote_memory::{
    BufferAvailability, BufferReply, CopyOp, MAX_COPY_OPS, MAX_REGIONS, RemoteSpace,
    ResultBufferReply, WriteReceipt,
};

mod metrics;
pub(crate) use metrics::Metrics;

mod task;
pub(crate) use task::Receiver;
pub use task::{TaskSupervisor, TaskSupervisorHandle, Waiter, WaiterCleaner};

mod sockets;
pub use sockets::*;

pub use ruapc_bufpool::Buffer;
pub use ruapc_bufpool::BufferPool;
pub use ruapc_bufpool::DEFAULT_BUFFER_POOL_MEMORY;
pub use ruapc_bufpool::Device as _;
pub use ruapc_bufpool::Devices as _;

#[cfg(feature = "rdma")]
pub mod rdma;

/// The TCP registration device and the configured RDMA devices.
#[cfg(feature = "rdma")]
pub type Devices = ruapc_bufpool::DeviceSet<rdma::RdmaDevice>;

/// The TCP registration device for stream transports.
#[cfg(not(feature = "rdma"))]
pub type Devices = ruapc_bufpool::DeviceSet;
