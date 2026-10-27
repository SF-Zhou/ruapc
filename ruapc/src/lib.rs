//! Typed, bidirectional RPC over TCP, WebSocket, HTTP and optional RDMA.
//!
//! Define an async trait with [`service`], implement it, and register the
//! implementation in a [`Router`]. A [`Server`] hosts that router; generated
//! methods on [`Client`] use a [`Context`] and [`Endpoint`] to reach it.
//!
//! ## Configuration
//!
//! [`SocketPoolConfig`] controls listeners, connection resources and handler
//! admission. TCP is the default listener; [`ListenMode::UNIFIED`] accepts
//! multiple protocols on one port. [`Client`] controls connection/response
//! budgets, retries and payload format. Payloads default to MessagePack;
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
//! logical [`CopyOp`] ranges. [`WithBuffers`] returns a response with available
//! write buffers; [`RemoteIoError`] describes recovery after a failed transfer.
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
pub use client::{CallPlain, CallWithBuffer, RawCall, RpcCall};
pub use core::{Context, Listener, MethodSchema, Router, Server, State};
#[doc(hidden)]
pub use core::{catch_handler_panic, spawn_handler};

mod remote_memory;
pub use remote_memory::{
    CopyOp, MAX_COPY_OPS, MAX_REGIONS, RemoteSpace, ResultWithBuffers, SentBuffers, WithBuffers,
};

mod metrics;
pub(crate) use metrics::Metrics;

mod task;
pub(crate) use task::Receiver;
pub use task::{TaskSupervisor, TaskSupervisorHandle, Waiter, WaiterCleaner};

mod devices;
pub use devices::{Buffer, BufferPool, Devices};

mod sockets;
pub use sockets::*;

mod services;

pub use ruapc_bufpool::DEFAULT_BUFFER_POOL_MEMORY;
pub use ruapc_bufpool::Device as _;
pub use ruapc_bufpool::Devices as _;

#[cfg(feature = "rdma")]
pub mod rdma;
