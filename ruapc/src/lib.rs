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

pub mod services;

pub use ruapc_bufpool::DEFAULT_BUFFER_POOL_MEMORY;
pub use ruapc_bufpool::Device as _;
pub use ruapc_bufpool::Devices as _;

#[cfg(feature = "rdma")]
pub mod rdma;
