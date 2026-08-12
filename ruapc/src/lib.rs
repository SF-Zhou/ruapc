#![deny(unsafe_code)]
// `SocketTrait` / `SocketPoolTrait` / the call-glue traits use `async fn`
// without `+ Send` in the trait declaration. That is deliberate: per the
// project's enum-dispatch design these traits are only consumed through the
// concrete `Socket` / `SocketPool` enums (never as generic bounds), so auto
// traits like `Send` leak structurally from the concrete impls and the
// lint's concern does not apply. `#[service]` traits, by contrast, ARE
// implemented by users and are desugared by the macro to
// `fn -> impl Future + Send`.
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
pub use core::{CallPlain, CallWithBuffer, RawCall, RpcCall, catch_handler_panic, spawn_handler};
pub use core::{
    Context, CopyOp, Listener, MAX_COPY_OPS, MAX_REGIONS, MethodInfo, RemoteSpace,
    ResultWithBuffers, Router, SentBuffers, Server, State, WithBuffers,
};

mod metrics;
pub(crate) use metrics::{MethodMetrics, Metrics};

mod task;
pub(crate) use task::Receiver;
pub use task::{TaskSupervisor, TaskSupervisorHandle, Waiter, WaiterCleaner};

mod devices;
pub use devices::{Buffer, BufferPool, Devices};

mod sockets;
pub use sockets::*;

pub mod services;

pub use ruapc_bufpool::Device as _;
pub use ruapc_bufpool::Devices as _;

#[cfg(feature = "rdma")]
mod rdma;
#[cfg(feature = "rdma")]
pub use rdma::{
    RdmaConnDirection, RdmaDeviceLoad, RdmaNicInfo, RdmaPathEntry, RdmaPathInfo, RdmaPathReport,
    StripePhase,
};
