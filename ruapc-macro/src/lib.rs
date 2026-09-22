//! RPC service declarations, router registration, and client implementations.
//!
//! `#[service]` generates `ruapc_export` and implements the service trait for
//! `ruapc::Client` and `ruapc::ClientWithBuffers`.
//!
//! ```rust,ignore
//! #[ruapc::service]
//! pub trait MyService {
//!     async fn my_method(&self, ctx: &Context, req: &Request) -> Result<Response>;
//! }
//! ```
//!
//! ## Names and visibility
//!
//! The wire name defaults to the Rust trait name. `name = "..."` overrides it;
//! `internal` excludes the service from unary HTTP, reflection, and OpenAPI
//! while allowing framed peer RPCs:
//!
//! ```rust,ignore
//! #[ruapc::service(name = "Control", internal)]
//! pub trait ControlService {
//!     async fn ping(&self, ctx: &Context, req: &()) -> Result<()>;
//! }
//! ```
//!
//! Names must be nonempty, have no leading or trailing whitespace, and exclude
//! `/`, the separator in wire method names such as `Control/ping`.
//!
//! ## Method contract
//!
//! Traits contain only async method declarations with `&self`, `&Context`,
//! and `&Request` arguments and an explicit result type. Generic parameters,
//! where clauses, default bodies, and unsafe traits or methods are unsupported.
//! Return types may be `Result<T, E>` or aliases, including `ruapc::Result<T>`.
//! The generated call traits enforce serialization, schema, and error bounds.
//!
//! Methods become `fn -> impl Future<Output = ...> + Send`. Implementations
//! may still use `async fn`; the compiler checks that their futures are `Send`.
//!
//! ## Responses with buffers
//!
//! `Result<WithBuffers<T>, E>` returns a response plus recovered client write
//! buffers. Type aliases such as `ruapc::ResultWithBuffers<T>` work because
//! dispatch uses the underlying type, not its spelling.
//!
//! The handler awaits `ctx.remote_write` or `ctx.remote_write_all`, then calls
//! `SentBuffers::reply` to form its response. `ctx.sent_nothing().reply(...)`
//! handles paths with no transfer. The client attaches destination buffers:
//!
//! ```rust,ignore
//! let (rsp, buffers) = client
//!     .with_write_buffers(destinations)
//!     .download(&ctx, &req)
//!     .await?
//!     .into_parts();
//! ```
//!
//! Recovery requires an available, uniquely held target. See
//! `ruapc::ClientWithBuffers` for cancellation and in-flight DMA behavior.

mod args;
mod expand;
mod model;

use proc_macro::TokenStream;
use syn::parse_macro_input;

/// Define an RPC service and its router registration and client implementations.
///
/// `name = "..."` overrides the wire name; `internal` hides the service from
/// public discovery and OpenAPI output. Methods must be declarations of the form
/// `async fn method(&self, ctx: &Context, req: &Request) -> ResponseResult`.
/// The return type may be an alias; RuaPC's call traits enforce its contract.
/// Unsupported items and signatures produce diagnostics at their source spans.
#[proc_macro_attribute]
pub fn service(attr: TokenStream, input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(attr as args::ServiceArgs);
    let declaration = parse_macro_input!(input as syn::ItemTrait);

    match model::Service::parse(args, declaration).and_then(expand::service) {
        Ok(tokens) => tokens.into(),
        Err(error) => error.into_compile_error().into(),
    }
}
