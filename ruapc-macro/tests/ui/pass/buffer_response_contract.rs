//! Handlers and clients share the declared trait and exact aliased result types.

use std::sync::Arc;

use ruapc::{BufferReply, Client, ClientWithBuffers, Context, Error};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize, Serialize, JsonSchema)]
struct CustomError(Error);

impl From<Error> for CustomError {
    fn from(error: Error) -> Self {
        Self(error)
    }
}

impl std::fmt::Display for CustomError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(formatter)
    }
}

impl std::error::Error for CustomError {}

type AliasedReply = ruapc::ResultBufferReply<u32, CustomError>;
type PlainReply = Result<u32, CustomError>;

#[ruapc::service]
trait Download {
    async fn download(&self, ctx: &Context, req: &()) -> AliasedReply;
    async fn plain(&self, ctx: &Context, req: &()) -> PlainReply;
}

struct Handler;

impl Download for Handler {
    async fn download(&self, ctx: &Context, _req: &()) -> AliasedReply {
        Ok(ctx.sent_nothing().into_reply(7))
    }

    async fn plain(&self, _ctx: &Context, _req: &()) -> PlainReply {
        Ok(7)
    }
}

// A forwarding handler can return the downstream client's result unchanged.
#[allow(dead_code)]
struct Forwarder(Client);

impl Download for Forwarder {
    async fn download(&self, ctx: &Context, req: &()) -> AliasedReply {
        self.0.download(ctx, req).await
    }

    async fn plain(&self, ctx: &Context, req: &()) -> PlainReply {
        self.0.plain(ctx, req).await
    }
}

async fn download<S: Download>(service: &S, ctx: &Context) -> AliasedReply {
    service.download(ctx, &()).await
}

#[allow(dead_code)]
async fn shared_contract(client: &Client, attached: &ClientWithBuffers<'_>, ctx: &Context) {
    // The same generic call works for local handlers and both remote clients.
    let _: AliasedReply = download(&Handler, ctx).await;
    let _: AliasedReply = download(client, ctx).await;
    let _: AliasedReply = download(attached, ctx).await;

    // Direct and qualified client calls also keep the exact declared type.
    let _: Result<BufferReply<u32>, CustomError> = client.download(ctx, &()).await;
    let _: AliasedReply = Download::download(attached, ctx, &()).await;
    let _: PlainReply = client.plain(ctx, &()).await;
    let _: PlainReply = attached.plain(ctx, &()).await;
}

fn main() {
    assert_eq!(<Client as Download>::NAME, <Handler as Download>::NAME);
    assert_eq!(
        <ClientWithBuffers<'_> as Download>::NAME,
        <Handler as Download>::NAME,
    );
    let mut router = ruapc::Router::default();
    Arc::new(Handler).ruapc_export(&mut router);
}
