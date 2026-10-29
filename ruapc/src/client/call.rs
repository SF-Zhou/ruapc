//! Type-driven calls that preserve the declared service result.

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::client::ReadAttachment;
use crate::{BufferAvailability, BufferReply, Client, ClientWithBuffers, Context, Error};

/// Uniform request entry point used by generated client methods.
#[doc(hidden)]
pub trait RawCall {
    async fn ruapc_raw_call<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        slot: Option<&mut BufferAvailability>,
        method_name: &str,
    ) -> Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>;
}

impl RawCall for Client {
    async fn ruapc_raw_call<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        slot: Option<&mut BufferAvailability>,
        method_name: &str,
    ) -> Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
    {
        self.ruapc_request(
            ctx,
            req,
            ReadAttachment::new(None, None),
            &mut None,
            slot,
            method_name,
        )
        .await
    }
}

impl RawCall for ClientWithBuffers<'_> {
    async fn ruapc_raw_call<Req, Rsp, E>(
        &self,
        ctx: &Context,
        req: &Req,
        slot: Option<&mut BufferAvailability>,
        method_name: &str,
    ) -> Result<Rsp, E>
    where
        Req: Serialize + JsonSchema,
        Rsp: for<'c> Deserialize<'c> + JsonSchema,
        E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
    {
        self.ruapc_request(ctx, req, slot, method_name).await
    }
}

/// Executes a call returning exactly the declared service result.
///
/// Aliases and custom errors retain their meaning. `BufferReply` deliberately
/// does not implement `Deserialize`, so these two implementations cannot overlap.
#[doc(hidden)]
pub trait ResponseContract: Sized {
    async fn call<C, Req>(client: &C, ctx: &Context, req: &Req, method_name: &str) -> Self
    where
        C: RawCall,
        Req: Serialize + JsonSchema;
}

impl<T, E> ResponseContract for Result<T, E>
where
    T: for<'c> Deserialize<'c> + JsonSchema,
    E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
{
    async fn call<C, Req>(client: &C, ctx: &Context, req: &Req, method_name: &str) -> Self
    where
        C: RawCall,
        Req: Serialize + JsonSchema,
    {
        client.ruapc_raw_call(ctx, req, None, method_name).await
    }
}

impl<T, E> ResponseContract for Result<BufferReply<T>, E>
where
    T: for<'c> Deserialize<'c> + JsonSchema,
    E: std::error::Error + From<Error> + for<'c> Deserialize<'c>,
{
    async fn call<C, Req>(client: &C, ctx: &Context, req: &Req, method_name: &str) -> Self
    where
        C: RawCall,
        Req: Serialize + JsonSchema,
    {
        let mut destinations = BufferAvailability::NotAttached;
        let result: Result<T, E> = client
            .ruapc_raw_call(ctx, req, Some(&mut destinations), method_name)
            .await;
        result.map(|response| BufferReply::new(response, destinations))
    }
}
