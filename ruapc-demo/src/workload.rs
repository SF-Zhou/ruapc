//! RPC operations used by both the one-shot client and its load generators.
//!
//! Each worker owns one operation. Buffers are prepared before timing starts
//! and reused across calls. Read sources remain immutable in their owning
//! client wrapper; failed writes only recycle buffers when RuaPC has released
//! their ownership.

use ruapc::{Buffer, Client, ClientWithBuffers, Context, Error, ErrorKind, Result};

use crate::{
    EchoService, MemBenchService, ReadCrcReq, Request, WriteCrcReq, crc32c_of, fill_pattern,
};

#[derive(clap::ValueEnum, Debug, Clone, Copy, PartialEq, Eq)]
pub enum Rpc {
    /// Plain payload echo.
    Echo,
    /// Server reads the client's buffers and returns their CRC32C.
    Read,
    /// Server writes into the client's buffers and returns their CRC32C.
    Write,
}

pub struct Workload<'a> {
    client: &'a Client,
    operation: Operation<'a>,
}

enum Operation<'a> {
    Echo(Request),
    Read {
        source: ClientWithBuffers<'a>,
        expected_crc: u32,
    },
    Write {
        buffers: Option<Vec<Buffer>>,
        len: usize,
    },
}

impl<'a> Workload<'a> {
    pub async fn create(
        client: &'a Client,
        rpc: Rpc,
        ctx: &Context,
        payload: Request,
        buffer_size: usize,
        seed: u64,
    ) -> Result<Self> {
        let operation = match rpc {
            Rpc::Echo => Operation::Echo(payload),
            Rpc::Read => {
                let mut buffer = allocate(ctx, buffer_size).await?;
                fill_pattern(&mut buffer, seed);
                let expected_crc = crc32c_of([&buffer]);
                Operation::Read {
                    source: client.with_read_buffer(buffer),
                    expected_crc,
                }
            }
            Rpc::Write => Operation::Write {
                buffers: Some(vec![allocate(ctx, buffer_size).await?]),
                len: buffer_size,
            },
        };
        Ok(Self { client, operation })
    }

    #[inline]
    pub async fn call(&mut self, ctx: &Context) -> Result<()> {
        match &mut self.operation {
            Operation::Echo(payload) => {
                self.client.echo(ctx, payload).await?;
                Ok(())
            }
            Operation::Read {
                source,
                expected_crc,
            } => {
                let crc = source.read_crc(ctx, &ReadCrcReq {}).await?;
                verify_crc(crc, *expected_crc)
            }
            Operation::Write { buffers, len } => {
                let destination = match buffers.take() {
                    Some(buffers) => buffers,
                    None => vec![allocate(ctx, *len).await?],
                };
                let call = self.client.with_write_buffers(destination);
                match call.write_crc(ctx, &WriteCrcReq { len: *len }).await {
                    Ok(response) => {
                        let (expected_crc, returned) = response.into_parts();
                        let crc = crc32c_of(&returned);
                        *buffers = Some(returned);
                        verify_crc(crc, expected_crc)
                    }
                    Err(error) => {
                        // An in-flight transfer may retain its buffers after
                        // timeout. Allocate replacements on the next call if
                        // they are not yet safe to reclaim.
                        *buffers = call.take_write_buffers();
                        Err(error)
                    }
                }
            }
        }
    }
}

async fn allocate(ctx: &Context, len: usize) -> Result<Buffer> {
    let mut buffer = ctx
        .state
        .buffer_pool
        .async_allocate(len.max(1))
        .await
        .map_err(|error| Error::new(ErrorKind::InvalidArgument, error.to_string()))?;
    buffer.set_len(len);
    Ok(buffer)
}

#[inline]
fn verify_crc(actual: u32, expected: u32) -> Result<()> {
    if actual != expected {
        return Err(Error::new(
            ErrorKind::InvalidArgument,
            format!("crc32c mismatch: got {actual:#010x}, expect {expected:#010x}"),
        ));
    }
    Ok(())
}
