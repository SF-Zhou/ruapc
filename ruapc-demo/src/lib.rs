use ruapc::{Context, Result, WithBuffers};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct Request(pub String);

#[ruapc::service]
pub trait EchoService {
    async fn echo(&self, c: &Context, r: &Request) -> Result<String>;
}

#[ruapc::service]
pub trait GreetService {
    async fn greet(&self, c: &Context, r: &Request) -> Result<String>;
}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct ReadCrcReq {}

#[derive(Serialize, Deserialize, Debug, Clone, JsonSchema)]
pub struct WriteCrcReq {
    /// Number of bytes the server should write into the client's buffers.
    pub len: usize,
}

/// Remote-memory benchmark service: exercises `remote_read` / `remote_write`
/// with CRC32C (CRC-32/iSCSI) end-to-end verification.
#[ruapc::service]
pub trait MemBenchService {
    /// Reads the client's attached read buffers and returns their CRC32C.
    async fn read_crc(&self, c: &Context, r: &ReadCrcReq) -> Result<u32>;

    /// Fills the client's pinned write buffers with `r.len` bytes of a
    /// varying pattern and returns the CRC32C of the written data.
    async fn write_crc(&self, c: &Context, r: &WriteCrcReq) -> Result<WithBuffers<u32>>;
}

/// Computes the CRC32C (CRC-32/iSCSI) of a logical contiguous space made of
/// multiple buffers.
pub fn crc32c_of(bufs: impl IntoIterator<Item = impl AsRef<[u8]>>) -> u32 {
    let mut digest = crc_fast::Digest::new(crc_fast::CrcAlgorithm::Crc32Iscsi);
    for buf in bufs {
        digest.update(buf.as_ref());
    }
    digest.finalize() as u32
}

/// Fills `buf` with a cheap deterministic pattern derived from `seed`.
pub fn fill_pattern(buf: &mut [u8], seed: u64) {
    let mut x = seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1;
    for chunk in buf.chunks_mut(8) {
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        chunk.copy_from_slice(&x.to_le_bytes()[..chunk.len()]);
    }
}
