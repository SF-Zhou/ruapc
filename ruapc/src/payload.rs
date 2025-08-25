use bytes::{Buf, Bytes, BytesMut};

#[derive(Debug, Default)]
pub enum Payload {
    #[default]
    Empty,
    Normal(Bytes),
    #[cfg(feature = "rdma")]
    RDMA(ruapc_rdma::Buffer, usize),
}

impl Payload {
    pub fn len(&self) -> usize {
        match self {
            Payload::Empty => 0,
            Payload::Normal(bytes) => bytes.len(),
            #[cfg(feature = "rdma")]
            Payload::RDMA(buffer, off) => buffer.len() - off,
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Payload::Empty => true,
            Payload::Normal(bytes) => bytes.is_empty(),
            #[cfg(feature = "rdma")]
            Payload::RDMA(buffer, off) => buffer.len() == *off,
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Payload::Empty => &[],
            Payload::Normal(bytes) => bytes,
            #[cfg(feature = "rdma")]
            Payload::RDMA(buffer, off) => &buffer[*off..],
        }
    }

    pub fn advance(&mut self, offset: usize) {
        match self {
            Payload::Empty => {}
            Payload::Normal(bytes) => bytes.advance(offset),
            #[cfg(feature = "rdma")]
            Payload::RDMA(_, off) => *off += offset,
        }
    }
}

impl std::ops::Deref for Payload {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl From<Bytes> for Payload {
    fn from(value: Bytes) -> Self {
        Payload::Normal(value)
    }
}

impl From<BytesMut> for Payload {
    fn from(value: BytesMut) -> Self {
        Payload::Normal(value.into())
    }
}

impl From<Payload> for Bytes {
    fn from(value: Payload) -> Self {
        match value {
            Payload::Empty => Bytes::new(),
            Payload::Normal(bytes) => bytes,
            #[cfg(feature = "rdma")]
            Payload::RDMA(buffer, off) => Bytes::copy_from_slice(&buffer[off..]),
        }
    }
}

#[cfg(feature = "rdma")]
impl From<ruapc_rdma::Buffer> for Payload {
    fn from(value: ruapc_rdma::Buffer) -> Self {
        Payload::RDMA(value, 0)
    }
}
