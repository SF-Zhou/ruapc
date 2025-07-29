use bytes::Buf;

#[derive(Debug, Clone, Default)]
pub enum Bytes {
    #[default]
    Empty,
    Normal(bytes::Bytes),
}

impl Bytes {
    pub fn len(&self) -> usize {
        match self {
            Bytes::Empty => 0,
            Bytes::Normal(bytes) => bytes.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Bytes::Empty => true,
            Bytes::Normal(bytes) => bytes.is_empty(),
        }
    }

    pub fn as_slice(&self) -> &[u8] {
        match self {
            Bytes::Empty => &[],
            Bytes::Normal(bytes) => bytes,
        }
    }

    pub fn advance(&mut self, offset: usize) {
        match self {
            Bytes::Empty => {}
            Bytes::Normal(bytes) => bytes.advance(offset),
        }
    }
}

impl std::ops::Deref for Bytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.as_slice()
    }
}

impl From<bytes::Bytes> for Bytes {
    fn from(value: bytes::Bytes) -> Self {
        Bytes::Normal(value)
    }
}
