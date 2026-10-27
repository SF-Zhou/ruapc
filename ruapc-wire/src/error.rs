/// Failures in message encoding, decoding and stream framing.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CodecErrorKind {
    /// An offset, length or integer conversion is invalid.
    InvalidArgument,
    /// MessagePack encoding or a direct storage write failed.
    SerializeFailed,
    /// A message body or RDMA frame is malformed, or MessagePack decoding failed.
    DeserializeFailed,
    /// JSON encoding or decoding failed.
    Json,
    /// A stream frame has invalid magic bytes or exceeds the size limit.
    InvalidFrame,
}

/// A codec failure and its diagnostic message.
///
/// Transport runtimes map this error into their own error categories. It does
/// not describe connection state, request deadlines or transfer completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CodecError {
    /// Category of the codec failure.
    pub kind: CodecErrorKind,
    /// Diagnostic message, which may be empty.
    pub msg: String,
}

impl CodecError {
    /// Creates a codec failure with a diagnostic message.
    #[must_use]
    pub fn new(kind: CodecErrorKind, msg: String) -> Self {
        Self { kind, msg }
    }

    /// Creates a codec failure without allocating a diagnostic message.
    #[must_use]
    pub fn kind(kind: CodecErrorKind) -> Self {
        Self {
            kind,
            msg: String::new(),
        }
    }
}

impl std::error::Error for CodecError {}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.msg.is_empty() {
            write!(f, "{:?}", self.kind)
        } else {
            write!(f, "{:?}: {}", self.kind, self.msg)
        }
    }
}

impl From<CodecErrorKind> for CodecError {
    fn from(kind: CodecErrorKind) -> Self {
        Self::kind(kind)
    }
}

impl From<std::num::TryFromIntError> for CodecError {
    fn from(error: std::num::TryFromIntError) -> Self {
        Self::new(CodecErrorKind::InvalidArgument, error.to_string())
    }
}

impl From<serde_json::Error> for CodecError {
    fn from(error: serde_json::Error) -> Self {
        Self::new(CodecErrorKind::Json, error.to_string())
    }
}

impl From<rmp_serde::encode::Error> for CodecError {
    fn from(error: rmp_serde::encode::Error) -> Self {
        Self::new(CodecErrorKind::SerializeFailed, error.to_string())
    }
}

impl From<rmp_serde::decode::Error> for CodecError {
    fn from(error: rmp_serde::decode::Error) -> Self {
        Self::new(CodecErrorKind::DeserializeFailed, error.to_string())
    }
}

/// Result of an operation that can fail during encoding, decoding or framing.
pub type CodecResult<T> = std::result::Result<T, CodecError>;
