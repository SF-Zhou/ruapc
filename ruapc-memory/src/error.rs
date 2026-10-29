/// Failures in logical memory validation and copy planning.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum MemoryErrorKind {
    /// A copy operation or logical memory space is invalid.
    InvalidCopyOp,
    /// An integer conversion or planner argument is invalid.
    InvalidArgument,
}

/// A memory operation failure and its diagnostic message.
///
/// Runtimes map this error into their own error categories. It does not
/// describe connection state, request deadlines or transfer completion.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MemoryError {
    /// Category of the memory operation failure.
    pub kind: MemoryErrorKind,
    /// Diagnostic message, which may be empty.
    pub msg: String,
}

impl MemoryError {
    /// Creates a memory operation failure with a diagnostic message.
    #[must_use]
    pub fn new(kind: MemoryErrorKind, msg: String) -> Self {
        Self { kind, msg }
    }

    /// Creates a memory operation failure without allocating a diagnostic message.
    #[must_use]
    pub fn kind(kind: MemoryErrorKind) -> Self {
        Self {
            kind,
            msg: String::new(),
        }
    }
}

impl std::error::Error for MemoryError {}

impl std::fmt::Display for MemoryError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.msg.is_empty() {
            write!(f, "{:?}", self.kind)
        } else {
            write!(f, "{:?}: {}", self.kind, self.msg)
        }
    }
}

impl From<MemoryErrorKind> for MemoryError {
    fn from(kind: MemoryErrorKind) -> Self {
        Self::kind(kind)
    }
}

impl From<std::num::TryFromIntError> for MemoryError {
    fn from(error: std::num::TryFromIntError) -> Self {
        Self::new(MemoryErrorKind::InvalidArgument, error.to_string())
    }
}

/// Result of a checked logical memory operation.
pub type MemoryResult<T> = std::result::Result<T, MemoryError>;
