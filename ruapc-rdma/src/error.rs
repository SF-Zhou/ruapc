use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

/// Error kinds for RDMA operations.
///
/// This enum categorizes errors that can occur during RDMA operations,
/// including device initialization, queue pair management, and data transfer.
#[derive(Serialize, Deserialize, JsonSchema, Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    /// Memory allocation failed.
    AllocMemoryFailed,
    /// Failed to get InfiniBand device list.
    IBGetDeviceListFail,
    /// No InfiniBand device found.
    IBDeviceNotFound,
    /// Failed to open InfiniBand device.
    IBOpenDeviceFail,
    /// Failed to query InfiniBand device attributes.
    IBQueryDeviceFail,
    /// Failed to query Global Identifier (GID).
    IBQueryGidFail,
    /// Failed to query GID type.
    IBQueryGidTypeFail,
    /// Failed to query port attributes.
    IBQueryPortFail,
    /// Failed to allocate Protection Domain.
    IBAllocPDFail,
    /// Failed to create completion channel.
    IBCreateCompChannelFail,
    /// Failed to set completion channel to non-blocking mode.
    IBSetCompChannelNonBlockFail,
    /// Failed to get completion queue event.
    IBGetCompQueueEventFail,
    /// Failed to create completion queue.
    IBCreateCompQueueFail,
    /// Failed to request notification on completion queue.
    IBReqNotifyCompQueueFail,
    /// Failed to poll completion queue.
    IBPollCompQueueFail,
    /// Failed to register memory region.
    IBRegMemoryRegionFail,
    /// Failed to create queue pair.
    IBCreateQueuePairFail,
    /// Failed to modify queue pair state.
    IBModifyQueuePairFail,
    /// Failed to post receive work request.
    IBPostRecvFailed,
    /// Failed to post send work request.
    IBPostSendFailed,
    /// Failed to set non-blocking mode.
    IBSetNonBlockFailed,
    /// Buffer size insufficient for operation.
    InsufficientBuffer,
    /// Unknown or unclassified error with a custom message.
    #[serde(untagged)]
    Unknown(String),
}

/// RDMA error type containing error kind and optional message.
///
/// This is the primary error type used throughout the ruapc-rdma library.
/// It combines an error kind for categorization with an optional message
/// for additional context.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Error {
    /// The category of error that occurred.
    pub kind: ErrorKind,
    /// Additional error message providing context.
    pub msg: String,
}

impl ErrorKind {
    /// Creates an error with the current OS error as the message.
    ///
    /// # Returns
    ///
    /// Returns an `Error` with this kind and the OS error message.
    pub fn with_errno(self) -> Error {
        Error::new(self, std::io::Error::last_os_error().to_string())
    }
}

impl Error {
    /// Creates a new error with the specified kind and message.
    ///
    /// # Arguments
    ///
    /// * `kind` - The error category
    /// * `msg` - Additional error message
    ///
    /// # Returns
    ///
    /// Returns a new `Error` instance.
    pub fn new(kind: ErrorKind, msg: String) -> Self {
        Self { kind, msg }
    }
}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self {
            kind,
            msg: String::new(),
        }
    }
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.msg.is_empty() {
            write!(f, "{:?}", self.kind)
        } else {
            write!(f, "{:?}: {}", self.kind, self.msg)
        }
    }
}

impl std::error::Error for Error {}

/// Result type alias using [`Error`] as the error type.
///
/// This is a convenience type alias used throughout the ruapc-rdma library.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error() {
        let err = Error::new(
            ErrorKind::IBGetDeviceListFail,
            "Failed to get device list".to_string(),
        );
        let json = serde_json::to_value(err).unwrap();
        assert_eq!(
            json,
            serde_json::json!({
                "kind": "IBGetDeviceListFail",
                "msg": "Failed to get device list"
            })
        );

        let json = serde_json::json!({
            "kind": "NewKindError",
            "msg": "new kind error message",
        });
        let err = serde_json::from_value::<Error>(json).unwrap();
        assert_eq!(
            err,
            Error {
                kind: ErrorKind::Unknown("NewKindError".to_string()),
                msg: "new kind error message".to_string()
            }
        );

        let err: Error = ErrorKind::IBGetDeviceListFail.into();
        assert_eq!(err.to_string(), "IBGetDeviceListFail");
    }
}
