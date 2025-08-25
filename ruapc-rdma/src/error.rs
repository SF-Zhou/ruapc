use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    AllocMemoryFailed,
    IBGetDeviceListFail,
    IBDeviceNotFound,
    IBOpenDeviceFail,
    IBQueryDeviceFail,
    IBQueryGidFail,
    IBQueryGidTypeFail,
    IBQueryPortFail,
    IBAllocPDFail,
    IBCreateCompChannelFail,
    IBSetCompChannelNonBlockFail,
    IBGetCompQueueEventFail,
    IBCreateCompQueueFail,
    IBReqNotifyCompQueueFail,
    IBPollCompQueueFail,
    IBRegMemoryRegionFail,
    IBCreateQueuePairFail,
    IBModifyQueuePairFail,
    IBPostRecvFailed,
    IBPostSendFailed,
    IBSetNonBlockFailed,
    InsufficientBuffer,
    #[serde(untagged)]
    Unknown(String),
}

#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    pub msg: String,
}

impl ErrorKind {
    pub fn with_errno(self) -> Error {
        Error::new(self, std::io::Error::last_os_error().to_string())
    }
}

impl Error {
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
