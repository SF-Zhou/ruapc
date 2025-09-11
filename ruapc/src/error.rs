use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, JsonSchema, Clone, Debug, PartialEq, Eq)]
pub enum ErrorKind {
    Timeout,
    InvalidArgument,
    SerializeFailed,
    DeserializeFailed,
    SerdeJsonError,
    TcpConnectFailed,
    TcpBindFailed,
    TcpSendMsgFailed,
    TcpRecvMsgFailed,
    TcpParseMsgFailed,
    WebSocketConnectFailed,
    WebSocketAcceptFailed,
    WebSocketSendFailed,
    WebSocketRecvFailed,
    WebSocketClosed,
    HttpWaitRspFailed,
    HttpBuildReqFailed,
    HttpSendReqFailed,
    HttpUpgradeFailed,
    RdmaSendFailed,
    RdmaRecvFailed,
    #[cfg(feature = "rdma")]
    RdmaError(ruapc_rdma::ErrorKind),
    #[serde(untagged)]
    Unknown(String),
}

#[derive(Serialize, Deserialize, JsonSchema, Clone, Debug, PartialEq, Eq)]
pub struct Error {
    pub kind: ErrorKind,
    pub msg: String,
}

impl Error {
    #[must_use]
    pub fn new(kind: ErrorKind, msg: String) -> Self {
        Self { kind, msg }
    }

    #[must_use]
    pub fn kind(kind: ErrorKind) -> Self {
        Self {
            kind,
            msg: String::default(),
        }
    }
}

impl std::error::Error for Error {}

impl From<ErrorKind> for Error {
    fn from(kind: ErrorKind) -> Self {
        Self::kind(kind)
    }
}

#[cfg(feature = "rdma")]
impl From<ruapc_rdma::Error> for Error {
    fn from(value: ruapc_rdma::Error) -> Self {
        Self::new(ErrorKind::RdmaError(value.kind), value.msg)
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(value: std::num::TryFromIntError) -> Self {
        Self {
            kind: ErrorKind::InvalidArgument,
            msg: value.to_string(),
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(value: serde_json::Error) -> Self {
        Self {
            kind: ErrorKind::SerdeJsonError,
            msg: value.to_string(),
        }
    }
}

impl From<rmp_serde::encode::Error> for Error {
    fn from(value: rmp_serde::encode::Error) -> Self {
        Self {
            kind: ErrorKind::SerializeFailed,
            msg: value.to_string(),
        }
    }
}

impl From<rmp_serde::decode::Error> for Error {
    fn from(value: rmp_serde::decode::Error) -> Self {
        Self {
            kind: ErrorKind::DeserializeFailed,
            msg: value.to_string(),
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

pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_error_kind() {
        let kind = ErrorKind::Timeout;
        let error: Error = kind.into();
        assert_eq!(error.to_string(), "Timeout");

        let error: Error = Error::new(ErrorKind::TcpConnectFailed, "connection refused".into());
        assert_eq!(error.to_string(), "TcpConnectFailed: connection refused");

        let error: Error = serde_json::from_str::<serde_json::Value>("{")
            .unwrap_err()
            .into();
        assert_eq!(error.kind, ErrorKind::SerdeJsonError);
    }

    #[test]
    fn test_error_new() {
        let error = Error::new(ErrorKind::Timeout, "operation timeout".to_string());
        assert_eq!(error.kind, ErrorKind::Timeout);
        assert_eq!(error.msg, "operation timeout");
    }

    #[test]
    fn test_error_kind_constructor() {
        let error = Error::kind(ErrorKind::InvalidArgument);
        assert_eq!(error.kind, ErrorKind::InvalidArgument);
        assert_eq!(error.msg, String::default());
    }

    #[test]
    fn test_error_display() {
        // Test display with empty message
        let error = Error::kind(ErrorKind::Timeout);
        assert_eq!(format!("{}", error), "Timeout");

        // Test display with message
        let error = Error::new(
            ErrorKind::TcpConnectFailed,
            "connection refused".to_string(),
        );
        assert_eq!(format!("{}", error), "TcpConnectFailed: connection refused");
    }

    #[test]
    fn test_from_try_from_int_error() {
        let value: std::result::Result<u8, _> = (-1i32).try_into();
        let int_error = value.unwrap_err();
        let error: Error = int_error.into();

        assert_eq!(error.kind, ErrorKind::InvalidArgument);
        assert!(!error.msg.is_empty());
    }

    #[test]
    fn test_from_serde_json_error() {
        let json_error = serde_json::from_str::<serde_json::Value>("invalid json").unwrap_err();
        let error: Error = json_error.into();

        assert_eq!(error.kind, ErrorKind::SerdeJsonError);
        assert!(!error.msg.is_empty());
    }

    #[test]
    fn test_from_rmp_serde_encode_error() {
        let e: Error = rmp_serde::encode::Error::UnknownLength.into();
        assert_eq!(e.kind, ErrorKind::SerializeFailed);
    }

    #[test]
    fn test_from_rmp_serde_decode_error() {
        // Try to decode invalid MessagePack data
        let invalid_data = vec![0xFF, 0xFF, 0xFF];
        let decode_error = rmp_serde::from_slice::<String>(&invalid_data).unwrap_err();
        let error: Error = decode_error.into();

        assert_eq!(error.kind, ErrorKind::DeserializeFailed);
        assert!(!error.msg.is_empty());
    }
}
