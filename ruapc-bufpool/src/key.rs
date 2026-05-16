#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct MemoryKey {
    pub lkey: u32,
    pub rkey: u32,
}

#[derive(
    Debug,
    Default,
    Clone,
    Copy,
    PartialEq,
    Eq,
    serde::Serialize,
    serde::Deserialize,
    schemars::JsonSchema,
)]
pub struct RemoteBufferInfo {
    pub key: MemoryKey,
    pub addr: u64,
    pub len: u64,
}
