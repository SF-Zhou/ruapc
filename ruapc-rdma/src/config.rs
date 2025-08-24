use std::collections::HashSet;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
pub enum GidType {
    IB,
    RoCEv1,
    RoCEv2,
    Other(String),
}

#[derive(Debug, Default)]
pub struct Config {
    pub device: DeviceConfig,
}

#[derive(Debug, Default)]
pub struct DeviceConfig {
    pub device_filter: HashSet<String>,
    pub gid_type_filter: HashSet<GidType>,
    pub skip_inactive_port: bool,
    pub roce_v2_skip_link_local_addr: bool,
}
