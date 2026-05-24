use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::types::{pthread_cond_t, pthread_mutex_t};
use crate::{FwVer, Guid, LinkLayer, WRID};

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));

#[allow(clippy::derivable_impls)]
impl Default for ibv_transport_type {
    fn default() -> Self {
        ibv_transport_type::IBV_TRANSPORT_UNKNOWN
    }
}
