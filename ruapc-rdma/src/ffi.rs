use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::types::{pthread_cond_t, pthread_mutex_t};
use crate::{FwVer, Guid, LinkLayer, WRID};

include!(concat!(env!("OUT_DIR"), "/bindings.rs"));
