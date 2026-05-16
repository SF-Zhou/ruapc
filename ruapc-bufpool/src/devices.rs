use std::io::Result;
use std::sync::Arc;

use crate::{AlignedMemory, Registration};

pub trait Devices: Send + Sync + std::fmt::Debug {
    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn register(&self, mem: &Arc<AlignedMemory>) -> Result<Vec<Box<dyn Registration>>>;
}
