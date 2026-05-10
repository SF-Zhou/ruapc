use std::io::Result;
use std::sync::Arc;

use crate::{AlignedMemory, RegisteredMemory};

pub trait Registration: Send + Sync + std::fmt::Debug {
    fn unregister(&self, buf: &AlignedMemory);
}

pub trait Device: Send + Sync + std::fmt::Debug {
    type Registration: Registration;

    fn index(&self) -> usize;

    fn set_index(&mut self, idx: usize);

    fn register(self: &Arc<Self>, mem: &mut RegisteredMemory<Self::Registration>) -> Result<()>;
}

pub trait Devices: Send + Sync + std::fmt::Debug {
    type Device: Device;

    type Iter<'a>: Iterator<Item = &'a Arc<Self::Device>>
    where
        Self: 'a;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> Self::Iter<'_>;
}
