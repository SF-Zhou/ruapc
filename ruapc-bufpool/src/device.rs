use std::io::Result;

use crate::RegisteredMemory;

pub trait Device: Send + Sync + std::fmt::Debug {
    type Registration: Send + Sync + std::fmt::Debug;

    fn index(&self) -> usize;

    fn set_index(&mut self, idx: usize);

    fn register(&self, mem: &mut RegisteredMemory<Self::Registration>) -> Result<()>;
}

pub trait Devices: Send + Sync + std::fmt::Debug {
    type Device: Device;

    type Iter<'a>: Iterator<Item = &'a Self::Device>
    where
        Self: 'a;

    fn len(&self) -> usize;

    fn is_empty(&self) -> bool {
        self.len() == 0
    }

    fn iter(&self) -> Self::Iter<'_>;
}
