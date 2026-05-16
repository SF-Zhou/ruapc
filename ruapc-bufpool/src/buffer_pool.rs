use std::collections::VecDeque;
use std::io::{Error, Result};
use std::ptr::NonNull;
use std::sync::{Arc, Mutex};

use aliasable::boxed::AliasableBox;
use tokio::sync::oneshot;

use crate::{Buffer, Devices, RegisteredMemory};

pub struct BufferPool {
    devices: Arc<dyn Devices>,
    inner: Mutex<PoolInner>,
    block_size: usize,
    chunk_size: usize,
    max_memory: usize,
}

struct PoolInner {
    memories: Vec<AliasableBox<RegisteredMemory>>,
    free_list: VecDeque<FreeSlot>,
    allocated_memory: usize,
    waiters: VecDeque<oneshot::Sender<()>>,
}

#[derive(Debug, Clone, Copy)]
struct FreeSlot {
    memory_index: usize,
    block_index: usize,
}

impl BufferPool {
    pub fn new(
        devices: Arc<dyn Devices>,
        block_size: usize,
        chunk_size: usize,
        max_memory: usize,
    ) -> Arc<Self> {
        assert!(block_size > 0, "block_size must be > 0");
        assert!(chunk_size >= block_size, "chunk_size must be >= block_size");
        assert!(
            chunk_size.is_multiple_of(block_size),
            "chunk_size must be a multiple of block_size"
        );
        Arc::new(Self {
            devices,
            inner: Mutex::new(PoolInner {
                memories: Vec::new(),
                free_list: VecDeque::new(),
                allocated_memory: 0,
                waiters: VecDeque::new(),
            }),
            block_size,
            chunk_size,
            max_memory,
        })
    }

    pub fn allocate(self: &Arc<Self>) -> Result<Buffer> {
        let mut inner = self.inner.lock().unwrap();
        self.allocate_inner(&mut inner)
    }

    pub async fn async_allocate(self: &Arc<Self>) -> Result<Buffer> {
        loop {
            let rx = {
                let mut inner = self.inner.lock().unwrap();
                match self.allocate_inner(&mut inner) {
                    Ok(buf) => return Ok(buf),
                    Err(_) => {
                        let (tx, rx) = oneshot::channel();
                        inner.waiters.push_back(tx);
                        rx
                    }
                }
            };
            let _ = rx.await;
        }
    }

    fn allocate_inner(self: &Arc<Self>, inner: &mut PoolInner) -> Result<Buffer> {
        if let Some(slot) = inner.free_list.pop_front() {
            return self.make_buffer(inner, slot);
        }

        if self.max_memory == 0 || inner.allocated_memory + self.chunk_size <= self.max_memory {
            self.allocate_chunk(inner)?;
            let slot = inner
                .free_list
                .pop_front()
                .expect("allocate_chunk pushed no slots");
            return self.make_buffer(inner, slot);
        }

        Err(Error::other("buffer pool exhausted"))
    }

    fn allocate_chunk(&self, inner: &mut PoolInner) -> Result<()> {
        let mem = RegisteredMemory::new(self.chunk_size, &*self.devices)?;
        let memory_index = inner.memories.len();
        let blocks_per_chunk = self.chunk_size / self.block_size;

        for block_index in 0..blocks_per_chunk {
            inner.free_list.push_back(FreeSlot {
                memory_index,
                block_index,
            });
        }

        inner.allocated_memory += mem.aligned_memory().size();
        inner
            .memories
            .push(AliasableBox::from_unique(Box::new(mem)));
        Ok(())
    }

    fn make_buffer(self: &Arc<Self>, inner: &PoolInner, slot: FreeSlot) -> Result<Buffer> {
        let mem: &RegisteredMemory = &inner.memories[slot.memory_index];
        let base = mem.aligned_memory().as_ptr();
        let offset = slot.block_index * self.block_size;
        let ptr = unsafe { NonNull::new_unchecked(base.add(offset) as *mut u8) };
        let memory = NonNull::from(mem);
        Ok(Buffer::new(
            Arc::clone(self),
            ptr,
            self.block_size,
            memory,
            slot.memory_index,
            slot.block_index,
        ))
    }

    pub(crate) fn return_buffer(&self, memory_index: usize, block_index: usize) {
        let mut inner = self.inner.lock().unwrap();
        inner.free_list.push_back(FreeSlot {
            memory_index,
            block_index,
        });
        if let Some(tx) = inner.waiters.pop_front() {
            let _ = tx.send(());
        }
    }

    pub fn allocated_memory(&self) -> usize {
        self.inner.lock().unwrap().allocated_memory
    }
    pub fn free_count(&self) -> usize {
        self.inner.lock().unwrap().free_list.len()
    }
    pub fn block_size(&self) -> usize {
        self.block_size
    }
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
    pub fn devices(&self) -> &Arc<dyn Devices> {
        &self.devices
    }
}

impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let inner = self.inner.lock().unwrap();
        f.debug_struct("BufferPool")
            .field("block_size", &self.block_size)
            .field("chunk_size", &self.chunk_size)
            .field("max_memory", &self.max_memory)
            .field("allocated_memory", &inner.allocated_memory)
            .field("free_count", &inner.free_list.len())
            .field("chunks", &inner.memories.len())
            .finish()
    }
}
