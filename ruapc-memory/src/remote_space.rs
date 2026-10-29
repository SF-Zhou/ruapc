use ruapc_bufpool::RemoteBufferInfo;

use crate::{MAX_REGIONS, MemoryError, MemoryErrorKind, MemoryResult, SpaceLayout};

/// A checked logical view of advertised remote memory regions.
///
/// Validating a region's address and length does not authorize access to its
/// memory or establish its lifetime; the transport owns those protocols.
///
/// The regions form one logical contiguous space (in region order, each
/// contributing its advertised length); [`CopyOp`](crate::CopyOp) offsets address this
/// space.
#[derive(Debug)]
pub struct RemoteSpace<'a> {
    regions: &'a [RemoteBufferInfo],
    layout: SpaceLayout,
}

impl<'a> RemoteSpace<'a> {
    /// Validates region address arithmetic, segment count and total length.
    pub fn new(regions: &'a [RemoteBufferInfo]) -> MemoryResult<Self> {
        if regions.len() > MAX_REGIONS {
            return Err(MemoryError::new(
                MemoryErrorKind::InvalidCopyOp,
                format!("remote space exceeds {MAX_REGIONS} regions"),
            ));
        }
        for region in regions {
            if region.addr.checked_add(region.len).is_none() {
                return Err(MemoryError::new(
                    MemoryErrorKind::InvalidCopyOp,
                    "remote region addr + len overflows u64".into(),
                ));
            }
        }
        let layout = SpaceLayout::from_lens(regions.iter().map(|r| r.len))?;
        Ok(Self { regions, layout })
    }

    /// Total length of the logical space in bytes.
    #[must_use]
    pub fn total_len(&self) -> u64 {
        self.layout.total()
    }

    /// The raw regions composing the space, in order.
    #[must_use]
    pub fn regions(&self) -> &'a [RemoteBufferInfo] {
        self.regions
    }

    /// Number of regions.
    #[must_use]
    pub fn region_count(&self) -> usize {
        self.regions.len()
    }

    /// Returns the checked logical layout for copy planning.
    #[must_use]
    pub fn layout(&self) -> &SpaceLayout {
        &self.layout
    }
}
