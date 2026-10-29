use std::sync::Arc;

use ruapc_bufpool::{Buffer, BufferPool, EmptyDevices, RemoteBufferInfo};
use ruapc_memory::{
    BufferSet, CopyBatch, CopyOp, MAX_COPY_OPS, MAX_REGIONS, MemoryError, MemoryErrorKind,
    ReadSource, RemoteSpace, SpaceLayout,
};

fn buffers(parts: &[&[u8]]) -> Vec<Buffer> {
    let pool = BufferPool::new(Arc::new(EmptyDevices));
    parts
        .iter()
        .map(|part| {
            let mut buffer = pool.allocate(part.len().max(1)).unwrap();
            buffer.set_len(part.len());
            buffer.copy_from_slice(part);
            buffer
        })
        .collect()
}

fn flatten(buffers: &[Buffer]) -> Vec<u8> {
    buffers
        .iter()
        .flat_map(|buffer| buffer.as_slice().iter().copied())
        .collect()
}

#[test]
fn invalid_slice_ranges_are_rejected_before_callback() {
    let layout = SpaceLayout::from_lens([4, 0, 4]).unwrap();
    for (offset, len) in [(8, 1), (7, 2), (u64::MAX, 1), (1, u64::MAX)] {
        let mut called = false;
        let error = layout
            .for_each_slice::<MemoryError>(offset, len, |_, _, _| {
                called = true;
                Ok(())
            })
            .unwrap_err();
        assert_eq!(error.kind, MemoryErrorKind::InvalidCopyOp);
        assert!(!called, "invalid range must not partially execute");
    }
    SpaceLayout::from_lens([])
        .unwrap()
        .for_each_slice::<MemoryError>(u64::MAX, 0, |_, _, _| panic!("empty range has no slices"))
        .unwrap();
}

#[test]
fn slice_callback_failure_stops_iteration_and_preserves_error() {
    #[derive(Debug, PartialEq, Eq)]
    enum CopyFailure {
        Validation(MemoryError),
        SinkClosed,
    }
    impl From<MemoryError> for CopyFailure {
        fn from(error: MemoryError) -> Self {
            Self::Validation(error)
        }
    }
    let layout = SpaceLayout::from_lens([4, 4]).unwrap();
    let mut visited = Vec::new();
    let error = layout
        .for_each_slice(0, 8, |segment, _, _| {
            visited.push(segment);
            Err(CopyFailure::SinkClosed)
        })
        .unwrap_err();
    assert_eq!(error, CopyFailure::SinkClosed);
    assert_eq!(visited, [0]);
}

#[test]
fn structural_validation_and_actual_space_checks_are_separate() {
    let operations = [CopyOp::new(6, 20, 2), CopyOp::new(10, 0, 4)];
    let batch = CopyBatch::new(&operations).unwrap();
    assert_eq!(batch.byte_count(), 6);
    assert_eq!(batch.ops().as_ptr(), operations.as_ptr());
    batch.check_bounds(14, 22).unwrap();
    batch.check_bounds(100, 100).unwrap();
    assert!(batch.check_source(13).is_err());
    assert!(batch.check_destination(21).is_err());
    assert!(batch.check_bounds(13, 22).is_err());
    assert!(batch.check_bounds(14, 21).is_err());

    // Packed CPU copies need only the actually accessed side's space length.
    let source_ops = [CopyOp::new(0, 1_000_000, 4)];
    let source_batch = CopyBatch::new(&source_ops).unwrap();
    assert_eq!(source_batch.gather(&buffers(&[b"abcd"])).unwrap(), b"abcd");
    let destination_ops = [CopyOp::new(1_000_000, 0, 4)];
    let destination_batch = CopyBatch::new(&destination_ops).unwrap();
    let mut destination = buffers(&[b"...."]);
    destination_batch
        .scatter(b"data", &mut destination)
        .unwrap();
    assert_eq!(flatten(&destination), b"data");
}

#[test]
fn malformed_operations_are_rejected_without_a_space() {
    let invalid = [
        vec![CopyOp::new(u64::MAX, 0, 1)],
        vec![CopyOp::new(0, u64::MAX, 1)],
        vec![CopyOp::new(0, 0, 4), CopyOp::new(4, 2, 4)],
        vec![CopyOp::new(0, 0, u64::MAX), CopyOp::new(0, 0, 1)],
        vec![CopyOp::new(0, 0, 0); MAX_COPY_OPS + 1],
    ];
    for operations in invalid {
        assert_eq!(
            CopyBatch::new(&operations).unwrap_err().kind,
            MemoryErrorKind::InvalidCopyOp
        );
    }
}

#[test]
fn planner_checks_actual_bounds_and_nonzero_scatter_limit() {
    let operations = [CopyOp::new(1, 2, 6)];
    let batch = CopyBatch::new(&operations).unwrap();
    let source = SpaceLayout::from_lens([4, 4]).unwrap();
    let destination = SpaceLayout::from_lens([3, 5]).unwrap();
    let small = SpaceLayout::from_lens([6]).unwrap();
    assert_eq!(
        batch.plan_chunks(&small, &destination, 2).unwrap_err().kind,
        MemoryErrorKind::InvalidCopyOp
    );
    assert_eq!(
        batch.plan_chunks(&source, &small, 2).unwrap_err().kind,
        MemoryErrorKind::InvalidCopyOp
    );
    assert_eq!(
        batch
            .plan_chunks(&source, &destination, 0)
            .unwrap_err()
            .kind,
        MemoryErrorKind::InvalidArgument
    );
    let chunks = batch.plan_chunks(&source, &destination, 2).unwrap();
    assert_eq!(chunks.iter().map(|chunk| chunk.len).sum::<u64>(), 6);
    assert!(chunks.iter().all(|chunk| chunk.dst.len() <= 2));
}

#[test]
fn remote_space_rejects_invalid_addresses_lengths_and_region_counts() {
    let invalid = [
        vec![RemoteBufferInfo {
            addr: u64::MAX,
            len: 1,
            ..Default::default()
        }],
        vec![
            RemoteBufferInfo {
                len: u64::MAX,
                ..Default::default()
            },
            RemoteBufferInfo {
                len: 1,
                ..Default::default()
            },
        ],
        vec![RemoteBufferInfo::default(); MAX_REGIONS + 1],
    ];
    for regions in invalid {
        assert_eq!(
            RemoteSpace::new(&regions).unwrap_err().kind,
            MemoryErrorKind::InvalidCopyOp
        );
    }
    let oversized = vec![
        RemoteBufferInfo {
            addr: u64::MAX,
            len: 1,
            ..Default::default()
        };
        MAX_REGIONS + 1
    ];
    assert!(
        RemoteSpace::new(&oversized)
            .unwrap_err()
            .msg
            .contains("exceeds")
    );
}

#[test]
fn packed_copy_order_overlap_gaps_and_segmentation_match_for_cached_and_raw_buffers() {
    let operations = [
        CopyOp::new(5, 5, 3),
        CopyOp::new(1, 0, 4),
        CopyOp::new(u64::MAX, u64::MAX, 0),
        CopyOp::new(2, 8, 2),
    ];
    let batch = CopyBatch::new(&operations).unwrap();
    let source = BufferSet::try_new(buffers(&[b"ab", b"", b"cde", b"fgh", b""])).unwrap();
    let single_source = buffers(&[b"abcdefghMORE"]);
    assert_eq!(source.layout().total(), 8);
    let packed = source.gather(&batch).unwrap();
    assert_eq!(packed, b"fghbcdecd");
    assert_eq!(batch.gather(&single_source).unwrap(), packed);
    let source = ReadSource::try_new(source.into_buffers()).unwrap();
    assert_eq!(source.gather(&batch).unwrap(), packed);

    let mut destination = BufferSet::try_new(buffers(&[b".", b"", b"...", b"......"])).unwrap();
    destination.scatter(&batch, &packed).unwrap();
    assert_eq!(flatten(destination.buffers()), b"bcde.fghcd");
    let mut single_destination = buffers(&[b"..........++"]);
    batch.scatter(&packed, &mut single_destination).unwrap();
    assert_eq!(flatten(&single_destination), b"bcde.fghcd++");
}

#[test]
fn scatter_rejects_invalid_packed_data_or_bounds_before_any_modification() {
    let operations = [CopyOp::new(0, 0, 2), CopyOp::new(2, 4, 2)];
    let batch = CopyBatch::new(&operations).unwrap();
    for packed in [b"ab".as_slice(), b"abcde"] {
        let mut destination = BufferSet::try_new(buffers(&[b"111", b"222"])).unwrap();
        assert_eq!(
            destination.scatter(&batch, packed).unwrap_err().kind,
            MemoryErrorKind::InvalidCopyOp
        );
        assert_eq!(flatten(destination.buffers()), b"111222");
    }
    let mut too_small = buffers(&[b"111", b"22"]);
    assert_eq!(
        batch.scatter(b"abcd", &mut too_small).unwrap_err().kind,
        MemoryErrorKind::InvalidCopyOp
    );
    assert_eq!(flatten(&too_small), b"11122");
    let mut too_small = BufferSet::try_new(too_small).unwrap();
    assert!(too_small.scatter(&batch, b"abcd").is_err());
    assert_eq!(flatten(too_small.buffers()), b"11122");
}

#[test]
fn empty_copy_batches_still_check_actual_layout_and_packed_length() {
    for operations in [vec![], vec![CopyOp::new(u64::MAX, u64::MAX, 0)]] {
        let batch = CopyBatch::new(&operations).unwrap();
        batch.check_bounds(0, 0).unwrap();
        let mut empty = BufferSet::try_new(buffers(&[b"", b"", b""])).unwrap();
        assert!(empty.gather(&batch).unwrap().is_empty());
        empty.scatter(&batch, &[]).unwrap();
        assert_eq!(
            empty.scatter(&batch, b"x").unwrap_err().kind,
            MemoryErrorKind::InvalidCopyOp
        );
        let mut oversized = buffers(&vec![b"".as_slice(); MAX_REGIONS + 1]);
        assert!(batch.gather(&oversized).is_err());
        assert!(batch.scatter(&[], &mut oversized).is_err());
    }
}

#[test]
fn invalid_owned_layouts_return_every_original_buffer_in_order() {
    for read_source in [false, true] {
        let original = buffers(&vec![b"data".as_slice(); MAX_REGIONS + 1]);
        let addresses: Vec<_> = original.iter().map(|buffer| buffer.as_ptr()).collect();
        let failure = if read_source {
            ReadSource::try_new(original).map(drop).unwrap_err()
        } else {
            BufferSet::try_new(original).map(drop).unwrap_err()
        };
        assert_eq!(failure.error.kind, MemoryErrorKind::InvalidCopyOp);
        assert_eq!(failure.buffers().len(), MAX_REGIONS + 1);
        let (_, recovered) = failure.into_parts();
        assert_eq!(
            recovered
                .iter()
                .map(|buffer| buffer.as_ptr())
                .collect::<Vec<_>>(),
            addresses
        );
        assert!(recovered.iter().all(|buffer| buffer.as_slice() == b"data"));
    }
}

#[test]
fn read_source_bound_failure_preserves_buffers_for_retry_and_recovery() {
    let source = ReadSource::try_new(buffers(&[b"abcd", b"efgh"])).unwrap();
    let invalid = [CopyOp::new(7, 0, 2)];
    assert_eq!(
        source
            .gather(&CopyBatch::new(&invalid).unwrap())
            .unwrap_err()
            .kind,
        MemoryErrorKind::InvalidCopyOp
    );
    let valid = [CopyOp::new(2, 0, 4)];
    assert_eq!(
        source.gather(&CopyBatch::new(&valid).unwrap()).unwrap(),
        b"cdef"
    );
    let recovered = source.into_buffers();
    assert_eq!(recovered.len(), 2);
    assert_eq!(flatten(&recovered), b"abcdefgh");
}
