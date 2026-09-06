# Refactor evidence

See [the validation report](../refactoring.md) for the comparison method and
interpretation. Baseline is commit `36c8352`, built with the original lockfile;
benchmark controls were applied identically to both versions. The remote-memory
harness differs only where the owned read-source API requires it.

- `checks.json`: baseline/final test counts and build/check status.
- `echo.json`: final three-pair echo comparison and original output.
- `remote_memory-long.json`: final three-pair long remote-memory comparison.
- `remote_memory.json`: initial short full-matrix comparison.
- `remote_memory-isolated-short.json`: short isolated-transport diagnostics.
- `remote_memory-pinned-rdma.json`: nine-pair RDMA comparison with separately
  pinned runtime/poll threads, including actual assignments and raw output.
- `bufpool.json`: allocator results with exact version labels, source hashes,
  sequential and fresh-process cases, and excluded-experiment descriptions.
- `bufpool-raw.json`: original allocator logs and the diagnostic harness, keyed
  by their original filenames. References ending in `::filename` address keys
  in this object.

Latency changes are `(current / baseline - 1) * 100`; negative is faster.
Throughput changes use the same formula; positive is faster. Samples are not
trimmed, and datasets with different placement or iteration counts are not
pooled. Supporting allocator initialization/lazy-merge measurements explicitly
predate the final TLS optimization; they are not final whole-library results.
