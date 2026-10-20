#!/usr/bin/env bash
# Hardware-independent tests for the compatibility query ABI and netlink parser.
set -euo pipefail

test_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
build_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruapc-query-tests.XXXXXX")"
trap 'rm -rf -- "$build_dir"' EXIT
compiler="${CC:-cc}"
read -r -a extra_flags <<< "${CFLAGS:-}"

for name in uverbs_driver gid_query; do
    "$compiler" -std=gnu11 -O2 -Wall -Wextra -Werror "${extra_flags[@]}" \
        "$test_dir/$name.c" -o "$build_dir/$name"
    "$build_dir/$name"
done

# Modern kernel headers use a flexible array in ib_uverbs_ioctl_hdr. Compile
# with that layout even on older hosts, so Clang catches nested-array regressions.
"$compiler" -std=gnu11 -O2 -Wall -Wextra -Werror "${extra_flags[@]}" \
    -I"$test_dir/fixtures/flexible-array" \
    "$test_dir/gid_query.c" -o "$build_dir/gid_query_flexible_array"
"$build_dir/gid_query_flexible_array"

echo "GID ioctl and RDMA netlink query tests passed."
