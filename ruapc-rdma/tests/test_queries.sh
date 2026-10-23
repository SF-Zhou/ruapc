#!/usr/bin/env bash
# Hardware-independent query tests. Use --native for extended libibverbs APIs.
set -euo pipefail

native=false
if [[ $# -gt 0 ]]; then
    if [[ $# -ne 1 || $1 != --native ]]; then
        echo "Usage: $0 [--native]" >&2
        exit 2
    fi
    native=true
fi

test_dir="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
build_dir="$(mktemp -d "${TMPDIR:-/tmp}/ruapc-query-tests.XXXXXX")"
trap 'rm -rf -- "$build_dir"' EXIT
compiler="${CC:-cc}"
read -r -a extra_flags <<< "${CFLAGS:-}"

for name in driver ioctl; do
    "$compiler" -std=gnu11 -O2 -Wall -Wextra -Werror "${extra_flags[@]}" \
        "$test_dir/gid/$name.c" -o "$build_dir/$name"
    "$build_dir/$name"
done

# Modern kernel headers use a flexible array in ib_uverbs_ioctl_hdr. Compile
# with that layout even on older hosts, so Clang catches nested-array regressions.
"$compiler" -std=gnu11 -O2 -Wall -Wextra -Werror "${extra_flags[@]}" \
    -I"$test_dir/fixtures/flexible-array" \
    "$test_dir/gid/ioctl.c" -o "$build_dir/gid_ioctl_flexible_array"
"$build_dir/gid_ioctl_flexible_array"

echo "GID ioctl and RDMA netlink query tests passed."

# Keep native tests opt-in: the compatibility backend supports verbs.h versions
# predating ibv_query_gid_ex / ibv_query_gid_table.
if $native; then
    "$compiler" -std=gnu11 -O2 -Wall -Wextra -Werror "${extra_flags[@]}" \
        "$test_dir/gid/native.c" -o "$build_dir/gid_native"
    "$build_dir/gid_native"
    echo "Native libibverbs GID query tests passed."
fi
