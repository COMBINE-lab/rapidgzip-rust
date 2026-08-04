#!/usr/bin/env bash
# Deprecated one-input compatibility wrapper around run-fair.sh.
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 INPUT.gz" >&2
    exit 2
fi

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository=$(cd -- "$script_dir/.." && pwd)
results_dir=${RESULTS_DIR:-$repository/target/bench-results/matrix-$(date -u +%Y%m%dT%H%M%SZ)-$$}

# The old aggregate variable denoted the zlib-ng reference. Preserve that
# translation, but do not infer a backend from an arbitrary `rapidgzip` found
# on PATH.
if [[ -z ${RAPIDGZIP_CPP_ZLIB_NG:-} && -n ${RAPIDGZIP_CPP:-} ]]; then
    export RAPIDGZIP_CPP_ZLIB_NG=$RAPIDGZIP_CPP
fi

declare -a options=(
    --threads "${THREAD_CELLS:-1 4 16 44}"
    --modes verify
    --runs "${RUNS:-9}"
    --warmups "${WARMUPS:-2}"
    --results-dir "$results_dir"
)
if [[ -z ${RAPIDGZIP_CPP_ISAL:-} && -z ${RAPIDGZIP_CPP_ZLIB_NG:-} && -z ${GZIPPY:-} ]]; then
    options+=(--rust-only)
fi

echo "run-matrix.sh is deprecated; use run-fair.sh (results: $results_dir)" >&2
"$script_dir/run-fair.sh" "${options[@]}" "$1" >&2
cat -- "$results_dir/raw.tsv"
