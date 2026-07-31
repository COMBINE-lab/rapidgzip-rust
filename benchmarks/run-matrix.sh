#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
    echo "usage: $0 INPUT.gz" >&2
    exit 2
fi

input=$1
runs=${RUNS:-9}
warmups=${WARMUPS:-2}
thread_cells=${THREAD_CELLS:-"1 4 16 44"}
rust_binary=${RAPIDGZIP_RUST:-target/release/rapidgzip-rust}
cpp_isal_binary=${RAPIDGZIP_CPP_ISAL:-}
cpp_zlib_ng_binary=${RAPIDGZIP_CPP_ZLIB_NG:-${RAPIDGZIP_CPP:-rapidgzip}}
gzippy_binary=${GZIPPY:-gzippy}
decoded_bytes=${DECODED_BYTES:-}

if [[ ! -r "$input" ]]; then
    echo "input is not readable: $input" >&2
    exit 2
fi
if [[ ! -x "$rust_binary" ]]; then
    echo "Rust decoder is not executable: $rust_binary" >&2
    exit 2
fi

timing_file=$(mktemp)
trap 'rm -f "$timing_file"' EXIT

printf 'tool\tthreads\trun\tseconds\tuser_seconds\tsystem_seconds\tmax_rss_kib\tdecoded_mib_per_second\n'

is_available() {
    local executable=$1
    [[ -n "$executable" ]] \
        && (command -v "$executable" > /dev/null 2>&1 || [[ -x "$executable" ]])
}

benchmark_one() {
    local tool=$1
    local threads=$2
    local run=$3
    shift 3
    local started=$EPOCHREALTIME
    /usr/bin/time -q -o "$timing_file" -f '%U\t%S\t%M' "$@" > /dev/null 2> /dev/null
    local finished=$EPOCHREALTIME
    local elapsed
    elapsed=$(awk -v started="$started" -v finished="$finished" \
        'BEGIN { printf "%.6f", finished - started }')
    local timing
    timing=$(<"$timing_file")
    local throughput=
    if [[ -n "$decoded_bytes" ]]; then
        throughput=$(awk -v bytes="$decoded_bytes" -v seconds="$elapsed" \
            'BEGIN { printf "%.3f", bytes / 1048576 / seconds }')
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$tool" "$threads" "$run" "$elapsed" "$timing" "$throughput"
}

for threads in $thread_cells; do
    for _ in $(seq 1 "$warmups"); do
        "$rust_binary" -P "$threads" -t "$input" > /dev/null 2> /dev/null
        if is_available "$cpp_isal_binary"; then
            "$cpp_isal_binary" -t --verify -P "$threads" "$input" > /dev/null 2> /dev/null
        fi
        if is_available "$cpp_zlib_ng_binary"; then
            "$cpp_zlib_ng_binary" -t --verify -P "$threads" "$input" > /dev/null 2> /dev/null
        fi
        if is_available "$gzippy_binary"; then
            "$gzippy_binary" -d -c -p "$threads" "$input" > /dev/null 2> /dev/null
        fi
    done
    for run in $(seq 1 "$runs"); do
        benchmark_one rapidgzip-rust "$threads" "$run" \
            "$rust_binary" -P "$threads" -t "$input"
        if is_available "$cpp_isal_binary"; then
            benchmark_one rapidgzip-cpp-isal "$threads" "$run" \
                "$cpp_isal_binary" -t --verify -P "$threads" "$input"
        fi
        if is_available "$cpp_zlib_ng_binary"; then
            benchmark_one rapidgzip-cpp-zlib-ng "$threads" "$run" \
                "$cpp_zlib_ng_binary" -t --verify -P "$threads" "$input"
        fi
        if is_available "$gzippy_binary"; then
            benchmark_one gzippy "$threads" "$run" \
                "$gzippy_binary" -d -c -p "$threads" "$input"
        fi
    done
done
