#!/usr/bin/env bash
# Fair comparative matrix: rapidgzip-rust vs C++ rapidgzip (ISA-L / zlib-ng) vs gzippy.
#
# Usage:
#   benchmarks/run-matrix.sh [OPTIONS] INPUT.gz [INPUT2.gz ...]
#   benchmarks/run-matrix.sh [OPTIONS] --corpus-dir DIR
#
# Options:
#   --corpus-dir DIR   Run every readable *.gz (and *.bgz) under DIR (non-recursive)
#   --mode MODE        verify (default) | stdout
#   --tsv PATH         Write TSV to PATH (default: stdout)
#   --json PATH        Also write a median summary JSON to PATH
#   -h, --help         Show this help
#
# Environment:
#   RUNS                 Measured runs per cell (default: 9)
#   WARMUPS              Warmup runs per cell (default: 2)
#   THREAD_CELLS         Space-separated thread budgets (default: "1 4 16 44")
#   RAPIDGZIP_RUST       Path to rapidgzip-rust (default: target/release/rapidgzip-rust)
#   RAPIDGZIP_CPP_ISAL   C++ rapidgzip built with ISA-L (optional)
#   RAPIDGZIP_CPP_ZLIB_NG  C++ rapidgzip with zlib-ng only (optional)
#   RAPIDGZIP_CPP        Alias for RAPIDGZIP_CPP_ZLIB_NG; if unset and no CPP_* are
#                        set, auto-detects `rapidgzip` on PATH
#   GZIPPY               Path to gzippy (optional; auto-detected if on PATH)
#   DECODED_BYTES        Uncompressed size for throughput (auto --count if unset)
#   TASKSET              Optional affinity: "0-43" or full "taskset -c 0-43"
#   SKIP_RUST=1          Skip Rust cells
#   SKIP_CPP=1           Skip all C++ cells
#   SKIP_GZIPPY=1        Skip gzippy cells
#
# Fairness (default --mode verify):
#   Rust:  -P N -t --chunk-size K  (CRC/ISIZE verify, discard payload)
#   C++:   -P N -t --verify --chunk-size K when -t is accepted;
#          else -d -P N --verify -c -f --chunk-size K
#          (0.16+ requires -d for stdout decompress; -f forces real decode to /dev/null)
#   gzippy: -d -c -p N             (footer verify; no separate --test honor for -p)
# Failed timed runs are dropped (no row) so a CLI flag mismatch cannot invent thrpt.
#
# Output TSV columns:
#   corpus tool threads run seconds user_seconds system_seconds max_rss_kib decoded_mib_per_second
set -euo pipefail

usage() {
    sed -n '2,40p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

runs=${RUNS:-9}
warmups=${WARMUPS:-2}
thread_cells=${THREAD_CELLS:-"1 4 16 44"}
rust_binary=${RAPIDGZIP_RUST:-target/release/rapidgzip-rust}
cpp_isal_binary=${RAPIDGZIP_CPP_ISAL:-}
cpp_zlib_ng_binary=${RAPIDGZIP_CPP_ZLIB_NG:-}
gzippy_binary=${GZIPPY:-}
decoded_bytes_env=${DECODED_BYTES:-}
# Shared decoded-chunk budget (KiB) — both CLIs default to 4096; pin explicitly.
chunk_size_kib=${CHUNK_SIZE_KIB:-4096}
mode=verify
tsv_path=
json_path=
corpus_dir=
inputs=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage ;;
        --mode)
            mode=$2
            shift 2
            ;;
        --tsv)
            tsv_path=$2
            shift 2
            ;;
        --json)
            json_path=$2
            shift 2
            ;;
        --corpus-dir)
            corpus_dir=$2
            shift 2
            ;;
        --)
            shift
            inputs+=("$@")
            break
            ;;
        -*)
            echo "unknown option: $1" >&2
            exit 2
            ;;
        *)
            inputs+=("$1")
            shift
            ;;
    esac
done

if [[ -n "$corpus_dir" ]]; then
    if [[ ! -d "$corpus_dir" ]]; then
        echo "corpus dir not found: $corpus_dir" >&2
        exit 2
    fi
    while IFS= read -r -d '' f; do
        inputs+=("$f")
    done < <(find "$corpus_dir" -maxdepth 1 -type f \( -name '*.gz' -o -name '*.bgz' -o -name '*.zz' \) -print0 | sort -z)
fi

if [[ ${#inputs[@]} -eq 0 ]]; then
    echo "usage: $0 [OPTIONS] INPUT.gz [INPUT2.gz ...]" >&2
    echo "       $0 [OPTIONS] --corpus-dir DIR" >&2
    exit 2
fi

case "$mode" in
    verify|stdout) ;;
    *)
        echo "unknown --mode: $mode (want verify|stdout)" >&2
        exit 2
        ;;
esac

# --- tool resolution -------------------------------------------------------

is_available() {
    local executable=$1
    [[ -n "$executable" ]] \
        && (command -v "$executable" > /dev/null 2>&1 || [[ -x "$executable" ]])
}

# Detect ISA-L symbols in a rapidgzip CLI (often a Python wrapper + .so).
# Use grep -a on the file (not strings|grep -q): with pipefail, grep -q can
# SIGPIPE strings and make a real match look like a miss.
cpp_binary_has_isal() {
    local bin=$1
    local so dir
    if [[ -f "$bin" ]] && grep -aqiE 'isal_|libisal' "$bin" 2>/dev/null; then
        return 0
    fi
    # Python entrypoint next to site-packages rapidgzip*.so
    dir=$(cd "$(dirname "$bin")" && pwd)
    while IFS= read -r -d '' so; do
        if grep -aqiE 'isal_|libisal' "$so" 2>/dev/null; then
            return 0
        fi
    done < <(find "$dir/../lib" -name 'rapidgzip*.so' -print0 2>/dev/null || true)
    return 1
}

# Rust CLI: label ISA-L builds distinctly (binary text and/or DT_NEEDED via ldd).
rust_binary_has_isal() {
    local bin=$1
    if [[ -f "$bin" ]] && grep -aqiE 'isal_|libisal' "$bin" 2>/dev/null; then
        return 0
    fi
    if command -v ldd > /dev/null 2>&1 && ldd "$bin" 2>/dev/null | grep -aqiE 'libisal'; then
        return 0
    fi
    return 1
}

rust_tool=rapidgzip-rust
if [[ "${SKIP_RUST:-0}" != 1 ]] && is_available "$rust_binary" && rust_binary_has_isal "$rust_binary"; then
    rust_tool=rapidgzip-rust-isal
fi

# Auto-detect C++ rapidgzip when no explicit CPP env vars are set.
# Prefer labeling PyPI/source builds with ISA-L as RAPIDGZIP_CPP_ISAL when
# only one binary is available (fair headline competitor).
if [[ -z "$cpp_isal_binary" && -z "$cpp_zlib_ng_binary" && -z "${RAPIDGZIP_CPP:-}" ]]; then
    if command -v rapidgzip > /dev/null 2>&1; then
        _auto=$(command -v rapidgzip)
        if cpp_binary_has_isal "$_auto"; then
            cpp_isal_binary=$_auto
        else
            cpp_zlib_ng_binary=$_auto
        fi
        unset _auto
    fi
else
    # Explicit RAPIDGZIP_CPP: classify by ISA-L symbols when slots are free.
    if [[ -n "${RAPIDGZIP_CPP:-}" ]]; then
        if [[ -z "$cpp_isal_binary" && -z "$cpp_zlib_ng_binary" ]]; then
            if cpp_binary_has_isal "${RAPIDGZIP_CPP}"; then
                cpp_isal_binary=$RAPIDGZIP_CPP
            else
                cpp_zlib_ng_binary=$RAPIDGZIP_CPP
            fi
        fi
    fi
    # Reclassify a lone "zlib-ng" path that actually embeds ISA-L (common PyPI wheels).
    if [[ -z "$cpp_isal_binary" && -n "$cpp_zlib_ng_binary" ]] \
        && cpp_binary_has_isal "$cpp_zlib_ng_binary"; then
        cpp_isal_binary=$cpp_zlib_ng_binary
        cpp_zlib_ng_binary=
    fi
fi

if [[ -z "$gzippy_binary" ]]; then
    if command -v gzippy > /dev/null 2>&1; then
        gzippy_binary=$(command -v gzippy)
    else
        gzippy_binary=gzippy
    fi
fi

if [[ "${SKIP_RUST:-0}" != 1 ]]; then
    if [[ ! -x "$rust_binary" ]] && ! command -v "$rust_binary" > /dev/null 2>&1; then
        echo "Rust decoder is not executable: $rust_binary" >&2
        echo "Build with: cargo build --locked --release -p rapidgzip-rust-cli" >&2
        exit 2
    fi
fi

# Optional CPU affinity wrapper from TASKSET env.
# Accepts "0-43", "-c 0-43", or a full "taskset -c 0-43" prefix.
run_affinity=()
if [[ -n "${TASKSET:-}" ]]; then
    if [[ "$TASKSET" == taskset* ]]; then
        # shellcheck disable=SC2206
        run_affinity=($TASKSET)
    elif [[ "$TASKSET" == -c* ]]; then
        # shellcheck disable=SC2206
        run_affinity=(taskset $TASKSET)
    else
        run_affinity=(taskset -c "$TASKSET")
    fi
fi

with_affinity() {
    if [[ ${#run_affinity[@]} -gt 0 ]]; then
        "${run_affinity[@]}" "$@"
    else
        "$@"
    fi
}

# --- C++ flag probing (once, on first available sample) --------------------

# C++ rapidgzip 0.16 CLI quirks (fair harness must match real decode + CRC):
#   - -t/--test verifies without writing payload (preferred verify mode).
#   - stdout decompress needs -d/--decompress; -c alone is not an action.
#   - Without -f, C++ may elide real decode when the sink is /dev/null.
#   - Do not pass a literal "-" for stdin; use redirection only.
cpp_verify_style=none   # t_verify | force_stdout | none
cpp_stdout_style=force  # always -d -c -f for fair sink

probe_cpp_flags() {
    local bin=$1
    local sample=$2
    if ! is_available "$bin"; then
        return 0
    fi
    if with_affinity "$bin" -t --verify -P 1 --chunk-size "$chunk_size_kib" "$sample" > /dev/null 2> /dev/null; then
        cpp_verify_style=t_verify
    elif with_affinity "$bin" -d --verify -c -f -P 1 --chunk-size "$chunk_size_kib" "$sample" > /dev/null 2> /dev/null; then
        cpp_verify_style=force_stdout
    elif with_affinity "$bin" -d -c -f -P 1 --chunk-size "$chunk_size_kib" "$sample" > /dev/null 2> /dev/null; then
        cpp_verify_style=force_stdout
        echo "warning: $bin accepts -d -c -f but not --verify; CRC verify may be off" >&2
    else
        echo "warning: could not probe fair flags for $bin; C++ cells may be skipped" >&2
        cpp_verify_style=none
    fi
}

first_sample=
for f in "${inputs[@]}"; do
    if [[ -r "$f" ]]; then
        first_sample=$f
        break
    fi
done
if [[ -z "$first_sample" ]]; then
    echo "no readable inputs" >&2
    exit 2
fi

if [[ "${SKIP_CPP:-0}" != 1 ]]; then
    if is_available "$cpp_isal_binary"; then
        probe_cpp_flags "$cpp_isal_binary" "$first_sample"
    elif is_available "$cpp_zlib_ng_binary"; then
        probe_cpp_flags "$cpp_zlib_ng_binary" "$first_sample"
    fi
fi

# --- timing helpers --------------------------------------------------------

timing_file=$(mktemp)
raw_tsv=$(mktemp)
trap 'rm -f "$timing_file" "$raw_tsv"' EXIT

# Resolve decoded byte count for throughput (env wins; else rust --count).
resolve_decoded_bytes() {
    local input=$1
    if [[ -n "$decoded_bytes_env" ]]; then
        printf '%s' "$decoded_bytes_env"
        return 0
    fi
    if is_available "$rust_binary"; then
        local n
        n=$(with_affinity "$rust_binary" -q --count -P 1 "$input" 2>/dev/null | head -n1 | tr -d '[:space:]' || true)
        if [[ "$n" =~ ^[0-9]+$ ]]; then
            printf '%s' "$n"
            return 0
        fi
    fi
    printf ''
}

# /usr/bin/time format: use spaces (some builds do not expand \t in -f strings).
# Output: user_seconds system_seconds max_rss_kib. Returns the child exit status.
run_timed() {
    # /usr/bin/time returns the child exit code when present.
    with_affinity /usr/bin/time -q -o "$timing_file" -f '%U %S %M' "$@" > /dev/null 2> /dev/null
}

benchmark_one() {
    local corpus=$1
    local tool=$2
    local threads=$3
    local run=$4
    local decoded_bytes=$5
    shift 5
    local started=$EPOCHREALTIME
    local ec=0
    set +e
    run_timed "$@"
    ec=$?
    set -e
    local finished=$EPOCHREALTIME
    if [[ $ec -ne 0 ]]; then
        echo "warning: timed command failed (exit $ec) for $tool threads=$threads run=$run corpus=$corpus: $*" >&2
        return 0
    fi
    local elapsed
    elapsed=$(awk -v started="$started" -v finished="$finished" \
        'BEGIN { printf "%.6f", finished - started }')
    local user_s sys_s rss_kib
    read -r user_s sys_s rss_kib <"$timing_file" || true
    user_s=${user_s:-}
    sys_s=${sys_s:-}
    rss_kib=${rss_kib:-}
    local throughput=
    if [[ -n "$decoded_bytes" ]]; then
        throughput=$(awk -v bytes="$decoded_bytes" -v seconds="$elapsed" \
            'BEGIN {
                if (seconds <= 0) { printf ""; exit }
                printf "%.3f", bytes / 1048576 / seconds
            }')
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$corpus" "$tool" "$threads" "$run" "$elapsed" "$user_s" "$sys_s" "$rss_kib" "$throughput"
}

# Build argv for C++ under current mode; print nothing and return 1 if unsupported.
# Fairness: CRC on (--verify / -t), -P N, explicit --chunk-size, -d for stdout
# actions, and -f so C++ cannot elide decode on /dev/null.
cpp_cmd_prefix() {
    local bin=$1
    local threads=$2
    case "$mode" in
        verify)
            case "$cpp_verify_style" in
                t_verify)
                    printf '%s\0' "$bin" -t --verify -P "$threads" --chunk-size "$chunk_size_kib"
                    return 0
                    ;;
                force_stdout)
                    printf '%s\0' "$bin" -d --verify -c -f -P "$threads" --chunk-size "$chunk_size_kib"
                    return 0
                    ;;
                *)
                    return 1
                    ;;
            esac
            ;;
        stdout)
            printf '%s\0' "$bin" -d --verify -c -f -P "$threads" --chunk-size "$chunk_size_kib"
            return 0
            ;;
    esac
}

run_cpp_warmup() {
    local bin=$1
    local input=$2
    local threads=$3
    local -a args=()
    local item
    while IFS= read -r -d '' item; do
        args+=("$item")
    done < <(cpp_cmd_prefix "$bin" "$threads" || true)
    if [[ ${#args[@]} -eq 0 ]]; then
        return 0
    fi
    args+=("$input")
    with_affinity "${args[@]}" > /dev/null 2> /dev/null || true
}

benchmark_cpp() {
    local corpus=$1
    local tool=$2
    local bin=$3
    local threads=$4
    local run=$5
    local decoded_bytes=$6
    local input=$7
    local -a args=()
    local item
    while IFS= read -r -d '' item; do
        args+=("$item")
    done < <(cpp_cmd_prefix "$bin" "$threads" || true)
    if [[ ${#args[@]} -eq 0 ]]; then
        return 0
    fi
    args+=("$input")
    benchmark_one "$corpus" "$tool" "$threads" "$run" "$decoded_bytes" "${args[@]}"
}

# --- main matrix -----------------------------------------------------------

header=$'corpus\ttool\tthreads\trun\tseconds\tuser_seconds\tsystem_seconds\tmax_rss_kib\tdecoded_mib_per_second\n'
printf '%s' "$header" > "$raw_tsv"

for input in "${inputs[@]}"; do
    if [[ ! -r "$input" ]]; then
        echo "input is not readable: $input" >&2
        exit 2
    fi
    corpus=$(basename "$input")
    decoded_bytes=$(resolve_decoded_bytes "$input")
    if [[ -z "$decoded_bytes" ]]; then
        echo "warning: DECODED_BYTES unset and --count failed for $input; throughput column empty" >&2
    fi

    for threads in $thread_cells; do
        for _ in $(seq 1 "$warmups"); do
            if [[ "${SKIP_RUST:-0}" != 1 ]]; then
                case "$mode" in
                    verify) with_affinity "$rust_binary" -P "$threads" --chunk-size "$chunk_size_kib" -t "$input" > /dev/null 2> /dev/null || true ;;
                    stdout) with_affinity "$rust_binary" -P "$threads" --chunk-size "$chunk_size_kib" -c "$input" > /dev/null 2> /dev/null || true ;;
                esac
            fi
            if [[ "${SKIP_CPP:-0}" != 1 ]]; then
                if is_available "$cpp_isal_binary"; then
                    run_cpp_warmup "$cpp_isal_binary" "$input" "$threads"
                fi
                if is_available "$cpp_zlib_ng_binary"; then
                    run_cpp_warmup "$cpp_zlib_ng_binary" "$input" "$threads"
                fi
            fi
            if [[ "${SKIP_GZIPPY:-0}" != 1 ]] && is_available "$gzippy_binary"; then
                with_affinity "$gzippy_binary" -d -c -p "$threads" "$input" > /dev/null 2> /dev/null || true
            fi
        done

        for run in $(seq 1 "$runs"); do
            if [[ "${SKIP_RUST:-0}" != 1 ]]; then
                case "$mode" in
                    verify)
                        benchmark_one "$corpus" "$rust_tool" "$threads" "$run" "$decoded_bytes" \
                            "$rust_binary" -P "$threads" --chunk-size "$chunk_size_kib" -t "$input" >> "$raw_tsv"
                        ;;
                    stdout)
                        benchmark_one "$corpus" "$rust_tool" "$threads" "$run" "$decoded_bytes" \
                            "$rust_binary" -P "$threads" --chunk-size "$chunk_size_kib" -c "$input" >> "$raw_tsv"
                        ;;
                esac
            fi
            if [[ "${SKIP_CPP:-0}" != 1 ]]; then
                if is_available "$cpp_isal_binary"; then
                    benchmark_cpp "$corpus" rapidgzip-cpp-isal "$cpp_isal_binary" \
                        "$threads" "$run" "$decoded_bytes" "$input" >> "$raw_tsv"
                fi
                if is_available "$cpp_zlib_ng_binary"; then
                    # Label PATH-detected builds as rapidgzip-cpp (backend unknown).
                    local_tool=rapidgzip-cpp-zlib-ng
                    if [[ -z "${RAPIDGZIP_CPP_ZLIB_NG:-}" && -z "${RAPIDGZIP_CPP:-}" && -z "${RAPIDGZIP_CPP_ISAL:-}" ]]; then
                        local_tool=rapidgzip-cpp
                    fi
                    benchmark_cpp "$corpus" "$local_tool" "$cpp_zlib_ng_binary" \
                        "$threads" "$run" "$decoded_bytes" "$input" >> "$raw_tsv"
                fi
            fi
            if [[ "${SKIP_GZIPPY:-0}" != 1 ]] && is_available "$gzippy_binary"; then
                benchmark_one "$corpus" gzippy "$threads" "$run" "$decoded_bytes" \
                    "$gzippy_binary" -d -c -p "$threads" "$input" >> "$raw_tsv"
            fi
        done
    done
done

if [[ -n "$tsv_path" ]]; then
    mkdir -p "$(dirname "$tsv_path")"
    cp "$raw_tsv" "$tsv_path"
    cat "$raw_tsv"
else
    cat "$raw_tsv"
fi

# --- optional JSON median summary ------------------------------------------

if [[ -n "$json_path" ]]; then
    mkdir -p "$(dirname "$json_path")"
    python3 - "$raw_tsv" "$json_path" "$mode" <<'PY'
import json, statistics, sys
from collections import defaultdict

tsv_path, out_path, mode = sys.argv[1], sys.argv[2], sys.argv[3]
rows = []
with open(tsv_path, encoding="utf-8") as fh:
    header = fh.readline().rstrip("\n").split("\t")
    for line in fh:
        line = line.rstrip("\n")
        if not line:
            continue
        parts = line.split("\t")
        if len(parts) < len(header):
            parts += [""] * (len(header) - len(parts))
        row = dict(zip(header, parts))
        rows.append(row)

def median_nums(vals):
    vals = [v for v in vals if v is not None]
    if not vals:
        return None
    return float(statistics.median(vals))

def fnum(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None

groups = defaultdict(lambda: {"seconds": [], "mib_s": [], "rss_kib": []})
for r in rows:
    key = (r["corpus"], r["tool"], r["threads"])
    groups[key]["seconds"].append(fnum(r.get("seconds")))
    groups[key]["mib_s"].append(fnum(r.get("decoded_mib_per_second")))
    groups[key]["rss_kib"].append(fnum(r.get("max_rss_kib")))

cells = []
for (corpus, tool, threads), g in sorted(groups.items(), key=lambda x: (x[0][0], x[0][1], int(x[0][2]))):
    cells.append({
        "corpus": corpus,
        "tool": tool,
        "threads": int(threads),
        "runs": len(g["seconds"]),
        "median_seconds": median_nums(g["seconds"]),
        "median_mib_per_second": median_nums(g["mib_s"]),
        "median_max_rss_kib": median_nums(g["rss_kib"]),
        "median_max_rss_mib": (
            None if median_nums(g["rss_kib"]) is None
            else round(median_nums(g["rss_kib"]) / 1024.0, 3)
        ),
    })

doc = {
    "mode": mode,
    "cells": cells,
}
with open(out_path, "w", encoding="utf-8") as fh:
    json.dump(doc, fh, indent=2)
    fh.write("\n")
print(f"wrote JSON summary: {out_path}", file=sys.stderr)
PY
fi
