#!/usr/bin/env bash
# Fair multi-mode parity comparison: rapidgzip-rust vs C++ rapidgzip.
#
# Modes (each cell: warmups + RUNS, same threads, same sink policy):
#
#   | Mode              | Rust                         | C++ rapidgzip                          |
#   |-------------------|------------------------------|----------------------------------------|
#   | verify            | -P N -t                      | -P N -t --verify  (or --verify -c -f)  |
#   | stdout            | -P N -c > /dev/null          | -P N --verify -c -f > /dev/null        |
#   | index             | export index; import+decode  | export index; import+decode (if ok)    |
#   | stdin             | cat f | rust -c - > /dev/null| cat f | rapidgzip -c -f > /dev/null    |
#
# Usage:
#   benchmarks/parity-compare.sh [OPTIONS] INPUT.gz [INPUT2.gz ...]
#   benchmarks/parity-compare.sh [OPTIONS] --corpus-dir DIR
#
# Options:
#   --modes "verify stdout index stdin"   Subset of modes (default: all available)
#   --tsv PATH                            Write raw TSV (default: stdout)
#   --json PATH                           Median summary JSON
#   --corpus-dir DIR                      Expand *.gz/*.bgz from DIR
#   -h, --help
#
# Environment: same as run-matrix.sh (RUNS, WARMUPS, THREAD_CELLS, RAPIDGZIP_*,
# TASKSET, DECODED_BYTES). SKIP_CPP=1 skips C++. Index mode is skipped when C++
# lacks --export-index/--import-index.
#
# Flag differences worth knowing:
#   - Rust verifies CRC by default; --no-verify disables. C++ defaults may skip
#     CRC unless --verify is passed (upstream documents --verify as optional).
#   - C++ may elide real decode when writing to /dev/null without -f / -l /
#     --count; this harness always passes -f for stdout sinks.
#   - C++ -t (test) is probed; if missing, verify mode uses --verify -c -f.
#   - Stdin is sequential on both tools (no parallel gzip without ReadAt/seek).
#   - Index format defaults to indexed_gzip (GZIDX) on both when supported.
set -euo pipefail

usage() {
    sed -n '2,35p' "$0" | sed 's/^# \{0,1\}//'
    exit 0
}

runs=${RUNS:-5}
warmups=${WARMUPS:-1}
thread_cells=${THREAD_CELLS:-"1 4"}
rust_binary=${RAPIDGZIP_RUST:-target/release/rapidgzip-rust}
cpp_isal_binary=${RAPIDGZIP_CPP_ISAL:-}
cpp_zlib_ng_binary=${RAPIDGZIP_CPP_ZLIB_NG:-}
decoded_bytes_env=${DECODED_BYTES:-}
modes_str="verify stdout index stdin"
tsv_path=
json_path=
corpus_dir=
inputs=()

while [[ $# -gt 0 ]]; do
    case "$1" in
        -h|--help) usage ;;
        --modes) modes_str=$2; shift 2 ;;
        --tsv) tsv_path=$2; shift 2 ;;
        --json) json_path=$2; shift 2 ;;
        --corpus-dir) corpus_dir=$2; shift 2 ;;
        -*)
            echo "unknown option: $1" >&2
            exit 2
            ;;
        *) inputs+=("$1"); shift ;;
    esac
done

if [[ -n "$corpus_dir" ]]; then
    [[ -d "$corpus_dir" ]] || { echo "corpus dir not found: $corpus_dir" >&2; exit 2; }
    while IFS= read -r -d '' f; do
        inputs+=("$f")
    done < <(find "$corpus_dir" -maxdepth 1 -type f \( -name '*.gz' -o -name '*.bgz' \) -print0 | sort -z)
fi

if [[ ${#inputs[@]} -eq 0 ]]; then
    echo "usage: $0 [OPTIONS] INPUT.gz ..." >&2
    exit 2
fi

is_available() {
    local executable=$1
    [[ -n "$executable" ]] \
        && (command -v "$executable" > /dev/null 2>&1 || [[ -x "$executable" ]])
}

if [[ -z "$cpp_isal_binary" && -z "$cpp_zlib_ng_binary" && -z "${RAPIDGZIP_CPP:-}" ]]; then
    if command -v rapidgzip > /dev/null 2>&1; then
        cpp_zlib_ng_binary=$(command -v rapidgzip)
    fi
else
    if [[ -z "$cpp_zlib_ng_binary" && -n "${RAPIDGZIP_CPP:-}" ]]; then
        cpp_zlib_ng_binary=$RAPIDGZIP_CPP
    fi
fi

if [[ ! -x "$rust_binary" ]] && ! command -v "$rust_binary" > /dev/null 2>&1; then
    echo "Rust decoder is not executable: $rust_binary" >&2
    exit 2
fi

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

first_sample=
for f in "${inputs[@]}"; do
    [[ -r "$f" ]] && { first_sample=$f; break; }
done
[[ -n "$first_sample" ]] || { echo "no readable inputs" >&2; exit 2; }

# Probe C++ capabilities once.
cpp_has_t_verify=0
cpp_has_force_stdout=0
cpp_has_index=0
cpp_bins=()
if [[ "${SKIP_CPP:-0}" != 1 ]]; then
    is_available "$cpp_isal_binary" && cpp_bins+=("$cpp_isal_binary")
    is_available "$cpp_zlib_ng_binary" && cpp_bins+=("$cpp_zlib_ng_binary")
fi

if [[ ${#cpp_bins[@]} -gt 0 ]]; then
    probe_bin=${cpp_bins[0]}
    if with_affinity "$probe_bin" -t --verify -P 1 "$first_sample" > /dev/null 2> /dev/null; then
        cpp_has_t_verify=1
    fi
    if with_affinity "$probe_bin" --verify -c -f -P 1 "$first_sample" > /dev/null 2> /dev/null; then
        cpp_has_force_stdout=1
    fi
    idx_probe=$(mktemp "${TMPDIR:-/tmp}/rgz-idx.XXXXXX")
    if with_affinity "$probe_bin" --export-index "$idx_probe" -P 1 -c -f "$first_sample" > /dev/null 2> /dev/null \
        && [[ -s "$idx_probe" ]] \
        && with_affinity "$probe_bin" --import-index "$idx_probe" -P 1 -c -f "$first_sample" > /dev/null 2> /dev/null; then
        cpp_has_index=1
    fi
    rm -f "$idx_probe"
fi

# Tool label for a cpp binary.
cpp_label() {
    local bin=$1
    if [[ -n "$cpp_isal_binary" && "$bin" == "$cpp_isal_binary" ]]; then
        echo rapidgzip-cpp-isal
    elif [[ -n "${RAPIDGZIP_CPP_ZLIB_NG:-}" || -n "${RAPIDGZIP_CPP:-}" ]]; then
        echo rapidgzip-cpp-zlib-ng
    else
        echo rapidgzip-cpp
    fi
}

resolve_decoded_bytes() {
    local input=$1
    if [[ -n "$decoded_bytes_env" ]]; then
        printf '%s' "$decoded_bytes_env"
        return 0
    fi
    local n
    n=$(with_affinity "$rust_binary" -q --count -P 1 "$input" 2>/dev/null | head -n1 | tr -d '[:space:]' || true)
    if [[ "$n" =~ ^[0-9]+$ ]]; then
        printf '%s' "$n"
    else
        printf ''
    fi
}

timing_file=$(mktemp)
raw_tsv=$(mktemp)
work_dir=$(mktemp -d)
trap 'rm -rf "$timing_file" "$raw_tsv" "$work_dir"' EXIT

benchmark_one() {
    local corpus=$1 mode=$2 tool=$3 threads=$4 run=$5 decoded_bytes=$6
    shift 6
    local started=$EPOCHREALTIME
    with_affinity /usr/bin/time -q -o "$timing_file" -f '%U\t%S\t%M' "$@" > /dev/null 2> /dev/null
    local finished=$EPOCHREALTIME
    local elapsed
    elapsed=$(awk -v s="$started" -v f="$finished" 'BEGIN { printf "%.6f", f - s }')
    local timing
    timing=$(<"$timing_file")
    local throughput=
    if [[ -n "$decoded_bytes" ]]; then
        throughput=$(awk -v b="$decoded_bytes" -v s="$elapsed" \
            'BEGIN { if (s<=0) exit; printf "%.3f", b/1048576/s }')
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$corpus" "$mode" "$tool" "$threads" "$run" "$elapsed" "$timing" "$throughput"
}

printf 'corpus\tmode\ttool\tthreads\trun\tseconds\tuser_seconds\tsystem_seconds\tmax_rss_kib\tdecoded_mib_per_second\n' > "$raw_tsv"

mode_enabled() {
    local m=$1
    [[ " $modes_str " == *" $m "* ]]
}

for input in "${inputs[@]}"; do
    [[ -r "$input" ]] || { echo "not readable: $input" >&2; exit 2; }
    corpus=$(basename "$input")
    decoded_bytes=$(resolve_decoded_bytes "$input")
    # Skip zlib for modes that assume gzip framing if needed — tools auto-detect.

    for threads in $thread_cells; do
        # ---- verify ----
        if mode_enabled verify; then
            for _ in $(seq 1 "$warmups"); do
                with_affinity "$rust_binary" -P "$threads" -t "$input" > /dev/null 2> /dev/null || true
                for bin in "${cpp_bins[@]+"${cpp_bins[@]}"}"; do
                    if [[ $cpp_has_t_verify -eq 1 ]]; then
                        with_affinity "$bin" -t --verify -P "$threads" "$input" > /dev/null 2> /dev/null || true
                    elif [[ $cpp_has_force_stdout -eq 1 ]]; then
                        with_affinity "$bin" --verify -c -f -P "$threads" "$input" > /dev/null 2> /dev/null || true
                    fi
                done
            done
            for run in $(seq 1 "$runs"); do
                benchmark_one "$corpus" verify rapidgzip-rust "$threads" "$run" "$decoded_bytes" \
                    "$rust_binary" -P "$threads" -t "$input" >> "$raw_tsv"
                for bin in "${cpp_bins[@]+"${cpp_bins[@]}"}"; do
                    label=$(cpp_label "$bin")
                    if [[ $cpp_has_t_verify -eq 1 ]]; then
                        benchmark_one "$corpus" verify "$label" "$threads" "$run" "$decoded_bytes" \
                            "$bin" -t --verify -P "$threads" "$input" >> "$raw_tsv"
                    elif [[ $cpp_has_force_stdout -eq 1 ]]; then
                        benchmark_one "$corpus" verify "$label" "$threads" "$run" "$decoded_bytes" \
                            "$bin" --verify -c -f -P "$threads" "$input" >> "$raw_tsv"
                    fi
                done
            done
        fi

        # ---- stdout sink ----
        if mode_enabled stdout; then
            for _ in $(seq 1 "$warmups"); do
                with_affinity "$rust_binary" -P "$threads" -c "$input" > /dev/null 2> /dev/null || true
                for bin in "${cpp_bins[@]+"${cpp_bins[@]}"}"; do
                    with_affinity "$bin" --verify -c -f -P "$threads" "$input" > /dev/null 2> /dev/null || true
                done
            done
            for run in $(seq 1 "$runs"); do
                benchmark_one "$corpus" stdout rapidgzip-rust "$threads" "$run" "$decoded_bytes" \
                    "$rust_binary" -P "$threads" -c "$input" >> "$raw_tsv"
                for bin in "${cpp_bins[@]+"${cpp_bins[@]}"}"; do
                    label=$(cpp_label "$bin")
                    benchmark_one "$corpus" stdout "$label" "$threads" "$run" "$decoded_bytes" \
                        "$bin" --verify -c -f -P "$threads" "$input" >> "$raw_tsv"
                done
            done
        fi

        # ---- prebuilt index ----
        # Export once, then import+decompress to sink. Rust: -c --import-index
        # (not -t: --test forces the verified parallel path and skips index decode).
        # C++: --import-index --verify -c -f. Index files are tool-native.
        if mode_enabled index; then
            rust_idx="$work_dir/${corpus}.rust.gzidx"
            if [[ ! -s "$rust_idx" ]]; then
                with_affinity "$rust_binary" -P 1 -t --export-index "$rust_idx" -f "$input" \
                    > /dev/null 2> /dev/null || true
            fi
            for bin in "${cpp_bins[@]+"${cpp_bins[@]}"}"; do
                if [[ $cpp_has_index -eq 1 ]]; then
                    cpp_idx="$work_dir/${corpus}.$(cpp_label "$bin").gzidx"
                    if [[ ! -s "$cpp_idx" ]]; then
                        with_affinity "$bin" --export-index "$cpp_idx" -P 1 -c -f "$input" \
                            > /dev/null 2> /dev/null || true
                    fi
                fi
            done

            if [[ -s "$rust_idx" ]]; then
                for _ in $(seq 1 "$warmups"); do
                    with_affinity "$rust_binary" -P "$threads" -c --import-index "$rust_idx" "$input" \
                        > /dev/null 2> /dev/null || true
                done
                for run in $(seq 1 "$runs"); do
                    benchmark_one "$corpus" index rapidgzip-rust "$threads" "$run" "$decoded_bytes" \
                        "$rust_binary" -P "$threads" -c --import-index "$rust_idx" "$input" >> "$raw_tsv"
                done
            else
                echo "warning: rust index export failed for $input; skipping rust index mode" >&2
            fi

            for bin in "${cpp_bins[@]+"${cpp_bins[@]}"}"; do
                if [[ $cpp_has_index -ne 1 ]]; then
                    continue
                fi
                label=$(cpp_label "$bin")
                cpp_idx="$work_dir/${corpus}.${label}.gzidx"
                if [[ ! -s "$cpp_idx" ]]; then
                    echo "warning: C++ index export failed for $label; skipping" >&2
                    continue
                fi
                for _ in $(seq 1 "$warmups"); do
                    with_affinity "$bin" --import-index "$cpp_idx" --verify -c -f -P "$threads" "$input" \
                        > /dev/null 2> /dev/null || true
                done
                for run in $(seq 1 "$runs"); do
                    benchmark_one "$corpus" index "$label" "$threads" "$run" "$decoded_bytes" \
                        "$bin" --import-index "$cpp_idx" --verify -c -f -P "$threads" "$input" >> "$raw_tsv"
                done
            done
        fi

        # ---- stdin stream (sequential fairness; -P still accepted) ----
        if mode_enabled stdin; then
            for _ in $(seq 1 "$warmups"); do
                # shellcheck disable=SC2094
                with_affinity "$rust_binary" -P "$threads" -c - < "$input" > /dev/null 2> /dev/null || true
                for bin in "${cpp_bins[@]+"${cpp_bins[@]}"}"; do
                    with_affinity "$bin" -P "$threads" -c -f - < "$input" > /dev/null 2> /dev/null || true
                done
            done
            for run in $(seq 1 "$runs"); do
                # Time the decoder only: feed via stdin redirection (not `cat |`).
                # This avoids measuring an extra process while still testing the
                # non-seekable sequential path. Documented as stdin stream mode.
                benchmark_one "$corpus" stdin rapidgzip-rust "$threads" "$run" "$decoded_bytes" \
                    "$rust_binary" -P "$threads" -c - < "$input" >> "$raw_tsv"
                for bin in "${cpp_bins[@]+"${cpp_bins[@]}"}"; do
                    label=$(cpp_label "$bin")
                    benchmark_one "$corpus" stdin "$label" "$threads" "$run" "$decoded_bytes" \
                        "$bin" -P "$threads" -c -f - < "$input" >> "$raw_tsv"
                done
            done
        fi
    done
done

if [[ -n "$tsv_path" ]]; then
    mkdir -p "$(dirname "$tsv_path")"
    cp "$raw_tsv" "$tsv_path"
    cat "$raw_tsv"
else
    cat "$raw_tsv"
fi

if [[ -n "$json_path" ]]; then
    mkdir -p "$(dirname "$json_path")"
    python3 - "$raw_tsv" "$json_path" <<'PY'
import json, statistics, sys
from collections import defaultdict

tsv_path, out_path = sys.argv[1], sys.argv[2]
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
        rows.append(dict(zip(header, parts)))

def fnum(s):
    s = (s or "").strip()
    if not s:
        return None
    try:
        return float(s)
    except ValueError:
        return None

def med(vals):
    vals = [v for v in vals if v is not None]
    return float(statistics.median(vals)) if vals else None

groups = defaultdict(lambda: {"seconds": [], "mib_s": [], "rss_kib": []})
for r in rows:
    key = (r["corpus"], r["mode"], r["tool"], r["threads"])
    groups[key]["seconds"].append(fnum(r.get("seconds")))
    groups[key]["mib_s"].append(fnum(r.get("decoded_mib_per_second")))
    groups[key]["rss_kib"].append(fnum(r.get("max_rss_kib")))

cells = []
for (corpus, mode, tool, threads), g in sorted(
    groups.items(), key=lambda x: (x[0][0], x[0][1], x[0][2], int(x[0][3]))
):
    cells.append({
        "corpus": corpus,
        "mode": mode,
        "tool": tool,
        "threads": int(threads),
        "runs": len(g["seconds"]),
        "median_seconds": med(g["seconds"]),
        "median_mib_per_second": med(g["mib_s"]),
        "median_max_rss_kib": med(g["rss_kib"]),
    })

with open(out_path, "w", encoding="utf-8") as fh:
    json.dump({"cells": cells}, fh, indent=2)
    fh.write("\n")
print(f"wrote JSON summary: {out_path}", file=sys.stderr)
PY
fi
