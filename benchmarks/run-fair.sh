#!/usr/bin/env bash
# One-shot fair comparative benchmark driver (Rust vs C++ rapidgzip).
#
# Usage:
#   ./benchmarks/run-fair.sh [--ci] [--threads "1 4 8"] [--runs 5] [--warmups 1]
#                            [--modes "verify stdout"] [--skip-parity]
#                            [--corpus-dir DIR] [--results-dir DIR]
#
# --ci   CI-light defaults: small corpora, threads "1 2", runs 2, warmups 1;
#        C++ rapidgzip is optional (skipped with a note if missing).
#        Does not download FASTQ or any network corpora.
#
# Without --ci, missing C++ rapidgzip is a hard error with an install hint.
#
# Steps:
#   1. Build release rapidgzip-rust if missing
#   2. Resolve rapidgzip (PATH / RAPIDGZIP_CPP*)
#   3. Generate corpora into target/bench-corpora/ if needed
#   4. Run run-matrix.sh (verify mode) + optional parity-compare.sh
#   5. Write results under target/bench-results/<timestamp>/
#   6. Print a markdown summary table (median thrpt + RSS)
set -euo pipefail

root=$(cd "$(dirname "$0")/.." && pwd)
cd "$root"

ci=0
threads=
runs=
warmups=
modes=
skip_parity=0
corpus_dir=
results_base=

while [[ $# -gt 0 ]]; do
    case "$1" in
        --ci) ci=1; shift ;;
        --threads) threads=$2; shift 2 ;;
        --runs) runs=$2; shift 2 ;;
        --warmups) warmups=$2; shift 2 ;;
        --modes) modes=$2; shift 2 ;;
        --skip-parity) skip_parity=1; shift ;;
        --corpus-dir) corpus_dir=$2; shift 2 ;;
        --results-dir) results_base=$2; shift 2 ;;
        -h|--help)
            sed -n '2,25p' "$0" | sed 's/^# \{0,1\}//'
            exit 0
            ;;
        *)
            echo "unknown option: $1" >&2
            exit 2
            ;;
    esac
done

if [[ $ci -eq 1 ]]; then
    threads=${threads:-"1 2"}
    runs=${runs:-2}
    warmups=${warmups:-1}
    modes=${modes:-"verify stdout"}
    export CORPUS_BYTES=${CORPUS_BYTES:-$((2 * 1024 * 1024))}
else
    # Fair local defaults: large enough corpora that startup does not dominate,
    # and a thread ladder that includes oversubscribe on multi-core hosts.
    threads=${threads:-"1 4 16 44"}
    runs=${runs:-9}
    warmups=${warmups:-2}
    modes=${modes:-"verify stdout index stdin"}
    export CORPUS_BYTES=${CORPUS_BYTES:-$((32 * 1024 * 1024))}
    export CHUNK_SIZE_KIB=${CHUNK_SIZE_KIB:-4096}
    # Pin affinity across all CPUs when the caller did not set TASKSET.
    if [[ -z "${TASKSET:-}" ]] && command -v nproc > /dev/null 2>&1; then
        export TASKSET="0-$(($(nproc) - 1))"
    fi
fi

export RUNS=$runs
export WARMUPS=$warmups
export THREAD_CELLS=$threads
export CHUNK_SIZE_KIB=${CHUNK_SIZE_KIB:-4096}

rust_binary=${RAPIDGZIP_RUST:-target/release/rapidgzip-rust}
export RAPIDGZIP_RUST=$rust_binary

# --- build rust if needed --------------------------------------------------

if [[ ! -x "$rust_binary" ]]; then
    echo "==> building release rapidgzip-rust"
    cargo build --locked --release -p rapidgzip-rust-cli
fi
if [[ ! -x "$rust_binary" ]]; then
    echo "failed to produce $rust_binary" >&2
    exit 1
fi

# --- resolve C++ rapidgzip -------------------------------------------------
# Prefer an explicit env var, then a local bench venv (0.16 + ISA-L wheel),
# then PATH. Harness scripts auto-label ISA-L via symbol scan of the .so.

cpp_found=0
if [[ -n "${RAPIDGZIP_CPP_ISAL:-}" && -x "${RAPIDGZIP_CPP_ISAL}" ]] \
    || command -v "${RAPIDGZIP_CPP_ISAL:-}" > /dev/null 2>&1; then
    cpp_found=1
fi
if [[ -n "${RAPIDGZIP_CPP_ZLIB_NG:-}" && -x "${RAPIDGZIP_CPP_ZLIB_NG}" ]] \
    || command -v "${RAPIDGZIP_CPP_ZLIB_NG:-}" > /dev/null 2>&1; then
    cpp_found=1
fi
if [[ -n "${RAPIDGZIP_CPP:-}" && -x "${RAPIDGZIP_CPP}" ]] \
    || command -v "${RAPIDGZIP_CPP:-}" > /dev/null 2>&1; then
    cpp_found=1
fi
# Project-local fair competitor: uv/pip venv with rapidgzip 0.16.x.
if [[ $cpp_found -eq 0 && -x "$root/target/bench-venv/bin/rapidgzip" ]]; then
    export PATH="$root/target/bench-venv/bin:$PATH"
    export RAPIDGZIP_CPP=$root/target/bench-venv/bin/rapidgzip
    cpp_found=1
fi
if [[ $cpp_found -eq 0 ]] && command -v rapidgzip > /dev/null 2>&1; then
    export RAPIDGZIP_CPP=$(command -v rapidgzip)
    cpp_found=1
fi

if [[ $cpp_found -eq 0 ]]; then
    if [[ $ci -eq 1 ]]; then
        echo "note: C++ rapidgzip not found; CI mode continues with Rust-only cells"
        echo "      install: uv venv target/bench-venv && uv pip install --python target/bench-venv/bin/python 'rapidgzip==0.16.0'"
        export SKIP_CPP=1
    else
        cat >&2 <<'EOF'
error: C++ rapidgzip not found on PATH and RAPIDGZIP_CPP* unset.

Fair competitor install (0.16.x wheel embeds ISA-L on manylinux):
  uv venv target/bench-venv
  uv pip install --python target/bench-venv/bin/python 'rapidgzip==0.16.0'
  export PATH="$PWD/target/bench-venv/bin:$PATH"

Or: python3 -m pip install --user 'rapidgzip==0.16.0'
Or build from source: https://github.com/mxmlnkn/rapidgzip

Then either ensure `rapidgzip` is on PATH or set:
  RAPIDGZIP_CPP=/path/to/rapidgzip
  RAPIDGZIP_CPP_ISAL=/path/to/rapidgzip-with-isal      # optional ISA-L build
  RAPIDGZIP_CPP_ZLIB_NG=/path/to/rapidgzip-zlib-ng     # optional zlib-ng control

Re-run with --ci to allow a Rust-only matrix.
EOF
        exit 1
    fi
else
    echo "==> C++ rapidgzip: ${RAPIDGZIP_CPP_ISAL:-}${RAPIDGZIP_CPP_ZLIB_NG:-}${RAPIDGZIP_CPP:-rapidgzip (PATH)}"
    if command -v "${RAPIDGZIP_CPP:-rapidgzip}" > /dev/null 2>&1 || [[ -x "${RAPIDGZIP_CPP:-}" ]]; then
        _cpp_bin=${RAPIDGZIP_CPP_ISAL:-${RAPIDGZIP_CPP_ZLIB_NG:-${RAPIDGZIP_CPP:-rapidgzip}}}
        echo "==> C++ version: $("$_cpp_bin" --version 2>/dev/null | head -1 || true)"
        unset _cpp_bin
    fi
fi

# --- corpora ---------------------------------------------------------------

corpus_dir=${corpus_dir:-target/bench-corpora}
if [[ ! -d "$corpus_dir" ]] || [[ -z "$(find "$corpus_dir" -maxdepth 1 -type f \( -name '*.gz' -o -name '*.bgz' \) 2>/dev/null | head -1)" ]]; then
    echo "==> generating synthetic corpora in $corpus_dir"
    benchmarks/gen-corpora.sh "$corpus_dir"
else
    echo "==> using existing corpora in $corpus_dir"
fi

# Prefer gzip corpora for the default matrix (skip zlib .zz — C++ rapidgzip is gzip-focused).
mapfile -t corpora < <(find "$corpus_dir" -maxdepth 1 -type f \( -name '*.gz' -o -name '*.bgz' \) | sort)
if [[ ${#corpora[@]} -eq 0 ]]; then
    echo "no corpora found in $corpus_dir" >&2
    exit 1
fi
echo "==> corpora: ${corpora[*]}"

# --- results dir -----------------------------------------------------------

ts=$(date -u +%Y%m%dT%H%M%SZ)
results_base=${results_base:-target/bench-results}
results_dir=$results_base/$ts
mkdir -p "$results_dir"
echo "==> results: $results_dir"

# Record environment snapshot
{
    echo "timestamp_utc=$ts"
    echo "ci=$ci"
    echo "threads=$threads"
    echo "runs=$runs"
    echo "warmups=$warmups"
    echo "modes=$modes"
    echo "rust_binary=$rust_binary"
    echo "rust_version=$(rustc --version 2>/dev/null || true)"
    echo "RAPIDGZIP_CPP=${RAPIDGZIP_CPP:-}"
    echo "RAPIDGZIP_CPP_ISAL=${RAPIDGZIP_CPP_ISAL:-}"
    echo "RAPIDGZIP_CPP_ZLIB_NG=${RAPIDGZIP_CPP_ZLIB_NG:-}"
    echo "TASKSET=${TASKSET:-}"
    echo "SKIP_CPP=${SKIP_CPP:-0}"
    echo "hostname=$(hostname 2>/dev/null || true)"
    echo "uname=$(uname -a 2>/dev/null || true)"
    if [[ -x "$rust_binary" ]]; then
        echo "rust_cli=$("$rust_binary" --version 2>/dev/null || true)"
    fi
    if command -v rapidgzip > /dev/null 2>&1; then
        echo "cpp_rapidgzip=$(rapidgzip --version 2>/dev/null | head -1 || true)"
    fi
} > "$results_dir/env.txt"

# --- matrix (verify) -------------------------------------------------------

echo "==> run-matrix.sh --mode verify"
benchmarks/run-matrix.sh \
    --mode verify \
    --tsv "$results_dir/matrix-verify.tsv" \
    --json "$results_dir/matrix-verify.json" \
    "${corpora[@]}" \
    > /dev/null

# --- parity multi-mode -----------------------------------------------------

if [[ $skip_parity -eq 0 ]]; then
    echo "==> parity-compare.sh --modes $modes"
    # shellcheck disable=SC2086
    benchmarks/parity-compare.sh \
        --modes "$modes" \
        --tsv "$results_dir/parity.tsv" \
        --json "$results_dir/parity.json" \
        "${corpora[@]}" \
        > /dev/null
fi

# --- markdown summary ------------------------------------------------------

summary_md=$results_dir/SUMMARY.md
python3 - "$results_dir" "$summary_md" <<'PY'
import json
import sys
from pathlib import Path

results_dir = Path(sys.argv[1])
out_path = Path(sys.argv[2])

lines = []
lines.append("# Fair benchmark summary")
lines.append("")
lines.append(f"Results directory: `{results_dir}`")
lines.append("")

def load_json(name):
    p = results_dir / name
    if not p.is_file():
        return None
    return json.loads(p.read_text(encoding="utf-8"))

def fmt(v, nd=1):
    if v is None:
        return "—"
    if isinstance(v, float):
        return f"{v:.{nd}f}"
    return str(v)

matrix = load_json("matrix-verify.json")
if matrix and matrix.get("cells"):
    lines.append("## Verify mode (`-t` / `--verify`)")
    lines.append("")
    lines.append("Median decoded throughput (MiB/s) and median peak RSS (MiB).")
    lines.append("")
    # Pivot: rows = tool@corpus, cols = threads
    cells = matrix["cells"]
    threads = sorted({c["threads"] for c in cells})
    corpora = sorted({c["corpus"] for c in cells})
    tools = sorted({c["tool"] for c in cells})
    by = {(c["corpus"], c["tool"], c["threads"]): c for c in cells}

    for corpus in corpora:
        lines.append(f"### `{corpus}`")
        header = "| tool | " + " | ".join(f"{t} thr MiB/s" for t in threads) + " | " + " | ".join(f"{t} thr RSS" for t in threads) + " |"
        sep = "|---|" + "|".join("---:" for _ in threads) + "|" + "|".join("---:" for _ in threads) + "|"
        lines.append(header)
        lines.append(sep)
        for tool in tools:
            thr_cols = []
            rss_cols = []
            any_cell = False
            for t in threads:
                c = by.get((corpus, tool, t))
                if c:
                    any_cell = True
                thr_cols.append(fmt(c["median_mib_per_second"] if c else None, 1))
                rss_mib = None
                if c and c.get("median_max_rss_kib") is not None:
                    rss_mib = c["median_max_rss_kib"] / 1024.0
                rss_cols.append(fmt(rss_mib, 1))
            if not any_cell:
                continue
            lines.append("| " + tool + " | " + " | ".join(thr_cols) + " | " + " | ".join(rss_cols) + " |")
        lines.append("")

parity = load_json("parity.json")
if parity and parity.get("cells"):
    lines.append("## Parity modes")
    cells = parity["cells"]
    modes = sorted({c["mode"] for c in cells})
    threads = sorted({c["threads"] for c in cells})
    tools = sorted({c["tool"] for c in cells})
    corpora = sorted({c["corpus"] for c in cells})
    by = {(c["corpus"], c["mode"], c["tool"], c["threads"]): c for c in cells}

    for corpus in corpora:
        lines.append(f"### `{corpus}`")
        for mode in modes:
            lines.append(f"#### mode `{mode}`")
            header = "| tool | " + " | ".join(f"{t} thr MiB/s" for t in threads) + " | " + " | ".join(f"{t} thr RSS MiB" for t in threads) + " |"
            sep = "|---|" + "|".join("---:" for _ in threads) + "|" + "|".join("---:" for _ in threads) + "|"
            lines.append(header)
            lines.append(sep)
            for tool in tools:
                thr_cols = []
                rss_cols = []
                any_cell = False
                for t in threads:
                    c = by.get((corpus, mode, tool, t))
                    if c:
                        any_cell = True
                    thr_cols.append(fmt(c["median_mib_per_second"] if c else None, 1))
                    rss_mib = None
                    if c and c.get("median_max_rss_kib") is not None:
                        rss_mib = c["median_max_rss_kib"] / 1024.0
                    rss_cols.append(fmt(rss_mib, 1))
                if not any_cell:
                    continue
                lines.append("| " + tool + " | " + " | ".join(thr_cols) + " | " + " | ".join(rss_cols) + " |")
            lines.append("")

lines.append("## Fairness notes")
lines.append("- Same thread budgets, warmup count, and measured run count for every tool.")
lines.append("- Verify mode enables CRC checks on both tools (`-t` Rust; `-t --verify` or `--verify -c -f` C++).")
lines.append("- Medians over N runs; wall time via `EPOCHREALTIME`, peak RSS via `/usr/bin/time -f %M`.")
lines.append("- ISA-L vs zlib-rs is an intentional backend difference; report separate C++ builds when available.")
lines.append("- Stdin mode is sequential on both implementations (not a parallel-scaling cell).")

text = "\n".join(lines) + "\n"
out_path.write_text(text, encoding="utf-8")
print(text)
PY

echo "==> done. Summary: $summary_md"
echo "    TSV: $results_dir/matrix-verify.tsv"
if [[ $skip_parity -eq 0 ]]; then
    echo "    Parity: $results_dir/parity.tsv"
fi
