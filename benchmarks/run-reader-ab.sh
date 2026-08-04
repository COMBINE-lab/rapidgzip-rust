#!/usr/bin/env bash
# Alternating A/B benchmark for the public programmatic reader on FASTQ data.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository=$(cd -- "$script_dir/.." && pwd)

corpus_dir=
baseline=
candidate=
threads="1 4 16"
buffers="8192 1048576"
corpus_selection="fastq-single fastq-sparse-members fastq-dense-members fastq-bgzf"
runs=9
warmups=2
delay_micros=0
stop_after=all
iterations=1
reader_mode=ordinary
cpus=${TASKSET_CPUS:-}
results_dir=

usage() {
    cat <<'EOF'
usage: benchmarks/run-reader-ab.sh OPTIONS

Required:
  --corpus-dir DIR    generated corpus directory containing manifest.tsv
  --baseline PATH     reader_decode binary built from the baseline revision
  --candidate PATH    reader_decode binary built from the candidate revision

Matrix:
  --corpora "LIST"    manifest corpus names (default: four FASTQ gzip shapes)
  --threads "LIST"    configured decoder-worker counts (default: "1 4 16")
  --buffers "LIST"    consumer Read buffer sizes in bytes (default: "8192 1048576")
  --runs N            measured alternating pairs per cell (default: 9)
  --warmups N         untimed runs per implementation and cell (default: 2)
  --delay-micros N    sleep after each successful Read (default: 0)
  --stop-after N      call finish after consuming N bytes (default: all)
  --iterations N      archives decoded per timed process (default: 1)
  --reader-mode MODE  ordinary or indexed (default: ordinary)
  --cpus LIST         taskset CPU list applied to both implementations
  --results-dir DIR   explicit new output directory
EOF
}

while (($#)); do
    case $1 in
        --corpus-dir|--baseline|--candidate|--corpora|--threads|--buffers|--runs|--warmups|--delay-micros|--stop-after|--iterations|--reader-mode|--cpus|--results-dir)
            (($# >= 2)) || { echo "$1 requires a value" >&2; exit 2; }
            case $1 in
                --corpus-dir) corpus_dir=$2 ;;
                --baseline) baseline=$2 ;;
                --candidate) candidate=$2 ;;
                --corpora) corpus_selection=$2 ;;
                --threads) threads=$2 ;;
                --buffers) buffers=$2 ;;
                --runs) runs=$2 ;;
                --warmups) warmups=$2 ;;
                --delay-micros) delay_micros=$2 ;;
                --stop-after) stop_after=$2 ;;
                --iterations) iterations=$2 ;;
                --reader-mode) reader_mode=$2 ;;
                --cpus) cpus=$2 ;;
                --results-dir) results_dir=$2 ;;
            esac
            shift 2
            ;;
        --help|-h) usage; exit 0 ;;
        *) echo "unknown argument: $1" >&2; usage >&2; exit 2 ;;
    esac
done

[[ -n $corpus_dir ]] || { echo "--corpus-dir is required" >&2; exit 2; }
[[ -x $baseline ]] || { echo "baseline is not executable: $baseline" >&2; exit 2; }
[[ -x $candidate ]] || { echo "candidate is not executable: $candidate" >&2; exit 2; }
[[ $runs =~ ^[1-9][0-9]*$ ]] || { echo "--runs must be nonzero" >&2; exit 2; }
[[ $warmups =~ ^[0-9]+$ ]] || { echo "--warmups must be an integer" >&2; exit 2; }
[[ $delay_micros =~ ^[0-9]+$ ]] || { echo "--delay-micros must be an integer" >&2; exit 2; }
[[ $iterations =~ ^[1-9][0-9]*$ ]] || { echo "--iterations must be nonzero" >&2; exit 2; }
[[ $reader_mode == ordinary || $reader_mode == indexed ]] || {
    echo "--reader-mode must be ordinary or indexed" >&2
    exit 2
}
[[ $stop_after == all || $stop_after =~ ^[0-9]+$ ]] || {
    echo "--stop-after must be 'all' or an integer" >&2
    exit 2
}
for thread_count in $threads; do
    [[ $thread_count =~ ^[1-9][0-9]*$ ]] || { echo "invalid thread count: $thread_count" >&2; exit 2; }
done
for buffer_bytes in $buffers; do
    [[ $buffer_bytes =~ ^[1-9][0-9]*$ ]] || { echo "invalid buffer size: $buffer_bytes" >&2; exit 2; }
done
[[ -n $corpus_selection ]] || { echo "--corpora must not be empty" >&2; exit 2; }
[[ -n ${EPOCHREALTIME:-} ]] || { echo "Bash EPOCHREALTIME is required" >&2; exit 2; }
command -v /usr/bin/time >/dev/null 2>&1 || { echo "GNU time is required" >&2; exit 2; }
[[ $(/usr/bin/time --version 2>&1) == *GNU* ]] || { echo "GNU time is required" >&2; exit 2; }
if [[ -n $cpus ]]; then
    command -v taskset >/dev/null 2>&1 || { echo "taskset is required for --cpus" >&2; exit 2; }
fi

corpus_dir=$(realpath -- "$corpus_dir")
baseline=$(realpath -- "$baseline")
candidate=$(realpath -- "$candidate")
manifest="$corpus_dir/manifest.tsv"
[[ -r $manifest ]] || { echo "missing corpus manifest: $manifest" >&2; exit 2; }
expected_header=$'corpus\tpath\tformat\tcompressed_bytes\tdecoded_bytes\tmember_count\tseed\tparameters\tcross_tool'
IFS= read -r manifest_header < "$manifest"
[[ $manifest_header == "$expected_header" ]] || { echo "unexpected corpus manifest header" >&2; exit 2; }

if [[ -z $results_dir ]]; then
    results_dir="$repository/target/reader-ab-results/$(date -u +%Y%m%dT%H%M%SZ)-$$"
fi
[[ ! -e $results_dir ]] || { echo "result directory already exists: $results_dir" >&2; exit 2; }
mkdir -p -- "$results_dir/logs"
results_dir=$(realpath -- "$results_dir")

summarizer=${SUMMARIZE_RESULTS:-$repository/target/release/summarize_results}
if [[ ! -x $summarizer ]]; then
    cargo build --release --locked -p rapidgzip-bench --bin summarize_results
fi

declare -A selected=() corpus_path=() corpus_decoded=() corpus_members=()
for corpus in $corpus_selection; do
    [[ -z ${selected[$corpus]:-} ]] || { echo "duplicate corpus: $corpus" >&2; exit 2; }
    selected[$corpus]=requested
done
while IFS=$'\t' read -r name relative format compressed decoded members seed parameters cross extra; do
    [[ -n $name ]] || continue
    [[ -z ${extra:-} ]] || { echo "too many manifest fields for $name" >&2; exit 2; }
    [[ ${selected[$name]:-} == requested ]] || continue
    [[ $format == gzip ]] || { echo "$name is not gzip" >&2; exit 2; }
    path="$corpus_dir/$relative"
    [[ -r $path ]] || { echo "missing corpus file: $path" >&2; exit 2; }
    [[ $(stat -c %s -- "$path") == "$compressed" ]] || { echo "$name compressed size mismatch" >&2; exit 2; }
    selected[$name]=found
    corpus_path[$name]=$(realpath -- "$path")
    corpus_decoded[$name]=$decoded
    corpus_members[$name]=$members
done < <(tail -n +2 "$manifest")
for corpus in $corpus_selection; do
    [[ ${selected[$corpus]} == found ]] || { echo "corpus not found in manifest: $corpus" >&2; exit 2; }
done

raw="$results_dir/raw.tsv"
environment="$results_dir/environment.tsv"
corpora="$results_dir/corpora.tsv"
printf 'timestamp_utc\tcorpus\tmode\ttool\tbackend\tthreads\trepetition\torder\twall_seconds\tuser_seconds\tsystem_seconds\tmax_rss_kib\tdecoded_bytes\tdecoded_mib_per_second\texit_status\tstatus\tstdout_log\tstderr_log\n' > "$raw"
{
    printf 'key\tvalue\n'
    printf 'runner\tprogrammatic-reader-ab-v1\n'
    printf 'started_utc\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'git_revision\t%s\n' "$(git -C "$repository" rev-parse HEAD 2>/dev/null || printf unavailable)"
    printf 'baseline\t%s\n' "$baseline"
    printf 'candidate\t%s\n' "$candidate"
    printf 'visible_cpus\t%s\n' "$(nproc)"
    printf 'affinity\t%s\n' "${cpus:-unrestricted}"
    printf 'threads\t%s\n' "$threads"
    printf 'buffers\t%s\n' "$buffers"
    printf 'delay_micros\t%s\n' "$delay_micros"
    printf 'stop_after\t%s\n' "$stop_after"
    printf 'iterations\t%s\n' "$iterations"
    printf 'reader_mode\t%s\n' "$reader_mode"
    printf 'runs\t%s\n' "$runs"
    printf 'warmups\t%s\n' "$warmups"
    printf 'uname\t%s\n' "$(uname -a)"
    printf 'rustc\t%s\n' "$(rustc --version)"
} > "$environment"
{
    printf 'corpus\tpath\tdecoded_bytes\tmember_count\n'
    for corpus in $corpus_selection; do
        printf '%s\t%s\t%s\t%s\n' "$corpus" "${corpus_path[$corpus]}" \
            "${corpus_decoded[$corpus]}" "${corpus_members[$corpus]}"
    done
} > "$corpora"

declare -a affinity=()
if [[ -n $cpus ]]; then
    affinity=(taskset -c "$cpus")
fi

build_command() {
    local binary=$1 input=$2 thread_count=$3 buffer_bytes=$4 decoded=$5
    command=("$binary" "$input" "$thread_count" "$buffer_bytes" "$decoded" "$delay_micros" "$stop_after" "$iterations" "$reader_mode")
}

run_warmup() {
    local binary=$1 input=$2 thread_count=$3 buffer_bytes=$4 decoded=$5 label=$6
    local error="$results_dir/logs/warmup-$label.stderr"
    build_command "$binary" "$input" "$thread_count" "$buffer_bytes" "$decoded"
    if ! "${affinity[@]}" "${command[@]}" > /dev/null 2> "$error"; then
        echo "warmup failed; see $error" >&2
        exit 1
    fi
    rm -f -- "$error"
}

failures=0
run_timed() {
    local corpus=$1 tool=$2 binary=$3 thread_count=$4 buffer_bytes=$5 repetition=$6 order=$7
    local input=${corpus_path[$corpus]} decoded=${corpus_decoded[$corpus]}
    local work_bytes=$((decoded * iterations))
    local mode="$reader_mode-read-${buffer_bytes}b-delay-${delay_micros}us-stop-${stop_after}-iterations-${iterations}"
    local prefix="$results_dir/logs/$corpus.$mode.$tool.t$thread_count.r$repetition"
    local timing="$prefix.time" stdout="$prefix.stdout" stderr="$prefix.stderr"
    local timestamp started finished elapsed exit_status status throughput timing_values
    local user_seconds=0 system_seconds=0 rss=0
    build_command "$binary" "$input" "$thread_count" "$buffer_bytes" "$decoded"
    timestamp=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
    started=$EPOCHREALTIME
    set +e
    /usr/bin/time -q -o "$timing" -f '%U %S %M' "${affinity[@]}" "${command[@]}" > "$stdout" 2> "$stderr"
    exit_status=$?
    set -e
    finished=$EPOCHREALTIME
    elapsed=$(awk -v started="$started" -v finished="$finished" 'BEGIN { printf "%.6f", finished - started }')
    if [[ -s $timing ]]; then
        timing_values=$(<"$timing")
        read -r user_seconds system_seconds rss <<< "$timing_values"
    fi
    if ((exit_status == 0)); then
        status=success
        throughput=$(awk -v bytes="$work_bytes" -v seconds="$elapsed" 'BEGIN { printf "%.6f", bytes / 1048576 / seconds }')
        rm -f -- "$timing" "$stdout" "$stderr"
        stdout=
        stderr=
    else
        status="exit-$exit_status"
        throughput=
        failures=$((failures + 1))
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$timestamp" "$corpus" "$mode" "$tool" gzip-rs "$thread_count" "$repetition" "$order" \
        "$elapsed" "$user_seconds" "$system_seconds" "$rss" "$work_bytes" "$throughput" \
        "$exit_status" "$status" "$stdout" "$stderr" >> "$raw"
}

cell=0
for corpus in $corpus_selection; do
    for thread_count in $threads; do
        for buffer_bytes in $buffers; do
            cell=$((cell + 1))
            for ((warmup = 1; warmup <= warmups; warmup++)); do
                run_warmup "$baseline" "${corpus_path[$corpus]}" "$thread_count" "$buffer_bytes" \
                    "${corpus_decoded[$corpus]}" "$corpus-main-$thread_count-$buffer_bytes-$warmup"
                run_warmup "$candidate" "${corpus_path[$corpus]}" "$thread_count" "$buffer_bytes" \
                    "${corpus_decoded[$corpus]}" "$corpus-candidate-$thread_count-$buffer_bytes-$warmup"
            done
            for ((repetition = 1; repetition <= runs; repetition++)); do
                if ((((cell + repetition) % 2) == 0)); then
                    run_timed "$corpus" main "$baseline" "$thread_count" "$buffer_bytes" "$repetition" 1
                    run_timed "$corpus" candidate "$candidate" "$thread_count" "$buffer_bytes" "$repetition" 2
                else
                    run_timed "$corpus" candidate "$candidate" "$thread_count" "$buffer_bytes" "$repetition" 1
                    run_timed "$corpus" main "$baseline" "$thread_count" "$buffer_bytes" "$repetition" 2
                fi
            done
        done
    done
done

"$summarizer" --input "$raw" --summary-tsv "$results_dir/summary.tsv" \
    --summary-markdown "$results_dir/SUMMARY.md" --environment "$environment" --corpora "$corpora"
echo "reader A/B results: $results_dir"
if ((failures != 0)); then
    echo "$failures measured command(s) failed" >&2
    exit 1
fi
