#!/usr/bin/env bash
# Reproducible cross-tool decode benchmark driver for Linux release hosts.
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
repository=$(cd -- "$script_dir/.." && pwd)

corpus_dir=
generate=false
decoded_mib=256
threads="1 4 16 44"
modes="verify stdout indexed stdin"
runs=9
warmups=2
cpus=${TASKSET_CPUS:-}
results_dir=
rust_only=false
ci_smoke=false
declare -a inputs=()

usage() {
    cat <<'EOF'
usage: benchmarks/run-fair.sh [OPTIONS] [INPUT.gz ...]

  --corpus-dir DIR   use a generated corpus manifest
  --generate         generate deterministic corpora before running
  --decoded-mib N    decoded MiB per generated corpus (default: 256)
  --threads "LIST"   requested decoder-worker cells (default: "1 4 16 44")
  --modes "LIST"     verify, stdout, indexed, and/or stdin
  --runs N           measured repetitions per cell (default: 9)
  --warmups N        untimed repetitions per tool and cell (default: 2)
  --cpus LIST        taskset CPU list applied to every decode command
  --results-dir DIR  explicit output directory
  --rust-only        permit labeled single-implementation parity
  --ci-smoke         small Rust-only harness validation
EOF
}

while (($#)); do
    case $1 in
        --corpus-dir|--decoded-mib|--threads|--modes|--runs|--warmups|--cpus|--results-dir)
            (($# >= 2)) || { echo "$1 requires a value" >&2; exit 2; }
            case $1 in
                --corpus-dir) corpus_dir=$2 ;;
                --decoded-mib) decoded_mib=$2 ;;
                --threads) threads=$2 ;;
                --modes) modes=$2 ;;
                --runs) runs=$2 ;;
                --warmups) warmups=$2 ;;
                --cpus) cpus=$2 ;;
                --results-dir) results_dir=$2 ;;
            esac
            shift 2
            ;;
        --generate) generate=true; shift ;;
        --rust-only) rust_only=true; shift ;;
        --ci-smoke) ci_smoke=true; shift ;;
        --help|-h) usage; exit 0 ;;
        --*) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
        *) inputs+=("$1"); shift ;;
    esac
done

if $ci_smoke; then
    rust_only=true
    decoded_mib=2
    threads="1 2"
    modes="verify stdout"
    runs=1
    warmups=1
    generate=true
fi

[[ $decoded_mib =~ ^[1-9][0-9]*$ ]] || { echo "--decoded-mib must be nonzero" >&2; exit 2; }
[[ $runs =~ ^[1-9][0-9]*$ ]] || { echo "--runs must be nonzero" >&2; exit 2; }
[[ $warmups =~ ^[0-9]+$ ]] || { echo "--warmups must be an integer" >&2; exit 2; }
declare -A seen_threads=() seen_modes=()
for thread in $threads; do
    [[ $thread =~ ^[1-9][0-9]*$ ]] || { echo "invalid thread cell: $thread" >&2; exit 2; }
    [[ -z ${seen_threads[$thread]:-} ]] || { echo "duplicate thread cell: $thread" >&2; exit 2; }
    seen_threads[$thread]=1
done
for mode in $modes; do
    case $mode in verify|stdout|indexed|stdin) ;; *) echo "invalid mode: $mode" >&2; exit 2 ;; esac
    [[ -z ${seen_modes[$mode]:-} ]] || { echo "duplicate mode: $mode" >&2; exit 2; }
    seen_modes[$mode]=1
done
((${#seen_threads[@]} > 0)) || { echo "--threads must select at least one cell" >&2; exit 2; }
((${#seen_modes[@]} > 0)) || { echo "--modes must select at least one mode" >&2; exit 2; }

if [[ -z $corpus_dir && ${#inputs[@]} -eq 0 ]] && ! $generate; then
    echo "provide --generate, --corpus-dir, or at least one input" >&2
    exit 2
fi
if $generate && [[ -z $corpus_dir ]]; then
    corpus_dir="$repository/target/bench-corpora"
fi
if [[ -z $results_dir ]]; then
    results_dir="$repository/target/bench-results/$(date -u +%Y%m%dT%H%M%SZ)-$$"
fi

require_tsv_safe() {
    local label=$1 value=$2
    if [[ $value == *$'\t'* || $value == *$'\r'* || $value == *$'\n'* ]]; then
        echo "$label contains a tab or newline and cannot be represented safely in TSV" >&2
        exit 2
    fi
}
require_tsv_safe "result directory" "$results_dir"
[[ -z $corpus_dir ]] || require_tsv_safe "corpus directory" "$corpus_dir"

require_command() {
    command -v -- "$1" >/dev/null 2>&1 || { echo "required command is unavailable: $1" >&2; exit 2; }
}

require_command awk
require_command sha256sum
require_command realpath
require_command /usr/bin/time
time_version=$(/usr/bin/time --version 2>&1) || {
    echo "unable to query /usr/bin/time" >&2
    exit 2
}
if [[ ${time_version,,} != *gnu*time* ]]; then
    echo "release benchmarks require GNU /usr/bin/time" >&2
    exit 2
fi
[[ -n ${EPOCHREALTIME:-} ]] || { echo "this runner requires Bash EPOCHREALTIME" >&2; exit 2; }
if [[ -n $cpus ]]; then
    require_command taskset
fi

rust_binary=${RAPIDGZIP_RUST:-$repository/target/release/rapidgzip-rust}
generator=${GENERATE_CORPORA:-$repository/target/release/generate_corpora}
summarizer=${SUMMARIZE_RESULTS:-$repository/target/release/summarize_results}
if [[ ! -x $rust_binary || ! -x $generator || ! -x $summarizer ]]; then
    cargo build --release --locked -p rapidgzip-rust-cli -p rapidgzip-bench \
        --bin rapidgzip-rust --bin generate_corpora --bin summarize_results
fi

resolve_executable() {
    local requested=$1
    local resolved
    if [[ $requested == */* ]]; then
        [[ -x $requested ]] || return 1
        realpath -- "$requested"
    else
        resolved=$(command -v -- "$requested") || return 1
        realpath -- "$resolved"
    fi
}

declare -a tools=(rapidgzip-rust)
declare -A tool_binary tool_backend tool_version
tool_binary[rapidgzip-rust]=$(resolve_executable "$rust_binary") || {
    echo "Rust decoder is not executable: $rust_binary" >&2
    exit 2
}
tool_backend[rapidgzip-rust]=gzip-rs

register_optional() {
    local name=$1
    local backend=$2
    local requested=$3
    [[ -n $requested ]] || return 0
    local resolved
    resolved=$(resolve_executable "$requested") || {
        echo "configured $name decoder is not executable: $requested" >&2
        exit 2
    }
    tools+=("$name")
    tool_binary[$name]=$resolved
    tool_backend[$name]=$backend
}

register_optional rapidgzip-cpp-isal isa-l "${RAPIDGZIP_CPP_ISAL:-}"
register_optional rapidgzip-cpp-zlib-ng zlib-ng "${RAPIDGZIP_CPP_ZLIB_NG:-}"
register_optional gzippy tool-default "${GZIPPY:-}"

for tool in "${tools[@]}"; do
    set +e
    version=$(${tool_binary[$tool]} --version 2>&1)
    version_status=$?
    set -e
    version=${version//$'\t'/ }
    version=${version//$'\n'/ ; }
    if ((version_status != 0)) || [[ -z $version ]]; then
        echo "configured tool failed --version: $tool (${tool_binary[$tool]})" >&2
        exit 2
    fi
    if [[ $tool == rapidgzip-cpp-* && $version != *0.16.* ]]; then
        echo "$tool has unsupported version (expected rapidgzip 0.16.x): $version" >&2
        exit 2
    fi
    tool_version[$tool]=$version
done

if [[ -e $results_dir ]]; then
    echo "result directory already exists; choose a new path: $results_dir" >&2
    exit 2
fi
mkdir -p -- "$results_dir/failures" "$results_dir/parity"
results_dir=$(realpath -- "$results_dir")
environment="$results_dir/environment.tsv"
corpora="$results_dir/corpora.tsv"
commands="$results_dir/commands.tsv"
parity="$results_dir/parity.tsv"
raw="$results_dir/raw.tsv"

sanitize() {
    local value=$1
    value=${value//$'\t'/ }
    value=${value//$'\r'/ }
    value=${value//$'\n'/ ; }
    printf '%s' "$value"
}

git_revision=$(git -C "$repository" rev-parse HEAD 2>/dev/null || printf unavailable)
git_dirty=false
[[ -z $(git -C "$repository" status --porcelain 2>/dev/null) ]] || git_dirty=true
{
    printf 'key\tvalue\n'
    printf 'runner_version\t1\n'
    printf 'started_utc\t%s\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)"
    printf 'repository\t%s\n' "$(sanitize "$repository")"
    printf 'result_directory\t%s\n' "$(sanitize "$results_dir")"
    printf 'git_revision\t%s\n' "$git_revision"
    printf 'git_dirty\t%s\n' "$git_dirty"
    printf 'uname\t%s\n' "$(sanitize "$(uname -a)")"
    if command -v lscpu >/dev/null 2>&1; then
        printf 'lscpu\t%s\n' "$(sanitize "$(lscpu)")"
    else
        printf 'lscpu\tunavailable\n'
    fi
    printf 'visible_cpus\t%s\n' "$(nproc)"
    if [[ -r /proc/self/status ]]; then
        printf 'process_allowed_cpus\t%s\n' "$(awk '/^Cpus_allowed_list:/ { print $2 }' /proc/self/status)"
    else
        printf 'process_allowed_cpus\tunavailable\n'
    fi
    printf 'affinity\t%s\n' "${cpus:-unrestricted}"
    printf 'threads\t%s\n' "$(sanitize "$threads")"
    printf 'modes\t%s\n' "$(sanitize "$modes")"
    printf 'runs\t%s\n' "$runs"
    printf 'warmups\t%s\n' "$warmups"
    printf 'rust_only\t%s\n' "$rust_only"
    printf 'ci_smoke\t%s\n' "$ci_smoke"
    printf 'rustc\t%s\n' "$(sanitize "$(rustc --version --verbose)")"
    printf 'cargo\t%s\n' "$(sanitize "$(cargo --version)")"
    printf 'gnu_time\t%s\n' "$(sanitize "$time_version")"
    printf 'RUSTFLAGS\t%s\n' "$(sanitize "${RUSTFLAGS:-}")"
    printf 'LD_LIBRARY_PATH\t%s\n' "$(sanitize "${LD_LIBRARY_PATH:-}")"
    if [[ -r /sys/devices/system/cpu/cpu0/cpufreq/scaling_governor ]]; then
        printf 'cpu0_governor\t%s\n' "$(</sys/devices/system/cpu/cpu0/cpufreq/scaling_governor)"
    else
        printf 'cpu0_governor\tunavailable\n'
    fi
    for tool in "${tools[@]}"; do
        printf 'tool.%s.path\t%s\n' "$tool" "$(sanitize "${tool_binary[$tool]}")"
        printf 'tool.%s.backend\t%s\n' "$tool" "${tool_backend[$tool]}"
        printf 'tool.%s.version\t%s\n' "$tool" "$(sanitize "${tool_version[$tool]}")"
    done
    for optional in RAPIDGZIP_CPP_ISAL RAPIDGZIP_CPP_ZLIB_NG GZIPPY; do
        if [[ -z ${!optional:-} ]]; then
            printf 'optional.%s\tnot-configured\n' "$optional"
        fi
    done
} > "$environment"

{
    printf 'tool\tbackend\tmode\tsupported\tverification_policy\tcommand_template\n'
    printf 'rapidgzip-rust\tgzip-rs\tverify\t1\tcontainer checks always enforced\trapidgzip-rust -P THREADS -t --format FORMAT INPUT\n'
    printf 'rapidgzip-rust\tgzip-rs\tstdout\t1\tcontainer checks always enforced\trapidgzip-rust -P THREADS -c --format FORMAT INPUT\n'
    printf 'rapidgzip-rust\tgzip-rs\tindexed\t1\tprebuilt native index; full verified decode\trapidgzip-rust -P THREADS -t --import-index INDEX INPUT\n'
    printf 'rapidgzip-rust\tgzip-rs\tstdin\t1\tcontainer checks always enforced\trapidgzip-rust -P THREADS -c --format FORMAT - < INPUT\n'
    for tool in rapidgzip-cpp-isal rapidgzip-cpp-zlib-ng; do
        [[ -n ${tool_binary[$tool]:-} ]] || continue
        printf '%s\t%s\tverify\t1\t--verify enabled\trapidgzip -t --verify -P THREADS INPUT\n' "$tool" "${tool_backend[$tool]}"
        printf '%s\t%s\tstdout\t1\t--verify enabled\trapidgzip -d --verify -c -f -P THREADS INPUT\n' "$tool" "${tool_backend[$tool]}"
        printf '%s\t%s\tindexed\t1\tprebuilt own index; --verify enabled\trapidgzip -d --import-index INDEX --verify -c -f -P THREADS INPUT\n' "$tool" "${tool_backend[$tool]}"
        printf '%s\t%s\tstdin\t1\t--verify enabled\trapidgzip -d --verify -c -f -P THREADS < INPUT\n' "$tool" "${tool_backend[$tool]}"
    done
    if [[ -n ${tool_binary[gzippy]:-} ]]; then
        printf 'gzippy\ttool-default\tverify\t1\ttool-default trailer policy; output discarded\tgzippy -d -c -p THREADS INPUT\n'
        printf 'gzippy\ttool-default\tstdout\t1\ttool-default trailer policy\tgzippy -d -c -p THREADS INPUT\n'
        printf 'gzippy\ttool-default\tindexed\t0\tunsupported\t\n'
        printf 'gzippy\ttool-default\tstdin\t1\ttool-default trailer policy\tgzippy -d -c -p THREADS < INPUT\n'
    fi
} > "$commands"

if $generate; then
    "$generator" --output "$corpus_dir" --decoded-mib "$decoded_mib" --seed 1
fi

declare -a corpus_names=()
declare -A corpus_path corpus_format corpus_compressed corpus_decoded corpus_members
declare -A corpus_seed corpus_parameters corpus_cross corpus_compressed_sha

add_corpus() {
    local name=$1 path=$2 format=$3 compressed=$4 decoded=$5 members=$6 seed=$7 parameters=$8 cross=$9
    [[ -n $name && -n $path && -n $format ]] || { echo "corpus metadata has an empty identity field" >&2; exit 2; }
    require_tsv_safe "corpus name" "$name"
    require_tsv_safe "corpus path" "$path"
    require_tsv_safe "corpus format" "$format"
    require_tsv_safe "corpus parameters" "$parameters"
    [[ -z ${corpus_path[$name]:-} ]] || { echo "duplicate corpus name: $name" >&2; exit 2; }
    [[ -r $path ]] || { echo "corpus is not readable: $path" >&2; exit 2; }
    local actual
    actual=$(stat -c %s -- "$path")
    if [[ $compressed != unknown && $actual != "$compressed" ]]; then
        echo "$name compressed size is $actual; manifest says $compressed" >&2
        exit 2
    fi
    corpus_names+=("$name")
    corpus_path[$name]=$(realpath -- "$path")
    corpus_format[$name]=$format
    corpus_compressed[$name]=$actual
    corpus_decoded[$name]=$decoded
    corpus_members[$name]=$members
    corpus_seed[$name]=$seed
    corpus_parameters[$name]=$parameters
    corpus_cross[$name]=$cross
    corpus_compressed_sha[$name]=$(sha256sum -- "$path" | cut -d' ' -f1)
}

if [[ -n $corpus_dir ]]; then
    manifest="$corpus_dir/manifest.tsv"
    [[ -r $manifest ]] || { echo "missing corpus manifest: $manifest" >&2; exit 2; }
    expected_header=$'corpus\tpath\tformat\tcompressed_bytes\tdecoded_bytes\tmember_count\tseed\tparameters\tcross_tool'
    IFS= read -r manifest_header < "$manifest"
    [[ $manifest_header == "$expected_header" ]] || { echo "unexpected manifest header" >&2; exit 2; }
    while IFS=$'\t' read -r name relative format compressed decoded members seed parameters cross extra; do
        [[ -n $name ]] || continue
        [[ -z ${extra:-} ]] || { echo "too many manifest fields for $name" >&2; exit 2; }
        [[ $format == gzip || $format == zlib || $format == raw-deflate ]] || { echo "invalid format for $name" >&2; exit 2; }
        [[ $cross == 0 || $cross == 1 ]] || { echo "invalid cross-tool flag for $name" >&2; exit 2; }
        add_corpus "$name" "$corpus_dir/$relative" "$format" "$compressed" "$decoded" "$members" "$seed" "$parameters" "$cross"
    done < <(tail -n +2 "$manifest")
fi

for input in "${inputs[@]}"; do
    base=$(basename -- "$input")
    name=${base%.gz}
    name=${name// /_}
    add_corpus "$name" "$input" gzip unknown unknown unknown external external 1
done
[[ ${#corpus_names[@]} -gt 0 ]] || { echo "no corpora selected" >&2; exit 2; }

printf 'corpus\tpath\tformat\tcompressed_bytes\tdecoded_bytes\tmember_count\tseed\tparameters\tcross_tool\tcompressed_sha256\tdecoded_sha256\tparity_scope\n' > "$corpora"
printf 'corpus\tmode\ttool\tbackend\tdecoded_bytes\tsha256\tstatus\tverification_policy\tindex_path\n' > "$parity"
printf '%s\n' 'timestamp_utc	corpus	mode	tool	backend	threads	repetition	order	wall_seconds	user_seconds	system_seconds	max_rss_kib	decoded_bytes	decoded_mib_per_second	exit_status	status	stdout_log	stderr_log' > "$raw"

declare -a affinity=()
if [[ -n $cpus ]]; then
    affinity=(taskset -c "$cpus")
fi
declare -a command=()
stdin_path=

format_argument() {
    case $1 in gzip) printf gzip ;; zlib) printf zlib ;; raw-deflate) printf raw-deflate ;; esac
}

mode_supported() {
    local tool=$1 mode=$2 format=$3
    [[ $tool == rapidgzip-rust ]] && return 0
    [[ $format == gzip ]] || return 1
    [[ $tool != gzippy || $mode == verify || $mode == stdout || $mode == stdin ]]
}

build_command() {
    local tool=$1 mode=$2 requested_threads=$3 input=$4 format=$5 index=${6:-}
    local binary=${tool_binary[$tool]}
    stdin_path=
    case $tool:$mode in
        rapidgzip-rust:verify) command=("$binary" -P "$requested_threads" -t --format "$(format_argument "$format")" "$input") ;;
        rapidgzip-rust:stdout) command=("$binary" -P "$requested_threads" -c --format "$(format_argument "$format")" "$input") ;;
        rapidgzip-rust:indexed) command=("$binary" -P "$requested_threads" -t --import-index "$index" "$input") ;;
        rapidgzip-rust:stdin) command=("$binary" -P "$requested_threads" -c --format "$(format_argument "$format")" -); stdin_path=$input ;;
        rapidgzip-cpp-*:verify) command=("$binary" -t --verify -P "$requested_threads" "$input") ;;
        rapidgzip-cpp-*:stdout) command=("$binary" -d --verify -c -f -P "$requested_threads" "$input") ;;
        rapidgzip-cpp-*:indexed) command=("$binary" -d --import-index "$index" --verify -c -f -P "$requested_threads" "$input") ;;
        rapidgzip-cpp-*:stdin) command=("$binary" -d --verify -c -f -P "$requested_threads"); stdin_path=$input ;;
        gzippy:verify|gzippy:stdout) command=("$binary" -d -c -p "$requested_threads" "$input") ;;
        gzippy:stdin) command=("$binary" -d -c -p "$requested_threads"); stdin_path=$input ;;
        *) echo "no command template for $tool $mode" >&2; return 2 ;;
    esac
}

build_parity_command() {
    local tool=$1 mode=$2 input=$3 format=$4 index=${5:-}
    if [[ $mode == stdin ]]; then
        build_command "$tool" stdin 1 "$input" "$format" "$index"
    elif [[ $mode == indexed ]]; then
        build_command "$tool" indexed 1 "$input" "$format" "$index"
        if [[ $tool == rapidgzip-rust ]]; then
            command[3]=-c
        fi
    else
        build_command "$tool" stdout 1 "$input" "$format" "$index"
    fi
}

run_redirected() {
    local output=$1 error=$2
    if [[ -n $stdin_path ]]; then
        "${affinity[@]}" "${command[@]}" < "$stdin_path" > "$output" 2> "$error"
    else
        "${affinity[@]}" "${command[@]}" > "$output" 2> "$error"
    fi
}

declare -A indexes
prepare_index() {
    local corpus=$1 tool=$2 input=$3 format=$4
    local key="$corpus|$tool"
    [[ -z ${indexes[$key]:-} ]] || return 0
    local safe=${corpus//[^A-Za-z0-9_.-]/_}
    local index="$results_dir/parity/$safe.$tool.index"
    local error="$results_dir/parity/$safe.$tool.index.stderr"
    case $tool in
        rapidgzip-rust)
            command=("${tool_binary[$tool]}" -P 1 -t --format "$(format_argument "$format")" --export-index "$index" --force "$input")
            ;;
        rapidgzip-cpp-*)
            command=("${tool_binary[$tool]}" -d -c -f -P 1 --export-index "$index" "$input")
            ;;
        *) return 2 ;;
    esac
    stdin_path=
    if ! run_redirected /dev/null "$error"; then
        echo "index preparation failed for $corpus with $tool; see $error" >&2
        exit 2
    fi
    indexes[$key]=$index
}

declare -A expected_digest expected_size
for corpus in "${corpus_names[@]}"; do
    input=${corpus_path[$corpus]}
    format=${corpus_format[$corpus]}
    declare -a corpus_tools=()
    for tool in "${tools[@]}"; do
        if [[ $tool == rapidgzip-rust || ${corpus_cross[$corpus]} == 1 ]]; then
            corpus_tools+=("$tool")
        fi
    done
    if ! $rust_only && [[ ${corpus_cross[$corpus]} == 1 && ${#corpus_tools[@]} -lt 2 ]]; then
        echo "$corpus requires an independent configured decoder; use --rust-only only for smoke testing" >&2
        exit 2
    fi
    baseline_digest=
    baseline_size=
    for mode in $modes; do
        declare -a participants=()
        for tool in "${corpus_tools[@]}"; do
            mode_supported "$tool" "$mode" "$format" && participants+=("$tool")
        done
        ((${#participants[@]} > 0)) || continue
        for tool in "${participants[@]}"; do
            index=
            if [[ $mode == indexed ]]; then
                prepare_index "$corpus" "$tool" "$input" "$format"
                index=${indexes["$corpus|$tool"]}
            fi
            safe=${corpus//[^A-Za-z0-9_.-]/_}
            output="$results_dir/parity/$safe.$mode.$tool.decoded"
            error="$results_dir/parity/$safe.$mode.$tool.stderr"
            build_parity_command "$tool" "$mode" "$input" "$format" "$index"
            if ! run_redirected "$output" "$error"; then
                printf '%s\t%s\t%s\t%s\t\t\tfailed\tpreflight decode failed\t%s\n' \
                    "$corpus" "$mode" "$tool" "${tool_backend[$tool]}" "$index" >> "$parity"
                echo "correctness preflight failed for $corpus/$mode with $tool; see $error" >&2
                exit 2
            fi
            size=$(stat -c %s -- "$output")
            digest=$(sha256sum -- "$output" | cut -d' ' -f1)
            if [[ ${corpus_decoded[$corpus]} != unknown && $size != "${corpus_decoded[$corpus]}" ]]; then
                echo "$tool emitted $size bytes for $corpus; manifest expects ${corpus_decoded[$corpus]}" >&2
                exit 2
            fi
            if [[ -n $baseline_digest && ($digest != "$baseline_digest" || $size != "$baseline_size") ]]; then
                echo "decoded parity mismatch for $corpus/$mode with $tool" >&2
                exit 2
            fi
            baseline_digest=${baseline_digest:-$digest}
            baseline_size=${baseline_size:-$size}
            policy='container verification requested or intrinsic'
            [[ $tool != gzippy ]] || policy='tool-default trailer policy'
            printf '%s\t%s\t%s\t%s\t%s\t%s\tsuccess\t%s\t%s\n' \
                "$corpus" "$mode" "$tool" "${tool_backend[$tool]}" "$size" "$digest" "$policy" "$index" >> "$parity"
            rm -f -- "$output" "$error"
        done
    done
    [[ -n $baseline_digest ]] || { echo "no supported modes for $corpus" >&2; exit 2; }
    expected_digest[$corpus]=$baseline_digest
    expected_size[$corpus]=$baseline_size
    scope=cross-tool
    if $rust_only; then scope=rust-only; elif [[ ${corpus_cross[$corpus]} == 0 ]]; then scope=rust-control; fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$corpus" "${corpus_path[$corpus]}" "$format" "${corpus_compressed[$corpus]}" "$baseline_size" \
        "${corpus_members[$corpus]}" "${corpus_seed[$corpus]}" "${corpus_parameters[$corpus]}" \
        "${corpus_cross[$corpus]}" "${corpus_compressed_sha[$corpus]}" "$baseline_digest" "$scope" >> "$corpora"
done

run_warmup() {
    local tool=$1 mode=$2 requested_threads=$3 input=$4 format=$5 index=$6 error=$7
    build_command "$tool" "$mode" "$requested_threads" "$input" "$format" "$index"
    if ! run_redirected /dev/null "$error"; then
        echo "warmup failed for $tool/$mode/$requested_threads; see $error" >&2
        exit 2
    fi
    rm -f -- "$error"
}

timed_failures=0
run_timed() {
    local corpus=$1 tool=$2 mode=$3 requested_threads=$4 repetition=$5 order=$6 input=$7 format=$8 index=$9
    local safe=${corpus//[^A-Za-z0-9_.-]/_}
    local prefix="$results_dir/failures/$safe.$mode.$tool.t$requested_threads.r$repetition"
    local timing="$prefix.time"
    local error="$prefix.stderr"
    local timestamp started finished elapsed timing_values user_seconds system_seconds rss throughput status exit_status
    build_command "$tool" "$mode" "$requested_threads" "$input" "$format" "$index"
    timestamp=$(date -u +%Y-%m-%dT%H:%M:%S.%NZ)
    started=$EPOCHREALTIME
    set +e
    if [[ -n $stdin_path ]]; then
        /usr/bin/time -q -o "$timing" -f '%U %S %M' "${affinity[@]}" "${command[@]}" < "$stdin_path" > /dev/null 2> "$error"
    else
        /usr/bin/time -q -o "$timing" -f '%U %S %M' "${affinity[@]}" "${command[@]}" > /dev/null 2> "$error"
    fi
    exit_status=$?
    set -e
    finished=$EPOCHREALTIME
    elapsed=$(awk -v started="$started" -v finished="$finished" 'BEGIN { printf "%.6f", finished - started }')
    timing_values=$(<"$timing")
    read -r user_seconds system_seconds rss <<< "$timing_values"
    if ((exit_status == 0)); then
        status=success
        throughput=$(awk -v bytes="${expected_size[$corpus]}" -v seconds="$elapsed" 'BEGIN { printf "%.6f", bytes / 1048576 / seconds }')
        rm -f -- "$timing" "$error"
        stdout_log=
        stderr_log=
    else
        status="exit-$exit_status"
        throughput=
        stdout_log=
        stderr_log=$error
        timed_failures=$((timed_failures + 1))
    fi
    printf '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' \
        "$timestamp" "$corpus" "$mode" "$tool" "${tool_backend[$tool]}" "$requested_threads" \
        "$repetition" "$order" "$elapsed" "$user_seconds" "$system_seconds" "$rss" \
        "${expected_size[$corpus]}" "$throughput" "$exit_status" "$status" "$stdout_log" "$stderr_log" >> "$raw"
}

for corpus in "${corpus_names[@]}"; do
    input=${corpus_path[$corpus]}
    format=${corpus_format[$corpus]}
    for mode in $modes; do
        declare -a participants=()
        for tool in "${tools[@]}"; do
            if { [[ $tool == rapidgzip-rust || ${corpus_cross[$corpus]} == 1 ]]; } && mode_supported "$tool" "$mode" "$format"; then
                participants+=("$tool")
            fi
        done
        ((${#participants[@]} > 0)) || continue
        for requested_threads in $threads; do
            for tool in "${participants[@]}"; do
                index=${indexes["$corpus|$tool"]:-}
                for ((warmup = 1; warmup <= warmups; warmup++)); do
                    warmup_error="$results_dir/failures/${corpus//[^A-Za-z0-9_.-]/_}.$mode.$tool.t$requested_threads.w$warmup.stderr"
                    run_warmup "$tool" "$mode" "$requested_threads" "$input" "$format" "$index" "$warmup_error"
                done
            done
            count=${#participants[@]}
            for ((repetition = 1; repetition <= runs; repetition++)); do
                first=$(((repetition - 1) % count))
                for ((rank = 0; rank < count; rank++)); do
                    position=$(((first + rank) % count))
                    tool=${participants[$position]}
                    index=${indexes["$corpus|$tool"]:-}
                    run_timed "$corpus" "$tool" "$mode" "$requested_threads" "$repetition" "$((rank + 1))" "$input" "$format" "$index"
                done
            done
        done
    done
done

"$summarizer" --input "$raw" --summary-tsv "$results_dir/summary.tsv" \
    --summary-markdown "$results_dir/SUMMARY.md" --environment "$environment" --corpora "$corpora" \
    --commands "$commands" --parity "$parity"

echo "benchmark results: $results_dir"
if ((timed_failures > 0)); then
    echo "$timed_failures timed command(s) failed; all attempts remain in raw.tsv" >&2
    exit 1
fi
