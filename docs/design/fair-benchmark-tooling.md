# Reproducible fair benchmark tooling

Status: implemented by PR #23

Origin: the corpus generator, parity comparison, and one-shot fair runner in
PR #5

## Decision

The repository's current `benchmarks/run-matrix.sh` is the retained real-FASTQ
comparison used during optimization. It correctly names explicit Rust, C++
ISA-L, C++ zlib-ng, and gzippy competitors and records wall time, CPU time,
peak RSS, and optional decoded throughput. It is intentionally small, however,
and is not yet a complete release benchmark workflow.

A deterministic corpus generator, correctness preflight, multi-mode runner,
environment capture, and reproducible summary are warranted. The tooling
should be implemented without changing decoder code, adding a publishable
dependency, downloading data, or checking generated corpora and result
artifacts into Git.

PR #5's scripts are useful prototypes, but should not be copied wholesale.
The replacement must distinguish true BGZF from ordinary concatenated gzip,
must not guess a backend by scanning binary strings, and must retain failed
runs instead of silently constructing a summary from only successful rows.

## Problems in the current matrix

The current script deliberately accepts exactly one input and one timed mode.
For release qualification it lacks:

- generated single-member, sparse/dense multi-member, true BGZF, stored, and
  low-compression controls;
- a byte-for-byte or digest correctness check across competitors before time
  is recorded;
- automatic decoded-size resolution for throughput;
- explicit input hashes and tool versions alongside results;
- affinity, host topology, governor, and environment capture;
- interleaving or rotation to reduce fixed tool-order bias;
- indexed, stdout, and non-seekable-input modes;
- raw failure/status rows;
- median/failure summaries in machine-readable and Markdown forms; and
- a CI-light smoke mode that validates the harness without treating shared CI
  timing as a performance gate.

It also relies on Bash `EPOCHREALTIME` and GNU `/usr/bin/time` without stating
the platform contract. Those choices are reasonable on the Linux release host,
but must be detected and reported rather than assumed silently.

## Problems in the PR #5 tooling

PR #5 adds a much broader runner, but its design has several weak points:

- the normal `bgzf-like.gz` corpus is concatenated ordinary gzip and lacks the
  mandatory BGZF `BC` extra subfield; it therefore may benchmark the dense-
  member route while being labeled BGZF;
- true BGZF is generated only when an external `bgzip` happens to be present;
- ISA-L identity is inferred by scanning executable/shared-object strings,
  which can mislabel statically stripped, dynamically loaded, or wrapper
  installations;
- corpus generation and summary require Python even though the workspace
  already contains deterministic zlib-rs fixture builders;
- the checked-in snapshot is tied to an obsolete aggregate branch, synthetic
  32 MiB inputs, and an ISA-L feature the project deliberately rejected;
- timed failures can be omitted from result rows, biasing medians toward the
  commands that happened to succeed; and
- several modes probe and mutate command syntax during the same workflow that
  is meant to produce comparable timing.

The clean runner resolves commands and validates every tool before any timed
cell begins.

## Scope and non-goals

The tooling measures decoding only. It does not:

- add compression to `rapidgzip-core`;
- download public sequencing data automatically;
- claim that generated corpora replace real FASTQ measurements;
- compare incompatible verification policies without labeling them;
- make noisy hosted-CI throughput a merge gate;
- auto-install C++, ISA-L, zlib-ng, gzippy, Python packages, or system tools;
  or
- retain large generated data or run artifacts in the repository.

The established release cells remain one, four, sixteen, and forty-four
requested decoder workers. Additional machine-scaled cells may be supplied,
but must not replace those historical comparison points in a release report.

## Components

### Deterministic Rust corpus generator

Add an unpublished `rapidgzip-bench` binary:

```text
cargo run --release -p rapidgzip-bench --bin generate_corpora -- \
    --output target/bench-corpora --decoded-mib 256 --seed 1
```

Reusable fixture construction moves from the Criterion source into a documented
module of the unpublished benchmark crate. It uses the workspace's existing
`libz-rs-sys` dependency for compression and the existing scalar fixture CRC
implementation. No new crate choice is required.

The generator emits:

| corpus | purpose |
|---|---|
| `fastq-single.gz` | one normal level-6 gzip member |
| `fastq-sparse-members.gz` | a small number of large ordinary members |
| `fastq-dense-members.gz` | many independently decodable ordinary members |
| `fastq.bgzf` | real BGZF blocks with `BC`/`BSIZE` and the canonical EOF block |
| `stored.gz` | stored-DEFLATE specialization control |
| `low-compression.gz` | pseudo-random fields that stress Huffman/match work |
| `fastq.zlib` | zlib framing control for Rust-only/container tests |
| `fastq.deflate` | raw-DEFLATE control for Rust-only/container tests |

Gzip headers, modification times, OS fields, compression level, member sizes,
BGZF block payloads, and pseudo-random generation are deterministic. Every BGZF
block must fit the 65,536-byte compressed block limit and carry a matching
`BSIZE`. Generation verifies every output with `rapidgzip-core` before the
manifest is written.

The generator writes `manifest.tsv` with corpus name, relative path, format,
compressed bytes, decoded bytes, expected member/stream count, seed, generation
parameters, and whether the input is eligible for cross-tool comparison. The
outer harness adds SHA-256 values using the required system digest command.
TSV avoids adding a serialization dependency merely for unpublished tooling.

Generated data lives under `target/`, which is already ignored.

### Tool registry

Competitor identity is explicit:

```text
RAPIDGZIP_RUST=/path/to/rapidgzip-rust
RAPIDGZIP_CPP_ISAL=/path/to/rapidgzip-with-isal
RAPIDGZIP_CPP_ZLIB_NG=/path/to/rapidgzip-with-zlib-ng
GZIPPY=/path/to/gzippy
```

No symbol scan changes those labels. Each configured path is resolved once,
checked for executability, queried for its version, and exercised on a tiny
known corpus. An absent optional competitor is recorded in `environment.tsv`;
a configured but failing competitor is a preflight failure rather than a
silent skip.

Command templates are versioned in the script and printed into the environment
record. The initial supported reference is C++ rapidgzip 0.16.x. Supporting a
different command-line version requires a deliberate template, not capability
probing inside timed runs.

### Correctness preflight

Before timing one input/mode combination, every participating tool decodes it
once to a temporary output or digest pipeline. The harness compares:

- decoded byte count;
- SHA-256 digest; and
- when available, tool-reported successful verification.

The digest process is outside timed cells. A mismatch aborts that corpus before
performance numbers can be mistaken for valid decoding. The expected digest
for an external FASTQ file is learned only after at least two independent tools
agree; Rust alone can be used in explicit `--rust-only` smoke mode but is
labeled accordingly.

Index mode builds each tool's own compatible index outside timed runs, then
checks the imported-index output against the same digest. Cross-import is a
separate interoperability test and is not mixed with performance unless the
index format and producer are named in the cell.

### Timed modes

The initial modes are:

| mode | timed work |
|---|---|
| `verify` | complete decode, integrity verification, decoded bytes discarded internally when supported |
| `stdout` | complete verified decode written to `/dev/null` |
| `indexed` | import a prebuilt per-tool index and perform complete verified decode |
| `stdin` | consume compressed input through standard input and write decoded output to `/dev/null` |

The command record states unavoidable policy differences. rapidgzip-rust always
verifies accepted container data. C++ receives its explicit verification flag.
If gzippy cannot provide equivalent trailer verification in a mode, its row is
labeled with that difference rather than presented as identical work.

Library `DecoderReader` and paraseq integration remain Criterion/benchmark-
binary workloads, because timing the CLI cannot represent the public
`Read + Send` handoff. Structural analysis also remains its own benchmark and
is not folded into decode parity.

### Fair run order

For each corpus, mode, and worker cell:

1. run the configured number of untimed warmups for every tool;
2. rotate the first tool deterministically for each measured repetition;
3. execute one sample per tool before starting the next repetition; and
4. synchronize only through normal process completion, without clearing the
   operating-system page cache.

Rotation reduces thermal, frequency, NUMA, and background-load bias compared
with running every Rust sample before every competitor sample. The raw table
retains run order and start timestamp so residual drift is visible.

Affinity is explicit through `--cpus` or `TASKSET_CPUS`. The same wrapper is
applied to every tool, warmup, correctness preflight where relevant, and timed
cell. The environment record includes affinity-visible processors and requested
worker count. Oversubscription is allowed but never hidden.

### Measurements and failures

Linux release runs require GNU `/usr/bin/time`. Each raw row contains:

- corpus, mode, tool, backend label, workers, repetition, and run-order rank;
- wall, user, and system seconds;
- maximum RSS in KiB;
- decoded bytes and MiB/s;
- exit status and a short status category; and
- references to captured stdout/stderr when the command failed.

Every attempted row is retained. A failed or timed-out command has empty
throughput and a non-success status; it is never dropped before summaries are
computed. The initial harness does not impose a timeout because valid large
release inputs and slow one-worker cells vary widely; an explicit caller
timeout can be recorded later.

Wall time uses one monotonic mechanism for every tool. If Bash
`EPOCHREALTIME` or GNU time is unavailable, release mode fails with a precise
prerequisite message. CI-smoke mode may use a reduced portable wall-only path,
but its rows are never combined with release results.

### Result directory

One invocation creates:

```text
target/bench-results/<UTC timestamp>/
    environment.tsv
    corpora.tsv
    commands.tsv
    parity.tsv
    raw.tsv
    summary.tsv
    SUMMARY.md
    failures/
```

`raw.tsv` is the source of truth. Summary generation reads it after all cells
finish and reports median wall time, throughput, CPU time, RSS, successful
sample count, and failed sample count. It does not overwrite raw observations
or invent values for missing tools.

An unpublished `summarize_results` Rust binary parses and validates the TSV,
groups rows, computes medians with deterministic ordering, and writes both
summary forms. This avoids a Python dependency and avoids non-portable `awk`
floating-point/statistics code. It uses only the standard library and rejects
malformed or inconsistent rows instead of skipping them.

`SUMMARY.md` includes the exact result directory, Git revision, dirty status,
host/affinity synopsis, tool versions, corpus hashes, modes, verification
differences, warmups, repetitions, and medians. It makes no universal
performance claim.

No result snapshot is checked in automatically. A maintainer may deliberately
copy a release table into `BENCHMARKING.md` with its environment and corpus
provenance. PR #5's obsolete ISA-L-enabled Rust snapshot is not carried over.

## Driver interface

The one-shot entry point is:

```text
benchmarks/run-fair.sh [OPTIONS] [INPUT.gz ...]

  --corpus-dir DIR
  --generate
  --decoded-mib N
  --threads "1 4 16 44"
  --modes "verify stdout indexed stdin"
  --runs N
  --warmups N
  --cpus LIST
  --results-dir DIR
  --rust-only
  --ci-smoke
```

`--generate` invokes the deterministic Rust generator. Caller-supplied inputs
are never copied into the repository. `--rust-only` is explicit because a
single implementation cannot establish cross-decoder parity. Release defaults
require at least one independent competitor.

The current `run-matrix.sh` remains as a compatibility wrapper for one release
cycle, translating its environment variables into `run-fair.sh verify` and
printing raw TSV. It is then removed only after documentation and downstream
automation have migrated.

## CI strategy

Hosted CI validates tooling, not speed:

1. build the release CLI and corpus generator;
2. generate small deterministic corpora twice and compare hashes;
3. decode every corpus and check manifest sizes/member counts;
4. run `bash -n` on shell entry points;
5. execute a Rust-only one/two-worker smoke matrix with one warmup and one
   measured sample; and
6. verify that raw rows, failure rows, and summaries parse deterministically.

The existing index interoperability job continues to exercise external tools.
It may run a tiny cross-tool preflight, but no hosted-CI timing threshold is
introduced.

## Implementation plan

1. Extract deterministic payload, gzip member, true BGZF, zlib, and raw fixture
   construction into the unpublished benchmark crate with unit tests.
2. Add `generate_corpora`, manifest writing, duplicate-generation hash tests,
   and decode self-verification.
3. Factor explicit tool templates and prerequisite/environment capture into a
   shell library used by one driver.
4. Add correctness/digest preflight and per-tool index preparation outside
   timed cells.
5. Add interleaved timed execution with raw failure rows and normalized GNU
   time parsing.
6. Add the standard-library-only Rust summarizer and deterministic TSV and
   Markdown summaries from raw rows.
7. Add CI-smoke coverage and migrate `run-matrix.sh` to a compatibility wrapper.
8. Run the complete public FASTQ and generated-corpus matrices, then document
   the results without checking in large data or transient artifacts.

## Acceptance gates

The tooling is accepted when:

- two generated runs with the same parameters are byte-identical;
- `fastq.bgzf` is structurally proven BGZF and reaches the intended decoder
  route;
- every performance corpus passes cross-tool decoded-size and SHA-256 parity
  before timing;
- explicit ISA-L, zlib-ng, and gzippy labels cannot be changed by heuristic
  detection;
- every attempted timed command produces a raw success or failure row;
- summary medians reproduce from `raw.tsv` alone;
- affinity, versions, commands, hashes, and verification differences are
  captured with the result;
- the current real-FASTQ invocation can migrate without losing the established
  1/4/16/44, throughput, CPU, and RSS fields; and
- CI proves harness correctness without enforcing noisy performance numbers.
