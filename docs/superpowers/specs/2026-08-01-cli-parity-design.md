# Command-line parity with rapidgzip

Date: 2026-08-01
Status: approved design
Scope: sub-project 4a of 4 replacing PR #5 (COMBINE-lab/rapidgzip-rust)

## Background

`rapidgzip-rust` currently accepts five options: `-P`, `-c`, `-o`, `-t`, and an
input path. Reference rapidgzip 0.16.0 accepts twenty-two. Everything the
library learned in sub-projects 1 through 3, random-access indexes, four index
formats, zlib and raw DEFLATE, is unreachable from the command line.

This branch closes that gap for everything except `--analyze`, which is
sub-project 4b and needs a DEFLATE block walker in the core library.

This branch stacks on `inflate-backends` (PR #10), which stacks on
`multi-format` (PR #9), which stacks on `index-and-seek` (PR #8).

## Goals

- Accept every rapidgzip 0.16.0 option, with matching short and long names.
- Reach the index, seeking, and container features from the command line.
- Add line counting to the core library, which three of those options need and
  which no decode path currently performs.
- Keep the CLI crate in focused modules rather than one growing file.

## Non-goals

- `--analyze`. Sub-project 4b.
- Compression. The tool remains decode-only, so `-d` and `-k` stay accepted
  no-ops as they are in rapidgzip itself.
- Reproducing rapidgzip's optimization of skipping the decode entirely when
  output goes to `/dev/null`. We always decode. The difference is documented
  because it changes what `rapidgzip-rust file.gz > /dev/null` costs.

## A gap this design has to fill first

`Checkpoint::line_offset` and `GzipIndex::total_line_count` exist in
`index/mod.rs`. The gztool reader populates both. **Every decode path writes
zero.** Exporting `--index-format gztool-with-lines` from an index this crate
built therefore produces an index full of zero line counters, which gztool
accepts and then misuses.

So the core library gains line counting, which `-l`, `--ranges` with line
addressing, and correct gztool-with-lines export all require.

### Core additions

`DecoderBuilder::count_lines(bool)`, off by default. When on:

- The coordinator counts `\n` in each ordered chunk as it emits it. Counting
  happens there, not in the workers, because the marker path's chunks contain
  16-bit marker symbols until resolution and a marker can resolve to `\n`.
  Counting after resolution is correct for every path and costs one pass over
  bytes the coordinator already holds.
- `DecodeReport` gains `line_count: Option<u64>`, `Some` only when counting
  was requested.
- When indexing is also on, each checkpoint records the line offset at its
  decompressed offset and the finished index records `total_line_count`.

`IndexedReader::seek_to_line(u64) -> io::Result<u64>` seeks to the first byte
of a given zero-based line and returns its decompressed byte offset. It selects
the checkpoint at or before that line, resumes there, and scans forward
counting newlines. It returns an error when the index carries no line offsets,
which is the honest answer rather than a scan from the start.

## Options

Names and defaults follow rapidgzip 0.16.0 exactly. Options marked *(new)* have
no rapidgzip counterpart and cover this crate's extra containers.

| Option | Behaviour |
| --- | --- |
| `-c`, `--stdout` | Write to standard output. |
| `-o`, `--output PATH` | Write to a file. |
| `-f`, `--force` | Overwrite an existing output file. |
| `-k`, `--keep` | Accepted no-op. This tool never deletes input. |
| `-d`, `--decompress` | Accepted no-op. Decoding is all it does. |
| `-t`, `--test` | Verify without retaining output. |
| `-P`, `--decoder-parallelism N` | Worker budget. `0` means automatic. |
| `--ranges SPEC` | Extract byte or line ranges. |
| `--count` | Print the decompressed size. |
| `-l`, `--count-lines` | Print the newline count. |
| `--export-index PATH` | Write an index built during this decode. |
| `--import-index PATH` | Use an existing index. |
| `--index-format FORMAT` | `indexed_gzip` (default), `gztool`, `gztool-with-lines`, `native` *(new)*, `gzi` *(new)*. |
| `--format FORMAT` *(new)* | `auto` (default), `gzip`, `zlib`, `raw-deflate`. |
| `--chunk-size KIB` | Parallel chunk size, default 4096. |
| `--verify` / `--no-verify` | `--verify` accepted; `--no-verify` rejected, see below. |
| `--io-read-method M` | Accepted no-op, see below. |
| `--sparse-windows` / `--no-sparse-windows` | Accepted no-op, see below. |
| `-q`, `--quiet` / `-v`, `--verbose` | Message volume. |
| `--oss-attributions` / `--oss-attributions-yaml` | Print dependency licenses. |
| `-h`, `--help` / `-V`, `--version` | Standard. |

`-P`/`--threads` stays as an alias of `--decoder-parallelism`, since it is the
name this tool has shipped. `0` means automatic, so it leaves
`DecoderBuilder::decoder_threads` unset rather than passing zero, which the
builder rejects.

### The three honest no-ops

`--io-read-method` selects between `pread`, `sequential`, and `locked-read`.
This crate has exactly one read strategy, positional reads through `ReadAt`,
and its streaming path is chosen by whether the source is seekable, not by a
flag. All three values are accepted and ignored.

`--sparse-windows` zeroes index-window bytes the following stream never
references, shrinking the index. This crate does not compute that reachability
yet; the machinery to do so is the back-reference analysis in sub-project 4b.
Both flags are accepted, both produce a dense window, and a dense index is
fully valid and interoperable, just larger.

`--verify` is accepted and ignored because verification is unconditional here.
`--no-verify` is rejected rather than silently ignored: a user asking to skip
checksums is asking for something this decoder structurally cannot do, and
accepting it would imply a speedup that does not exist.

### Output destination

With no `-o` and no `-c`, rapidgzip derives the output path: the input name
with `.gz` stripped, else `<input>.out`, and standard output when reading
standard input. We follow that. Writing to an existing file fails unless
`-f` is given. Writing to a terminal without `-c` or `-o` fails, as
decompressed binary on a terminal is nobody's intent.

### Range syntax

`--ranges 10@0,1KiB@15KiB,5L@20L,inf@40L`

A comma-separated list of `SIZE@OFFSET`. Each of `SIZE` and `OFFSET` is
independently either a byte quantity, a decimal integer with an optional
`B`, `KiB`, `MiB`, `GiB`, `TiB`, `PiB`, or `EiB` suffix, or a line quantity,
a decimal integer with an `L` suffix. `SIZE` may also be `inf`, meaning to the
end of the input. Ranges are emitted in the order given, concatenated, and may
overlap.

Line-addressed ranges need an index carrying line offsets. When one is
imported without them, or when the input is not seekable, the command fails
with a message naming the reason rather than silently decoding from the start.

## CLI structure

`main.rs` is 126 lines and this roughly triples it. Split by responsibility:

| File | Responsibility |
| --- | --- |
| `main.rs` | Argument definitions, dispatch, exit codes. |
| `source.rs` | Input classification, output path derivation, overwrite rules. |
| `ranges.rs` | Range parsing and extraction through `IndexedReader`. |
| `index.rs` | Index format selection, import, export. |
| `report.rs` | `--count`, `--count-lines`, `--test`, verbose and quiet output. |
| `attributions.rs` | Embedded license text. |

The two pieces with real logic, range parsing and output path derivation, are
pure functions with unit tests, so the integration tests do not have to
enumerate their edge cases.

## Errors

Exit code 0 on success, 1 on failure, and 0 on a broken pipe, which the current
CLI already treats as normal. A failure prints one line to standard error
prefixed `rapidgzip-rust:`. `--quiet` suppresses everything except that line.
`--verbose` adds the decode path taken, the worker count, and the elapsed time.

## Testing

`crates/rapidgzip-rust-cli/tests/cli.rs` drives the built binary through
`std::process::Command` and `CARGO_BIN_EXE_rapidgzip-rust`, adding no
dependency. It covers:

- each output mode, including derived output names and the overwrite rules;
- `--count` and `--count-lines` against known corpora;
- an index exported in every format and reimported, decoding to the same bytes;
- ranges: byte, line, mixed, `inf`, overlapping, and the two failure cases;
- `--format zlib` and `--format raw-deflate`;
- exit codes for corrupt input, a missing file, and an existing output file;
- `--no-verify` rejected, and each accepted no-op accepted.

Core-side, line counting is tested for agreement across the sequential,
parallel, BGZF, and streaming paths on the same corpus, and for round-tripping
through every index format. The interop CI job gains a gztool-with-lines case,
since real gztool reading our line counters is the check that they mean what
gztool thinks they mean.

## Delivery

One branch, `cli-parity`, stacked on `inflate-backends`, and one pull request.
Commits: line counting in core, indexed line seeking, the module split, the
option surface, ranges, index import and export, then documentation.
