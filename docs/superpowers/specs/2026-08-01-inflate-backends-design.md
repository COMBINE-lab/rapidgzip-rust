# Pluggable inflate backends

Date: 2026-08-01
Status: approved design
Scope: sub-project 3 of 4 replacing PR #5 (COMBINE-lab/rapidgzip-rust)

## Background

Every path in the crate inflates through zlib-rs. PR #5 added an ISA-L backend
behind a cargo feature, reaching it through an `InflateBackend` trait so call
sites stay shared. ISA-L is Intel's optimized library and is meaningfully
faster at raw inflate on x86-64, which matters for the paths that are not
parallel: the sequential fallback, single-stream zlib and raw DEFLATE, and
BGZF blocks.

This branch stacks on `multi-format` (PR #9), which stacks on `index-and-seek`
(PR #8).

## Goals

- Introduce a crate-internal `InflateBackend` trait so an alternative inflate
  implementation plugs in without touching call sites.
- Provide an ISA-L implementation behind an off-by-default `isal` feature.
- Measure the difference on a real corpus and report it honestly, including
  the case where there is none.

## Non-goals

- Replacing zlib-rs as the default. The default build keeps its current
  dependency set and behaviour exactly.
- Using ISA-L on the marker/window path. That path needs `inflatePrime` for a
  bit-accurate resume and zlib's `Z_BLOCK` contract to find block boundaries,
  neither of which ISA-L exposes. It stays on zlib-rs, as does
  `IndexedReader`, which resumes at arbitrary bit offsets.

## Which paths can switch

| Path | Backend | Why |
| --- | --- | --- |
| Sequential gzip members | pluggable | whole-stream inflate, no bit-level resume |
| Single-stream zlib and raw DEFLATE | pluggable | same |
| BGZF blocks | pluggable | each block is a complete stream |
| Estimated grid (marker/window) | zlib-rs | needs `inflatePrime` and `Z_BLOCK` |
| `IndexedReader` | zlib-rs | resumes at arbitrary bit offsets |

The split is deliberate and documented: a backend that cannot express the
bit-accurate contract must not be reachable from the paths that depend on it.

## Interface

```rust
pub(crate) trait InflateBackend: Sized {
    fn new() -> Result<Self, DecodeError>;
    fn reset(&mut self, bit_offset: u64) -> Result<(), DecodeError>;
    /// Inflates from `input` into the spare capacity of `output`.
    fn inflate(&mut self, input: &[u8], output: &mut Vec<u8>, finish: bool)
        -> Result<InflateStep, DecodeError>;
    fn message(&self) -> Option<String>;
}

pub(crate) struct InflateStep {
    pub outcome: InflateOutcome, // Progress | StreamEnd | NeedsMoreInput
    pub consumed: usize,
    pub produced: usize,
}
```

The outcome is an enum rather than a zlib status code, so a backend that does
not speak zlib's numbering does not have to fake one. `RawInflater` implements
it over zlib-rs; `IsalInflater` implements it over ISA-L. A type alias selects
which one the pluggable paths monomorphize against:

```rust
#[cfg(not(feature = "isal"))]
pub(crate) type ActiveInflater = RawInflater;
#[cfg(feature = "isal")]
pub(crate) type ActiveInflater = IsalInflater;
```

Call sites stay generic over `I: InflateBackend` where practical, so the
default build compiles to what it compiles to today.

## The ISA-L feature

`isal` is off by default and pulls `isal-sys`, which links a system `libisal`
rather than building ISA-L from source. Documentation states the requirement:
`libisal-dev` on Debian and Ubuntu, `isa-l` on Homebrew, or
`ISAL_INSTALL_PREFIX` pointing at a prefix containing the library.

ISA-L status codes map onto the outcome enum. Its dictionary call is
`isal_inflate_set_dict`. A member that ISA-L rejects is reported through the
existing `DeflateErrorKind` variants, so error handling above the backend is
unchanged.

CI gains a job that installs `libisal-dev` and runs the suite with
`--features isal`. The default jobs are untouched.

## Measurement

A criterion benchmark in `rapidgzip-bench` decodes the same corpora through
the sequential path with each backend. The pull request reports the measured
difference. If ISA-L does not win on the corpora that matter, that is the
finding, and the feature stays off by default either way.

## Testing

- The existing suite runs unchanged with the default backend, which is the
  regression net for the refactor.
- The same suite runs under `--features isal`, so both backends decode every
  corpus identically, including corrupt inputs, which must fail the same way.
- A test asserts the paths that need bit-accurate resume still use zlib-rs
  even when the feature is on, by decoding with an index and seeking.

## Delivery

One branch, `inflate-backends`, stacked on `multi-format`, and one pull
request. Commits: the trait and the zlib-rs implementation, the call-site
switch, the ISA-L backend, the CI job, then the benchmark and documentation.
