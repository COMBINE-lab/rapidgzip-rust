# DEFLATE analysis and the `--analyze` report

Date: 2026-08-01
Status: approved design
Scope: sub-project 4b of 4 replacing PR #5 (COMBINE-lab/rapidgzip-rust)

## Background

`rapidgzip --analyze` walks every DEFLATE block in a file and prints its
structure: header fields, block boundaries to the bit, Huffman alphabet shape,
symbol type ratios, back-reference reach, then file-wide distributions. It is
the tool people reach for when a gzip file decodes but behaves oddly.

Nothing in this crate exposes that, although `parallel/deflate.rs` already
contains the native DEFLATE decoder it needs: precode, literal, and distance
tree construction, and symbol decoding.

This branch stacks on `cli-parity` (sub-project 4a).

## Goals

- Add a block-walking analysis to the core library, returning structured data.
- Print it in rapidgzip 0.16.0's exact format, verified by diffing against the
  real tool.

## Non-goals

- Inventing a better report. The value here is being drop-in for anything that
  reads rapidgzip's output, so the format is not ours to improve. A structured
  form is available to library callers, who can format it as they wish.
- bzip2, which rapidgzip's analyzer also handles and this crate does not decode.

## Where the split falls

The core library owns the data and knows nothing about rapidgzip's text. The
CLI owns the text.

`Decoder::analyze<R: ReadAt>(&self, source: &R) -> Result<Analysis, DecodeError>`

`Analysis` is public, documented, and holds:

- per stream: container kind, header fields as parsed, compressed and
  decompressed start offsets, footer values, encoded and decoded sizes;
- per block: final flag, compression type, compressed offset and data offset in
  bits, decompressed offset, compressed size in bits, decompressed size,
  the three code-length count maps, literal and back-reference symbol counts,
  copied-symbol count, farthest back-reference distance, the count of
  back-references reaching into the preceding window, the merged count, and the
  used-window-symbol count;
- file-wide: block counts by compression type, and the vectors the
  distributions are computed from.

Per-block back-reference lists are aggregated, not retained, since a block can
hold tens of thousands. `AnalyzeOptions { retain_backreferences: bool }` keeps
them when the caller wants the detail `--verbose` prints, and that is the only
knob.

Analysis is single-threaded and sequential by nature: every block's statistics
depend on the window the previous blocks produced.

## The report format

The authority is rapidgzip 0.16.0, and the differential test below is what
enforces it. The pieces that are subtle enough to state here:

**Offsets.** `formatBits(v)` is `"{v/8} B {v%8} b"`. `formatBytes(v)` walks
EiB down to B emitting `"{(v/unit)%1024} {unit}"` for each non-zero remainder,
space separated, and returns `"0 B"` when every remainder is zero. So
`31 KiB 180 B`, not `31.2 KiB`.

**Floats.** C++ streams default to `%.6g`, giving `3.29108` and `1.6955`.
Histogram bin labels switch to `%.6e` when the value is not integral, giving
`1.935158e+05`. A helper reproducing both is a unit-tested pure function.

**Histograms.** Eight bins. For integral types, the bin count shrinks to
`max - min + 1` when that is smaller. A value lands in bin
`floor((v - min) / (max - min) * bins)`, except that `v == max` lands in the
last bin. Bars are 20 characters of `=`, scaled to the largest bin, left
aligned in a 20-wide field. Only the first bin, the last bin, and the largest
bin carry labels, right aligned to the widest label. Each line is
`label + " |" + bar + " " + count`, where the count is `(n)` when non-zero and
empty otherwise, so most lines end in a trailing space.

**Section order.** Stream headers, blocks, and footers interleave in file
order. Then the benchmark profile, alphabet statistics, the code-length
distributions, farthest back-references, the back-reference length table, the
window symbol usage histogram, encoded and decoded block size distributions,
the compression ratio distribution, stream size distributions, and the
compression type counts. Every section after the first is conditional on
having more than one sample, exactly as the reference is.

### The one section that cannot match

```
== Benchmark Profile (Cumulative Times) ==

readDynamicHuffmanCoding : 0.000133919 s (1.6955 %)
readData                 : 0.00776458 s (98.3045 %)
```

These are wall-clock durations. We emit the section with the same labels and
shape, filled with durations we actually measure, so a consumer finds it where
it expects it. The differential test masks these lines. No other section
differs, and the documentation says so in exactly these terms rather than
claiming unqualified byte-for-byte equality.

## Testing

`crates/rapidgzip-rust-cli/tests/analyze_interop.rs`, ignored by default, runs
`rapidgzip --analyze` and ours over the same file and diffs, masking the
benchmark-profile durations. Corpora: single member, multi-member, BGZF,
fully stored, fixed Huffman, a file mixing all three block types, zlib, raw
DEFLATE, empty input, and a file whose gzip header carries a name, comment,
extra field, and header CRC. The existing `interop` CI job installs
`rapidgzip==0.16.0` from PyPI, alongside the indexed_gzip it already installs.

Unit tests cover the formatting primitives directly, since a histogram or float
helper that is wrong only on an edge case would otherwise surface as an opaque
diff: `formatBytes` at zero and at unit boundaries, `%.6g` and `%.6e`
reproduction, and histogram bucketing when all values are equal, when the
range is smaller than the bin count, and at the `max` boundary.

Core-side, analysis is checked against decoded output: summed block
decompressed sizes equal the total, block offsets are strictly increasing, and
the last block of each stream is marked final.

## Delivery

One branch, `analyze`, stacked on `cli-parity`, and one pull request.
Commits: the block walker and `Analysis` in core, the formatting primitives,
the report, the differential test and CI, then documentation.
