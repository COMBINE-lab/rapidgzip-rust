# Safety

The crate denies unsafe operations inside unsafe functions:

```rust
#![deny(unsafe_op_in_unsafe_fn)]
```

There is no unsafe public API and no manual `Send` or `Sync` implementation.

## zlib-rs ABI adapter

`backend.rs` contains a private RAII wrapper (`RawInflater`) around
`libz-rs-sys`. Paths that reach inflate through the crate-private
`InflateBackend` trait in `inflate_backend.rs` (monomorphized to `RawInflater`;
no `dyn`; no extra unsafe beyond the same zlib-rs contracts):

- sequential positional gzip/zlib/raw and streaming `stream_decode`;
- structure analysis (`analyze`) with `InflateFlush::Block`;
- parallel BGZF workers (`create`/`reset`/trait `inflate` with `Finish`);
- multi-stream zlib index/decode workers (`create`/`reset`/`inflate` /
  `inflate_capped`);
- independent-member workers (`create`/`reset`/`inflate_capped` for the
  per-member decoded budget);
- estimated-path residual continue (`inflate_from_block`) and `inflate_tail`
  (`inflate_capped` with `NoFlush` / `Block`);
- seek / `indexed_decode` (`inflate_into_slice` into fixed caller `out`
  slices; member restarts use trait `create`/`set_dictionary`).

**Direct `z::inflate` lives only in** `inflate_backend.rs` (the
`RawInflater` implementor of `inflate_capped` / `inflate_into_slice`).
**Lifecycle ABI** (`inflateInit2_`, `inflateReset`, `inflatePrime`,
`inflateSetDictionary`, `inflateEnd`) lives only in `RawInflater` inherent
methods in `backend.rs`. Call sites in sequential, stream, analyze, BGZF,
independent-member, multi-stream zlib, estimated residual, seek, and
indexed_decode paths use the trait surface only.

With the optional **`isal`** feature, `ActiveInflater` is `IsalInflater`
(`isal_backend.rs`): raw inflate goes through `isal_inflate` /
`isal_inflate_init` / `reset` / `set_dict` (system or prefix `libisal` via
`isal-sys`). `InflateFlush::Block` still uses an internal zlib-rs
`RawInflater`. Default builds without `isal` link only zlib-rs.

- `inflateInit2_` receives a live, uniquely borrowed `z_stream`, the matching
  Rust structure size, zlib-rs's static version string, and raw-window value
  `-15`.
- Every `inflate` call sets `next_in/avail_in` to a live immutable page and
  `next_out/avail_out` to either a live unique `Vec<u8>` allocation, its spare
  capacity, or a fixed caller-owned output slice. Neither allocation moves
  during the call. Consumed and produced lengths come only from the backend's
  reduced `avail_*` fields. When spare capacity is used, `Vec::set_len`
  exposes exactly the byte count zlib-rs reported initialized and never more
  than the proven capacity. The `InflateBackend::inflate` /
  `inflate_capped` path always writes into spare capacity (optionally further
  capped by `max_produce`) and extends length by the reported produced count
  only. `inflate_into_slice` writes into a fixed slice (does not re-length
  the destination); `produced` is the `avail_out` reduction.
- `inflatePrime` supplies at most seven unread low-order bits from one source
  byte before the first inflate call.
- `inflateSetDictionary` receives an immutable slice no larger than 32 KiB.
- `inflateReset` receives the same uniquely owned initialized stream between
  completed BGZF blocks, independent ordinary gzip members, sequential
  multi-member gzip/zlib paths (and multi-stream zlib indexing), and parallel
  multi-stream zlib workers. It preserves the raw-window mode, clears inflate
  history so the next member starts with an empty window, and is never called
  concurrently with `inflate`.
- `inflateEnd` runs exactly once for each successfully initialized stream.
- `crc32_z` receives a live immutable byte slice and uses its exact length.
- `compress_z` / `uncompress_z` (index window zlib helpers in
  `index/mod.rs`, used for in-memory keep_index windows and gztool on-disk
  window payloads) receive a live exclusive destination buffer sized to
  `compressBound_z` or 32 KiB and a live immutable source slice; only the
  backend-reported length is retained after success.

The BGZF fast path reserves the footer-declared output size plus one byte and
inflates via `InflateBackend::inflate` with `InflateFlush::Finish` (which
extends length by the backend-reported produced count only). It checks
`Z_STREAM_END`, exact input consumption, and exact output size. The extra
spare byte distinguishes an exact-size decode from an output-buffer
exhaustion condition, including empty EOF blocks.

The dense ordinary-member path supplies live immutable input chunks and
hard-caps newly produced bytes via `InflateBackend::inflate_capped` beneath
its per-member output bound. It exposes exactly the initialized byte count
reported by zlib-rs, verifies that the stream reached `Z_STREAM_END`, and
authenticates CRC32 and ISIZE before the coordinator can emit the buffer.

## SIMD gzip-header scan

The x86-64 header prefilter calls its AVX2 implementation only after runtime
feature detection. Each unaligned 32-byte load is guarded by
`offset + 32 <= candidate_limit <= input.len()`. The resulting bit mask only
selects byte offsets for safe Rust to validate; it never establishes a gzip
boundary or bypasses full member inflation and trailer verification.
The AArch64 NEON implementation similarly guards each 16-byte input load and
stores its comparison lanes only into a live local 16-byte array.

## Native DEFLATE bit loads

The hot bit reader and Huffman peek perform an unaligned `u64` load only after
proving that eight bytes remain beginning at the requested offset.
`read_unaligned` removes the alignment requirement; conversion with `to_le`
gives the RFC 1951 stream bit order on every target. A read requests at most 24
bits and a peek at most 15, so either fits in the loaded word even at the
largest seven-bit starting shift. The short end-of-input path is entirely safe
Rust and retains the authoritative EOF checks.

## SIMD marker replacement

The x86_64 implementation is called only after runtime SSE4.1 detection. Each
unaligned 128-bit input load operates on an eight-symbol chunk proven to be
inside the source slice; each 64-bit output store has the same proof. Vectors
containing any marker use the scalar checked mapper.

The AArch64 implementation relies on baseline NEON. Its eight-lane loads and
stores use the same chunk bounds. Both implementations are differentially
tested against scalar replacement with mixed literal/marker input.

Large marker buffers with a complete predecessor window use a branch-free
64 KiB lookup table. The output vector reserves exactly one byte per input
symbol. A safe loop writes every corresponding `MaybeUninit<u8>` slot in its
spare capacity exactly once; only then does one `Vec::set_len` expose precisely
that initialized count. The allocation does not move during the loop and no
uninitialized byte is read.
