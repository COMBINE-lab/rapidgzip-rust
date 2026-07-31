# Safety

The crate denies unsafe operations inside unsafe functions:

```rust
#![deny(unsafe_op_in_unsafe_fn)]
```

There is no unsafe public API and no manual `Send` or `Sync` implementation.

## zlib-rs ABI adapter

`backend.rs` contains a private RAII wrapper around `libz-rs-sys`.

- `inflateInit2_` receives a live, uniquely borrowed `z_stream`, the matching
  Rust structure size, zlib-rs's static version string, and raw-window value
  `-15`.
- Every `inflate` call sets `next_in/avail_in` to a live immutable page and
  `next_out/avail_out` to either a live unique `Vec<u8>` allocation or its spare
  capacity. Neither allocation moves during the call. Consumed and produced
  lengths come only from the backend's reduced `avail_*` fields. When spare
  capacity is used, `Vec::set_len` exposes exactly the byte count zlib-rs
  reported initialized and never more than the proven capacity.
- `inflatePrime` supplies at most seven unread low-order bits from one source
  byte before the first inflate call.
- `inflateSetDictionary` receives an immutable slice no larger than 32 KiB.
- `inflateReset` receives the same uniquely owned initialized stream between
  completed BGZF blocks. It preserves the raw-window mode and is never called
  concurrently with `inflate`.
- `inflateEnd` runs exactly once for each successfully initialized stream.
- `crc32_z` receives a live immutable byte slice and uses its exact length.

The BGZF fast path reserves the footer-declared output size plus one byte,
passes only that spare capacity to zlib-rs, checks `Z_STREAM_END`, exact input
consumption, and exact output size, then calls `Vec::set_len` with the backend's
reported initialized byte count. The extra byte distinguishes an exact-size
decode from an output-buffer exhaustion condition, including empty EOF blocks.

## Native DEFLATE bit loads

The hot bit reader performs an unaligned `u64` load only after proving that
eight bytes remain beginning at the requested offset. `read_unaligned` removes
the alignment requirement; conversion with `to_le` gives the RFC 1951 stream
bit order on every target. The short end-of-input path is entirely safe Rust.

## SIMD marker replacement

The x86_64 implementation is called only after runtime SSE4.1 detection. Each
unaligned 128-bit input load operates on an eight-symbol chunk proven to be
inside the source slice; each 64-bit output store has the same proof. Vectors
containing any marker use the scalar checked mapper.

The AArch64 implementation relies on baseline NEON. Its eight-lane loads and
stores use the same chunk bounds. Both implementations are differentially
tested against scalar replacement with mixed literal/marker input.
