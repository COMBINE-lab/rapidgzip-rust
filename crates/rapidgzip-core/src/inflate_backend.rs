//! Crate-private sequential raw-inflate backend abstraction.
//!
//! Sequential and block-oriented paths talk to inflate through [`InflateBackend`]
//! so alternate implementations can be monomorphized in without changing call
//! sites. Default builds use zlib-rs [`RawInflater`]. With the `isal` feature,
//! [`ActiveInflater`] is ISA-L's [`crate::isal_backend::IsalInflater`].
//!
//! Prefer generics (`I: InflateBackend`) over `dyn` so the hot path stays
//! zero-cost relative to a concrete inflater type.

use crate::backend::RawInflater;
use crate::error::DecodeError;
use crate::parallel::Window;
use libz_rs_sys as z;

/// Flush mode for one sequential inflate step.
///
/// Maps to zlib `Z_NO_FLUSH` / `Z_BLOCK` / `Z_FINISH`. Other backends should
/// honor the same semantics (stream continuously, surface DEFLATE block
/// boundaries, or finish a complete member in one call).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum InflateFlush {
    /// Ordinary streaming inflate.
    NoFlush,
    /// Prefer stopping at DEFLATE block boundaries (used for `keep_index`).
    Block,
    /// Finish the current stream (used for sized one-shot members such as BGZF).
    Finish,
}

impl InflateFlush {
    #[inline]
    const fn to_zlib(self) -> i32 {
        match self {
            Self::NoFlush => z::Z_NO_FLUSH,
            Self::Block => z::Z_BLOCK,
            Self::Finish => z::Z_FINISH,
        }
    }
}

/// Outcome of one [`InflateBackend::inflate`] / [`InflateBackend::inflate_capped`] /
/// [`InflateBackend::inflate_into_slice`] call.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct InflateStep {
    /// Backend status code (zlib-compatible: `Z_OK`, `Z_STREAM_END`, …).
    pub status: i32,
    /// Compressed bytes consumed from the supplied input slice.
    pub consumed: usize,
    /// Decompressed bytes appended to the output buffer.
    pub produced: usize,
    /// Bits still held in the inflater bit buffer (zlib `data_type & 0x3f`).
    /// Meaningful for [`InflateFlush::Block`] checkpointing.
    pub unused_bits: u8,
    /// True when inflate stopped at a DEFLATE block end (zlib `data_type & 0x80`).
    pub at_block_end: bool,
    /// True when the block that just ended was BFINAL (zlib `data_type & 0x40`).
    /// Meaningful for [`InflateFlush::Block`] structure walks (e.g. analyze).
    pub last_block: bool,
}

/// Sequential/block raw-inflate operations used by gzip/zlib/raw decode loops.
///
/// Minimal surface so zlib-rs [`RawInflater`] implements it by thin wrappers
/// around existing inherent methods plus a single buffered `inflate` step.
///
/// Mid-stream resume (bit seeks, estimated-tail continue, index checkpoints)
/// goes through [`Self::prime`] + [`Self::set_dictionary`], or the
/// [`Self::install_bit_resume`] / [`Self::prepare_at_bit_offset`] helpers.
pub(crate) trait InflateBackend: Sized {
    /// Create a fresh raw-inflate stream (`windowBits = -15` semantics).
    fn create() -> Result<Self, DecodeError>;

    /// Reset history for a new independent member / stream at `bit_offset`.
    fn reset(&mut self, bit_offset: u64) -> Result<(), DecodeError>;

    /// Install remaining mid-byte bits before the first inflate after a bit seek.
    ///
    /// `bits` is the count of still-unread bits in the first compressed byte
    /// (1..=7 for a true mid-byte start; `0` is a no-op). `value` holds those
    /// bits in the low-order positions (zlib `inflatePrime` convention).
    fn prime(&mut self, bits: u8, value: u8, bit_offset: u64) -> Result<(), DecodeError>;

    /// Install predecessor window history (empty window is a no-op).
    fn set_dictionary(&mut self, window: &Window, bit_offset: u64) -> Result<(), DecodeError>;

    /// One inflate step with a hard cap on newly produced output bytes.
    ///
    /// Consumes from `input` and appends into spare capacity of `output`, but
    /// never initializes more than `min(spare, max_produce)` new bytes this
    /// call (zlib backends set `avail_out` accordingly). Caller reserves
    /// capacity; this method extends `output` by `produced` only.
    /// Status codes are zlib-compatible so existing match arms stay shared.
    ///
    /// Used by independent-member workers to enforce the per-member decoded
    /// budget without touching raw stream fields at the call site.
    fn inflate_capped(
        &mut self,
        input: &[u8],
        output: &mut Vec<u8>,
        flush: InflateFlush,
        max_produce: usize,
    ) -> Result<InflateStep, DecodeError>;

    /// One inflate step: consume from `input`, write into a fixed `output` slice.
    ///
    /// Unlike [`Self::inflate`] / [`Self::inflate_capped`], this does **not**
    /// grow or re-length the destination — `produced` is the number of bytes
    /// written into the front of `output` (backend `avail_out` reduction).
    /// Input and output lengths are capped to `u32::MAX` for zlib-compatible
    /// backends. Used by seek / indexed paths that pre-size destination buffers.
    fn inflate_into_slice(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        flush: InflateFlush,
    ) -> Result<InflateStep, DecodeError>;

    /// One inflate step: consume from `input`, append into spare capacity of `output`.
    ///
    /// Default: [`Self::inflate_capped`] with an unlimited produce budget
    /// (still bounded by spare capacity and backend `uInt` limits).
    fn inflate(
        &mut self,
        input: &[u8],
        output: &mut Vec<u8>,
        flush: InflateFlush,
    ) -> Result<InflateStep, DecodeError> {
        self.inflate_capped(input, output, flush, usize::MAX)
    }

    /// Optional backend diagnostic string (zlib `msg`); may be ignored.
    fn message(&self) -> Option<String>;

    /// Install mid-byte prime bits and predecessor dictionary on an existing stream.
    ///
    /// Call after [`Self::create`] or [`Self::reset`]. When `bit_offset` is not
    /// byte-aligned, `first_byte` holds the already-consumed low bits; remaining
    /// high bits are primed. When byte-aligned, `first_byte` is ignored.
    ///
    /// After success, subsequent inflate input must start at the first **full**
    /// compressed byte still unread (`bit_offset / 8` when aligned, else
    /// `bit_offset / 8 + 1` — the primed byte must not be fed again).
    fn install_bit_resume(
        &mut self,
        first_byte: u8,
        bit_offset: u64,
        window: &Window,
    ) -> Result<(), DecodeError> {
        let skipped_bits = (bit_offset % 8) as u8;
        if skipped_bits != 0 {
            let remaining_bits = 8 - skipped_bits;
            self.prime(remaining_bits, first_byte >> skipped_bits, bit_offset)?;
        }
        self.set_dictionary(window, bit_offset)?;
        Ok(())
    }

    /// Create a raw inflater ready to resume at an absolute compressed bit offset.
    ///
    /// Default: [`Self::create`] then [`Self::install_bit_resume`]. See that
    /// method for `first_byte` / cursor semantics. Concrete backends may override
    /// if they have a faster combined setup path.
    fn prepare_at_bit_offset(
        first_byte: u8,
        bit_offset: u64,
        window: &Window,
    ) -> Result<Self, DecodeError> {
        let mut inflater = Self::create()?;
        inflater.install_bit_resume(first_byte, bit_offset, window)?;
        Ok(inflater)
    }
}

impl InflateBackend for RawInflater {
    #[inline]
    fn create() -> Result<Self, DecodeError> {
        Self::new()
    }

    #[inline]
    fn reset(&mut self, bit_offset: u64) -> Result<(), DecodeError> {
        RawInflater::reset(self, bit_offset)
    }

    #[inline]
    fn prime(&mut self, bits: u8, value: u8, bit_offset: u64) -> Result<(), DecodeError> {
        RawInflater::prime(self, bits, value, bit_offset)
    }

    #[inline]
    fn set_dictionary(&mut self, window: &Window, bit_offset: u64) -> Result<(), DecodeError> {
        RawInflater::set_dictionary(self, window, bit_offset)
    }

    fn inflate_capped(
        &mut self,
        input: &[u8],
        output: &mut Vec<u8>,
        flush: InflateFlush,
        max_produce: usize,
    ) -> Result<InflateStep, DecodeError> {
        let start_len = output.len();
        let spare = output.capacity().saturating_sub(start_len);
        let out_len = spare.min(max_produce).min(u32::MAX as usize);
        if out_len == 0 {
            return Ok(InflateStep {
                status: z::Z_BUF_ERROR,
                consumed: 0,
                produced: 0,
                unused_bits: 0,
                at_block_end: false,
                last_block: false,
            });
        }

        let in_len = input.len().min(u32::MAX as usize);

        self.stream.next_in = if in_len == 0 {
            std::ptr::null()
        } else {
            input.as_ptr()
        };
        self.stream.avail_in = in_len as u32;
        self.stream.next_out = output.spare_capacity_mut().as_mut_ptr().cast::<u8>();
        self.stream.avail_out = out_len as u32;
        let input_before = self.stream.avail_in;
        let output_before = self.stream.avail_out;

        // SAFETY:
        // - `self.stream` is initialized and uniquely borrowed.
        // - `next_in/avail_in` describe a live immutable slice (or null/0).
        // - `next_out/avail_out` describe uniquely owned spare capacity of
        //   `output`, capped to `out_len = min(spare, max_produce, u32::MAX)`;
        //   the backend reports how many bytes it initialized.
        let status = unsafe { z::inflate(&mut self.stream, flush.to_zlib()) };

        let consumed =
            usize::try_from(input_before - self.stream.avail_in).expect("zlib uInt fits usize");
        let produced =
            usize::try_from(output_before - self.stream.avail_out).expect("zlib uInt fits usize");
        debug_assert!(produced <= out_len);
        debug_assert!(
            start_len
                .checked_add(produced)
                .is_some_and(|n| n <= output.capacity())
        );
        // SAFETY: zlib-rs initialized exactly `produced` spare bytes and
        // `produced <= out_len <= spare capacity`.
        unsafe {
            output.set_len(start_len + produced);
        }

        let data_type = self.stream.data_type;
        Ok(InflateStep {
            status,
            consumed,
            produced,
            unused_bits: (data_type & 0x3F) as u8,
            at_block_end: data_type & 0x80 != 0,
            last_block: data_type & 0x40 != 0,
        })
    }

    fn inflate_into_slice(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        flush: InflateFlush,
    ) -> Result<InflateStep, DecodeError> {
        let out_len = output.len().min(u32::MAX as usize);
        if out_len == 0 {
            return Ok(InflateStep {
                status: z::Z_BUF_ERROR,
                consumed: 0,
                produced: 0,
                unused_bits: 0,
                at_block_end: false,
                last_block: false,
            });
        }

        let in_len = input.len().min(u32::MAX as usize);

        self.stream.next_in = if in_len == 0 {
            std::ptr::null()
        } else {
            input.as_ptr()
        };
        self.stream.avail_in = in_len as u32;
        self.stream.next_out = output.as_mut_ptr();
        self.stream.avail_out = out_len as u32;
        let input_before = self.stream.avail_in;
        let output_before = self.stream.avail_out;

        // SAFETY:
        // - `self.stream` is initialized and uniquely borrowed.
        // - `next_in/avail_in` describe a live immutable slice (or null/0).
        // - `next_out/avail_out` describe the uniquely owned `output` prefix of
        //   length `out_len = min(output.len(), u32::MAX)`; the backend
        //   reports how many of those bytes it initialized.
        let status = unsafe { z::inflate(&mut self.stream, flush.to_zlib()) };

        let consumed =
            usize::try_from(input_before - self.stream.avail_in).expect("zlib uInt fits usize");
        let produced =
            usize::try_from(output_before - self.stream.avail_out).expect("zlib uInt fits usize");
        debug_assert!(produced <= out_len);

        let data_type = self.stream.data_type;
        Ok(InflateStep {
            status,
            consumed,
            produced,
            unused_bits: (data_type & 0x3F) as u8,
            at_block_end: data_type & 0x80 != 0,
            last_block: data_type & 0x40 != 0,
        })
    }

    #[inline]
    fn message(&self) -> Option<String> {
        RawInflater::message(self)
    }
}

/// Compile-time inflater used by monomorphized decode paths.
///
/// With `--features isal`, this is ISA-L; otherwise zlib-rs [`RawInflater`].
#[cfg(feature = "isal")]
pub(crate) type ActiveInflater = crate::isal_backend::IsalInflater;

/// Compile-time inflater used by monomorphized decode paths (zlib-rs default).
#[cfg(not(feature = "isal"))]
pub(crate) type ActiveInflater = RawInflater;

/// zlib status aliases used by sequential match arms (backend-agnostic names).
pub(crate) mod status {
    use libz_rs_sys as z;

    pub(crate) const OK: i32 = z::Z_OK;
    pub(crate) const STREAM_END: i32 = z::Z_STREAM_END;
    pub(crate) const BUF_ERROR: i32 = z::Z_BUF_ERROR;
    pub(crate) const NEED_DICT: i32 = z::Z_NEED_DICT;
    pub(crate) const DATA_ERROR: i32 = z::Z_DATA_ERROR;
}

#[cfg(test)]
mod tests {
    use super::{InflateBackend, InflateFlush, status};
    use crate::backend::RawInflater;
    use crate::parallel::Window;

    /// Empty final fixed-Huffman block (valid raw DEFLATE stream end).
    const EMPTY_FINAL_BLOCK: &[u8] = &[0x03, 0x00];

    #[test]
    fn raw_inflater_backend_reset_reuses_stream() {
        let mut inflater = <RawInflater as InflateBackend>::create().unwrap();
        let mut out = Vec::with_capacity(64);

        let step = inflater
            .inflate(EMPTY_FINAL_BLOCK, &mut out, InflateFlush::NoFlush)
            .unwrap();
        assert_eq!(step.status, status::STREAM_END);
        assert!(out.is_empty());
        assert_eq!(step.consumed, EMPTY_FINAL_BLOCK.len());

        inflater.reset(0).unwrap();
        out.clear();

        let step = inflater
            .inflate(EMPTY_FINAL_BLOCK, &mut out, InflateFlush::NoFlush)
            .unwrap();
        assert_eq!(step.status, status::STREAM_END);
        assert!(out.is_empty());
        assert_eq!(step.consumed, EMPTY_FINAL_BLOCK.len());
    }

    #[test]
    fn raw_inflater_backend_set_dictionary_empty_is_ok() {
        let mut inflater = <RawInflater as InflateBackend>::create().unwrap();
        inflater.set_dictionary(&Window::empty(), 0).unwrap();
        inflater.prime(0, 0, 0).unwrap();
    }

    /// Empty final stored block: BFINAL=1, BTYPE=00, padded, LEN=0, NLEN=0xffff.
    const EMPTY_STORED_FINAL: &[u8] = &[0x01, 0x00, 0x00, 0xff, 0xff];

    /// Final stored block with five literal payload bytes ("hello").
    /// BFINAL=1 BTYPE=00 pad; LEN=5; NLEN=0xfffa; data.
    const STORED_HELLO: &[u8] = &[0x01, 0x05, 0x00, 0xfa, 0xff, b'h', b'e', b'l', b'l', b'o'];

    #[test]
    fn prepare_at_bit_offset_empty_dict_and_prime_zero_is_noop_setup() {
        // Byte-aligned: first_byte ignored; empty window is a no-op dictionary.
        let mut inflater =
            <RawInflater as InflateBackend>::prepare_at_bit_offset(0xAB, 0, &Window::empty())
                .unwrap();
        let mut out = Vec::with_capacity(16);
        let step = inflater
            .inflate(EMPTY_STORED_FINAL, &mut out, InflateFlush::NoFlush)
            .unwrap();
        assert_eq!(step.status, status::STREAM_END);
        assert!(out.is_empty());
        assert_eq!(step.consumed, EMPTY_STORED_FINAL.len());
    }

    #[test]
    fn prepare_at_bit_offset_mid_byte_stored_block() {
        // Same empty final stored block as EMPTY_STORED_FINAL, but shifted so
        // absolute start is bit 1: first compressed byte is 0x02 (LSB already
        // "consumed"), remaining bits prime to 0x01, then LEN/NLEN follow.
        //   absolute bits: [pad0][BFINAL=1 BTYPE=00 pad][LEN][NLEN]
        let compressed: &[u8] = &[0x02, 0x00, 0x00, 0xff, 0xff];
        let start_bit = 1_u64;
        let first_byte = compressed[0];
        let mut inflater = <RawInflater as InflateBackend>::prepare_at_bit_offset(
            first_byte,
            start_bit,
            &Window::empty(),
        )
        .unwrap();
        // Mid-byte prime consumed compressed[0]; feed the rest.
        let mut out = Vec::with_capacity(16);
        let step = inflater
            .inflate(&compressed[1..], &mut out, InflateFlush::NoFlush)
            .unwrap();
        assert_eq!(
            step.status,
            status::STREAM_END,
            "msg={:?}",
            inflater.message()
        );
        assert!(out.is_empty());
        assert_eq!(step.consumed, compressed.len() - 1);
    }

    #[test]
    fn inflate_capped_does_not_exceed_max_produce() {
        // Spare capacity is large, but max_produce hard-caps output for this step.
        let mut inflater = <RawInflater as InflateBackend>::create().unwrap();
        let mut out = Vec::with_capacity(64);
        let max_produce = 2_usize;
        let step = inflater
            .inflate_capped(STORED_HELLO, &mut out, InflateFlush::NoFlush, max_produce)
            .unwrap();
        assert!(
            step.produced <= max_produce,
            "produced={} max_produce={max_produce}",
            step.produced
        );
        assert_eq!(
            step.produced, max_produce,
            "stored block should yield full cap"
        );
        assert_eq!(out, b"he");
        // Cap exhausted mid-stream: not STREAM_END yet; progress must be positive.
        assert_ne!(step.status, status::STREAM_END);
        assert!(step.consumed > 0 || step.produced > 0);

        // Same payload unrestricted produces the full stored body.
        inflater.reset(0).unwrap();
        let mut full = Vec::with_capacity(16);
        let done = inflater
            .inflate(STORED_HELLO, &mut full, InflateFlush::NoFlush)
            .unwrap();
        assert_eq!(done.status, status::STREAM_END);
        assert_eq!(full, b"hello");
    }

    #[test]
    fn inflate_capped_zero_max_produce_is_buf_error() {
        let mut inflater = <RawInflater as InflateBackend>::create().unwrap();
        // Large spare capacity; zero produce budget must not write.
        let mut out = Vec::with_capacity(64);
        let step = inflater
            .inflate_capped(STORED_HELLO, &mut out, InflateFlush::NoFlush, 0)
            .unwrap();
        assert_eq!(step.status, status::BUF_ERROR);
        assert_eq!(step.produced, 0);
        assert_eq!(step.consumed, 0);
        assert!(out.is_empty());
    }

    #[test]
    fn inflate_into_slice_fills_only_provided_slice_length() {
        let mut inflater = <RawInflater as InflateBackend>::create().unwrap();
        // Destination is shorter than the stored payload; only that many bytes
        // may be written, and the slice length itself is unchanged.
        let mut out = [0_u8; 3];
        let step = InflateBackend::inflate_into_slice(
            &mut inflater,
            STORED_HELLO,
            &mut out,
            InflateFlush::NoFlush,
        )
        .unwrap();
        assert_eq!(step.produced, 3);
        assert_eq!(&out, b"hel");
        assert_ne!(step.status, status::STREAM_END);
        assert!(step.consumed > 0 || step.produced > 0);

        // Empty destination: no write, BUF_ERROR, slice length unchanged.
        inflater.reset(0).unwrap();
        let mut empty: [u8; 0] = [];
        let empty_len = empty.len();
        let step = InflateBackend::inflate_into_slice(
            &mut inflater,
            STORED_HELLO,
            &mut empty,
            InflateFlush::NoFlush,
        )
        .unwrap();
        assert_eq!(step.status, status::BUF_ERROR);
        assert_eq!(step.produced, 0);
        assert_eq!(step.consumed, 0);
        assert_eq!(empty.len(), empty_len);

        // Full payload into a large enough slice ends the stream.
        inflater.reset(0).unwrap();
        let mut full = [0_u8; 8];
        let step = InflateBackend::inflate_into_slice(
            &mut inflater,
            STORED_HELLO,
            &mut full,
            InflateFlush::NoFlush,
        )
        .unwrap();
        assert_eq!(step.status, status::STREAM_END);
        assert_eq!(step.produced, 5);
        assert_eq!(&full[..5], b"hello");
        // Bytes beyond `produced` must remain untouched.
        assert_eq!(&full[5..], &[0, 0, 0]);
    }
}
