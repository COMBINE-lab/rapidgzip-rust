//! Optional ISA-L raw-inflate backend ([`IsalInflater`]).
//!
//! Enabled with the `isal` feature. Links `libisal` (shared or system) via
//! `isal-sys`. Set `ISAL_INSTALL_PREFIX` to a prefix containing `lib/libisal.so`
//! (or install `libisal-dev` system-wide) so the build can find the library.
//!
//! Maps ISA-L status codes onto zlib-compatible [`super::inflate_backend::status`]
//! values so existing call-site match arms stay shared.

use crate::error::{DecodeError, DeflateErrorKind};
use crate::inflate_backend::{InflateBackend, InflateFlush, InflateStep, status};
use crate::parallel::Window;
use isal_sys::igzip_lib::{
    ISAL_DECOMP_OK, ISAL_DEFLATE, ISAL_END_INPUT, ISAL_INCORRECT_CHECKSUM, ISAL_INVALID_BLOCK,
    ISAL_INVALID_LOOKBACK, ISAL_INVALID_SYMBOL, ISAL_INVALID_WRAPPER, ISAL_NEED_DICT,
    ISAL_OUT_OVERFLOW, ISAL_UNSUPPORTED_METHOD, inflate_state, isal_block_state_ISAL_BLOCK_FINISH,
    isal_block_state_ISAL_BLOCK_INPUT_DONE, isal_block_state_ISAL_BLOCK_NEW_HDR, isal_inflate,
    isal_inflate_init, isal_inflate_reset, isal_inflate_set_dict,
};
use std::os::raw::c_int;

/// ISA-L-backed raw inflater implementing [`InflateBackend`].
///
/// Uses `crc_flag = ISAL_DEFLATE` (raw DEFLATE, no gzip/zlib wrapper). Wrapper
/// and checksum handling stay in the gzip/zlib framing layers above.
///
/// [`InflateFlush::Block`] (keep_index / analyze bit-accurate block walks) is
/// delegated to an internal zlib-rs inflater: ISA-L does not expose a
/// zlib-compatible `Z_BLOCK` stop contract.
pub(crate) struct IsalInflater {
    state: Box<inflate_state>,
    /// Fallback for [`InflateFlush::Block`] only.
    block_zlib: crate::backend::RawInflater,
}

impl IsalInflater {
    fn map_status(ret: c_int, state: &inflate_state, produced: usize, consumed: usize) -> i32 {
        // Positive ISA-L codes are success classes; negatives are errors.
        if ret < 0 {
            return match ret {
                ISAL_INVALID_BLOCK
                | ISAL_INVALID_SYMBOL
                | ISAL_INVALID_LOOKBACK
                | ISAL_INVALID_WRAPPER
                | ISAL_UNSUPPORTED_METHOD
                | ISAL_INCORRECT_CHECKSUM => status::DATA_ERROR,
                _ => status::DATA_ERROR,
            };
        }
        let ok = ret as u32;
        match ok {
            ISAL_DECOMP_OK => {
                // INPUT_DONE means compressed input is exhausted but ISA-L may
                // still hold decompressed bytes in tmp_out (common for large
                // stored blocks with a small avail_out). Only FINISH means the
                // stream is fully flushed — treat INPUT_DONE as Z_OK so callers
                // keep draining until FINISH.
                if state.block_state == isal_block_state_ISAL_BLOCK_FINISH {
                    status::STREAM_END
                } else {
                    status::OK
                }
            }
            ISAL_END_INPUT => {
                if produced == 0 && consumed == 0 {
                    status::BUF_ERROR
                } else {
                    status::OK
                }
            }
            ISAL_OUT_OVERFLOW => {
                if produced == 0 {
                    status::BUF_ERROR
                } else {
                    status::OK
                }
            }
            ISAL_NEED_DICT => status::NEED_DICT,
            _ => {
                if produced > 0 || consumed > 0 {
                    status::OK
                } else {
                    status::BUF_ERROR
                }
            }
        }
    }

    fn block_flags(state: &inflate_state) -> (bool, bool) {
        // Approximate zlib data_type block-end flags using ISA-L state.
        // INPUT_DONE is not a block boundary for keep_index purposes while
        // tmp_out may still be draining; FINISH / NEW_HDR are.
        let at_block_end = state.block_state == isal_block_state_ISAL_BLOCK_NEW_HDR
            || state.block_state == isal_block_state_ISAL_BLOCK_FINISH
            || state.bfinal != 0;
        let last_block = state.bfinal != 0
            || state.block_state == isal_block_state_ISAL_BLOCK_FINISH;
        (at_block_end, last_block)
    }

    fn bit_buffer_len(state: &inflate_state) -> i32 {
        state.read_in_length.max(0)
    }

    /// Bits still held in ISA-L's bit buffer (zlib `data_type & 0x3f` style, capped).
    fn unused_bits(state: &inflate_state) -> u8 {
        (Self::bit_buffer_len(state).min(63) as u8) & 0x3F
    }

    /// Refund full bytes prefetched into `read_in` past the DEFLATE end, and
    /// drop them from the bit buffer so a later call can re-present trailers.
    ///
    /// Applies when compressed input is exhausted (`INPUT_DONE` or `FINISH`),
    /// not only on [`status::STREAM_END`]: large stored/Huffman members often
    /// hit `INPUT_DONE` while `tmp_out` still drains, and a STREAM_END-only
    /// refund would run on a later step with `consumed == 0`.
    fn refund_prefetch(state: &mut inflate_state, consumed: usize) -> (usize, u8) {
        let bits = Self::bit_buffer_len(state) as u32;
        let refund = (bits / 8) as usize;
        let residual = (bits % 8) as u8;
        if residual == 0 {
            state.read_in = 0;
            state.read_in_length = 0;
        } else {
            // Keep residual low bits (LSB-first padding); drop full prefetched bytes.
            state.read_in &= (1_u64 << residual) - 1;
            state.read_in_length = i32::from(residual);
        }
        (consumed.saturating_sub(refund), residual)
    }

    /// Map an `isal_inflate` result to a zlib-compatible [`InflateStep`].
    fn step_from_result(
        ret: c_int,
        state: &mut inflate_state,
        mut consumed: usize,
        produced: usize,
    ) -> InflateStep {
        let status = Self::map_status(ret, state, produced, consumed);
        let (at_block_end, last_block) = Self::block_flags(state);
        let input_exhausted = state.block_state == isal_block_state_ISAL_BLOCK_FINISH
            || state.block_state == isal_block_state_ISAL_BLOCK_INPUT_DONE;
        let unused_bits = if input_exhausted {
            let (c, residual) = Self::refund_prefetch(state, consumed);
            consumed = c;
            residual
        } else {
            Self::unused_bits(state)
        };
        InflateStep {
            status,
            consumed,
            produced,
            unused_bits,
            at_block_end,
            last_block,
        }
    }

    unsafe fn run_inflate(
        state: &mut inflate_state,
        next_in: *mut u8,
        avail_in: u32,
        next_out: *mut u8,
        avail_out: u32,
    ) -> (c_int, usize, usize) {
        state.next_in = next_in;
        state.avail_in = avail_in;
        state.next_out = next_out;
        state.avail_out = avail_out;
        let in_before = state.avail_in;
        let out_before = state.avail_out;
        // SAFETY: caller ensures next_in/out point at live buffers of the
        // stated lengths for the duration of isal_inflate.
        let ret = unsafe { isal_inflate(state) };
        let consumed = (in_before - state.avail_in) as usize;
        let produced = (out_before - state.avail_out) as usize;
        (ret, consumed, produced)
    }
}

impl InflateBackend for IsalInflater {
    fn create() -> Result<Self, DecodeError> {
        // Zero then init so temporary buffers start clean.
        // SAFETY: `inflate_state` is a C POD; zeroed then fully initialized by
        // `isal_inflate_init` before any other use.
        let mut state = Box::new(unsafe { std::mem::zeroed::<inflate_state>() });
        // SAFETY: state is a valid, exclusively owned inflate_state.
        unsafe { isal_inflate_init(&mut *state) };
        state.crc_flag = ISAL_DEFLATE;
        Ok(Self {
            state,
            block_zlib: crate::backend::RawInflater::new()?,
        })
    }

    fn reset(&mut self, bit_offset: u64) -> Result<(), DecodeError> {
        // SAFETY: state was initialized by create/reset.
        unsafe { isal_inflate_reset(&mut *self.state) };
        self.state.crc_flag = ISAL_DEFLATE;
        self.block_zlib.reset(bit_offset)?;
        Ok(())
    }

    fn prime(&mut self, bits: u8, value: u8, bit_offset: u64) -> Result<(), DecodeError> {
        if bits == 0 {
            return Ok(());
        }
        if bits > 16 {
            return Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::InvalidData,
            });
        }
        // zlib inflatePrime: low `bits` of value are pushed into the bit buffer.
        // ISA-L uses read_in (LSB-first bit buffer) + read_in_length.
        self.state.read_in = u64::from(value) & ((1_u64 << bits) - 1);
        self.state.read_in_length = i32::from(bits);
        self.block_zlib.prime(bits, value, bit_offset)?;
        Ok(())
    }

    fn set_dictionary(&mut self, window: &Window, bit_offset: u64) -> Result<(), DecodeError> {
        let bytes = window.as_slice();
        if bytes.is_empty() {
            self.block_zlib.set_dictionary(window, bit_offset)?;
            return Ok(());
        }
        // SAFETY: dict points at live window bytes; length fits u32 for DEFLATE window.
        let status = unsafe {
            isal_inflate_set_dict(
                &mut *self.state,
                bytes.as_ptr().cast_mut(),
                bytes.len() as u32,
            )
        };
        if status != 0 {
            return Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::BackendStatus(status),
            });
        }
        self.block_zlib.set_dictionary(window, bit_offset)?;
        Ok(())
    }

    fn inflate_capped(
        &mut self,
        input: &[u8],
        output: &mut Vec<u8>,
        flush: InflateFlush,
        max_produce: usize,
    ) -> Result<InflateStep, DecodeError> {
        if matches!(flush, InflateFlush::Block) {
            return self
                .block_zlib
                .inflate_capped(input, output, flush, max_produce);
        }
        // Finish/NoFlush: ISA-L (Finish approximated by running until stream end).
        let start_len = output.len();
        let spare = output.capacity().saturating_sub(start_len);
        let out_len = spare.min(max_produce).min(u32::MAX as usize);
        if out_len == 0 {
            return Ok(InflateStep {
                status: status::BUF_ERROR,
                consumed: 0,
                produced: 0,
                unused_bits: 0,
                at_block_end: false,
                last_block: false,
            });
        }
        let in_len = input.len().min(u32::MAX as usize);
        let next_in = if in_len == 0 {
            std::ptr::null_mut()
        } else {
            input.as_ptr().cast_mut()
        };
        let next_out = output.spare_capacity_mut().as_mut_ptr().cast::<u8>();

        // SAFETY: next_in/out cover live slices of the stated lengths.
        let (ret, consumed, produced) = unsafe {
            Self::run_inflate(
                &mut self.state,
                next_in,
                in_len as u32,
                next_out,
                out_len as u32,
            )
        };
        debug_assert!(produced <= out_len);
        // SAFETY: isal_inflate initialized `produced` bytes of spare capacity.
        unsafe {
            output.set_len(start_len + produced);
        }
        Ok(Self::step_from_result(ret, &mut self.state, consumed, produced))
    }

    fn inflate_into_slice(
        &mut self,
        input: &[u8],
        output: &mut [u8],
        flush: InflateFlush,
    ) -> Result<InflateStep, DecodeError> {
        if matches!(flush, InflateFlush::Block) {
            return self.block_zlib.inflate_into_slice(input, output, flush);
        }
        if output.is_empty() {
            return Ok(InflateStep {
                status: status::BUF_ERROR,
                consumed: 0,
                produced: 0,
                unused_bits: 0,
                at_block_end: false,
                last_block: false,
            });
        }
        let in_len = input.len().min(u32::MAX as usize);
        let out_len = output.len().min(u32::MAX as usize);
        let next_in = if in_len == 0 {
            std::ptr::null_mut()
        } else {
            input.as_ptr().cast_mut()
        };
        // SAFETY: next_in/out cover live slices of the stated lengths.
        let (ret, consumed, produced) = unsafe {
            Self::run_inflate(
                &mut self.state,
                next_in,
                in_len as u32,
                output.as_mut_ptr(),
                out_len as u32,
            )
        };
        Ok(Self::step_from_result(ret, &mut self.state, consumed, produced))
    }

    fn message(&self) -> Option<String> {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::inflate_backend::InflateBackend;

    /// Final stored block containing "hi" (raw DEFLATE).
    fn stored_hi() -> Vec<u8> {
        // BFINAL=1, BTYPE=00, LEN=2, NLEN, 'h','i'
        vec![0x01, 0x02, 0x00, 0xfd, 0xff, b'h', b'i']
    }

    /// Empty final stored block (raw DEFLATE).
    fn stored_empty() -> Vec<u8> {
        vec![0x01, 0x00, 0x00, 0xff, 0xff]
    }

    #[test]
    fn isal_inflates_stored_block() {
        let mut inf = IsalInflater::create().unwrap();
        let input = stored_hi();
        let mut out = Vec::with_capacity(16);
        let step = inf
            .inflate_capped(&input, &mut out, InflateFlush::NoFlush, 16)
            .unwrap();
        assert_eq!(&out, b"hi");
        assert_eq!(step.status, status::STREAM_END);
        assert_eq!(step.consumed, input.len());
        assert_eq!(step.produced, 2);
    }

    #[test]
    fn isal_inflate_into_slice_respects_length() {
        let mut inf = IsalInflater::create().unwrap();
        let input = stored_hi();
        let mut buf = [0_u8; 1];
        let step = inf
            .inflate_into_slice(&input, &mut buf, InflateFlush::NoFlush)
            .unwrap();
        assert_eq!(step.produced, 1);
        assert_eq!(buf[0], b'h');
    }

    #[test]
    fn isal_stream_end_does_not_consume_trailer_bytes() {
        // ISA-L prefetches into its bit buffer; at STREAM_END those full
        // prefetched bytes must be refunded so Adler/CRC trailers stay visible.
        let mut with_trailer = stored_empty();
        with_trailer.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]); // empty Adler
        with_trailer.extend_from_slice(&[0x78, 0x01]); // next zlib header

        let mut inf = IsalInflater::create().unwrap();
        let mut out = Vec::with_capacity(16);
        let step = inf
            .inflate_capped(&with_trailer, &mut out, InflateFlush::NoFlush, 16)
            .unwrap();
        assert_eq!(step.status, status::STREAM_END);
        assert_eq!(step.produced, 0);
        assert_eq!(step.consumed, stored_empty().len());
        assert!(out.is_empty());

        let mut hi_extra = stored_hi();
        hi_extra.extend_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd]);
        let mut inf2 = IsalInflater::create().unwrap();
        let mut out2 = Vec::with_capacity(16);
        let step2 = inf2
            .inflate_capped(&hi_extra, &mut out2, InflateFlush::NoFlush, 16)
            .unwrap();
        assert_eq!(step2.status, status::STREAM_END);
        assert_eq!(&out2, b"hi");
        assert_eq!(step2.consumed, stored_hi().len());
    }

    #[test]
    fn isal_reset_then_empty_member_with_trailer() {
        let first = stored_hi();
        let mut empty_with_trailer = stored_empty();
        empty_with_trailer.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);

        let mut inf = IsalInflater::create().unwrap();
        let mut out = Vec::with_capacity(32);
        let step = inf
            .inflate_capped(&first, &mut out, InflateFlush::NoFlush, 32)
            .unwrap();
        assert_eq!(step.status, status::STREAM_END);
        assert_eq!(&out, b"hi");

        inf.reset(0).unwrap();
        out.clear();
        let step2 = inf
            .inflate_capped(&empty_with_trailer, &mut out, InflateFlush::NoFlush, 32)
            .unwrap();
        assert_eq!(step2.status, status::STREAM_END);
        assert_eq!(step2.produced, 0);
        assert_eq!(step2.consumed, stored_empty().len());
    }

    #[test]
    fn large_stored_with_trailer_stops_at_deflate_end() {
        // While draining tmp_out, trailer bytes after DEFLATE must remain for
        // the framing layer (CRC/ISIZE or Adler) — not be counted as consumed.
        let payload: Vec<u8> = (0..8600u32).map(|i| (i % 251) as u8).collect();
        let mut input = Vec::new();
        input.push(1);
        let len = payload.len() as u16;
        input.extend_from_slice(&len.to_le_bytes());
        input.extend_from_slice(&(!len).to_le_bytes());
        input.extend_from_slice(&payload);
        input.extend_from_slice(&[0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88]);
        let deflate_len = input.len() - 8;

        let mut inf = IsalInflater::create().unwrap();
        let mut all = Vec::new();
        let mut out = vec![0u8; 4096];
        let mut pos = 0usize;
        for _ in 0..12 {
            let step = inf
                .inflate_into_slice(&input[pos..], &mut out, InflateFlush::NoFlush)
                .unwrap();
            all.extend_from_slice(&out[..step.produced]);
            pos += step.consumed;
            if step.status == status::STREAM_END {
                break;
            }
            assert!(step.produced > 0 || step.consumed > 0);
        }
        assert_eq!(all, payload);
        assert_eq!(pos, deflate_len, "must stop at trailer boundary");
    }

    #[test]
    fn large_stored_partial_out_drains_tmp() {
        // ISA-L may consume a whole stored block into tmp_out while only
        // filling avail_out; callers must keep inflating until STREAM_END
        // (FINISH), not stop at INPUT_DONE.
        let payload: Vec<u8> = (0..8600u32).map(|i| (i % 251) as u8).collect();
        let mut input = Vec::new();
        input.push(1); // BFINAL stored
        let len = payload.len() as u16;
        input.extend_from_slice(&len.to_le_bytes());
        input.extend_from_slice(&(!len).to_le_bytes());
        input.extend_from_slice(&payload);

        let mut inf = IsalInflater::create().unwrap();
        let mut all = Vec::new();
        let mut out = vec![0u8; 4096];
        let mut consumed_total = 0usize;
        let mut saw_non_end = false;
        for _ in 0..10 {
            let step = inf
                .inflate_into_slice(&input[consumed_total..], &mut out, InflateFlush::NoFlush)
                .unwrap();
            all.extend_from_slice(&out[..step.produced]);
            consumed_total += step.consumed;
            if step.status == status::STREAM_END {
                break;
            }
            saw_non_end = true;
            assert!(
                step.produced > 0 || step.consumed > 0,
                "stalled before stream end"
            );
        }
        assert!(saw_non_end, "expected at least one non-STREAM_END step");
        assert_eq!(all, payload);
        assert_eq!(consumed_total, input.len());
    }
}


