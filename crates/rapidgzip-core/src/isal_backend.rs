//! ISA-L raw-inflate backend, compiled only with the `isal` feature.
//!
//! Intel's Intelligent Storage Acceleration Library decodes DEFLATE faster
//! than zlib-rs on x86-64. It cannot resume at a bit offset and does not
//! report block boundaries, so it serves only the whole-stream paths described
//! in [`crate::inflate_backend`].
//!
//! The library is not vendored. `isal-sys` links a system `libisal`:
//! `libisal-dev` on Debian and Ubuntu, `isa-l` on Homebrew, or a prefix named
//! by `ISAL_INSTALL_PREFIX`.

use crate::inflate_backend::{InflateBackend, InflateOutcome, InflateStep};
use crate::{DecodeError, DeflateErrorKind};
use isal_sys::igzip_lib as isal;
use std::os::raw::c_int;

/// Raw-inflate state owned by ISA-L.
///
/// `inflate_state` embeds a 64 KiB scratch buffer, so it lives behind a `Box`
/// rather than inline in whatever structure or stack frame holds the backend.
pub(crate) struct IsalInflater {
    state: Box<isal::inflate_state>,
}

impl IsalInflater {
    /// Returns the DEFLATE error a failing ISA-L status stands for.
    ///
    /// The position is left at zero. The call site fills it in through
    /// [`crate::inflate_backend::at_bit_offset`], since only it knows where
    /// the buffer it handed over sits in the stream.
    fn error(status: c_int) -> DecodeError {
        let reason = match status {
            isal::ISAL_INVALID_BLOCK
            | isal::ISAL_INVALID_SYMBOL
            | isal::ISAL_INVALID_LOOKBACK
            | isal::ISAL_INVALID_WRAPPER
            | isal::ISAL_UNSUPPORTED_METHOD
            | isal::ISAL_INCORRECT_CHECKSUM => DeflateErrorKind::InvalidData,
            status if status == isal::ISAL_NEED_DICT as c_int => {
                DeflateErrorKind::UnexpectedDictionary
            }
            other => DeflateErrorKind::BackendStatus(other),
        };
        DecodeError::InvalidDeflate {
            bit_offset: 0,
            reason,
        }
    }
}

impl InflateBackend for IsalInflater {
    fn new() -> Result<Self, DecodeError> {
        let mut state = Box::<isal::inflate_state>::new_uninit();
        // SAFETY:
        // - the box owns one uniquely borrowed, correctly aligned allocation
        //   the size of `inflate_state`;
        // - `inflate_state` is a C aggregate of integers, arrays, and raw
        //   pointers, for which all zeros is a valid bit pattern. Zeroing
        //   first means the scratch buffers `isal_inflate_init` leaves alone,
        //   because it sets their lengths to zero, still start defined;
        // - `isal_inflate_init` then writes every scalar field, which is what
        //   `assume_init` relies on.
        let state = unsafe {
            std::ptr::write_bytes(state.as_mut_ptr(), 0, 1);
            isal::isal_inflate_init(state.as_mut_ptr());
            state.assume_init()
        };
        // `isal_inflate_init` leaves `crc_flag` at ISAL_DEFLATE and
        // `hist_bits` at zero, which is raw DEFLATE with a 32 KiB window.
        debug_assert_eq!(state.crc_flag, isal::ISAL_DEFLATE);
        debug_assert_eq!(state.hist_bits, 0);
        Ok(Self { state })
    }

    fn reset(&mut self, _bit_offset: u64) -> Result<(), DecodeError> {
        // SAFETY: the state was initialized by `isal_inflate_init` and is
        // uniquely borrowed. `isal_inflate_reset` cannot fail and retains the
        // raw-DEFLATE mode selected above.
        unsafe { isal::isal_inflate_reset(&mut *self.state) };
        Ok(())
    }

    fn inflate(
        &mut self,
        input: &[u8],
        output: &mut Vec<u8>,
        _finish: bool,
    ) -> Result<InflateStep, DecodeError> {
        // ISA-L has no counterpart to `Z_FINISH`. It always decodes as much as
        // the buffers allow, so being told the whole stream is present changes
        // nothing here.
        let start = output.len();
        let spare = output.spare_capacity_mut();
        let output_length = spare.len().min(u32::MAX as usize);
        let input_length = input.len().min(u32::MAX as usize);

        self.state.next_in = input.as_ptr().cast_mut();
        self.state.avail_in = input_length as u32;
        self.state.next_out = spare.as_mut_ptr().cast::<u8>();
        self.state.avail_out = output_length as u32;

        // SAFETY:
        // - the state is initialized and uniquely borrowed for this call;
        // - `next_in` covers `input`, which stays live and unmoved. ISA-L
        //   treats it as read-only despite the mutable C pointer type;
        // - `next_out` covers only the uniquely owned spare capacity of
        //   `output`, whose length is extended below by exactly the count
        //   ISA-L reports as written.
        let status = unsafe { isal::isal_inflate(&mut *self.state) };
        if status < 0 || status as u32 == isal::ISAL_NEED_DICT {
            return Err(Self::error(status));
        }

        let mut consumed = input_length - self.state.avail_in as usize;
        let produced = output_length - self.state.avail_out as usize;
        // SAFETY: ISA-L wrote exactly `produced` bytes into the spare capacity
        // supplied above and cannot report more than that capacity.
        unsafe { output.set_len(start + produced) };

        let finished = self.state.block_state == isal::isal_block_state_ISAL_BLOCK_FINISH;
        if finished {
            // ISA-L reads ahead into a bit buffer, so at the end of the stream
            // it has taken input the stream does not own. Whole bytes still
            // sitting in that buffer belong to whatever follows: the gzip
            // footer, the next member, or trailing garbage the caller must be
            // free to see.
            let unread = (self.state.read_in_length.max(0) as usize) / 8;
            consumed = consumed.saturating_sub(unread);
        }

        let outcome = if finished {
            InflateOutcome::StreamEnd
        } else if consumed > 0 || produced > 0 {
            InflateOutcome::Progress
        } else {
            InflateOutcome::Blocked
        };

        Ok(InflateStep {
            outcome,
            consumed,
            produced,
        })
    }

    fn message(&self) -> Option<String> {
        // ISA-L reports status codes only, with no diagnostic string.
        None
    }
}
