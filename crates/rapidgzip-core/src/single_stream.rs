//! Sequential decoding of a single DEFLATE stream in a zlib container or none.
//!
//! gzip framing allows concatenated members and is handled in [`crate::backend`].
//! zlib (RFC 1950) and raw DEFLATE (RFC 1951) are each exactly one stream, so
//! they share this loop, which differs only in what it reads before the stream
//! and what it verifies after it.
//!
//! The loop runs over an [`InputCursor`], so a positional source and a
//! non-seekable one execute the identical framing and verification code.

use crate::backend::Output;
use crate::config::Config;
use crate::gzip::InputCursor;
use crate::index::Checkpoint;
use crate::inflate::RawInflater;
use crate::runtime::RuntimeState;
use crate::zlib::{self, Adler32};
use crate::{DecodeError, DecodeReport, DeflateErrorKind, Format, ZlibErrorKind};
use libz_rs_sys as z;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

/// Decodes one zlib or raw DEFLATE stream, verifying whatever the format can.
///
/// `format` must be [`Format::Zlib`] or [`Format::RawDeflate`]; gzip never
/// reaches this path.
pub(crate) fn decode_single_stream<C, O>(
    cursor: &mut C,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    format: Format,
    decoder_threads: usize,
    runtime: &Arc<RuntimeState>,
) -> Result<DecodeReport, DecodeError>
where
    C: InputCursor,
    O: Output,
{
    debug_assert!(matches!(format, Format::Zlib | Format::RawDeflate));

    if format == Format::Zlib {
        let header = read_header_bytes(cursor)?;
        zlib::validate_header(header[0], header[1], 0)?;
    }

    let deflate_start = cursor.position();
    runtime.offer_checkpoint(
        Checkpoint {
            compressed_offset_in_bits: deflate_start.saturating_mul(8),
            uncompressed_offset_in_bytes: 0,
            line_offset: 0,
        },
        &[],
    );

    let mut inflater = RawInflater::new()?;
    let mut checksum = Adler32::new();
    let mut total_output = 0_u64;
    let mut decoded = Vec::with_capacity(config.decoded_chunk_size);

    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }
        let (input_pointer, input_length) = {
            let input = cursor.available()?;
            if input.is_empty() {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
                });
            }
            (input.as_ptr(), input.len().min(u32::MAX as usize))
        };
        if decoded.capacity() < config.decoded_chunk_size {
            decoded.reserve_exact(config.decoded_chunk_size - decoded.capacity());
        }

        inflater.stream.next_in = input_pointer;
        inflater.stream.avail_in = input_length as u32;
        inflater.stream.next_out = decoded.spare_capacity_mut().as_mut_ptr().cast::<u8>();
        inflater.stream.avail_out = decoded.capacity() as u32;
        let input_before = inflater.stream.avail_in;
        let output_before = inflater.stream.avail_out;

        // SAFETY:
        // - `inflater.stream` was initialized and is uniquely borrowed.
        // - `next_in` and `avail_in` describe the cursor's current readable
        //   window, which neither cursor implementation moves or mutates
        //   outside `available`, and that is not called again until this call
        //   returns.
        // - `next_out` and `avail_out` describe the uniquely owned spare
        //   capacity of `decoded`, whose length is extended below by exactly
        //   the count zlib reports as initialized.
        let status = unsafe { z::inflate(&mut inflater.stream, z::Z_NO_FLUSH) };

        let consumed =
            usize::try_from(input_before - inflater.stream.avail_in).expect("zlib uInt fits usize");
        let produced = usize::try_from(output_before - inflater.stream.avail_out)
            .expect("zlib uInt fits usize");
        cursor.advance(consumed);
        // SAFETY: `output_before` was exactly `decoded.capacity()`, and
        // zlib-rs only reduces `avail_out` after initializing those bytes, so
        // precisely the first `produced` bytes are initialized.
        unsafe { decoded.set_len(produced) };

        if !decoded.is_empty() {
            let new_total = total_output.checked_add(decoded.len() as u64).ok_or(
                DecodeError::OutputLimitExceeded {
                    limit: config.output_limit.unwrap_or(u64::MAX),
                },
            )?;
            if config.output_limit.is_some_and(|limit| new_total > limit) {
                return Err(DecodeError::OutputLimitExceeded {
                    limit: config.output_limit.expect("checked as some"),
                });
            }
            total_output = new_total;
            if format == Format::Zlib {
                checksum.update(&decoded);
            }
            decoded = output.emit_reusable(decoded)?;
        }

        match status {
            z::Z_STREAM_END => break,
            z::Z_OK => {
                if consumed == 0 && produced == 0 {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: cursor.position().saturating_mul(8),
                        reason: DeflateErrorKind::Stalled,
                    });
                }
            }
            z::Z_BUF_ERROR if consumed > 0 || produced > 0 => {}
            z::Z_BUF_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
                });
            }
            z::Z_NEED_DICT => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: deflate_start.saturating_mul(8),
                    reason: DeflateErrorKind::UnexpectedDictionary,
                });
            }
            z::Z_DATA_ERROR => {
                let _diagnostic = inflater.message();
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::InvalidData,
                });
            }
            other => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::BackendStatus(other),
                });
            }
        }
    }

    if format == Format::Zlib {
        let trailer_offset = cursor.position();
        let trailer = read_trailer_bytes(cursor)?;
        zlib::verify_trailer(trailer, checksum.finish()).map_err(|error| match error {
            DecodeError::InvalidZlib { reason, .. } => DecodeError::InvalidZlib {
                offset: trailer_offset,
                reason,
            },
            other => other,
        })?;
    }

    if !cursor.is_at_end()? {
        return Err(match format {
            Format::Zlib => DecodeError::InvalidZlib {
                offset: cursor.position(),
                reason: ZlibErrorKind::TrailingGarbage,
            },
            _ => DecodeError::InvalidDeflate {
                bit_offset: cursor.position().saturating_mul(8),
                reason: DeflateErrorKind::TrailingGarbage,
            },
        });
    }

    if let Some(expected) = config.expected_uncompressed_size {
        if expected != total_output {
            return Err(DecodeError::UnexpectedOutputSize {
                expected,
                actual: total_output,
            });
        }
    }

    cursor.verify_source_unchanged()?;
    runtime.set_member_count(1);

    Ok(DecodeReport {
        compressed_bytes: cursor.position(),
        decompressed_bytes: total_output,
        member_count: 1,
        decoder_threads,
        index: None,
        format,
    })
}

/// Reads the two zlib header bytes, reporting truncation as a zlib error.
fn read_header_bytes<C: InputCursor>(cursor: &mut C) -> Result<[u8; 2], DecodeError> {
    let mut bytes = [0_u8; 2];
    for byte in &mut bytes {
        let Some(&value) = cursor.available()?.first() else {
            return Err(DecodeError::InvalidZlib {
                offset: cursor.position(),
                reason: ZlibErrorKind::Truncated,
            });
        };
        cursor.advance(1);
        *byte = value;
    }
    Ok(bytes)
}

/// Reads the four Adler-32 trailer bytes, reporting truncation as a zlib error.
fn read_trailer_bytes<C: InputCursor>(cursor: &mut C) -> Result<[u8; 4], DecodeError> {
    let mut bytes = [0_u8; 4];
    for byte in &mut bytes {
        let Some(&value) = cursor.available()?.first() else {
            return Err(DecodeError::InvalidZlib {
                offset: cursor.position(),
                reason: ZlibErrorKind::Truncated,
            });
        };
        cursor.advance(1);
        *byte = value;
    }
    Ok(bytes)
}
