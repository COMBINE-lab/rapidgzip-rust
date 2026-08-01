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
use crate::inflate_backend::{ActiveInflater, InflateBackend, InflateOutcome};
use crate::runtime::RuntimeState;
use crate::zlib::{self, Adler32};
use crate::{DecodeError, DecodeReport, DeflateErrorKind, Format, ZlibErrorKind};
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

    let mut inflater = ActiveInflater::new()?;
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

        // SAFETY: the cursor's readable window stays live and unmoved until
        // the next `available` call, which happens after this borrow ends.
        // Reconstructing the slice here, rather than holding the borrow,
        // leaves the cursor free to advance below.
        let input = unsafe { std::slice::from_raw_parts(input_pointer, input_length) };
        let step = inflater.inflate(input, &mut decoded, false)?;
        cursor.advance(step.consumed);

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

        match step.outcome {
            InflateOutcome::StreamEnd => break,
            InflateOutcome::Progress => {
                if step.consumed == 0 && step.produced == 0 {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: cursor.position().saturating_mul(8),
                        reason: DeflateErrorKind::Stalled,
                    });
                }
            }
            InflateOutcome::Blocked => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
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
