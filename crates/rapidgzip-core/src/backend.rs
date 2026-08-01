use crate::buffer_pool::ByteBufferFreeList;
use crate::config::{Config, Format};
use crate::crc32::Crc32;
use crate::gzip::{MemberHeader, SourceCursor, parse_member_header};
use crate::index::IndexBuilder;
use crate::inflate_backend::{InflateBackend, InflateFlush, status as inflate_status};
use crate::parallel::adaptive::AdaptiveConcurrency;
use crate::parallel::deflate::{
    ChunkOutput, Error as NativeError, InitialHistory, ResolvedParts, decode_to_estimated_boundary,
    find_next_structural_candidate,
};
use crate::parallel::{Symbol, Window};
use crate::zlib::{Adler32, looks_like_zlib, parse_zlib_header};
use crate::{DecodeError, DecodeReport, DeflateErrorKind, GzipErrorKind, ReadAt, ZlibErrorKind};
use crossbeam_deque::{Injector, Steal};
use libz_rs_sys as z;
use std::collections::BTreeMap;
use std::ffi::CStr;
use std::io::Write;
use std::mem::size_of;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::mpsc::{self, RecvTimeoutError, TrySendError};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::{Duration, Instant};

/// Minimum `decoder_threads` before entering the generic rapidgzip
/// estimated/marker pipeline ([`decode_rapidgzip_estimated`]).
///
/// Throughput measurements on ordinary single-member gzip show that low
/// marker concurrency loses to sequential zlib-rs: the native marker path
/// pays fixed ranking/window overhead that only amortizes at higher thread
/// budgets. Prefer sequential zlib-rs below this threshold. BGZF, stored-block,
/// and independent multi-member paths still enter at `decoder_threads > 1` —
/// those scale well at 2 threads.
///
/// Re-measure (2026-07-31, i7-8750H, `target/bench-profile/single-member.gz`
/// ~70 MiB compressed / 128 MiB decoded, release CLI `-t -q`, 5 runs, median
/// wall thrpt on compressed bytes; threshold temporarily set to 2 so P≥2
/// takes the marker path):
///   P=1 sequential 110 MiB/s; P=2 marker 83; P=3 marker 92; P=4 marker 82.
/// P=3 / P=1 ≈ 0.84× (not a ≥1.05× win). Fair `large-single.gz` similarly:
/// P=3 ≈ 0.91× P=1. Keep gate at 4.
const MIN_MARKER_PARALLEL_THREADS: usize = 4;

/// Idle park when a worker finds empty work queues (stored path, independent
/// members, estimated workers, BGZF). 1 ms parks added measurable wake latency;
/// ~100 µs keeps cancellation/work responsiveness without busy-spinning.
const WORKER_IDLE_PARK: Duration = Duration::from_micros(100);

/// Park when a result channel is full (stored / independent / BGZF senders).
/// Matches the intent of the native-path 25 µs full-channel parks: short enough
/// to hand off quickly, long enough to avoid pegging a core on a full buffer.
const RESULT_CHANNEL_FULL_PARK: Duration = Duration::from_micros(100);

/// Already-tuned full-channel park for native estimated/resolve result senders.
const NATIVE_RESULT_CHANNEL_FULL_PARK: Duration = Duration::from_micros(25);

/// Coordinator `recv_timeout` while reordering worker results. Polls
/// cancellation promptly without the former 10 ms worst-case lag.
const COORDINATOR_RECV_TIMEOUT: Duration = Duration::from_millis(2);

pub(crate) trait Output {
    fn emit(&mut self, chunk: Vec<u8>) -> Result<(), DecodeError>;

    fn emit_reusable(&mut self, chunk: Vec<u8>) -> Result<Vec<u8>, DecodeError> {
        self.emit(chunk)?;
        Ok(Vec::new())
    }
}

pub(crate) struct DirectOutput<'a, W> {
    writer: &'a mut W,
}

impl<'a, W> DirectOutput<'a, W> {
    pub(crate) const fn new(writer: &'a mut W) -> Self {
        Self { writer }
    }
}

impl<W: Write> Output for DirectOutput<'_, W> {
    fn emit(&mut self, chunk: Vec<u8>) -> Result<(), DecodeError> {
        self.writer
            .write_all(&chunk)
            .map_err(DecodeError::output_io)
    }

    fn emit_reusable(&mut self, mut chunk: Vec<u8>) -> Result<Vec<u8>, DecodeError> {
        self.writer
            .write_all(&chunk)
            .map_err(DecodeError::output_io)?;
        chunk.clear();
        Ok(chunk)
    }
}

/// RAII wrapper around zlib-rs's zlib-compatible raw-inflate ABI.
pub(crate) struct RawInflater {
    pub(crate) stream: z::z_stream,
    initialized: bool,
}

impl RawInflater {
    pub(crate) fn new() -> Result<Self, DecodeError> {
        let mut result = Self {
            stream: z::z_stream::default(),
            initialized: false,
        };

        // SAFETY:
        // - `result.stream` is a live, uniquely borrowed `z_stream`.
        // - `zlibVersion` returns a static NUL-terminated version string.
        // - the structure size matches the exact Rust ABI type passed.
        // - `-15` requests raw DEFLATE with a 32 KiB window.
        let status = unsafe {
            z::inflateInit2_(
                &mut result.stream,
                -15,
                z::zlibVersion(),
                size_of::<z::z_stream>() as i32,
            )
        };
        if status != z::Z_OK {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: 0,
                reason: DeflateErrorKind::BackendStatus(status),
            });
        }
        result.initialized = true;
        Ok(result)
    }

    pub(crate) fn message(&self) -> Option<String> {
        if self.stream.msg.is_null() {
            return None;
        }
        // SAFETY: while the initialized zlib stream is live, zlib owns `msg`
        // as a valid NUL-terminated diagnostic string or leaves it null.
        Some(
            unsafe { CStr::from_ptr(self.stream.msg) }
                .to_string_lossy()
                .into_owned(),
        )
    }

    pub(crate) fn reset(&mut self, bit_offset: u64) -> Result<(), DecodeError> {
        // SAFETY: this wrapper owns a successfully initialized stream and
        // holds its unique mutable borrow. `inflateReset` retains the raw
        // window mode selected by `inflateInit2_`.
        let status = unsafe { z::inflateReset(&mut self.stream) };
        if status == z::Z_OK {
            Ok(())
        } else {
            Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::BackendStatus(status),
            })
        }
    }

    /// Installs remaining bits from a mid-byte compressed start (LSB-first).
    pub(crate) fn prime(
        &mut self,
        bits: u8,
        value: u8,
        bit_offset: u64,
    ) -> Result<(), DecodeError> {
        // SAFETY: the stream is initialized and uniquely borrowed. zlib accepts
        // at most 16 low-order bits before the first inflate call; this wrapper
        // supplies at most the seven unread bits from one source byte.
        let status =
            unsafe { z::inflatePrime(&mut self.stream, i32::from(bits), i32::from(value)) };
        if status == z::Z_OK {
            Ok(())
        } else {
            Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::BackendStatus(status),
            })
        }
    }

    /// Copies a predecessor window into zlib as the inflate dictionary.
    pub(crate) fn set_dictionary(
        &mut self,
        window: &Window,
        bit_offset: u64,
    ) -> Result<(), DecodeError> {
        if window.as_slice().is_empty() {
            return Ok(());
        }
        // SAFETY: `window` remains immutably borrowed for the call, and its
        // slice is no larger than DEFLATE's 32 KiB history limit.
        let status = unsafe {
            z::inflateSetDictionary(
                &mut self.stream,
                window.as_slice().as_ptr(),
                window.as_slice().len() as u32,
            )
        };
        if status == z::Z_OK {
            Ok(())
        } else {
            Err(DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::BackendStatus(status),
            })
        }
    }

    /// Prepares raw inflate at an absolute compressed bit offset with history.
    ///
    /// Returns the compressed **byte** cursor position from which subsequent
    /// inflate input must be read (past any mid-byte primed bits).
    ///
    /// When `skip_gzip_header` is true, `window` is empty, and `start_bit` is
    /// byte-aligned on a gzip/BGZF member header, inflate begins at the DEFLATE
    /// payload. htslib `.gzi` / BGZI indexes store block (header) starts; the
    /// seek and `decode_with_index` paths pass `true`. Parallel marker fallback
    /// must pass `false` so mid-stream `1f 8b` bytes are never misread as
    /// headers. Non-empty windows never skip a header.
    ///
    /// Inflate setup (create / mid-byte prime / dictionary) goes through
    /// [`InflateBackend::prepare_at_bit_offset`] so alternate backends share
    /// the same resume contract.
    pub(crate) fn prepare_at_bit_offset<R: ReadAt + ?Sized>(
        start_bit: u64,
        window: &Window,
        source: &R,
        page_size: usize,
        skip_gzip_header: bool,
    ) -> Result<(Self, u64), DecodeError> {
        let mut cursor = SourceCursor::new(source, page_size)?;
        let mut byte_offset = start_bit / 8;
        let mut effective_bit = start_bit;
        cursor.seek(byte_offset)?;

        if skip_gzip_header
            && window.as_slice().is_empty()
            && start_bit.is_multiple_of(8)
            && !cursor.at_end()
        {
            // Peek for gzip magic; always restore the cursor so a miss leaves
            // the offset as a raw DEFLATE bit position.
            let is_gzip_magic = matches!(cursor.read_exact::<2>(byte_offset), Ok([0x1f, 0x8b]));
            cursor.seek(byte_offset)?;
            if is_gzip_magic {
                let header = parse_member_header(&mut cursor, false)?;
                byte_offset = header.deflate_start;
                effective_bit = header.deflate_start.saturating_mul(8);
                cursor.seek(byte_offset)?;
            }
        }

        let skipped_bits = (effective_bit % 8) as u8;
        let first_byte = if skipped_bits != 0 {
            cursor.read_exact::<1>(byte_offset)?[0]
        } else {
            0
        };
        let inflater =
            <Self as InflateBackend>::prepare_at_bit_offset(first_byte, effective_bit, window)?;
        Ok((inflater, cursor.position()))
    }
}

impl Drop for RawInflater {
    fn drop(&mut self) {
        if self.initialized {
            // SAFETY: this wrapper calls `inflateEnd` exactly once for the
            // successfully initialized, uniquely owned stream.
            let _ = unsafe { z::inflateEnd(&mut self.stream) };
        }
    }
}

pub(crate) fn decode_source<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    match resolve_format(source, config)? {
        Format::Zlib => return decode_zlib(source, config, cancelled, output),
        Format::RawDeflate => return decode_raw_deflate(source, config, cancelled, output),
        Format::Gzip => {}
        Format::Auto => unreachable!("resolve_format returns Gzip, Zlib, or RawDeflate only"),
    }

    // BGZF block starts are normally tens of KiB apart and only their short
    // headers are needed for indexing. A small page avoids reading the
    // complete compressed payload before decoding.
    let bgzf_index = index_bgzf(source, config.input_page_size.min(256))?;
    if let Some(index) = bgzf_index {
        if index.len() > 1 {
            return decode_bgzf_parallel(source, config, cancelled, output, &index);
        }
    }
    if config.decoder_threads > 1 {
        if let Some(index) = index_stored_stream(source, config.input_page_size.min(256))? {
            if index.tasks.len() > 1 {
                return decode_stored_parallel(source, config, cancelled, output, &index);
            }
        }
        if let Some(index) = index_independent_members(source, config)? {
            return decode_independent_members(source, config, cancelled, output, &index);
        }
        // Expensive marker/estimated path: only when enough threads to beat
        // sequential zlib-rs (see MIN_MARKER_PARALLEL_THREADS).
        if config.decoder_threads >= MIN_MARKER_PARALLEL_THREADS {
            let grid_size = adjusted_compressed_chunk_size(source, config)?;
            return decode_rapidgzip_estimated(source, config, cancelled, output, grid_size);
        }
    }
    decode_source_sequential(source, config, cancelled, output)
}

/// Resolves [`Format::Auto`] into gzip or zlib; forced formats are returned as-is.
///
/// Auto never selects raw DEFLATE (no magic bytes).
fn resolve_format<R: ReadAt + ?Sized>(source: &R, config: &Config) -> Result<Format, DecodeError> {
    match config.format {
        Format::Gzip => Ok(Format::Gzip),
        Format::Zlib => Ok(Format::Zlib),
        Format::RawDeflate => Ok(Format::RawDeflate),
        Format::Auto => {
            if looks_like_zlib(source, config.input_page_size)? {
                Ok(Format::Zlib)
            } else {
                Ok(Format::Gzip)
            }
        }
    }
}

/// Raw DEFLATE (RFC 1951) entry: single-stream marker parallel when threads and
/// size allow, else sequential. No gzip/zlib wrapper; optional external CRC via
/// `raw_crc32_list`. `keep_index` is rejected at config build time.
fn decode_raw_deflate<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    debug_assert!(
        !config.keep_index,
        "keep_index for raw DEFLATE is rejected by DecoderBuilder::build"
    );
    if config.decoder_threads >= MIN_MARKER_PARALLEL_THREADS
        && raw_payload_large_enough_for_marker(source, config)?
    {
        let compressed_chunk_size = adjusted_compressed_chunk_size(source, config)?;
        return decode_raw_deflate_estimated(
            source,
            config,
            cancelled,
            output,
            compressed_chunk_size,
        );
    }
    decode_raw_deflate_sequential(source, config, cancelled, output)
}

/// Compressed length gate for raw DEFLATE marker parallel (no wrapper bytes).
fn raw_payload_large_enough_for_marker<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
) -> Result<bool, DecodeError> {
    payload_large_enough_for_marker(source, config, 0)
}

/// Sequential raw DEFLATE (RFC 1951) decode: no wrapper, no integrity trailer.
///
/// Single stream from compressed offset 0 to stream end. Leftover input after
/// end-of-stream is an error. No random-access index (`keep_index` is rejected
/// at config build time).
///
/// Inflate goes through [`InflateBackend`] monomorphized to [`RawInflater`].
fn decode_raw_deflate_sequential<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    decode_raw_deflate_sequential_with::<R, O, RawInflater>(source, config, cancelled, output)
}

/// Generic sequential raw DEFLATE path over [`InflateBackend`].
fn decode_raw_deflate_sequential_with<R, O, I>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
    I: InflateBackend,
{
    debug_assert!(
        !config.keep_index,
        "keep_index for raw DEFLATE is rejected by DecoderBuilder::build"
    );

    let mut cursor = SourceCursor::new(source, config.input_page_size)?;
    if cursor.at_end() {
        return Err(DecodeError::InvalidDeflate {
            bit_offset: 0,
            reason: DeflateErrorKind::Truncated,
        });
    }

    let mut index_builder = new_index_builder(config);
    let mut total_output = 0_u64;
    let mut inflater = I::create()?;
    let mut decoded = Vec::with_capacity(config.decoded_chunk_size);
    let verify_external_crc = !config.raw_crc32_list.is_empty();
    let mut external_crc = Crc32::new();

    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }
        if cursor.at_end() {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: cursor.position().saturating_mul(8),
                reason: DeflateErrorKind::Truncated,
            });
        }

        if decoded.capacity() < config.decoded_chunk_size {
            decoded.reserve_exact(config.decoded_chunk_size - decoded.capacity());
        }
        // Sequential path always emits then clears; force empty so
        // InflateBackend appends into a fresh buffer for this step.
        decoded.clear();

        let input = cursor.available()?;
        let step = inflater.inflate(input, &mut decoded, InflateFlush::NoFlush)?;
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
            if verify_external_crc {
                external_crc.update(&decoded);
            }
            index_builder.push_output(&decoded);
            decoded = output.emit_reusable(decoded)?;
        }

        match step.status {
            inflate_status::STREAM_END => break,
            inflate_status::OK => {
                if step.consumed == 0 && step.produced == 0 {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: cursor.position().saturating_mul(8),
                        reason: DeflateErrorKind::Stalled,
                    });
                }
            }
            inflate_status::BUF_ERROR if step.consumed > 0 || step.produced > 0 => {}
            inflate_status::BUF_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
                });
            }
            inflate_status::NEED_DICT => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: 0,
                    reason: DeflateErrorKind::UnexpectedDictionary,
                });
            }
            inflate_status::DATA_ERROR => {
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

    // Single stream must consume the entire source (no trailer, no concat).
    if !cursor.at_end() {
        return Err(DecodeError::InvalidDeflate {
            bit_offset: cursor.position().saturating_mul(8),
            reason: DeflateErrorKind::InvalidData,
        });
    }

    let final_length = source
        .len()
        .map_err(|error| DecodeError::input_io(cursor.position(), error))?;
    if final_length != cursor.length() {
        return Err(DecodeError::input_io(
            cursor.position(),
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during decoding",
            ),
        ));
    }

    crate::crc32::verify_raw_crc32_list(&config.raw_crc32_list, &external_crc)?;

    Ok(finish_report(
        config,
        cursor.position(),
        total_output,
        1,
        index_builder,
    ))
}

/// zlib (RFC 1950) entry: multi-stream parallel, single-stream marker parallel,
/// or sequential.
///
/// Composition when `decoder_threads > 1`:
/// 1. **Concatenated multi-stream** (≥2 CMF/FLG…Adler frames): two-pass
///    discard-index then stream-granularity parallel zlib-rs. The discard index
///    is skipped when the large-file single-stream marker path is preferred so a
///    solitary long stream is not inflated twice.
/// 2. **Single-stream DEFLATE-level parallel** when the thread budget and size
///    gate allow (`decode_zlib_parallel_or_sequential`). Remaining concatenated
///    streams after the first use multi-stream parallel (or sequential for a
///    single tiny remainder if indexing is skipped).
/// 3. **Sequential** otherwise.
fn decode_zlib<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    if config.decoder_threads > 1 {
        // Long solitary streams at threads ≥ 4: DEFLATE-level marker path.
        // Do not run the multi-stream discard-index first — that would fully
        // inflate once for boundary discovery before decoding again.
        if prefer_zlib_single_stream_parallel(source, config)? {
            return decode_zlib_parallel_or_sequential(source, config, cancelled, output);
        }
        // Multi-stream stream-granularity parallel only when a full discard
        // index is cheap: small archives (or when we already know multi-stream
        // tails). Large files at threads 2–3 stay sequential once so we never
        // pay discard-inflate + re-decode on a single long stream.
        if !zlib_payload_large_enough_for_marker(source, config)? {
            if let Some(index) = index_zlib_streams(source, config, cancelled)? {
                return decode_zlib_streams_parallel(source, config, cancelled, output, &index);
            }
        }
    }
    decode_zlib_sequential(source, config, cancelled, output)
}

/// Compressed length gate: at least two full grid cells plus optional wrapper.
fn payload_large_enough_for_marker<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
    wrapper_bytes: u64,
) -> Result<bool, DecodeError> {
    let compressed_chunk_size = adjusted_compressed_chunk_size(source, config)?;
    let length = source
        .len()
        .map_err(|error| DecodeError::input_io(0, error))?;
    let min_parallel_bytes = (compressed_chunk_size as u64)
        .saturating_mul(2)
        .saturating_add(wrapper_bytes);
    Ok(length >= min_parallel_bytes)
}

/// Compressed length gate shared by single-stream zlib marker parallel routing.
fn zlib_payload_large_enough_for_marker<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
) -> Result<bool, DecodeError> {
    // Header (2) + trailer (4) + at least two full grid cells of DEFLATE.
    payload_large_enough_for_marker(source, config, 6)
}

/// True when the single-stream marker/estimated zlib path is expected to win
/// (large payload + enough threads).
fn prefer_zlib_single_stream_parallel<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
) -> Result<bool, DecodeError> {
    if config.decoder_threads < MIN_MARKER_PARALLEL_THREADS {
        return Ok(false);
    }
    zlib_payload_large_enough_for_marker(source, config)
}

/// Mutable continuation state for sequential zlib multi-stream decoding.
///
/// Groups cursor, index builder, and running totals so continuation after a
/// parallel first stream (and the full-file sequential entry) share one arg.
struct ZlibSequentialState<'a, R: ReadAt + ?Sized> {
    cursor: SourceCursor<'a, R>,
    index_builder: IndexBuilder,
    member_count: u64,
    total_output: u64,
}

/// Sequential zlib (RFC 1950) decode: CMF/FLG, raw DEFLATE, Adler-32 trailer.
///
/// Concatenated zlib streams are accepted. Used for small streams, low thread
/// budgets, multi-stream tails after a parallel first stream, and as the
/// full-file fallback. Adler-32 verification follows `crc32_enabled`.
/// Random-access indexes remain gzip/BGZF-oriented (checkpoints at stream
/// starts only when `keep_index` is set).
fn decode_zlib_sequential<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    decode_zlib_sequential_from(
        source,
        config,
        cancelled,
        output,
        ZlibSequentialState {
            cursor: SourceCursor::new(source, config.input_page_size)?,
            index_builder: new_index_builder(config),
            member_count: 0,
            total_output: 0,
        },
    )
}

/// Sequential zlib members starting at the cursor's current position.
///
/// Used as the full sequential entry and as the multi-stream continuation
/// after a parallel first stream finishes.
///
/// Inflate goes through [`InflateBackend`] monomorphized to [`RawInflater`].
fn decode_zlib_sequential_from<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    state: ZlibSequentialState<'_, R>,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    decode_zlib_sequential_from_with::<R, O, RawInflater>(source, config, cancelled, output, state)
}

/// Generic sequential zlib path over [`InflateBackend`].
fn decode_zlib_sequential_from_with<R, O, I>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    state: ZlibSequentialState<'_, R>,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
    I: InflateBackend,
{
    let ZlibSequentialState {
        mut cursor,
        mut index_builder,
        mut member_count,
        mut total_output,
    } = state;
    // Prefer Block only when collecting an index so intermediate block
    // checkpoints are available; default path stays on NoFlush.
    let flush = if config.keep_index {
        InflateFlush::Block
    } else {
        InflateFlush::NoFlush
    };
    let started_with_members = member_count;
    // Reuse one raw-inflate stream across concatenated zlib members. Each
    // member starts with an empty window; `reset` clears history.
    let mut inflater = I::create()?;
    let mut decoded = Vec::with_capacity(config.decoded_chunk_size);

    while !cursor.at_end() {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }

        let header = parse_zlib_header(&mut cursor, member_count == 0)?;
        debug_assert!(header.start <= header.deflate_start);
        debug_assert_eq!(header.deflate_start, cursor.position());
        // Empty window at each zlib stream DEFLATE start.
        index_builder.force_checkpoint(header.deflate_start.saturating_mul(8), true);

        inflater.reset(header.deflate_start.saturating_mul(8))?;
        let mut adler = Adler32::new();

        loop {
            if cancelled.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            if cursor.at_end() {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
                });
            }

            if decoded.capacity() < config.decoded_chunk_size {
                decoded.reserve_exact(config.decoded_chunk_size - decoded.capacity());
            }
            // Sequential path always emits then clears; force empty so
            // InflateBackend appends into a fresh buffer for this step.
            decoded.clear();

            let input = cursor.available()?;
            let step = inflater.inflate(input, &mut decoded, flush)?;
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
                if config.crc32_enabled {
                    adler.update(&decoded);
                }
                index_builder.push_output(&decoded);
                decoded = output.emit_reusable(decoded)?;
            }

            // Bit-accurate offset: bytes consumed so far, less bits still in
            // the inflater bit buffer. Only meaningful with Block flush;
            // with NoFlush keep_index is false and this is unused.
            if config.keep_index {
                let bit_pos = cursor
                    .position()
                    .saturating_mul(8)
                    .saturating_sub(u64::from(step.unused_bits));
                if step.status == inflate_status::STREAM_END || step.at_block_end {
                    index_builder.maybe_checkpoint(bit_pos);
                }
            }

            match step.status {
                inflate_status::STREAM_END => break,
                inflate_status::OK => {
                    if step.consumed == 0 && step.produced == 0 {
                        return Err(DecodeError::InvalidDeflate {
                            bit_offset: cursor.position().saturating_mul(8),
                            reason: DeflateErrorKind::Stalled,
                        });
                    }
                }
                inflate_status::BUF_ERROR if step.consumed > 0 || step.produced > 0 => {}
                inflate_status::BUF_ERROR => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: cursor.position().saturating_mul(8),
                        reason: DeflateErrorKind::Truncated,
                    });
                }
                inflate_status::NEED_DICT => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: header.deflate_start.saturating_mul(8),
                        reason: DeflateErrorKind::UnexpectedDictionary,
                    });
                }
                inflate_status::DATA_ERROR => {
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

        // Adler-32 trailer is four big-endian bytes (RFC 1950).
        let footer_offset = cursor.position();
        let footer = match cursor.read_exact::<4>(footer_offset) {
            Ok(bytes) => bytes,
            Err(DecodeError::InvalidGzip {
                reason: GzipErrorKind::Truncated,
                ..
            }) => {
                return Err(DecodeError::InvalidZlib {
                    offset: footer_offset,
                    reason: ZlibErrorKind::Truncated,
                });
            }
            Err(error) => return Err(error),
        };
        let expected_adler = u32::from_be_bytes(footer);
        if config.crc32_enabled {
            let actual_adler = adler.finish();
            if expected_adler != actual_adler {
                return Err(DecodeError::ChecksumMismatch {
                    member: member_count,
                    expected: expected_adler,
                    actual: actual_adler,
                });
            }
        }

        member_count += 1;
    }

    if member_count == started_with_members && started_with_members == 0 {
        return Err(DecodeError::InvalidZlib {
            offset: 0,
            reason: ZlibErrorKind::BadHeader,
        });
    }

    let final_length = source
        .len()
        .map_err(|error| DecodeError::input_io(cursor.position(), error))?;
    if final_length != cursor.length() {
        return Err(DecodeError::input_io(
            cursor.position(),
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during decoding",
            ),
        ));
    }

    Ok(finish_report(
        config,
        cursor.position(),
        total_output,
        member_count,
        index_builder,
    ))
}

/// One complete zlib stream (CMF/FLG … DEFLATE … Adler-32) in a concatenated
/// multi-stream source.
#[derive(Clone, Copy, Debug)]
struct ZlibStreamRange {
    /// Absolute byte offset of the CMF byte.
    start: u64,
    /// Absolute byte offset of the first DEFLATE byte (after CMF/FLG).
    deflate_start: u64,
    /// Absolute byte offset past the four-byte Adler-32 trailer.
    end: u64,
}

/// Two-pass discovery: discard-inflate every zlib stream from the start of the
/// source. Returns `Some` only when ≥2 complete streams are present so the
/// caller can parallel re-decode; single-stream files return `None` (caller
/// uses sequential or single-stream marker parallel without depending on this
/// index for output).
fn index_zlib_streams<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
) -> Result<Option<Vec<ZlibStreamRange>>, DecodeError> {
    index_zlib_streams_from(source, config, cancelled, 0)
}

/// Discard-inflate zlib streams beginning at `start_offset`.
///
/// When `start_offset == 0`, returns `None` if fewer than two streams exist.
/// When continuing after an already-emitted first stream (`start_offset > 0`),
/// returns `Some` for one or more remaining streams (a single remainder is
/// still useful for a uniform parallel worker path, but callers may also fall
/// back to sequential for one stream).
fn index_zlib_streams_from<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    start_offset: u64,
) -> Result<Option<Vec<ZlibStreamRange>>, DecodeError> {
    let mut cursor = SourceCursor::new(source, config.input_page_size)?;
    if start_offset > 0 {
        cursor.seek(start_offset)?;
    }
    if cursor.at_end() {
        return Ok(None);
    }

    let mut ranges = Vec::new();
    // Discard scratch: we only need STREAM_END / consumed input, not payload.
    // Clear each step so InflateBackend writes into spare capacity only.
    let discard_cap = config.decoded_chunk_size.clamp(8 * 1024, 64 * 1024);
    let mut discard = Vec::with_capacity(discard_cap);
    // Reuse one inflater across multi-stream zlib indexing (empty window per stream).
    let mut inflater = <RawInflater as InflateBackend>::create()?;

    while !cursor.at_end() {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }

        let first_member = ranges.is_empty() && start_offset == 0;
        let header = parse_zlib_header(&mut cursor, first_member)?;
        debug_assert_eq!(header.deflate_start, cursor.position());

        InflateBackend::reset(&mut inflater, header.deflate_start.saturating_mul(8))?;
        loop {
            if cancelled.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            if cursor.at_end() {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
                });
            }

            if discard.capacity() < discard_cap {
                discard.reserve(discard_cap - discard.capacity());
            }
            discard.clear();
            let input = cursor.available()?;
            let step = inflater.inflate(input, &mut discard, InflateFlush::NoFlush)?;
            cursor.advance(step.consumed);

            match step.status {
                inflate_status::STREAM_END => break,
                inflate_status::OK => {
                    if step.consumed == 0 && step.produced == 0 {
                        return Err(DecodeError::InvalidDeflate {
                            bit_offset: cursor.position().saturating_mul(8),
                            reason: DeflateErrorKind::Stalled,
                        });
                    }
                }
                inflate_status::BUF_ERROR if step.consumed > 0 || step.produced > 0 => {}
                inflate_status::BUF_ERROR => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: cursor.position().saturating_mul(8),
                        reason: DeflateErrorKind::Truncated,
                    });
                }
                inflate_status::NEED_DICT => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: header.deflate_start.saturating_mul(8),
                        reason: DeflateErrorKind::UnexpectedDictionary,
                    });
                }
                inflate_status::DATA_ERROR => {
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

        // Skip Adler-32 trailer (verified on the emit pass).
        let footer_offset = cursor.position();
        match cursor.read_exact::<4>(footer_offset) {
            Ok(_) => {}
            Err(DecodeError::InvalidGzip {
                reason: GzipErrorKind::Truncated,
                ..
            }) => {
                return Err(DecodeError::InvalidZlib {
                    offset: footer_offset,
                    reason: ZlibErrorKind::Truncated,
                });
            }
            Err(error) => return Err(error),
        }

        ranges.push(ZlibStreamRange {
            start: header.start,
            deflate_start: header.deflate_start,
            end: cursor.position(),
        });
    }

    if start_offset == 0 && ranges.len() < 2 {
        return Ok(None);
    }
    if ranges.is_empty() {
        return Ok(None);
    }
    Ok(Some(ranges))
}

/// Decode one zlib stream range into `output`, verifying Adler-32 when enabled.
///
/// Inflate goes through [`InflateBackend`] monomorphized to [`RawInflater`].
/// Each step reserves at least `output_step` spare and hard-caps produce via
/// [`InflateBackend::inflate_capped`] so batch size matches the prior raw
/// `avail_out` limit.
///
/// Returns the number of decompressed bytes appended.
#[allow(clippy::too_many_arguments)]
fn decode_zlib_stream_into<R: ReadAt + ?Sized>(
    source: &R,
    range: ZlibStreamRange,
    member: u64,
    config: &Config,
    cancelled: &AtomicBool,
    compressed: &mut Vec<u8>,
    output: &mut Vec<u8>,
    inflater: &mut RawInflater,
) -> Result<usize, DecodeError> {
    let output_start = output.len();
    let mut input_offset = range.deflate_start;
    // Compressed payload ends at the Adler trailer.
    let payload_end = range.end.saturating_sub(4);
    if payload_end < range.deflate_start || range.end < range.deflate_start.saturating_add(4) {
        return Err(DecodeError::InvalidZlib {
            offset: range.start,
            reason: ZlibErrorKind::Truncated,
        });
    }

    let input_step = config.input_page_size.clamp(32 * 1024, 64 * 1024);
    let output_step = config.decoded_chunk_size.clamp(32 * 1024, 256 * 1024);
    let mut adler = Adler32::new();
    InflateBackend::reset(inflater, range.deflate_start.saturating_mul(8))?;

    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }
        if input_offset > payload_end {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: input_offset.saturating_mul(8),
                reason: DeflateErrorKind::InvalidData,
            });
        }

        let remaining = payload_end.saturating_sub(input_offset);
        let input_length = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(input_step);
        if input_length > 0 {
            read_range_reuse(source, input_offset, input_length, compressed)?;
        } else {
            compressed.clear();
        }

        let mut relative_input = 0_usize;
        loop {
            // Need more compressed input?
            if relative_input >= compressed.len() && input_offset < payload_end {
                break;
            }

            output.reserve(output_step);
            let chunk_start = output.len();
            let input = &compressed[relative_input..];
            let step =
                inflater.inflate_capped(input, output, InflateFlush::NoFlush, output_step)?;
            relative_input += step.consumed;
            input_offset = input_offset.saturating_add(step.consumed as u64);

            if config.crc32_enabled && step.produced > 0 {
                adler.update(&output[chunk_start..]);
            }

            match step.status {
                inflate_status::STREAM_END => {
                    // Adler-32 is four big-endian bytes at payload_end.
                    read_range_reuse(source, payload_end, 4, compressed)?;
                    let expected_adler =
                        u32::from_be_bytes(compressed[0..4].try_into().expect("four bytes"));
                    if config.crc32_enabled {
                        let actual_adler = adler.finish();
                        if expected_adler != actual_adler {
                            return Err(DecodeError::ChecksumMismatch {
                                member,
                                expected: expected_adler,
                                actual: actual_adler,
                            });
                        }
                    }
                    // Defensive: stream end should land at the trailer.
                    if input_offset != payload_end {
                        // Backend may not have consumed every spare byte of the
                        // last page if we over-read; accept any offset ≤ payload_end
                        // only when inflate reported stream end (canonical).
                        // Overshoot past the trailer is corrupt.
                        if input_offset > payload_end {
                            return Err(DecodeError::InvalidDeflate {
                                bit_offset: input_offset.saturating_mul(8),
                                reason: DeflateErrorKind::InvalidData,
                            });
                        }
                    }
                    return Ok(output.len() - output_start);
                }
                inflate_status::OK => {
                    if step.consumed == 0 && step.produced == 0 {
                        if input_offset >= payload_end {
                            return Err(DecodeError::InvalidDeflate {
                                bit_offset: input_offset.saturating_mul(8),
                                reason: DeflateErrorKind::Truncated,
                            });
                        }
                        // Need more input from the next page.
                        break;
                    }
                }
                inflate_status::BUF_ERROR if step.consumed > 0 || step.produced > 0 => {}
                inflate_status::BUF_ERROR => {
                    if input_offset >= payload_end {
                        return Err(DecodeError::InvalidDeflate {
                            bit_offset: input_offset.saturating_mul(8),
                            reason: DeflateErrorKind::Truncated,
                        });
                    }
                    break;
                }
                inflate_status::NEED_DICT => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: range.deflate_start.saturating_mul(8),
                        reason: DeflateErrorKind::UnexpectedDictionary,
                    });
                }
                inflate_status::DATA_ERROR => {
                    let _diagnostic = InflateBackend::message(inflater);
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: input_offset.saturating_mul(8),
                        reason: DeflateErrorKind::InvalidData,
                    });
                }
                other => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: input_offset.saturating_mul(8),
                        reason: DeflateErrorKind::BackendStatus(other),
                    });
                }
            }

            // Need a larger output buffer with the same input (avail_out was 0),
            // or more compressed input from the next page.
            if relative_input < compressed.len() {
                continue;
            }
            if input_offset < payload_end {
                break;
            }
            // All compressed bytes fed; keep calling inflate with empty input so
            // a final pending output chunk can surface STREAM_END.
            if step.produced == 0 {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: input_offset.saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
                });
            }
        }
    }
}

struct ZlibMultiResult {
    index: usize,
    result: Result<ZlibMultiDecoded, DecodeError>,
}

struct ZlibMultiDecoded {
    bytes: Vec<u8>,
    /// Uncompressed size of each stream in this task, emission order.
    stream_sizes: Vec<usize>,
}

fn send_zlib_multi_result(
    sender: &mpsc::SyncSender<ZlibMultiResult>,
    stopped: &AtomicBool,
    mut result: ZlibMultiResult,
) {
    loop {
        if stopped.load(Ordering::Relaxed) {
            return;
        }
        match sender.try_send(result) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => return,
            Err(TrySendError::Full(returned)) => {
                result = returned;
                thread::park_timeout(RESULT_CHANNEL_FULL_PARK);
            }
        }
    }
}

/// Parallel decode of independent concatenated zlib streams (stream granularity).
///
/// `ranges` must contain at least one stream. Member numbers for checksum
/// errors start at 0.
fn decode_zlib_streams_parallel<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    ranges: &[ZlibStreamRange],
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    decode_zlib_streams_parallel_from(
        source,
        config,
        cancelled,
        output,
        ranges,
        0,
        0,
        new_index_builder(config),
    )
}

/// Parallel decode of zlib streams with pre-existing member/output accounting
/// (used after a single-stream marker path finishes the first frame).
#[allow(clippy::too_many_arguments)]
fn decode_zlib_streams_parallel_from<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    ranges: &[ZlibStreamRange],
    member_base: u64,
    mut total_output: u64,
    mut index_builder: IndexBuilder,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    debug_assert!(!ranges.is_empty());
    // One stream per task keeps member error indices trivial and matches the
    // independent-stream model; many tiny streams still scale via the worker pool.
    let task_count = ranges.len();
    let worker_count = config.decoder_threads.min(task_count).max(1);
    let task_queue = Arc::new(Injector::new());
    let stopped = Arc::new(AtomicBool::new(false));
    let available_tasks = Arc::new(AtomicUsize::new(0));
    let work_signal = Arc::new((Mutex::new(()), Condvar::new()));
    let pending_limit = config.in_flight_chunks.max(worker_count).max(1);
    let task_window = pending_limit.min(task_count);
    for index in 0..task_window {
        task_queue.push(index);
    }
    available_tasks.store(task_window, Ordering::Release);
    let (sender, receiver) = mpsc::sync_channel::<ZlibMultiResult>(pending_limit);
    let mut next_to_schedule = task_window;
    let mut next_to_emit = 0_usize;
    let mut running = task_window;
    let mut reordered = BTreeMap::new();
    let mut member_count = member_base;

    let scoped_result = thread::scope(|scope| -> Result<(), DecodeError> {
        let _stop_on_exit = StopGuard(&stopped);
        for _ in 0..worker_count {
            let queue = Arc::clone(&task_queue);
            let worker_stopped = Arc::clone(&stopped);
            let available_tasks = Arc::clone(&available_tasks);
            let work_signal = Arc::clone(&work_signal);
            let sender = sender.clone();
            scope.spawn(move || {
                // Compressed page + inflater reused across streams; decoded
                // scratch keeps capacity after failed tasks (success transfers
                // ownership to the coordinator).
                let mut compressed = Vec::new();
                let mut decoded_scratch = Vec::new();
                let mut inflater = None;

                while !worker_stopped.load(Ordering::Relaxed) && !cancelled.load(Ordering::Relaxed)
                {
                    let index = loop {
                        match queue.steal() {
                            Steal::Success(index) => {
                                available_tasks.fetch_sub(1, Ordering::AcqRel);
                                break index;
                            }
                            Steal::Retry => std::hint::spin_loop(),
                            Steal::Empty => {
                                if worker_stopped.load(Ordering::Relaxed)
                                    || cancelled.load(Ordering::Relaxed)
                                {
                                    return;
                                }
                                let (lock, signal) = &*work_signal;
                                let guard = lock.lock().expect("zlib multi work mutex poisoned");
                                let _ = signal
                                    .wait_timeout_while(guard, WORKER_IDLE_PARK, |_| {
                                        available_tasks.load(Ordering::Acquire) == 0
                                            && !worker_stopped.load(Ordering::Relaxed)
                                            && !cancelled.load(Ordering::Relaxed)
                                    })
                                    .expect("zlib multi work mutex poisoned");
                            }
                        }
                    };

                    let range = ranges[index];
                    let member = member_base.saturating_add(index as u64);
                    decoded_scratch.clear();
                    let result = (|| {
                        if inflater.is_none() {
                            inflater = Some(<RawInflater as InflateBackend>::create()?);
                        }
                        let inflater = inflater
                            .as_mut()
                            .expect("inflater initialized immediately above");
                        let produced = decode_zlib_stream_into(
                            source,
                            range,
                            member,
                            config,
                            cancelled,
                            &mut compressed,
                            &mut decoded_scratch,
                            inflater,
                        )?;
                        Ok(ZlibMultiDecoded {
                            bytes: std::mem::take(&mut decoded_scratch),
                            stream_sizes: vec![produced],
                        })
                    })();
                    send_zlib_multi_result(
                        &sender,
                        &worker_stopped,
                        ZlibMultiResult { index, result },
                    );
                }
            });
        }
        drop(sender);

        while next_to_emit < task_count {
            if cancelled.load(Ordering::Relaxed) {
                stopped.store(true, Ordering::Relaxed);
                return Err(DecodeError::Cancelled);
            }
            let result = match receiver.recv_timeout(COORDINATOR_RECV_TIMEOUT) {
                Ok(result) => {
                    running = running.saturating_sub(1);
                    result
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    stopped.store(true, Ordering::Relaxed);
                    return Err(DecodeError::WorkerPanicked);
                }
            };
            reordered.insert(result.index, result.result);

            while let Some(result) = reordered.remove(&next_to_emit) {
                let decoded = match result {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        stopped.store(true, Ordering::Relaxed);
                        return Err(error);
                    }
                };
                let next_total = total_output.checked_add(decoded.bytes.len() as u64).ok_or(
                    DecodeError::OutputLimitExceeded {
                        limit: config.output_limit.unwrap_or(u64::MAX),
                    },
                )?;
                if config.output_limit.is_some_and(|limit| next_total > limit) {
                    stopped.store(true, Ordering::Relaxed);
                    return Err(DecodeError::OutputLimitExceeded {
                        limit: config.output_limit.expect("checked as some"),
                    });
                }

                // Empty history at each independent zlib stream DEFLATE start.
                if index_builder.tracks_output() {
                    let range = ranges[next_to_emit];
                    if index_builder.enabled() {
                        index_builder.force_checkpoint(range.deflate_start.saturating_mul(8), true);
                    }
                    let mut offset = 0_usize;
                    for &size in &decoded.stream_sizes {
                        index_builder.push_output(&decoded.bytes[offset..offset + size]);
                        offset += size;
                    }
                }
                total_output = next_total;
                member_count = member_count.saturating_add(1);
                if !decoded.bytes.is_empty() {
                    output.emit(decoded.bytes)?;
                }
                next_to_emit += 1;
            }

            while running < worker_count
                && next_to_schedule < task_count
                && reordered.len() < pending_limit
            {
                let (lock, signal) = &*work_signal;
                let _guard = lock.lock().expect("zlib multi work mutex poisoned");
                task_queue.push(next_to_schedule);
                available_tasks.fetch_add(1, Ordering::Release);
                next_to_schedule += 1;
                running += 1;
                signal.notify_one();
            }
        }
        stopped.store(true, Ordering::Relaxed);
        Ok(())
    });
    stopped.store(true, Ordering::Relaxed);
    scoped_result?;

    let compressed_bytes = ranges.last().map_or(0, |range| range.end);
    let final_length = source
        .len()
        .map_err(|error| DecodeError::input_io(compressed_bytes, error))?;
    // Last stream must end at EOF (full-file multi path and first-stream + tail path).
    if final_length != compressed_bytes {
        return Err(DecodeError::input_io(
            compressed_bytes,
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during decoding",
            ),
        ));
    }

    Ok(finish_report(
        config,
        compressed_bytes,
        total_output,
        member_count,
        index_builder,
    ))
}

fn adjusted_compressed_chunk_size<R: ReadAt + ?Sized>(
    _source: &R,
    config: &Config,
) -> Result<usize, DecodeError> {
    Ok(config.compressed_chunk_size)
}

fn new_index_builder(config: &Config) -> IndexBuilder {
    IndexBuilder::new(
        config.keep_index,
        config.gather_line_offsets,
        config.checkpoint_spacing,
        config.compress_index_windows,
    )
}

fn finish_report(
    config: &Config,
    compressed_bytes: u64,
    decompressed_bytes: u64,
    member_count: u64,
    index_builder: IndexBuilder,
) -> DecodeReport {
    let line_count = index_builder.report_line_count();
    DecodeReport {
        compressed_bytes,
        decompressed_bytes,
        member_count,
        decoder_threads: config.decoder_threads,
        index: index_builder.finish(compressed_bytes, decompressed_bytes),
        line_count,
    }
}

#[derive(Clone, Copy)]
struct StoredMember {
    expected_crc: u32,
    expected_size: u32,
}

#[derive(Clone)]
struct StoredTask {
    member: usize,
    /// Set on the first task of each member: absolute byte offset of the
    /// member's DEFLATE stream start (empty-window seek point).
    member_deflate_start: Option<u64>,
    ranges: Vec<CompressedRange>,
    decoded_size: usize,
    last_in_member: bool,
}

struct StoredIndex {
    members: Vec<StoredMember>,
    tasks: Vec<StoredTask>,
    compressed_size: u64,
}

fn index_stored_stream<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
) -> Result<Option<StoredIndex>, DecodeError> {
    let mut cursor = SourceCursor::new(source, page_size)?;
    let mut members = Vec::new();
    let mut tasks = Vec::new();

    while !cursor.at_end() {
        let member_number = members.len() as u64;
        let header = parse_member_header(&mut cursor, members.is_empty())?;
        let mut member_ranges = Vec::new();
        let mut decoded_size = 0_u32;
        let mut block_start = header.deflate_start;
        loop {
            cursor.seek(block_start)?;
            let block_header = cursor.read_exact::<5>(block_start)?;
            let final_block = block_header[0] & 1 != 0;
            let block_type = (block_header[0] >> 1) & 0b11;
            if block_type != 0 {
                return Ok(None);
            }
            let length = u16::from_le_bytes([block_header[1], block_header[2]]);
            let complement = u16::from_le_bytes([block_header[3], block_header[4]]);
            if length != !complement {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: block_start.saturating_mul(8),
                    reason: DeflateErrorKind::InvalidData,
                });
            }
            let data_start = block_start + 5;
            let data_end = data_start.saturating_add(u64::from(length));
            if data_end > cursor.length() {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: block_start.saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
                });
            }
            member_ranges.push(CompressedRange {
                start: data_start,
                end: data_end,
            });
            decoded_size = decoded_size.wrapping_add(u32::from(length));
            block_start = data_end;
            if final_block {
                break;
            }
        }

        cursor.seek(block_start)?;
        let footer = cursor.read_exact::<8>(block_start)?;
        let expected_crc = u32::from_le_bytes(footer[..4].try_into().expect("four bytes"));
        let expected_size = u32::from_le_bytes(footer[4..].try_into().expect("four bytes"));
        if expected_size != decoded_size {
            return Err(DecodeError::SizeMismatch {
                member: member_number,
                expected: expected_size,
                actual_mod32: decoded_size,
            });
        }
        let member = members.len();
        members.push(StoredMember {
            expected_crc,
            expected_size,
        });

        let mut task_ranges = Vec::new();
        let mut task_size: usize = 0;
        let mut first_task_of_member = true;
        for range in member_ranges {
            let range_size =
                usize::try_from(range.end - range.start).expect("a stored block length fits usize");
            if !task_ranges.is_empty() && task_size.saturating_add(range_size) > 4 * 1024 * 1024 {
                tasks.push(StoredTask {
                    member,
                    member_deflate_start: first_task_of_member.then_some(header.deflate_start),
                    ranges: std::mem::take(&mut task_ranges),
                    decoded_size: task_size,
                    last_in_member: false,
                });
                first_task_of_member = false;
                task_size = 0;
            }
            task_ranges.push(range);
            task_size += range_size;
        }
        tasks.push(StoredTask {
            member,
            member_deflate_start: first_task_of_member.then_some(header.deflate_start),
            ranges: task_ranges,
            decoded_size: task_size,
            last_in_member: true,
        });
    }

    Ok(Some(StoredIndex {
        members,
        tasks,
        compressed_size: cursor.position(),
    }))
}

struct StoredResult {
    index: usize,
    result: Result<Vec<u8>, DecodeError>,
}

struct StopGuard<'a>(&'a AtomicBool);

impl Drop for StopGuard<'_> {
    fn drop(&mut self) {
        self.0.store(true, Ordering::Relaxed);
    }
}

struct SignalledStopGuard<'a> {
    stopped: &'a AtomicBool,
    work_signal: &'a Condvar,
    limit_signal: &'a Condvar,
}

impl Drop for SignalledStopGuard<'_> {
    fn drop(&mut self) {
        self.stopped.store(true, Ordering::Relaxed);
        self.work_signal.notify_all();
        self.limit_signal.notify_all();
    }
}

fn send_stored_result(
    sender: &mpsc::SyncSender<StoredResult>,
    stopped: &AtomicBool,
    mut result: StoredResult,
) {
    loop {
        if stopped.load(Ordering::Relaxed) {
            return;
        }
        match sender.try_send(result) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => return,
            Err(TrySendError::Full(returned)) => {
                result = returned;
                thread::park_timeout(RESULT_CHANNEL_FULL_PARK);
            }
        }
    }
}

fn decode_stored_parallel<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    index: &StoredIndex,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    let task_queue = Arc::new(Injector::new());
    let stopped = Arc::new(AtomicBool::new(false));
    let task_window = config
        .in_flight_chunks
        .max(config.decoder_threads)
        .min(index.tasks.len());
    for task_index in 0..task_window {
        task_queue.push(task_index);
    }
    let (sender, receiver) = mpsc::sync_channel::<StoredResult>(task_window);
    let mut next_to_schedule = task_window;
    let mut next_to_emit = 0;
    let mut reordered = BTreeMap::new();
    let mut total_output = 0_u64;
    let mut accounting = MemberAccounting::new();
    let mut index_builder = new_index_builder(config);

    let scoped_result = thread::scope(|scope| -> Result<(), DecodeError> {
        let _stop_on_exit = StopGuard(&stopped);
        for _ in 0..config.decoder_threads.min(index.tasks.len()) {
            let queue = Arc::clone(&task_queue);
            let worker_stopped = Arc::clone(&stopped);
            let sender = sender.clone();
            scope.spawn(move || {
                while !worker_stopped.load(Ordering::Relaxed) && !cancelled.load(Ordering::Relaxed)
                {
                    let task_index = loop {
                        match queue.steal() {
                            Steal::Success(task_index) => break task_index,
                            Steal::Retry => std::hint::spin_loop(),
                            Steal::Empty => {
                                if worker_stopped.load(Ordering::Relaxed)
                                    || cancelled.load(Ordering::Relaxed)
                                {
                                    return;
                                }
                                thread::park_timeout(WORKER_IDLE_PARK);
                            }
                        }
                    };
                    let task = &index.tasks[task_index];
                    let result = (|| {
                        let mut decoded = Vec::with_capacity(task.decoded_size);
                        for range in &task.ranges {
                            let length = usize::try_from(range.end - range.start)
                                .expect("stored block length fits usize");
                            decoded.extend_from_slice(&read_range(source, range.start, length)?);
                        }
                        Ok(decoded)
                    })();
                    send_stored_result(
                        &sender,
                        &worker_stopped,
                        StoredResult {
                            index: task_index,
                            result,
                        },
                    );
                }
            });
        }
        drop(sender);

        while next_to_emit < index.tasks.len() {
            if cancelled.load(Ordering::Relaxed) {
                stopped.store(true, Ordering::Relaxed);
                return Err(DecodeError::Cancelled);
            }
            let result = match receiver.recv_timeout(COORDINATOR_RECV_TIMEOUT) {
                Ok(result) => result,
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    stopped.store(true, Ordering::Relaxed);
                    return Err(DecodeError::WorkerPanicked);
                }
            };
            reordered.insert(result.index, result.result);
            while let Some(result) = reordered.remove(&next_to_emit) {
                let task = &index.tasks[next_to_emit];
                let decoded = result?;
                if let Some(deflate_start) = task.member_deflate_start {
                    // Empty history at each independent member DEFLATE start.
                    // Intermediate within-member points are omitted: stored
                    // block headers are bit-aligned and task boundaries do not
                    // expose a safe resume offset with a matching window.
                    index_builder.force_checkpoint(deflate_start.saturating_mul(8), true);
                }
                index_builder.push_output(&decoded);
                // push_output already recorded lines/windows; do not double-count.
                emit_accounted(
                    decoded,
                    config,
                    output,
                    &mut accounting,
                    &mut total_output,
                    None,
                )?;
                if task.last_in_member {
                    let member = index.members[task.member];
                    if config.crc32_enabled {
                        let actual_crc = accounting.crc.finish();
                        if actual_crc != member.expected_crc {
                            stopped.store(true, Ordering::Relaxed);
                            return Err(DecodeError::ChecksumMismatch {
                                member: task.member as u64,
                                expected: member.expected_crc,
                                actual: actual_crc,
                            });
                        }
                    }
                    if accounting.size != member.expected_size {
                        stopped.store(true, Ordering::Relaxed);
                        return Err(DecodeError::SizeMismatch {
                            member: task.member as u64,
                            expected: member.expected_size,
                            actual_mod32: accounting.size,
                        });
                    }
                    accounting = MemberAccounting::new();
                }
                next_to_emit += 1;
                if next_to_schedule < index.tasks.len() {
                    task_queue.push(next_to_schedule);
                    next_to_schedule += 1;
                }
            }
        }
        stopped.store(true, Ordering::Relaxed);
        Ok(())
    });
    stopped.store(true, Ordering::Relaxed);
    scoped_result?;

    let final_length = source
        .len()
        .map_err(|error| DecodeError::input_io(index.compressed_size, error))?;
    if final_length != index.compressed_size {
        return Err(DecodeError::input_io(
            index.compressed_size,
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during decoding",
            ),
        ));
    }

    Ok(finish_report(
        config,
        index.compressed_size,
        total_output,
        index.members.len() as u64,
        index_builder,
    ))
}

fn decode_source_sequential<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    let mut index_builder = new_index_builder(config);
    decode_members_sequential(
        source,
        config,
        cancelled,
        output,
        0,
        0,
        0,
        &mut index_builder,
    )
}

/// Decodes complete members beginning at a known member boundary.
///
/// The independent-member path uses this as a correctness fallback if a byte
/// sequence inside DEFLATE happened to look like a gzip header. Previously
/// emitted members remain valid, so resuming from the first uncommitted task
/// avoids both duplicate output and trusting the speculative candidate index.
///
/// When `keep_index` is enabled, the sequential inflate loop uses `Z_BLOCK` so
/// intermediate checkpoints can be recorded at DEFLATE block boundaries with
/// bit-accurate compressed offsets and a rolling 32 KiB predecessor window.
///
/// Inflate goes through [`InflateBackend`] monomorphized to [`RawInflater`]
/// (zlib-rs only today; future ISA-L can swap the type parameter).
#[allow(clippy::too_many_arguments)]
fn decode_members_sequential<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    start_offset: u64,
    total_output: u64,
    member_count: u64,
    index_builder: &mut IndexBuilder,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    decode_members_sequential_with::<R, O, RawInflater>(
        source,
        config,
        cancelled,
        output,
        start_offset,
        total_output,
        member_count,
        index_builder,
    )
}

/// Generic sequential multi-member gzip path over [`InflateBackend`].
#[allow(clippy::too_many_arguments)]
fn decode_members_sequential_with<R, O, I>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    start_offset: u64,
    mut total_output: u64,
    mut member_count: u64,
    index_builder: &mut IndexBuilder,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
    I: InflateBackend,
{
    let mut cursor = SourceCursor::new(source, config.input_page_size)?;
    cursor.seek(start_offset)?;
    // Prefer Block only when collecting an index so the default path stays
    // on NoFlush. Block exposes DEFLATE block ends for checkpoint offsets.
    let flush = if config.keep_index {
        InflateFlush::Block
    } else {
        InflateFlush::NoFlush
    };
    // Reuse one raw-inflate stream across concatenated gzip members. Each member
    // starts with an empty window; `reset` clears history.
    let mut inflater = I::create()?;
    let mut decoded = Vec::with_capacity(config.decoded_chunk_size);

    while !cursor.at_end() {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }

        let header = parse_member_header(&mut cursor, member_count == 0 && start_offset == 0)?;
        debug_assert!(header.start <= header.deflate_start);
        debug_assert_eq!(header.deflate_start, cursor.position());
        let _observed_bgzf_size = header.bgzf_block_size;
        // Empty window at each member DEFLATE start (history resets).
        index_builder.force_checkpoint(header.deflate_start.saturating_mul(8), true);
        inflater.reset(header.deflate_start.saturating_mul(8))?;
        let mut crc = Crc32::new();
        let mut member_output = 0_u32;

        loop {
            if cancelled.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            if cursor.at_end() {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::Truncated,
                });
            }

            if decoded.capacity() < config.decoded_chunk_size {
                decoded.reserve_exact(config.decoded_chunk_size - decoded.capacity());
            }
            // Sequential path always emits then clears; force empty so
            // InflateBackend appends into a fresh buffer for this step.
            decoded.clear();

            let input = cursor.available()?;
            let step = inflater.inflate(input, &mut decoded, flush)?;
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
                member_output = member_output.wrapping_add(decoded.len() as u32);
                if config.crc32_enabled {
                    crc.update(&decoded);
                }
                index_builder.push_output(&decoded);
                decoded = output.emit_reusable(decoded)?;
            }

            // Bit-accurate offset: bytes consumed so far, less bits still in
            // the inflater bit buffer. Only meaningful with Block flush;
            // with NoFlush keep_index is false and this is unused.
            if config.keep_index {
                let bit_pos = cursor
                    .position()
                    .saturating_mul(8)
                    .saturating_sub(u64::from(step.unused_bits));
                if step.status == inflate_status::STREAM_END || step.at_block_end {
                    index_builder.maybe_checkpoint(bit_pos);
                }
            }

            match step.status {
                inflate_status::STREAM_END => break,
                inflate_status::OK => {
                    if step.consumed == 0 && step.produced == 0 {
                        return Err(DecodeError::InvalidDeflate {
                            bit_offset: cursor.position().saturating_mul(8),
                            reason: DeflateErrorKind::Stalled,
                        });
                    }
                }
                inflate_status::BUF_ERROR if step.consumed > 0 || step.produced > 0 => {}
                inflate_status::BUF_ERROR => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: cursor.position().saturating_mul(8),
                        reason: DeflateErrorKind::Truncated,
                    });
                }
                inflate_status::NEED_DICT => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: header.deflate_start.saturating_mul(8),
                        reason: DeflateErrorKind::UnexpectedDictionary,
                    });
                }
                inflate_status::DATA_ERROR => {
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

        let footer_offset = cursor.position();
        let footer = cursor.read_exact::<8>(footer_offset)?;
        let expected_crc = u32::from_le_bytes(footer[0..4].try_into().expect("four bytes"));
        let expected_size = u32::from_le_bytes(footer[4..8].try_into().expect("four bytes"));
        if config.crc32_enabled {
            let actual_crc = crc.finish();
            if expected_crc != actual_crc {
                return Err(DecodeError::ChecksumMismatch {
                    member: member_count,
                    expected: expected_crc,
                    actual: actual_crc,
                });
            }
        }
        if expected_size != member_output {
            return Err(DecodeError::SizeMismatch {
                member: member_count,
                expected: expected_size,
                actual_mod32: member_output,
            });
        }

        member_count += 1;
    }

    if member_count == 0 {
        return Err(DecodeError::InvalidGzip {
            offset: 0,
            reason: GzipErrorKind::BadMagic,
        });
    }

    let final_length = source
        .len()
        .map_err(|error| DecodeError::input_io(cursor.position(), error))?;
    if final_length != cursor.length() {
        return Err(DecodeError::input_io(
            cursor.position(),
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during decoding",
            ),
        ));
    }

    // Take ownership of the builder for finish without cloning checkpoints.
    let index_builder = std::mem::replace(index_builder, IndexBuilder::new(false, false, 1, false));
    Ok(finish_report(
        config,
        cursor.position(),
        total_output,
        member_count,
        index_builder,
    ))
}

const INDEPENDENT_MEMBER_SCAN_BYTES: usize = 4 * 1024 * 1024;
const INDEPENDENT_MEMBER_PROBE_BYTES: u64 = 8 * 1024 * 1024;
const INDEPENDENT_MEMBER_MIN_CANDIDATES: usize = 4;
// Four amortizes result-channel and coordinator work on tiny members without
// making a worker task so large that it starves peers or retains large output.
const INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES: usize = 4;

fn find_gzip_magic(bytes: &[u8], candidate_limit: usize, candidates: &mut Vec<usize>) {
    candidates.clear();
    let candidate_limit = candidate_limit.min(bytes.len());
    #[cfg(target_arch = "x86_64")]
    if std::arch::is_x86_feature_detected!("avx2") {
        // SAFETY: runtime detection proves AVX2 is available. The helper
        // bounds every unaligned load against `candidate_limit`, which the
        // caller has already bounded by `bytes.len()`.
        unsafe { find_gzip_magic_avx2(bytes, candidate_limit, candidates) };
        return;
    }
    #[cfg(target_arch = "aarch64")]
    if std::arch::is_aarch64_feature_detected!("neon") {
        // SAFETY: runtime detection proves NEON is available. The helper
        // bounds every 16-byte load against the input slice and stores only
        // into a live local lane array.
        unsafe { find_gzip_magic_neon(bytes, candidate_limit, candidates) };
        return;
    }
    find_gzip_magic_scalar(bytes, candidate_limit, candidates);
}

fn find_gzip_magic_scalar(bytes: &[u8], candidate_limit: usize, candidates: &mut Vec<usize>) {
    for (relative, &byte) in bytes.iter().take(candidate_limit).enumerate() {
        if byte == 0x1F {
            candidates.push(relative);
        }
    }
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn find_gzip_magic_avx2(bytes: &[u8], candidate_limit: usize, candidates: &mut Vec<usize>) {
    use std::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
    };

    let needle = _mm256_set1_epi8(0x1F);
    let mut offset = 0_usize;
    while offset.saturating_add(32) <= candidate_limit {
        // SAFETY: `offset + 32 <= candidate_limit <= bytes.len()`, and the
        // unaligned intrinsic imposes no alignment requirement.
        let input = unsafe { _mm256_loadu_si256(bytes.as_ptr().add(offset).cast::<__m256i>()) };
        let mut mask = _mm256_movemask_epi8(_mm256_cmpeq_epi8(input, needle)) as u32;
        while mask != 0 {
            let lane = mask.trailing_zeros() as usize;
            candidates.push(offset + lane);
            mask &= mask - 1;
        }
        offset += 32;
    }
    for (relative, &byte) in bytes.iter().enumerate().take(candidate_limit).skip(offset) {
        if byte == 0x1F {
            candidates.push(relative);
        }
    }
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn find_gzip_magic_neon(bytes: &[u8], candidate_limit: usize, candidates: &mut Vec<usize>) {
    use std::arch::aarch64::{vceqq_u8, vdupq_n_u8, vld1q_u8, vst1q_u8};

    let needle = vdupq_n_u8(0x1F);
    let mut offset = 0_usize;
    let mut lanes = [0_u8; 16];
    while offset.saturating_add(16) <= candidate_limit {
        // SAFETY: `offset + 16 <= candidate_limit <= bytes.len()`.
        let input = unsafe { vld1q_u8(bytes.as_ptr().add(offset)) };
        let matches = vceqq_u8(input, needle);
        // SAFETY: `lanes` is a live, writable 16-byte array.
        unsafe { vst1q_u8(lanes.as_mut_ptr(), matches) };
        for (lane, &matched) in lanes.iter().enumerate() {
            if matched != 0 {
                candidates.push(offset + lane);
            }
        }
        offset += 16;
    }
    for (relative, &byte) in bytes.iter().enumerate().take(candidate_limit).skip(offset) {
        if byte == 0x1F {
            candidates.push(relative);
        }
    }
}

struct IndependentMemberIndex {
    headers: Vec<MemberHeader>,
    scan_start: u64,
    compressed_size: u64,
    average_probe_spacing: u64,
}

/// A bounded group of neighboring header candidates assigned to one worker.
///
/// Candidates are not trusted merely because they share a task. The worker
/// still inflates and authenticates each one separately and only combines
/// output when the preceding member's verified end is exactly the following
/// candidate's start.
struct IndependentMemberTask {
    headers: [MemberHeader; INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES],
    header_count: usize,
}

impl IndependentMemberTask {
    fn new() -> Self {
        Self {
            headers: [MemberHeader {
                start: 0,
                deflate_start: 0,
                bgzf_block_size: None,
            }; INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES],
            header_count: 0,
        }
    }

    fn push(&mut self, header: MemberHeader) {
        debug_assert!(self.header_count < self.headers.len());
        self.headers[self.header_count] = header;
        self.header_count += 1;
    }

    fn headers(&self) -> &[MemberHeader] {
        &self.headers[..self.header_count]
    }
}

struct IndependentMemberTaskBuilder {
    task: IndependentMemberTask,
    target_compressed_span: u64,
    maximum_candidates: usize,
}

impl IndependentMemberTaskBuilder {
    fn new(target_compressed_span: usize, maximum_candidates: usize) -> Self {
        Self {
            task: IndependentMemberTask::new(),
            target_compressed_span: target_compressed_span as u64,
            maximum_candidates: maximum_candidates.clamp(1, INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES),
        }
    }

    fn push(&mut self, header: MemberHeader) -> Option<IndependentMemberTask> {
        let should_flush = self.task.headers().first().is_some_and(|first| {
            self.task.header_count >= self.maximum_candidates
                || header.start.saturating_sub(first.start) >= self.target_compressed_span
        });
        let completed = should_flush.then(|| self.take());
        self.task.push(header);
        completed
    }

    fn finish(mut self) -> Option<IndependentMemberTask> {
        (self.task.header_count != 0).then(|| self.take())
    }

    fn take(&mut self) -> IndependentMemberTask {
        std::mem::replace(&mut self.task, IndependentMemberTask::new())
    }
}

fn batch_independent_headers(
    headers: &[MemberHeader],
    target_compressed_span: usize,
    maximum_candidates: usize,
) -> Vec<IndependentMemberTask> {
    let mut builder = IndependentMemberTaskBuilder::new(target_compressed_span, maximum_candidates);
    let mut tasks = Vec::new();
    for &header in headers {
        if let Some(task) = builder.push(header) {
            tasks.push(task);
        }
    }
    if let Some(task) = builder.finish() {
        tasks.push(task);
    }
    tasks
}

fn independent_member_task_span(
    probe_bytes: u64,
    configured_span: usize,
    worker_count: usize,
) -> usize {
    // Seed at least two task waves from the prefix probe. The configured grid
    // remains the upper bound, while the floor avoids bookkeeping-sized tasks
    // on machines with very large affinity masks.
    let desired_initial_tasks = worker_count.saturating_mul(2).max(1) as u64;
    let parallel_span = probe_bytes.div_ceil(desired_initial_tasks);
    configured_span
        .min(usize::try_from(parallel_span).unwrap_or(configured_span))
        .max(32 * 1024)
}

fn independent_member_task_candidate_limit(
    average_probe_spacing: u64,
    configured_span: usize,
) -> usize {
    // Result collation helps only when member bookkeeping dominates inflate.
    // Preserve one-result-per-member scheduling unless the probe finds at
    // least 256 members per configured compressed work interval.
    if average_probe_spacing <= (configured_span / 256) as u64 {
        INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES
    } else {
        1
    }
}

fn scan_independent_headers<R, F>(
    source: &R,
    page_size: usize,
    mut scan_start: u64,
    scan_end: u64,
    stopped: Option<&AtomicBool>,
    mut found: F,
) -> Result<(), DecodeError>
where
    R: ReadAt + ?Sized,
    F: FnMut(MemberHeader),
{
    let mut header_cursor = SourceCursor::new(source, page_size.min(256))?;
    let compressed_size = header_cursor.length();
    let mut scan = Vec::new();
    let mut candidates = Vec::new();

    while scan_start < scan_end {
        if stopped.is_some_and(|stopped| stopped.load(Ordering::Relaxed)) {
            return Ok(());
        }
        // Nine bytes of forward overlap let the common ten-byte header be
        // parsed directly even when it straddles scan pages, without copying
        // a carry buffer.
        let unique_remaining = usize::try_from(scan_end - scan_start).unwrap_or(usize::MAX);
        let unique_length = unique_remaining.min(INDEPENDENT_MEMBER_SCAN_BYTES);
        let source_remaining =
            usize::try_from(compressed_size.saturating_sub(scan_start)).unwrap_or(usize::MAX);
        let read_length = source_remaining.min(unique_length.saturating_add(9));
        read_range_reuse(source, scan_start, read_length, &mut scan)?;

        let candidate_limit = unique_length.min(scan.len().saturating_sub(9));
        find_gzip_magic(&scan, candidate_limit, &mut candidates);
        for &relative in &candidates {
            if scan[relative + 1] != 0x8B
                || scan[relative + 2] != 8
                || scan[relative + 3] & 0xE0 != 0
            {
                continue;
            }
            let candidate = scan_start.saturating_add(relative as u64);
            if candidate == 0 {
                continue;
            }
            if scan[relative + 3] == 0 {
                // Most producer-generated members have no optional fields.
                // Their remaining six fixed header bytes are metadata and
                // require no validation, so avoid one positional read and
                // parser call per member.
                found(MemberHeader {
                    start: candidate,
                    deflate_start: candidate.saturating_add(10),
                    bgzf_block_size: None,
                });
            } else {
                header_cursor.seek(candidate)?;
                match parse_member_header(&mut header_cursor, false) {
                    Ok(header) => found(header),
                    Err(error @ DecodeError::Io { .. }) => return Err(error),
                    Err(_) => {}
                }
            }
        }
        scan_start = scan_start.saturating_add(unique_length as u64);
    }
    Ok(())
}

/// Finds plausible gzip headers without treating them as authoritative.
///
/// A candidate becomes trusted only when a worker starting there reaches
/// `Z_STREAM_END`, verifies the following trailer, and the coordinator was
/// already expecting that exact start from the preceding verified member.
/// Failed or skipped candidates cannot alter output; decoding resumes
/// sequentially at the first uncommitted real member boundary when necessary.
/// This is required because gzip magic bytes are legal inside DEFLATE data.
fn index_independent_members<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
) -> Result<Option<IndependentMemberIndex>, DecodeError> {
    let mut header_cursor = SourceCursor::new(source, config.input_page_size.min(256))?;
    if header_cursor.at_end() {
        return Ok(None);
    }
    let first_header = parse_member_header(&mut header_cursor, true)?;
    let compressed_size = header_cursor.length();
    let mut headers = vec![first_header];
    let probe_end = compressed_size.min(INDEPENDENT_MEMBER_PROBE_BYTES);
    scan_independent_headers(
        source,
        config.input_page_size,
        0,
        probe_end,
        None,
        |header| headers.push(header),
    )?;

    // Avoid a full-file scan on ordinary large members. A dense prefix is
    // sufficient evidence that exact gzip members expose finer parallel work
    // than the regular compressed grid. Remaining candidates are discovered
    // concurrently with decoding so a large reader can begin producing data.
    let average_probe_spacing = probe_end / headers.len() as u64;
    if headers.len() < INDEPENDENT_MEMBER_MIN_CANDIDATES
        || average_probe_spacing > config.compressed_chunk_size.saturating_mul(4) as u64
    {
        return Ok(None);
    }

    headers.sort_unstable_by_key(|header| header.start);
    headers.dedup_by_key(|header| header.start);
    if headers.len() < INDEPENDENT_MEMBER_MIN_CANDIDATES {
        return Ok(None);
    }

    Ok(Some(IndependentMemberIndex {
        headers,
        scan_start: probe_end,
        compressed_size,
        average_probe_spacing,
    }))
}

/// Inflate one dense independent gzip member, appending to `decoded`.
///
/// Fully driven through [`InflateBackend`] (`reset` + [`InflateBackend::inflate_capped`])
/// so the per-member decoded budget is enforced without raw `stream.*` field use
/// at this call site.
#[allow(clippy::too_many_arguments)]
fn inflate_independent_member_into<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    member_number: u64,
    header: MemberHeader,
    compressed_size: u64,
    inflater: &mut RawInflater,
    compressed: &mut Vec<u8>,
    decoded: &mut Vec<u8>,
) -> Result<u64, DecodeError> {
    let mut input_offset = header.deflate_start;
    let member_output_start = decoded.len();
    let maximum_member_output = config.decoded_chunk_size.max(16 * 1024 * 1024);
    let output_step = config.decoded_chunk_size.clamp(32 * 1024, 256 * 1024);
    let input_step = config.input_page_size.clamp(32 * 1024, 64 * 1024);
    InflateBackend::reset(inflater, header.deflate_start.saturating_mul(8))?;

    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }
        if input_offset >= compressed_size {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: input_offset.saturating_mul(8),
                reason: DeflateErrorKind::Truncated,
            });
        }
        let input_length = usize::try_from(compressed_size - input_offset)
            .unwrap_or(usize::MAX)
            .min(input_step);
        read_range_reuse(source, input_offset, input_length, compressed)?;

        let mut relative_input = 0_usize;
        while relative_input < compressed.len() {
            let member_output = decoded.len() - member_output_start;
            if member_output >= maximum_member_output {
                return Err(DecodeError::OutputLimitExceeded {
                    limit: maximum_member_output as u64,
                });
            }
            let remaining_budget = maximum_member_output - member_output;
            let wanted_output = output_step.min(remaining_budget);
            decoded.reserve(wanted_output);
            let step = InflateBackend::inflate_capped(
                inflater,
                &compressed[relative_input..],
                decoded,
                InflateFlush::NoFlush,
                remaining_budget,
            )?;
            relative_input += step.consumed;
            input_offset = input_offset.saturating_add(step.consumed as u64);

            match step.status {
                inflate_status::STREAM_END => {
                    let member_output = &decoded[member_output_start..];
                    let footer_offset = input_offset;
                    read_range_reuse(source, footer_offset, 8, compressed)?;
                    let expected_crc =
                        u32::from_le_bytes(compressed[0..4].try_into().expect("four bytes"));
                    let expected_size =
                        u32::from_le_bytes(compressed[4..8].try_into().expect("four bytes"));
                    if config.crc32_enabled {
                        let mut crc = Crc32::new();
                        crc.update(member_output);
                        let actual_crc = crc.finish();
                        if actual_crc != expected_crc {
                            return Err(DecodeError::ChecksumMismatch {
                                member: member_number,
                                expected: expected_crc,
                                actual: actual_crc,
                            });
                        }
                    }
                    let actual_size = member_output.len() as u32;
                    if actual_size != expected_size {
                        return Err(DecodeError::SizeMismatch {
                            member: member_number,
                            expected: expected_size,
                            actual_mod32: actual_size,
                        });
                    }
                    return Ok(footer_offset.saturating_add(8));
                }
                inflate_status::OK => {
                    if step.consumed == 0 && step.produced == 0 {
                        return Err(DecodeError::InvalidDeflate {
                            bit_offset: input_offset.saturating_mul(8),
                            reason: DeflateErrorKind::Stalled,
                        });
                    }
                }
                inflate_status::BUF_ERROR if step.consumed > 0 || step.produced > 0 => {}
                inflate_status::BUF_ERROR => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: input_offset.saturating_mul(8),
                        reason: DeflateErrorKind::Truncated,
                    });
                }
                inflate_status::NEED_DICT => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: header.deflate_start.saturating_mul(8),
                        reason: DeflateErrorKind::UnexpectedDictionary,
                    });
                }
                inflate_status::DATA_ERROR => {
                    let _diagnostic = InflateBackend::message(inflater);
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: input_offset.saturating_mul(8),
                        reason: DeflateErrorKind::InvalidData,
                    });
                }
                other => {
                    return Err(DecodeError::InvalidDeflate {
                        bit_offset: input_offset.saturating_mul(8),
                        reason: DeflateErrorKind::BackendStatus(other),
                    });
                }
            }
        }
    }
}

/// One or more separately authenticated, exactly adjacent gzip members.
struct DecodedIndependentMembers {
    start: u64,
    end: u64,
    bytes: Vec<u8>,
    member_sizes: [usize; INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES],
    member_deflate_starts: [u64; INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES],
    member_count: usize,
}

impl DecodedIndependentMembers {
    fn push_member(&mut self, size: usize, deflate_start: u64) {
        debug_assert!(self.member_count < self.member_sizes.len());
        self.member_sizes[self.member_count] = size;
        self.member_deflate_starts[self.member_count] = deflate_start;
        self.member_count += 1;
    }

    fn member_count(&self) -> usize {
        self.member_count
    }

    fn member_size(&self, index: usize) -> usize {
        self.member_sizes[index]
    }

    fn member_deflate_start(&self, index: usize) -> u64 {
        self.member_deflate_starts[index]
    }

    fn member_sizes(&self) -> impl Iterator<Item = usize> + '_ {
        self.member_sizes[..self.member_count].iter().copied()
    }
}

struct IndependentResult {
    start: u64,
    candidate_count: usize,
    result: Result<DecodedIndependentMembers, DecodeError>,
}

struct PendingIndependentRun {
    decoded: DecodedIndependentMembers,
}

fn send_independent_result(
    sender: &mpsc::SyncSender<IndependentResult>,
    stopped: &AtomicBool,
    mut result: IndependentResult,
) -> bool {
    loop {
        if stopped.load(Ordering::Relaxed) {
            return false;
        }
        match sender.try_send(result) {
            Ok(()) => return true,
            Err(TrySendError::Disconnected(_)) => return false,
            Err(TrySendError::Full(returned)) => {
                result = returned;
                thread::park_timeout(RESULT_CHANNEL_FULL_PARK);
            }
        }
    }
}

fn send_finished_independent_run(
    active: &mut Option<PendingIndependentRun>,
    sender: &mpsc::SyncSender<IndependentResult>,
    stopped: &AtomicBool,
) -> bool {
    let Some(run) = active.take() else {
        return true;
    };
    let candidate_count = run.decoded.member_count();
    send_independent_result(
        sender,
        stopped,
        IndependentResult {
            start: run.decoded.start,
            candidate_count,
            result: Ok(run.decoded),
        },
    )
}

#[allow(clippy::too_many_arguments)]
fn decode_independent_task<R: ReadAt + ?Sized>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    stopped: &AtomicBool,
    compressed_size: u64,
    task: IndependentMemberTask,
    inflater: &mut RawInflater,
    compressed: &mut Vec<u8>,
    sender: &mpsc::SyncSender<IndependentResult>,
) -> bool {
    let target_result_size = config
        .decoded_chunk_size
        .min(config.compressed_chunk_size / 2);
    let maximum_collatable_member_size = (target_result_size / 4).max(32 * 1024);
    let mut active = None::<PendingIndependentRun>;

    for &header in task.headers() {
        if stopped.load(Ordering::Relaxed) || cancelled.load(Ordering::Relaxed) {
            return false;
        }

        if active
            .as_ref()
            .is_some_and(|run| run.decoded.end != header.start)
            && !send_finished_independent_run(&mut active, sender, stopped)
        {
            return false;
        }

        let run = active.get_or_insert_with(|| PendingIndependentRun {
            decoded: DecodedIndependentMembers {
                start: header.start,
                end: header.start,
                bytes: Vec::new(),
                member_sizes: [0; INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES],
                member_deflate_starts: [0; INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES],
                member_count: 0,
            },
        });
        let member_output_start = run.decoded.bytes.len();
        match inflate_independent_member_into(
            source,
            config,
            cancelled,
            0,
            header,
            compressed_size,
            inflater,
            compressed,
            &mut run.decoded.bytes,
        ) {
            Ok(end) => {
                let member_size = run.decoded.bytes.len() - member_output_start;
                run.decoded.end = end;
                run.decoded.push_member(member_size, header.deflate_start);
                if (member_size > maximum_collatable_member_size
                    || run.decoded.bytes.len() >= target_result_size)
                    && !send_finished_independent_run(&mut active, sender, stopped)
                {
                    return false;
                }
            }
            Err(error) => {
                run.decoded.bytes.truncate(member_output_start);
                if run.decoded.member_count() == 0 {
                    active = None;
                } else if !send_finished_independent_run(&mut active, sender, stopped) {
                    return false;
                }
                if !send_independent_result(
                    sender,
                    stopped,
                    IndependentResult {
                        start: header.start,
                        candidate_count: 1,
                        result: Err(error),
                    },
                ) {
                    return false;
                }
            }
        }
    }

    send_finished_independent_run(&mut active, sender, stopped)
}

enum IndependentOutcome {
    Complete,
    SequentialFallback { offset: u64 },
}

fn decode_independent_members<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    index: &IndependentMemberIndex,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    let worker_count = config.decoder_threads;
    let task_compressed_span =
        independent_member_task_span(index.scan_start, config.compressed_chunk_size, worker_count);
    let task_candidate_limit = independent_member_task_candidate_limit(
        index.average_probe_spacing,
        config.compressed_chunk_size,
    );
    let initial_tasks =
        batch_independent_headers(&index.headers, task_compressed_span, task_candidate_limit);
    let queue = Arc::new(Injector::<IndependentMemberTask>::new());
    let stopped = Arc::new(AtomicBool::new(false));
    let available_tasks = Arc::new(AtomicUsize::new(initial_tasks.len()));
    let unresolved_candidates = Arc::new(AtomicUsize::new(index.headers.len()));
    let pending_candidates = Arc::new(AtomicUsize::new(index.headers.len()));
    let scanner_done = Arc::new(AtomicBool::new(index.scan_start >= index.compressed_size));
    let scanner_error = Arc::new(Mutex::new(None::<DecodeError>));
    let work_signal = Arc::new((Mutex::new(()), Condvar::new()));
    let scan_signal = Arc::new((Mutex::new(()), Condvar::new()));
    let pending_limit = config.in_flight_chunks.max(worker_count).max(1);
    let scan_ahead_limit = pending_limit.saturating_mul(16);
    for task in initial_tasks {
        queue.push(task);
    }
    let (sender, receiver) = mpsc::sync_channel::<IndependentResult>(pending_limit);
    let mut reordered = BTreeMap::new();
    let mut total_output = 0_u64;
    let mut member_count = 0_u64;
    let mut expected_start = 0_u64;
    let mut index_builder = new_index_builder(config);

    let outcome = thread::scope(|scope| -> Result<IndependentOutcome, DecodeError> {
        let _stop_on_exit = StopGuard(&stopped);
        if index.scan_start < index.compressed_size {
            let queue = Arc::clone(&queue);
            let scanner_stopped = Arc::clone(&stopped);
            let available_tasks = Arc::clone(&available_tasks);
            let unresolved_candidates = Arc::clone(&unresolved_candidates);
            let pending_candidates = Arc::clone(&pending_candidates);
            let scanner_done = Arc::clone(&scanner_done);
            let scanner_error = Arc::clone(&scanner_error);
            let work_signal = Arc::clone(&work_signal);
            let scan_signal = Arc::clone(&scan_signal);
            scope.spawn(move || {
                let enqueue = |task: IndependentMemberTask| {
                    let candidate_count = task.headers().len();
                    while unresolved_candidates.load(Ordering::Acquire) != 0
                        && unresolved_candidates
                            .load(Ordering::Acquire)
                            .saturating_add(candidate_count)
                            > scan_ahead_limit
                        && !scanner_stopped.load(Ordering::Relaxed)
                    {
                        let (lock, signal) = &*scan_signal;
                        let guard = lock.lock().expect("member scan mutex poisoned");
                        let _ = signal
                            .wait_timeout_while(guard, WORKER_IDLE_PARK, |_| {
                                let unresolved = unresolved_candidates.load(Ordering::Acquire);
                                unresolved != 0
                                    && unresolved.saturating_add(candidate_count) > scan_ahead_limit
                                    && !scanner_stopped.load(Ordering::Relaxed)
                            })
                            .expect("member scan mutex poisoned");
                    }
                    if scanner_stopped.load(Ordering::Relaxed) {
                        return;
                    }
                    unresolved_candidates.fetch_add(candidate_count, Ordering::AcqRel);
                    pending_candidates.fetch_add(candidate_count, Ordering::AcqRel);
                    queue.push(task);
                    available_tasks.fetch_add(1, Ordering::Release);
                    work_signal.1.notify_one();
                };
                let mut builder =
                    IndependentMemberTaskBuilder::new(task_compressed_span, task_candidate_limit);
                let result = scan_independent_headers(
                    source,
                    config.input_page_size,
                    index.scan_start,
                    index.compressed_size,
                    Some(&scanner_stopped),
                    |header| {
                        if let Some(task) = builder.push(header) {
                            enqueue(task);
                        }
                    },
                );
                match result {
                    Ok(()) => {
                        if let Some(task) = builder.finish() {
                            enqueue(task);
                        }
                    }
                    Err(error) => {
                        *scanner_error.lock().expect("member scanner mutex poisoned") = Some(error);
                    }
                }
                scanner_done.store(true, Ordering::Release);
                work_signal.1.notify_all();
            });
        }

        for _ in 0..worker_count {
            let queue = Arc::clone(&queue);
            let worker_stopped = Arc::clone(&stopped);
            let available_tasks = Arc::clone(&available_tasks);
            let work_signal = Arc::clone(&work_signal);
            let sender = sender.clone();
            scope.spawn(move || {
                let mut inflater = None;
                let mut compressed = Vec::new();
                while !worker_stopped.load(Ordering::Relaxed) && !cancelled.load(Ordering::Relaxed)
                {
                    let task = loop {
                        match queue.steal() {
                            Steal::Success(task) => {
                                available_tasks.fetch_sub(1, Ordering::AcqRel);
                                break task;
                            }
                            Steal::Retry => std::hint::spin_loop(),
                            Steal::Empty => {
                                if worker_stopped.load(Ordering::Relaxed)
                                    || cancelled.load(Ordering::Relaxed)
                                {
                                    return;
                                }
                                let (lock, signal) = &*work_signal;
                                let guard = lock.lock().expect("member work mutex poisoned");
                                let _ = signal
                                    .wait_timeout_while(guard, WORKER_IDLE_PARK, |_| {
                                        available_tasks.load(Ordering::Acquire) == 0
                                            && !worker_stopped.load(Ordering::Relaxed)
                                            && !cancelled.load(Ordering::Relaxed)
                                    })
                                    .expect("member work mutex poisoned");
                            }
                        }
                    };
                    if inflater.is_none() {
                        match <RawInflater as InflateBackend>::create() {
                            Ok(new_inflater) => inflater = Some(new_inflater),
                            Err(error) => {
                                let candidate_count = task.headers().len();
                                let start = task
                                    .headers()
                                    .first()
                                    .expect("independent tasks are never empty")
                                    .start;
                                if !send_independent_result(
                                    &sender,
                                    &worker_stopped,
                                    IndependentResult {
                                        start,
                                        candidate_count,
                                        result: Err(error),
                                    },
                                ) {
                                    return;
                                }
                                continue;
                            }
                        }
                    }
                    if !decode_independent_task(
                        source,
                        config,
                        cancelled,
                        &worker_stopped,
                        index.compressed_size,
                        task,
                        inflater
                            .as_mut()
                            .expect("the inflater was initialized immediately above"),
                        &mut compressed,
                        &sender,
                    ) {
                        return;
                    }
                }
            });
        }
        drop(sender);

        while expected_start < index.compressed_size {
            if cancelled.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            let result = match receiver.recv_timeout(COORDINATOR_RECV_TIMEOUT) {
                Ok(result) => {
                    pending_candidates.fetch_sub(result.candidate_count, Ordering::AcqRel);
                    result
                }
                Err(RecvTimeoutError::Timeout) => {
                    if scanner_done.load(Ordering::Acquire) {
                        if let Some(error) = scanner_error
                            .lock()
                            .expect("member scanner mutex poisoned")
                            .take()
                        {
                            return Err(error);
                        }
                        if pending_candidates.load(Ordering::Acquire) == 0 {
                            stopped.store(true, Ordering::Relaxed);
                            return Ok(IndependentOutcome::SequentialFallback {
                                offset: expected_start,
                            });
                        }
                    }
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => return Err(DecodeError::WorkerPanicked),
            };
            if result.start < expected_start {
                unresolved_candidates.fetch_sub(result.candidate_count, Ordering::AcqRel);
                scan_signal.1.notify_one();
                continue;
            }
            if let Some(replaced) = reordered.insert(result.start, result) {
                unresolved_candidates.fetch_sub(replaced.candidate_count, Ordering::AcqRel);
            }

            while let Some(result) = reordered.remove(&expected_start) {
                let result_candidate_count = result.candidate_count;
                let mut decoded = match result.result {
                    Ok(decoded) => decoded,
                    Err(_) => {
                        stopped.store(true, Ordering::Relaxed);
                        return Ok(IndependentOutcome::SequentialFallback {
                            offset: expected_start,
                        });
                    }
                };
                let decoded_size = decoded
                    .member_sizes()
                    .try_fold(0_usize, |total, size| total.checked_add(size));
                if decoded.start != expected_start
                    || decoded.end <= expected_start
                    || decoded.end > index.compressed_size
                    || decoded.member_count() == 0
                    || decoded_size != Some(decoded.bytes.len())
                {
                    stopped.store(true, Ordering::Relaxed);
                    return Ok(IndependentOutcome::SequentialFallback {
                        offset: expected_start,
                    });
                }
                debug_assert_eq!(result_candidate_count, decoded.member_count());
                let mut accepted_bytes = 0_usize;
                for member_index in 0..decoded.member_count() {
                    let member_size = decoded.member_size(member_index);
                    let next_total = total_output.checked_add(member_size as u64);
                    if next_total.is_none()
                        || config
                            .output_limit
                            .is_some_and(|limit| next_total.is_some_and(|next| next > limit))
                    {
                        if accepted_bytes != 0 {
                            decoded.bytes.truncate(accepted_bytes);
                            output.emit(decoded.bytes)?;
                        }
                        return Err(DecodeError::OutputLimitExceeded {
                            limit: config.output_limit.unwrap_or(u64::MAX),
                        });
                    }
                    // Empty-window checkpoint at each verified member's DEFLATE start.
                    index_builder.force_checkpoint(
                        decoded.member_deflate_start(member_index).saturating_mul(8),
                        true,
                    );
                    let member_bytes = &decoded.bytes[accepted_bytes..accepted_bytes + member_size];
                    index_builder.push_output(member_bytes);
                    total_output =
                        next_total.expect("the overflow case returned immediately above");
                    member_count += 1;
                    accepted_bytes += member_size;
                }
                unresolved_candidates.fetch_sub(result_candidate_count, Ordering::AcqRel);
                expected_start = decoded.end;
                if !decoded.bytes.is_empty() {
                    output.emit(decoded.bytes)?;
                }
                let mut removed_candidates = 0_usize;
                reordered.retain(|start, result| {
                    let keep = *start >= expected_start;
                    if !keep {
                        removed_candidates =
                            removed_candidates.saturating_add(result.candidate_count);
                    }
                    keep
                });
                if removed_candidates != 0 {
                    unresolved_candidates.fetch_sub(removed_candidates, Ordering::AcqRel);
                }
                scan_signal.1.notify_one();
            }
        }
        stopped.store(true, Ordering::Relaxed);
        Ok(IndependentOutcome::Complete)
    })?;
    stopped.store(true, Ordering::Relaxed);

    if let IndependentOutcome::SequentialFallback { offset } = outcome {
        return decode_members_sequential(
            source,
            config,
            cancelled,
            output,
            offset,
            total_output,
            member_count,
            &mut index_builder,
        );
    }

    let final_length = source
        .len()
        .map_err(|error| DecodeError::input_io(index.compressed_size, error))?;
    if final_length != index.compressed_size {
        return Err(DecodeError::input_io(
            index.compressed_size,
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during decoding",
            ),
        ));
    }

    Ok(finish_report(
        config,
        index.compressed_size,
        total_output,
        member_count,
        index_builder,
    ))
}

fn read_range<R: ReadAt + ?Sized>(
    source: &R,
    offset: u64,
    length: usize,
) -> Result<Vec<u8>, DecodeError> {
    let mut bytes = Vec::new();
    read_range_reuse(source, offset, length, &mut bytes)?;
    Ok(bytes)
}

fn read_range_reuse<R: ReadAt + ?Sized>(
    source: &R,
    offset: u64,
    length: usize,
    bytes: &mut Vec<u8>,
) -> Result<(), DecodeError> {
    bytes.resize(length, 0);
    let mut filled = 0;
    while filled < length {
        let absolute = offset.saturating_add(filled as u64);
        let read = source
            .read_at(absolute, &mut bytes[filled..])
            .map_err(|error| DecodeError::input_io(absolute, error))?;
        if read == 0 {
            return Err(DecodeError::input_io(
                absolute,
                std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "positional source ended before its snapshotted length",
                ),
            ));
        }
        filled += read;
    }
    Ok(())
}

struct MemberAccounting {
    crc: Crc32,
    adler: Adler32,
    size: u32,
}

impl MemberAccounting {
    const fn new() -> Self {
        Self {
            crc: Crc32::new(),
            adler: Adler32::new(),
            size: 0,
        }
    }
}

fn emit_accounted<O: Output>(
    decoded: Vec<u8>,
    config: &Config,
    output: &mut O,
    accounting: &mut MemberAccounting,
    total_output: &mut u64,
    index_builder: Option<&mut IndexBuilder>,
) -> Result<(), DecodeError> {
    let next_total =
        total_output
            .checked_add(decoded.len() as u64)
            .ok_or(DecodeError::OutputLimitExceeded {
                limit: config.output_limit.unwrap_or(u64::MAX),
            })?;
    if config.output_limit.is_some_and(|limit| next_total > limit) {
        return Err(DecodeError::OutputLimitExceeded {
            limit: config.output_limit.expect("checked as some"),
        });
    }
    *total_output = next_total;
    accounting.size = accounting.size.wrapping_add(decoded.len() as u32);
    // Gzip paths use CRC32; zlib uses Adler-32. Updating both when integrity
    // checks are on keeps the shared emit path format-agnostic. Parallel raw
    // DEFLATE also needs CRC when an external `raw_crc32_list` is configured
    // (even if `crc32_enabled` is false — that flag only gates on-stream
    // trailers).
    if config.crc32_enabled {
        accounting.crc.update(&decoded);
        accounting.adler.update(&decoded);
    } else if !config.raw_crc32_list.is_empty() {
        accounting.crc.update(&decoded);
    }
    if let Some(index_builder) = index_builder {
        index_builder.push_output(&decoded);
    }
    if !decoded.is_empty() {
        output.emit(decoded)?;
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn inflate_from_block<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    start_bit: u64,
    window: &Window,
    accounting: &mut MemberAccounting,
    total_output: &mut u64,
    index_builder: &mut IndexBuilder,
) -> Result<u64, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    // Mid-stream resume (prime + dictionary) via inherent ReadAt helper, which
    // builds the stream through [`InflateBackend::prepare_at_bit_offset`].
    let (mut inflater, resume_byte) = RawInflater::prepare_at_bit_offset(
        start_bit,
        window,
        source,
        config.input_page_size,
        false,
    )?;
    let mut cursor = SourceCursor::new(source, config.input_page_size)?;
    cursor.seek(resume_byte)?;

    // Reuse one buffer; each step appends into spare capacity via InflateBackend.
    let mut decoded = Vec::with_capacity(config.decoded_chunk_size);
    loop {
        if cancelled.load(Ordering::Relaxed) {
            return Err(DecodeError::Cancelled);
        }
        if cursor.at_end() {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: cursor.position().saturating_mul(8),
                reason: DeflateErrorKind::Truncated,
            });
        }
        decoded.clear();
        decoded.reserve(config.decoded_chunk_size);
        let input = cursor.available()?;
        // Hard-cap produce to the configured chunk so step size matches the
        // prior pre-sized `avail_out` limit even if capacity is larger.
        let step = InflateBackend::inflate_capped(
            &mut inflater,
            input,
            &mut decoded,
            InflateFlush::NoFlush,
            config.decoded_chunk_size,
        )?;
        cursor.advance(step.consumed);
        if !decoded.is_empty() {
            emit_accounted(
                std::mem::take(&mut decoded),
                config,
                output,
                accounting,
                total_output,
                Some(index_builder),
            )?;
        }
        match step.status {
            inflate_status::STREAM_END => return Ok(cursor.position()),
            inflate_status::OK if step.consumed != 0 || step.produced != 0 => {}
            inflate_status::BUF_ERROR if step.consumed != 0 || step.produced != 0 => {}
            inflate_status::DATA_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::InvalidData,
                });
            }
            inflate_status::NEED_DICT => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: start_bit,
                    reason: DeflateErrorKind::UnexpectedDictionary,
                });
            }
            inflate_status::OK | inflate_status::BUF_ERROR => {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: cursor.position().saturating_mul(8),
                    reason: DeflateErrorKind::Stalled,
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
}

struct EstimatedTask {
    search_start_bit: u64,
    search_end_bit: u64,
    estimated_stop_bit: u64,
    read_end_bit: u64,
    exact_start: bool,
}

struct NativeResult {
    index: usize,
    result: Result<crate::parallel::deflate::Chunk, NativeError>,
}

struct ResolveTask {
    sequence: usize,
    predecessor: Window,
    output: ChunkOutput,
}

struct ResolveResult {
    sequence: usize,
    result: Result<ResolvedParts, crate::parallel::MarkerError>,
}

/// Admission control shared by the generic decode and marker-resolution work.
///
/// Worker ranks are created lazily as upward probes request them. Stable ranks
/// keep per-worker compressed buffers local instead of rotating parked threads
/// through the active set. Candidate measurements count native completions
/// before ordered output handoff, and stable operation costs one atomic rank
/// check per task without touching the controller mutex.
struct AdaptiveWorkers {
    controller: Mutex<AdaptiveConcurrency>,
    current_limit: AtomicUsize,
    generation: AtomicUsize,
    calibrating: AtomicBool,
    worker_pool_limit: usize,
    limit_mutex: Mutex<()>,
    limit_signal: Condvar,
}

impl AdaptiveWorkers {
    fn new(
        maximum: usize,
        machine_parallelism: usize,
        sample_bytes: usize,
        work_items: usize,
    ) -> Self {
        let controller =
            AdaptiveConcurrency::new(maximum, machine_parallelism, sample_bytes, work_items);
        let current_limit = controller.current_limit();
        let generation = controller.generation();
        let calibrating = !controller.is_stable();
        let worker_pool_limit = controller.worker_pool_limit();
        Self {
            controller: Mutex::new(controller),
            current_limit: AtomicUsize::new(current_limit),
            generation: AtomicUsize::new(generation),
            calibrating: AtomicBool::new(calibrating),
            worker_pool_limit,
            limit_mutex: Mutex::new(()),
            limit_signal: Condvar::new(),
        }
    }

    fn current_limit(&self) -> usize {
        self.current_limit.load(Ordering::Acquire)
    }

    fn worker_enabled(&self, worker_index: usize) -> bool {
        worker_index < self.current_limit()
    }

    const fn worker_pool_limit(&self) -> usize {
        self.worker_pool_limit
    }

    fn is_calibrating(&self) -> bool {
        self.calibrating.load(Ordering::Acquire)
    }

    fn wait_until_enabled(&self, worker_index: usize, stopped: &AtomicBool) {
        let guard = self
            .limit_mutex
            .lock()
            .expect("adaptive limit mutex poisoned");
        let _guard = self
            .limit_signal
            .wait_while(guard, |_| {
                !self.worker_enabled(worker_index) && !stopped.load(Ordering::Relaxed)
            })
            .expect("adaptive limit mutex poisoned");
    }

    fn start_work(&self) -> Option<usize> {
        if !self.calibrating.load(Ordering::Acquire) {
            return None;
        }
        let generation = self.generation.load(Ordering::Acquire);
        self.controller
            .lock()
            .expect("adaptive worker mutex poisoned")
            .start_work(generation, Instant::now());
        Some(generation)
    }

    fn observe_work(&self, generation: Option<usize>, decoded_bytes: usize) -> bool {
        let Some(generation) = generation else {
            return false;
        };
        let mut controller = self
            .controller
            .lock()
            .expect("adaptive worker mutex poisoned");
        let changed = controller.observe_work(generation, decoded_bytes, Instant::now());
        if changed {
            self.current_limit
                .store(controller.current_limit(), Ordering::Release);
            self.generation
                .store(controller.generation(), Ordering::Release);
            self.calibrating
                .store(!controller.is_stable(), Ordering::Release);
            self.limit_signal.notify_all();
        }
        changed
    }
}

fn send_native_result(
    sender: &mpsc::SyncSender<NativeResult>,
    stopped: &AtomicBool,
    mut result: NativeResult,
) {
    loop {
        if stopped.load(Ordering::Relaxed) {
            return;
        }
        match sender.try_send(result) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => return,
            Err(TrySendError::Full(returned)) => {
                result = returned;
                thread::park_timeout(NATIVE_RESULT_CHANNEL_FULL_PARK);
            }
        }
    }
}

fn send_resolve_result(
    sender: &mpsc::SyncSender<ResolveResult>,
    stopped: &AtomicBool,
    mut result: ResolveResult,
) {
    loop {
        if stopped.load(Ordering::Relaxed) {
            return;
        }
        match sender.try_send(result) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => return,
            Err(TrySendError::Full(returned)) => {
                result = returned;
                thread::park_timeout(NATIVE_RESULT_CHANNEL_FULL_PARK);
            }
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn spawn_estimated_worker<'scope, 'env: 'scope, R>(
    scope: &'scope thread::Scope<'scope, 'env>,
    worker_index: usize,
    source: &'env R,
    tasks: &'env [EstimatedTask],
    maximum_output: usize,
    queue: Arc<Injector<usize>>,
    resolve_queue: Arc<Injector<ResolveTask>>,
    sender: mpsc::SyncSender<NativeResult>,
    resolve_sender: mpsc::SyncSender<ResolveResult>,
    stopped: Arc<AtomicBool>,
    available_decode_tasks: Arc<AtomicUsize>,
    available_resolve_tasks: Arc<AtomicUsize>,
    work_signal: Arc<(Mutex<()>, Condvar)>,
    adaptive_workers: Arc<AdaptiveWorkers>,
    output_free_list: Arc<ByteBufferFreeList>,
) where
    R: ReadAt + ?Sized + 'env,
{
    scope.spawn(move || {
        // Per-worker compressed page: `read_range_reuse` resizes in place so
        // capacity survives across tasks. Stable adaptive ranks keep this
        // buffer local instead of rotating threads through the active set.
        let mut compressed = Vec::new();
        // Symbol/clean scratches recycle capacity across failed structural
        // candidates within a task and, after successful resolve on this
        // worker, the emptied `Vec<Symbol>` from `resolve_parts`. Successful
        // `ChunkOutput` is owned by the resolve queue until a worker steals
        // it; post-emit the coordinator recycles empty `Vec<u8>` capacity into
        // `output_free_list` for the next task's clean/backend_tail scratch.
        let mut marked_scratch = Vec::new();
        let mut clean_scratch = Vec::new();
        // zlib-rs tail inflater reused for exact-start and backend-continuation
        // paths (same pattern as BGZF / independent-member workers).
        let mut tail_inflater = None;
        loop {
            if stopped.load(Ordering::Relaxed) {
                break;
            }
            if !adaptive_workers.worker_enabled(worker_index) {
                adaptive_workers.wait_until_enabled(worker_index, &stopped);
                continue;
            }
            match resolve_queue.steal() {
                Steal::Success(task) => {
                    available_resolve_tasks.fetch_sub(1, Ordering::AcqRel);
                    // Resolve consumes `ChunkOutput` on this worker; recycle
                    // emptied symbol capacity before publishing to the
                    // coordinator (never while the buffer is still enqueued).
                    let (result, emptied_symbols) = task.output.resolve_parts(&task.predecessor);
                    prefer_capacity(&mut marked_scratch, emptied_symbols);
                    send_resolve_result(
                        &resolve_sender,
                        &stopped,
                        ResolveResult {
                            sequence: task.sequence,
                            result,
                        },
                    );
                    continue;
                }
                Steal::Retry => {
                    std::hint::spin_loop();
                    continue;
                }
                Steal::Empty => {}
            }
            match queue.steal() {
                Steal::Success(index) => {
                    available_decode_tasks.fetch_sub(1, Ordering::AcqRel);
                    // Prefer free-list capacity before growing for a new task.
                    // Successful prior tasks leave clean_scratch empty (taken into
                    // ChunkOutput); this recovers post-emit capacity without
                    // aliasing any buffer still in the resolve/emit pipeline.
                    // Skip when a failed-candidate recycle already left capacity.
                    if clean_scratch.capacity() == 0 {
                        output_free_list.try_steal_into(&mut clean_scratch);
                    }
                    let generation = adaptive_workers.start_work();
                    let result = run_estimated_task(
                        source,
                        &tasks[index],
                        maximum_output,
                        &mut compressed,
                        &mut marked_scratch,
                        &mut clean_scratch,
                        &mut tail_inflater,
                    );
                    let decoded_bytes = result.as_ref().map_or(0, |chunk| chunk.output.len());
                    if adaptive_workers.observe_work(generation, decoded_bytes) {
                        work_signal.1.notify_all();
                    }
                    send_native_result(&sender, &stopped, NativeResult { index, result });
                    continue;
                }
                Steal::Retry => {
                    std::hint::spin_loop();
                    continue;
                }
                Steal::Empty => {}
            }
            let (lock, signal) = &*work_signal;
            let guard = lock.lock().expect("estimated work mutex poisoned");
            let _ = signal
                .wait_timeout_while(guard, WORKER_IDLE_PARK, |_| {
                    (available_decode_tasks.load(Ordering::Acquire) == 0
                        && available_resolve_tasks.load(Ordering::Acquire) == 0
                        || !adaptive_workers.worker_enabled(worker_index))
                        && !stopped.load(Ordering::Relaxed)
                })
                .expect("estimated work mutex poisoned");
        }
    });
}

struct BackendTail {
    output: Vec<u8>,
    end_bit: usize,
    reached_stream_end: bool,
}

/// Prefer the larger allocation when recycling emptied or failed-attempt buffers.
fn prefer_capacity<T>(dst: &mut Vec<T>, src: Vec<T>) {
    if src.capacity() > dst.capacity() {
        *dst = src;
    }
}

// Shared free-list of empty `Vec<u8>` capacity returned after resolved emit:
// `ByteBufferFreeList` lives in `buffer_pool` (shared with DecoderReader).
// Estimated workers drain successful `clean` / `backend_tail` allocations into
// `ChunkOutput`; those bytes stay live until resolve+emit. After a successful
// emit the cleared capacity is pushed here so the next task can
// `try_steal_into` instead of cold-allocating. Soft-capped at
// `2 × worker_count`. Emptied `Vec<Symbol>` capacity is recycled worker-locally
// after resolve (`prefer_capacity` into `marked_scratch`), not via this list.

#[allow(clippy::too_many_arguments)]
fn inflate_tail(
    bytes: &[u8],
    start_bit: usize,
    stop_bit: usize,
    window: &Window,
    maximum_output: usize,
    exact_stop: bool,
    inflater: &mut Option<RawInflater>,
    output: &mut Vec<u8>,
) -> Result<BackendTail, NativeError> {
    // Estimated-path tail continue: mid-stream bit resume through
    // [`InflateBackend`] (prime + dictionary). Same contract as
    // [`InflateBackend::install_bit_resume`], but keeps distinct NativeError
    // mappings (prime → InvalidSymbol, dictionary → InvalidDistance) and
    // reuses one zlib stream across candidates.
    if inflater.is_none() {
        *inflater = Some(
            <RawInflater as InflateBackend>::create().map_err(|_| NativeError::InvalidSymbol)?,
        );
    }
    let inflater = inflater
        .as_mut()
        .expect("inflater initialized immediately above");
    inflater
        .reset(start_bit as u64)
        .map_err(|_| NativeError::InvalidSymbol)?;
    let byte_offset = start_bit / 8;
    let skipped_bits = (start_bit % 8) as u8;
    let mut input_position = byte_offset;
    if skipped_bits != 0 {
        let first_byte = *bytes.get(byte_offset).ok_or(NativeError::UnexpectedEof)?;
        InflateBackend::prime(
            inflater,
            8 - skipped_bits,
            first_byte >> skipped_bits,
            start_bit as u64,
        )
        .map_err(|_| NativeError::InvalidSymbol)?;
        input_position += 1;
    }
    InflateBackend::set_dictionary(inflater, window, start_bit as u64)
        .map_err(|_| NativeError::InvalidDistance)?;

    // Typical 1 MiB compressed grid chunks expand to roughly 1--2 MiB. An
    // eager 2 MiB ceiling avoids repeated growth/copying without reserving the
    // much larger adversarial-output allowance for every worker. Capacity is
    // retained across failed attempts via the caller's scratch vector.
    output.clear();
    let target_cap = maximum_output.min(2 * 1024 * 1024);
    if output.capacity() < target_cap {
        output.reserve(target_cap - output.capacity());
    }
    loop {
        let input = bytes
            .get(input_position..)
            .ok_or(NativeError::UnexpectedEof)?;
        if input.is_empty() {
            return Err(NativeError::UnexpectedEof);
        }
        let remaining = maximum_output.saturating_sub(output.len());
        // Grow spare in moderate steps; produce is hard-capped by `remaining`.
        let reserve = remaining.clamp(1, 256 * 1024);
        output.reserve(reserve);

        // `InflateFlush::Block` surfaces DEFLATE block ends via
        // `InflateStep::{unused_bits, at_block_end, last_block}` (zlib
        // `data_type` bits under the hood for RawInflater).
        let step =
            InflateBackend::inflate_capped(inflater, input, output, InflateFlush::Block, remaining)
                .map_err(|_| NativeError::InvalidSymbol)?;
        input_position += step.consumed;

        // Cap prevents writing past `maximum_output`. When `remaining == 0`,
        // `inflate_capped` returns BUF_ERROR with zero progress without calling
        // zlib — map that stall to OutputLimit (prior path reserved ≥1 spare
        // and rejected any overshoot after the call).
        if step.consumed == 0 && step.produced == 0 && remaining == 0 {
            return Err(NativeError::OutputLimit);
        }

        // Unused bit-buffer bits (low six data_type bits). Optimized inflaters
        // may read several bytes ahead, so using only three bits can place a
        // gzip footer multiple bytes too late.
        let unused_bits = usize::from(step.unused_bits);
        let position = input_position.saturating_mul(8).saturating_sub(unused_bits);
        if step.status == inflate_status::STREAM_END {
            return Ok(BackendTail {
                output: std::mem::take(output),
                end_bit: position.div_ceil(8).saturating_mul(8),
                reached_stream_end: true,
            });
        }
        if step.status != inflate_status::OK && step.status != inflate_status::BUF_ERROR {
            return Err(NativeError::InvalidSymbol);
        }
        if step.at_block_end {
            // Block flush sets last_block after an end-of-block code for a
            // BFINAL block even when status is still OK rather than STREAM_END.
            // There is no following block; DEFLATE pads to the next byte.
            if step.last_block {
                return Ok(BackendTail {
                    output: std::mem::take(output),
                    end_bit: position.div_ceil(8).saturating_mul(8),
                    reached_stream_end: true,
                });
            }
            match position.cmp(&stop_bit) {
                std::cmp::Ordering::Equal => {
                    return Ok(BackendTail {
                        output: std::mem::take(output),
                        end_bit: position,
                        reached_stream_end: false,
                    });
                }
                std::cmp::Ordering::Greater if !exact_stop => {
                    return Ok(BackendTail {
                        output: std::mem::take(output),
                        end_bit: position,
                        reached_stream_end: false,
                    });
                }
                std::cmp::Ordering::Greater => return Err(NativeError::BoundaryMismatch),
                std::cmp::Ordering::Less => {}
            }
        }
        if step.consumed == 0 && step.produced == 0 {
            return Err(NativeError::UnexpectedEof);
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn run_estimated_task<R: ReadAt + ?Sized>(
    source: &R,
    task: &EstimatedTask,
    maximum_output: usize,
    bytes: &mut Vec<u8>,
    marked_scratch: &mut Vec<Symbol>,
    clean_scratch: &mut Vec<u8>,
    tail_inflater: &mut Option<RawInflater>,
) -> Result<crate::parallel::deflate::Chunk, NativeError> {
    let byte_start = task.search_start_bit / 8;
    let byte_end = task.read_end_bit.div_ceil(8);
    let length = usize::try_from(byte_end.saturating_sub(byte_start))
        .map_err(|_| NativeError::UnexpectedEof)?;
    read_range_reuse(source, byte_start, length, bytes).map_err(|_| NativeError::UnexpectedEof)?;
    let base_bit = byte_start
        .checked_mul(8)
        .ok_or(NativeError::UnexpectedEof)?;
    let local_search_start = usize::try_from(task.search_start_bit.saturating_sub(base_bit))
        .map_err(|_| NativeError::UnexpectedEof)?;
    let local_search_end = usize::try_from(task.search_end_bit.saturating_sub(base_bit))
        .map_err(|_| NativeError::UnexpectedEof)?;
    let local_stop = usize::try_from(task.estimated_stop_bit.saturating_sub(base_bit))
        .map_err(|_| NativeError::UnexpectedEof)?;
    if task.exact_start {
        let tail = inflate_tail(
            bytes,
            local_search_start,
            local_stop,
            &Window::empty(),
            maximum_output,
            false,
            tail_inflater,
            clean_scratch,
        )?;
        let chunk = crate::parallel::deflate::Chunk {
            start_bit: usize::try_from(task.search_start_bit)
                .map_err(|_| NativeError::UnexpectedEof)?,
            end_bit: tail
                .end_bit
                .checked_add(usize::try_from(base_bit).map_err(|_| NativeError::UnexpectedEof)?)
                .ok_or(NativeError::UnexpectedEof)?,
            output: crate::parallel::deflate::ChunkOutput::from_clean(tail.output),
            reached_stream_end: tail.reached_stream_end,
            backend_continuation: None,
        };
        return Ok(chunk);
    }
    let mut search_bit = local_search_start;

    loop {
        let local_start = find_next_structural_candidate(bytes, search_bit, local_search_end)
            .ok_or(NativeError::UnexpectedEof)?;
        let attempt = (|| {
            let mut chunk = decode_to_estimated_boundary(
                bytes,
                local_start,
                local_stop,
                InitialHistory::Unknown,
                maximum_output,
                marked_scratch,
            )?;
            if let Some(window) = chunk.backend_continuation.take() {
                let remaining = maximum_output.saturating_sub(chunk.output.len());
                match inflate_tail(
                    bytes,
                    chunk.end_bit,
                    local_stop,
                    &window,
                    remaining,
                    false,
                    tail_inflater,
                    clean_scratch,
                ) {
                    Ok(tail) => {
                        chunk.output.append_clean(tail.output);
                        chunk.end_bit = tail.end_bit;
                        chunk.reached_stream_end = tail.reached_stream_end;
                    }
                    Err(error) => {
                        // Decode already took the marked allocation; put the
                        // largest capacity back into the worker-local scratch
                        // so the next candidate does not start cold.
                        let (marked, clean, backend_tail) = chunk.output.into_recycle_parts();
                        prefer_capacity(marked_scratch, marked);
                        prefer_capacity(clean_scratch, clean);
                        prefer_capacity(clean_scratch, backend_tail);
                        return Err(error);
                    }
                }
            }
            Ok::<_, NativeError>(chunk)
        })();
        match attempt {
            Ok(mut chunk) => {
                chunk.start_bit +=
                    usize::try_from(base_bit).map_err(|_| NativeError::UnexpectedEof)?;
                chunk.end_bit +=
                    usize::try_from(base_bit).map_err(|_| NativeError::UnexpectedEof)?;
                return Ok(chunk);
            }
            Err(_) => search_bit = local_start.saturating_add(1),
        }
    }
}

struct PreparedNativeChunk {
    task: ResolveTask,
    next_window: Window,
    end_bit: u64,
    reached_stream_end: bool,
    decoded_size: usize,
}

fn prepare_native_chunk(
    chunk: crate::parallel::deflate::Chunk,
    sequence: usize,
    predecessor: &Window,
    bit_offset: u64,
) -> Result<PreparedNativeChunk, DecodeError> {
    let decoded_size = chunk.output.len();
    let next_window =
        chunk
            .output
            .window_after(predecessor)
            .map_err(|_| DecodeError::InvalidDeflate {
                bit_offset,
                reason: DeflateErrorKind::InvalidData,
            })?;
    Ok(PreparedNativeChunk {
        task: ResolveTask {
            sequence,
            predecessor: predecessor.clone(),
            output: chunk.output,
        },
        next_window,
        end_bit: chunk.end_bit as u64,
        reached_stream_end: chunk.reached_stream_end,
        decoded_size,
    })
}

fn emit_resolved_parts<O: Output>(
    parts: ResolvedParts,
    config: &Config,
    output: &mut O,
    accounting: &mut MemberAccounting,
    total_output: &mut u64,
    index_builder: &mut IndexBuilder,
    free_list: &ByteBufferFreeList,
) -> Result<(), DecodeError> {
    let (marked, clean, backend_tail) = parts;
    emit_resolved_buffer(
        marked,
        config,
        output,
        accounting,
        total_output,
        index_builder,
        free_list,
    )?;
    emit_resolved_buffer(
        clean,
        config,
        output,
        accounting,
        total_output,
        index_builder,
        free_list,
    )?;
    emit_resolved_buffer(
        backend_tail,
        config,
        output,
        accounting,
        total_output,
        index_builder,
        free_list,
    )?;
    Ok(())
}

/// Account + emit one resolved part, then recycle empty capacity into the
/// estimated free-list. Only called after resolve completes and the buffer is
/// exclusively owned by the coordinator (never while still in the resolve
/// queue).
///
/// `DirectOutput` returns the same cleared chunk; `ChannelOutput` sends the
/// chunk to the reader and may return a *different* buffer stolen from the
/// reader-local free list (see `buffer_pool`). Default `emit_reusable` returns
/// a zero-capacity vec that this free-list drops.
fn emit_resolved_buffer<O: Output>(
    decoded: Vec<u8>,
    config: &Config,
    output: &mut O,
    accounting: &mut MemberAccounting,
    total_output: &mut u64,
    index_builder: &mut IndexBuilder,
    free_list: &ByteBufferFreeList,
) -> Result<(), DecodeError> {
    let next_total =
        total_output
            .checked_add(decoded.len() as u64)
            .ok_or(DecodeError::OutputLimitExceeded {
                limit: config.output_limit.unwrap_or(u64::MAX),
            })?;
    if config.output_limit.is_some_and(|limit| next_total > limit) {
        return Err(DecodeError::OutputLimitExceeded {
            limit: config.output_limit.expect("checked as some"),
        });
    }
    *total_output = next_total;
    accounting.size = accounting.size.wrapping_add(decoded.len() as u32);
    if config.crc32_enabled {
        accounting.crc.update(&decoded);
        accounting.adler.update(&decoded);
    } else if !config.raw_crc32_list.is_empty() {
        accounting.crc.update(&decoded);
    }
    index_builder.push_output(&decoded);
    if decoded.is_empty() {
        free_list.recycle(decoded);
        return Ok(());
    }
    let empty = output.emit_reusable(decoded)?;
    free_list.recycle(empty);
    Ok(())
}

fn wait_for_resolved(
    receiver: &mpsc::Receiver<ResolveResult>,
    pending: &mut BTreeMap<usize, Result<ResolvedParts, crate::parallel::MarkerError>>,
    next_sequence: usize,
    cancelled: &AtomicBool,
    bit_offset: u64,
) -> Result<ResolvedParts, DecodeError> {
    let result = loop {
        if let Some(result) = pending.remove(&next_sequence) {
            break result;
        }
        match receiver.recv_timeout(COORDINATOR_RECV_TIMEOUT) {
            Ok(result) if result.sequence < next_sequence => {}
            Ok(result) => {
                pending.insert(result.sequence, result.result);
            }
            Err(RecvTimeoutError::Timeout) => {
                if cancelled.load(Ordering::Relaxed) {
                    return Err(DecodeError::Cancelled);
                }
            }
            Err(RecvTimeoutError::Disconnected) => {
                return Err(DecodeError::WorkerPanicked);
            }
        }
    };
    result.map_err(|_| DecodeError::InvalidDeflate {
        bit_offset,
        reason: DeflateErrorKind::InvalidData,
    })
}

#[allow(clippy::too_many_arguments)]
fn enqueue_native_resolution(
    chunk: crate::parallel::deflate::Chunk,
    config: &Config,
    current_bit: &mut u64,
    window: &mut Window,
    prepared_total: &mut u64,
    next_sequence: &mut usize,
    outstanding: &mut usize,
    queue: &Injector<ResolveTask>,
    available_resolve_tasks: &AtomicUsize,
    work_signal: &(Mutex<()>, Condvar),
    index_builder: &mut IndexBuilder,
) -> Result<bool, DecodeError> {
    // Record a seek point at this chunk's start using the predecessor window
    // that will be installed as the inflate dictionary. Spacing is soft: tiny
    // chunks denser than checkpoint_spacing are skipped unless this is the
    // first point after a member reset (empty window / force via spacing).
    let start_bit = *current_bit;
    let uncompressed_at_start = *prepared_total;
    let predecessor_window = window.as_slice();
    index_builder.checkpoint_at(
        start_bit,
        uncompressed_at_start,
        if predecessor_window.is_empty() {
            None
        } else {
            Some(predecessor_window)
        },
        false,
    );

    let prepared = prepare_native_chunk(chunk, *next_sequence, window, *current_bit)?;
    let next_total = prepared_total
        .checked_add(prepared.decoded_size as u64)
        .ok_or(DecodeError::OutputLimitExceeded {
            limit: config.output_limit.unwrap_or(u64::MAX),
        })?;
    if config.output_limit.is_some_and(|limit| next_total > limit) {
        return Err(DecodeError::OutputLimitExceeded {
            limit: config.output_limit.expect("checked as some"),
        });
    }

    let reached_stream_end = prepared.reached_stream_end;
    *prepared_total = next_total;
    *current_bit = prepared.end_bit;
    *window = prepared.next_window;
    *next_sequence += 1;
    *outstanding += 1;
    let (lock, signal) = work_signal;
    let _guard = lock.lock().expect("estimated work mutex poisoned");
    queue.push(prepared.task);
    available_resolve_tasks.fetch_add(1, Ordering::Release);
    signal.notify_all();
    Ok(reached_stream_end)
}

#[allow(clippy::too_many_arguments)]
fn drain_native_resolutions<O: Output>(
    receiver: &mpsc::Receiver<ResolveResult>,
    pending: &mut BTreeMap<usize, Result<ResolvedParts, crate::parallel::MarkerError>>,
    next_sequence: &mut usize,
    outstanding: &mut usize,
    cancelled: &AtomicBool,
    bit_offset: u64,
    config: &Config,
    output: &mut O,
    accounting: &mut MemberAccounting,
    total_output: &mut u64,
    index_builder: &mut IndexBuilder,
    free_list: &ByteBufferFreeList,
) -> Result<(), DecodeError> {
    while *outstanding != 0 {
        let parts = wait_for_resolved(receiver, pending, *next_sequence, cancelled, bit_offset)?;
        emit_resolved_parts(
            parts,
            config,
            output,
            accounting,
            total_output,
            index_builder,
            free_list,
        )?;
        *next_sequence += 1;
        *outstanding -= 1;
    }
    Ok(())
}

fn validate_footer<R: ReadAt + ?Sized>(
    cursor: &mut SourceCursor<'_, R>,
    footer_offset: u64,
    member: u64,
    accounting: &MemberAccounting,
    crc32_enabled: bool,
) -> Result<u64, DecodeError> {
    const MAX_BACKEND_READ_AHEAD: u64 = 16;
    let actual_crc = if crc32_enabled {
        accounting.crc.finish()
    } else {
        0
    };
    let actual_size = accounting.size;
    let mut reported_footer = None;
    let mut first_error = None;

    for read_ahead in 0..=MAX_BACKEND_READ_AHEAD {
        let candidate = footer_offset.saturating_sub(read_ahead);
        if candidate.saturating_add(8) > cursor.length() {
            continue;
        }
        if let Err(error) = cursor.seek(candidate) {
            first_error.get_or_insert(error);
            continue;
        }
        let footer = match cursor.read_exact::<8>(candidate) {
            Ok(footer) => footer,
            Err(error) => {
                first_error.get_or_insert(error);
                continue;
            }
        };
        let expected_crc = u32::from_le_bytes(footer[..4].try_into().expect("four bytes"));
        let expected_size = u32::from_le_bytes(footer[4..].try_into().expect("four bytes"));
        if read_ahead == 0 {
            reported_footer = Some((expected_crc, expected_size));
        }
        let crc_ok = !crc32_enabled || expected_crc == actual_crc;
        if crc_ok && expected_size == actual_size {
            return Ok(candidate);
        }
    }

    let Some((expected_crc, expected_size)) = reported_footer else {
        return Err(first_error.unwrap_or(DecodeError::InvalidGzip {
            offset: footer_offset,
            reason: GzipErrorKind::Truncated,
        }));
    };
    if crc32_enabled && expected_crc != actual_crc {
        return Err(DecodeError::ChecksumMismatch {
            member,
            expected: expected_crc,
            actual: actual_crc,
        });
    }
    if expected_size != actual_size {
        return Err(DecodeError::SizeMismatch {
            member,
            expected: expected_size,
            actual_mod32: actual_size,
        });
    }
    unreachable!("a matching footer would have returned from the search")
}

/// Validates the 4-byte big-endian Adler-32 trailer at (or slightly before)
/// `footer_offset`. Leaves the cursor immediately after the accepted trailer.
///
/// Native bit-position bookkeeping can land a few bytes past the true stream
/// end (same class of issue as [`validate_footer`]); search backwards when
/// integrity checks are enabled so a matching Adler still wins.
fn validate_zlib_adler_footer<R: ReadAt + ?Sized>(
    cursor: &mut SourceCursor<'_, R>,
    footer_offset: u64,
    member: u64,
    accounting: &MemberAccounting,
    crc32_enabled: bool,
) -> Result<u64, DecodeError> {
    const MAX_BACKEND_READ_AHEAD: u64 = 16;
    let actual_adler = if crc32_enabled {
        accounting.adler.finish()
    } else {
        0
    };

    if !crc32_enabled {
        if footer_offset.saturating_add(4) > cursor.length() {
            return Err(DecodeError::InvalidZlib {
                offset: footer_offset,
                reason: ZlibErrorKind::Truncated,
            });
        }
        cursor.seek(footer_offset)?;
        let _ = cursor
            .read_exact::<4>(footer_offset)
            .map_err(|error| match error {
                DecodeError::InvalidGzip {
                    reason: GzipErrorKind::Truncated,
                    ..
                } => DecodeError::InvalidZlib {
                    offset: footer_offset,
                    reason: ZlibErrorKind::Truncated,
                },
                other => other,
            })?;
        return Ok(footer_offset);
    }

    let mut first_error = None;
    for read_ahead in 0..=MAX_BACKEND_READ_AHEAD {
        let candidate = footer_offset.saturating_sub(read_ahead);
        if candidate.saturating_add(4) > cursor.length() {
            continue;
        }
        if let Err(error) = cursor.seek(candidate) {
            first_error.get_or_insert(error);
            continue;
        }
        let footer = match cursor.read_exact::<4>(candidate) {
            Ok(footer) => footer,
            Err(error) => {
                first_error.get_or_insert(error);
                continue;
            }
        };
        let expected_adler = u32::from_be_bytes(footer);
        if expected_adler == actual_adler {
            return Ok(candidate);
        }
        // Keep the first exact-offset mismatch as the preferred error if no
        // read-ahead candidate matches.
        if read_ahead == 0 {
            first_error.get_or_insert(DecodeError::ChecksumMismatch {
                member,
                expected: expected_adler,
                actual: actual_adler,
            });
        }
    }

    Err(first_error.unwrap_or(DecodeError::InvalidZlib {
        offset: footer_offset,
        reason: ZlibErrorKind::Truncated,
    }))
}

/// Try the native estimated/marker path for a single long zlib stream; fall
/// back to sequential when the compressed payload is too small to amortize.
fn decode_zlib_parallel_or_sequential<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    debug_assert!(config.decoder_threads >= MIN_MARKER_PARALLEL_THREADS);

    let compressed_chunk_size = adjusted_compressed_chunk_size(source, config)?;
    // Smaller streams stay on sequential zlib-rs (marker overhead does not pay).
    if !zlib_payload_large_enough_for_marker(source, config)? {
        return decode_zlib_sequential(source, config, cancelled, output);
    }
    decode_zlib_estimated(source, config, cancelled, output, compressed_chunk_size)
}

/// Outcome of the shared single-stream estimated/marker DEFLATE body.
struct EstimatedDeflateBodyResult {
    /// First compressed byte after the DEFLATE bit stream (zlib/gzip trailer
    /// start, or EOF for raw). Byte-aligned via native `align_to_byte` /
    /// inflate unused-bits correction.
    deflate_end_byte: u64,
    total_output: u64,
    accounting: MemberAccounting,
    index_builder: IndexBuilder,
}

/// Parallel single-stream DEFLATE body shared by zlib and raw estimated paths.
///
/// Starts at `first_deflate_bit` (0 for raw, after CMF/FLG for zlib). Returns
/// when the DEFLATE stream ends; callers validate trailers / leftover input.
///
/// Returns `Ok(None)` when the grid would have fewer than two tasks so the
/// caller can fall back to sequential without starting workers.
fn decode_estimated_deflate_body<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    compressed_chunk_size: usize,
    first_deflate_bit: u64,
    mut index_builder: IndexBuilder,
) -> Result<Option<EstimatedDeflateBodyResult>, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    const SEARCH_BYTES: u64 = 512 * 1024;
    const LOOKAHEAD_BYTES: u64 = 512 * 1024;

    let source_len = source
        .len()
        .map_err(|error| DecodeError::input_io(0, error))?;
    let length_bits = source_len.saturating_mul(8);
    let spacing_bits = (compressed_chunk_size as u64).saturating_mul(8);
    let task_count = usize::try_from(
        length_bits
            .saturating_sub(first_deflate_bit)
            .div_ceil(spacing_bits),
    )
    .unwrap_or(usize::MAX)
    .max(1);
    // Defensive: size gate should have ensured ≥2 tasks; if not, sequential.
    if task_count < 2 {
        return Ok(None);
    }

    index_builder.force_checkpoint(first_deflate_bit, true);

    let tasks: Vec<_> = (0..task_count)
        .map(|index| {
            let estimated_start =
                first_deflate_bit.saturating_add((index as u64).saturating_mul(spacing_bits));
            let estimated_stop = estimated_start.saturating_add(spacing_bits);
            EstimatedTask {
                search_start_bit: estimated_start,
                search_end_bit: if index == 0 {
                    estimated_start
                } else {
                    estimated_start
                        .saturating_add(SEARCH_BYTES.saturating_mul(8))
                        .min(estimated_stop)
                        .min(length_bits)
                },
                estimated_stop_bit: estimated_stop,
                read_end_bit: estimated_stop
                    .saturating_add(LOOKAHEAD_BYTES.saturating_mul(8))
                    .min(length_bits),
                exact_start: index == 0,
            }
        })
        .collect();
    let maximum_output = config
        .decoded_chunk_size
        .max(compressed_chunk_size.saturating_mul(20));
    let task_queue = Arc::new(Injector::new());
    let resolve_queue = Arc::new(Injector::<ResolveTask>::new());
    let stopped = Arc::new(AtomicBool::new(false));
    let available_decode_tasks = Arc::new(AtomicUsize::new(0));
    let available_resolve_tasks = Arc::new(AtomicUsize::new(0));
    let work_signal = Arc::new((Mutex::new(()), Condvar::new()));
    let worker_count = config.decoder_threads.min(tasks.len());
    // Soft cap: ~2 free buffers per worker bounds RSS if emit outpaces decode.
    let output_free_list = Arc::new(ByteBufferFreeList::new(worker_count.saturating_mul(2)));
    let machine_parallelism = thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let adaptive_workers = Arc::new(AdaptiveWorkers::new(
        worker_count,
        machine_parallelism,
        compressed_chunk_size.saturating_mul(16),
        tasks.len(),
    ));
    let worker_pool_count = adaptive_workers.worker_pool_limit().min(worker_count);
    let pipeline_capacity = config
        .in_flight_chunks
        .max(worker_pool_count)
        .min(tasks.len());
    let initial_task_window = adaptive_workers.current_limit().min(pipeline_capacity);
    for index in 0..initial_task_window {
        task_queue.push(index);
    }
    available_decode_tasks.store(initial_task_window, Ordering::Release);
    let (sender, receiver) = mpsc::sync_channel::<NativeResult>(pipeline_capacity);
    let (resolve_sender, resolve_receiver) = mpsc::sync_channel::<ResolveResult>(pipeline_capacity);

    let mut total_output = 0_u64;
    let mut current_bit = first_deflate_bit;
    let mut next_to_schedule = initial_task_window;
    let mut next_to_emit = 0_usize;
    let mut pending = BTreeMap::new();
    let mut prepared_total_output = 0_u64;
    let mut next_resolve_sequence = 0_usize;
    let mut next_resolve_to_emit = 0_usize;
    let mut outstanding_resolves = 0_usize;
    let mut resolve_pending = BTreeMap::new();
    let mut stream_end_byte: Option<u64> = None;
    let mut finished_accounting = MemberAccounting::new();

    let scoped_result = thread::scope(|scope| -> Result<(), DecodeError> {
        let _stop_on_exit = SignalledStopGuard {
            stopped: &stopped,
            work_signal: &work_signal.1,
            limit_signal: &adaptive_workers.limit_signal,
        };
        let mut sender_template = Some(sender);
        let mut resolve_sender_template = Some(resolve_sender);
        let mut spawned_workers = 0_usize;

        let mut window = Window::empty();
        let mut accounting = MemberAccounting::new();
        let mut footer_offset = None;

        'decode: loop {
            let spawn_target = adaptive_workers.current_limit().min(worker_pool_count);
            while spawned_workers < spawn_target {
                spawn_estimated_worker(
                    scope,
                    spawned_workers,
                    source,
                    &tasks,
                    maximum_output,
                    Arc::clone(&task_queue),
                    Arc::clone(&resolve_queue),
                    sender_template
                        .as_ref()
                        .expect("worker sender remains while calibration can grow")
                        .clone(),
                    resolve_sender_template
                        .as_ref()
                        .expect("resolve sender remains while calibration can grow")
                        .clone(),
                    Arc::clone(&stopped),
                    Arc::clone(&available_decode_tasks),
                    Arc::clone(&available_resolve_tasks),
                    Arc::clone(&work_signal),
                    Arc::clone(&adaptive_workers),
                    Arc::clone(&output_free_list),
                );
                spawned_workers += 1;
            }
            if !adaptive_workers.is_calibrating() {
                drop(sender_template.take());
                drop(resolve_sender_template.take());
            }

            if let Some(offset) = footer_offset.take() {
                drain_native_resolutions(
                    &resolve_receiver,
                    &mut resolve_pending,
                    &mut next_resolve_to_emit,
                    &mut outstanding_resolves,
                    cancelled,
                    current_bit,
                    config,
                    output,
                    &mut accounting,
                    &mut total_output,
                    &mut index_builder,
                    &output_free_list,
                )?;
                finished_accounting = accounting;
                stream_end_byte = Some(offset);
                break 'decode;
            }

            if cancelled.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            let result = loop {
                if let Some(result) = pending.remove(&next_to_emit) {
                    break result;
                }
                match receiver.recv_timeout(COORDINATOR_RECV_TIMEOUT) {
                    Ok(result) if result.index < next_to_emit => {}
                    Ok(result) => {
                        pending.insert(result.index, result.result);
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        if cancelled.load(Ordering::Relaxed) {
                            return Err(DecodeError::Cancelled);
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(DecodeError::WorkerPanicked);
                    }
                }
            };

            match result {
                Ok(chunk) if chunk.start_bit as u64 == current_bit => {
                    let reached_stream_end = enqueue_native_resolution(
                        chunk,
                        config,
                        &mut current_bit,
                        &mut window,
                        &mut prepared_total_output,
                        &mut next_resolve_sequence,
                        &mut outstanding_resolves,
                        &resolve_queue,
                        &available_resolve_tasks,
                        &work_signal,
                        &mut index_builder,
                    )?;
                    let resolve_window = adaptive_workers.current_limit().min(pipeline_capacity);
                    if outstanding_resolves >= resolve_window {
                        let parts = wait_for_resolved(
                            &resolve_receiver,
                            &mut resolve_pending,
                            next_resolve_to_emit,
                            cancelled,
                            current_bit,
                        )?;
                        emit_resolved_parts(
                            parts,
                            config,
                            output,
                            &mut accounting,
                            &mut total_output,
                            &mut index_builder,
                            &output_free_list,
                        )?;
                        next_resolve_to_emit += 1;
                        outstanding_resolves -= 1;
                    }
                    next_to_emit += 1;
                    let task_window = adaptive_workers.current_limit().min(pipeline_capacity);
                    let schedule_end = next_to_emit.saturating_add(task_window).min(tasks.len());
                    if next_to_schedule < schedule_end {
                        let (lock, signal) = &*work_signal;
                        let _guard = lock.lock().expect("estimated work mutex poisoned");
                        while next_to_schedule < schedule_end {
                            task_queue.push(next_to_schedule);
                            available_decode_tasks.fetch_add(1, Ordering::Release);
                            next_to_schedule += 1;
                        }
                        signal.notify_all();
                    }
                    if reached_stream_end {
                        footer_offset = Some(current_bit / 8);
                    } else if next_to_emit >= tasks.len() {
                        drain_native_resolutions(
                            &resolve_receiver,
                            &mut resolve_pending,
                            &mut next_resolve_to_emit,
                            &mut outstanding_resolves,
                            cancelled,
                            current_bit,
                            config,
                            output,
                            &mut accounting,
                            &mut total_output,
                            &mut index_builder,
                            &output_free_list,
                        )?;
                        footer_offset = Some(inflate_from_block(
                            source,
                            config,
                            cancelled,
                            output,
                            current_bit,
                            &window,
                            &mut accounting,
                            &mut total_output,
                            &mut index_builder,
                        )?);
                        prepared_total_output = total_output;
                    }
                }
                Ok(_) | Err(_) => {
                    drain_native_resolutions(
                        &resolve_receiver,
                        &mut resolve_pending,
                        &mut next_resolve_to_emit,
                        &mut outstanding_resolves,
                        cancelled,
                        current_bit,
                        config,
                        output,
                        &mut accounting,
                        &mut total_output,
                        &mut index_builder,
                        &output_free_list,
                    )?;
                    footer_offset = Some(inflate_from_block(
                        source,
                        config,
                        cancelled,
                        output,
                        current_bit,
                        &window,
                        &mut accounting,
                        &mut total_output,
                        &mut index_builder,
                    )?);
                    prepared_total_output = total_output;
                }
            }
        }

        stopped.store(true, Ordering::Relaxed);
        work_signal.1.notify_all();
        Ok(())
    });
    stopped.store(true, Ordering::Relaxed);
    work_signal.1.notify_all();
    scoped_result?;

    let Some(deflate_end_byte) = stream_end_byte else {
        return Err(DecodeError::InvalidDeflate {
            bit_offset: current_bit,
            reason: DeflateErrorKind::Truncated,
        });
    };
    Ok(Some(EstimatedDeflateBodyResult {
        deflate_end_byte,
        total_output,
        accounting: finished_accounting,
        index_builder,
    }))
}

/// Parallel single-stream zlib via the same estimated grid / marker path as
/// gzip. Concatenated streams after the first fall back to sequential.
fn decode_zlib_estimated<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    compressed_chunk_size: usize,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    // Frame cursor only needs the zlib header and Adler trailer.
    let mut frame_cursor = SourceCursor::new(source, config.input_page_size.min(256))?;
    let index_builder = new_index_builder(config);
    if frame_cursor.at_end() {
        return Err(DecodeError::InvalidZlib {
            offset: 0,
            reason: ZlibErrorKind::Truncated,
        });
    }

    let first_header = parse_zlib_header(&mut frame_cursor, true)?;
    let first_deflate_bit = first_header.deflate_start.saturating_mul(8);

    let Some(body) = decode_estimated_deflate_body(
        source,
        config,
        cancelled,
        output,
        compressed_chunk_size,
        first_deflate_bit,
        index_builder,
    )?
    else {
        return decode_zlib_sequential(source, config, cancelled, output);
    };

    let actual_footer = validate_zlib_adler_footer(
        &mut frame_cursor,
        body.deflate_end_byte,
        0,
        &body.accounting,
        config.crc32_enabled,
    )?;
    let member_count = 1_u64;
    let total_output = body.total_output;
    let index_builder = body.index_builder;
    // Cursor sits after the Adler trailer.
    let stream_end = actual_footer.saturating_add(4);
    debug_assert_eq!(frame_cursor.position(), stream_end);
    if frame_cursor.position() != stream_end {
        frame_cursor.seek(stream_end)?;
    }

    // Concatenated zlib streams after the first: stream-granularity parallel
    // when ≥1 remaining stream and threads allow; else sequential.
    if !frame_cursor.at_end() {
        if config.decoder_threads > 1 {
            if let Some(rest) = index_zlib_streams_from(source, config, cancelled, stream_end)? {
                return decode_zlib_streams_parallel_from(
                    source,
                    config,
                    cancelled,
                    output,
                    &rest,
                    member_count,
                    total_output,
                    index_builder,
                );
            }
        }
        return decode_zlib_sequential_from(
            source,
            config,
            cancelled,
            output,
            ZlibSequentialState {
                cursor: frame_cursor,
                index_builder,
                member_count,
                total_output,
            },
        );
    }

    let final_length = source
        .len()
        .map_err(|error| DecodeError::input_io(frame_cursor.position(), error))?;
    if final_length != frame_cursor.length() {
        return Err(DecodeError::input_io(
            frame_cursor.position(),
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during decoding",
            ),
        ));
    }
    Ok(finish_report(
        config,
        frame_cursor.position(),
        total_output,
        member_count,
        index_builder,
    ))
}

/// Parallel single-stream raw DEFLATE via the shared estimated/marker body.
///
/// Starts at bit 0 (no CMF/FLG). On stream end there is no Adler/CRC trailer;
/// leftover compressed input is an error. Optional whole-stream CRC via
/// `config.raw_crc32_list`.
fn decode_raw_deflate_estimated<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    compressed_chunk_size: usize,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    debug_assert!(
        !config.keep_index,
        "keep_index for raw DEFLATE is rejected by DecoderBuilder::build"
    );

    let source_len = source
        .len()
        .map_err(|error| DecodeError::input_io(0, error))?;
    if source_len == 0 {
        return Err(DecodeError::InvalidDeflate {
            bit_offset: 0,
            reason: DeflateErrorKind::Truncated,
        });
    }

    let Some(body) = decode_estimated_deflate_body(
        source,
        config,
        cancelled,
        output,
        compressed_chunk_size,
        0,
        new_index_builder(config),
    )?
    else {
        return decode_raw_deflate_sequential(source, config, cancelled, output);
    };

    // Single stream must consume the entire source (no trailer, no concat).
    // Native end bits are byte-aligned after the final block; leftover bytes
    // after that position match the sequential InvalidData policy.
    if body.deflate_end_byte != source_len {
        return Err(DecodeError::InvalidDeflate {
            bit_offset: body.deflate_end_byte.saturating_mul(8),
            reason: if body.deflate_end_byte < source_len {
                DeflateErrorKind::InvalidData
            } else {
                DeflateErrorKind::Truncated
            },
        });
    }

    let final_length = source
        .len()
        .map_err(|error| DecodeError::input_io(body.deflate_end_byte, error))?;
    if final_length != source_len {
        return Err(DecodeError::input_io(
            body.deflate_end_byte,
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during decoding",
            ),
        ));
    }

    crate::crc32::verify_raw_crc32_list(&config.raw_crc32_list, &body.accounting.crc)?;

    Ok(finish_report(
        config,
        body.deflate_end_byte,
        body.total_output,
        1,
        body.index_builder,
    ))
}

fn decode_rapidgzip_estimated<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    compressed_chunk_size: usize,
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    const SEARCH_BYTES: u64 = 512 * 1024;
    const LOOKAHEAD_BYTES: u64 = 512 * 1024;
    // This cursor touches only gzip headers and footers. Large pages would
    // copy compressed payload that the positional workers read independently.
    let mut frame_cursor = SourceCursor::new(source, config.input_page_size.min(256))?;
    let mut index_builder = new_index_builder(config);
    if frame_cursor.at_end() {
        // Match sequential path: empty input is not a valid gzip stream.
        return Err(DecodeError::InvalidGzip {
            offset: 0,
            reason: GzipErrorKind::BadMagic,
        });
    }

    let first_header = parse_member_header(&mut frame_cursor, true)?;
    let first_deflate_bit = first_header.deflate_start.saturating_mul(8);
    // Empty window at the start of the first DEFLATE stream.
    index_builder.force_checkpoint(first_deflate_bit, true);
    let length_bits = frame_cursor.length().saturating_mul(8);
    let spacing_bits = (compressed_chunk_size as u64).saturating_mul(8);
    let task_count = usize::try_from(
        length_bits
            .saturating_sub(first_deflate_bit)
            .div_ceil(spacing_bits),
    )
    .unwrap_or(usize::MAX)
    .max(1);
    let tasks: Vec<_> = (0..task_count)
        .map(|index| {
            let estimated_start =
                first_deflate_bit.saturating_add((index as u64).saturating_mul(spacing_bits));
            let estimated_stop = estimated_start.saturating_add(spacing_bits);
            EstimatedTask {
                search_start_bit: estimated_start,
                search_end_bit: if index == 0 {
                    estimated_start
                } else {
                    estimated_start
                        .saturating_add(SEARCH_BYTES.saturating_mul(8))
                        .min(estimated_stop)
                        .min(length_bits)
                },
                estimated_stop_bit: estimated_stop,
                read_end_bit: estimated_stop
                    .saturating_add(LOOKAHEAD_BYTES.saturating_mul(8))
                    .min(length_bits),
                exact_start: index == 0,
            }
        })
        .collect();
    let maximum_output = config
        .decoded_chunk_size
        .max(compressed_chunk_size.saturating_mul(20));
    let task_queue = Arc::new(Injector::new());
    let resolve_queue = Arc::new(Injector::<ResolveTask>::new());
    let stopped = Arc::new(AtomicBool::new(false));
    let available_decode_tasks = Arc::new(AtomicUsize::new(0));
    let available_resolve_tasks = Arc::new(AtomicUsize::new(0));
    let work_signal = Arc::new((Mutex::new(()), Condvar::new()));
    let worker_count = config.decoder_threads.min(tasks.len());
    // Soft cap: ~2 free buffers per worker bounds RSS if emit outpaces decode.
    let output_free_list = Arc::new(ByteBufferFreeList::new(worker_count.saturating_mul(2)));
    let machine_parallelism = thread::available_parallelism().map_or(1, std::num::NonZero::get);
    let adaptive_workers = Arc::new(AdaptiveWorkers::new(
        worker_count,
        machine_parallelism,
        compressed_chunk_size.saturating_mul(16),
        tasks.len(),
    ));
    let worker_pool_count = adaptive_workers.worker_pool_limit().min(worker_count);
    // Result channels need spare slots beyond the active ranks because the
    // same pool executes marker resolution. If every worker blocks while
    // publishing speculative decode, no rank remains to resolve an exact
    // member bridge. The configured window carries the usual two-slot slack;
    // the scheduling horizon below still follows the adaptive active limit, so
    // this capacity does not admit extra speculative tasks.
    let pipeline_capacity = config
        .in_flight_chunks
        .max(worker_pool_count)
        .min(tasks.len());
    let initial_task_window = adaptive_workers.current_limit().min(pipeline_capacity);
    for index in 0..initial_task_window {
        task_queue.push(index);
    }
    available_decode_tasks.store(initial_task_window, Ordering::Release);
    let (sender, receiver) = mpsc::sync_channel::<NativeResult>(pipeline_capacity);
    let (resolve_sender, resolve_receiver) = mpsc::sync_channel::<ResolveResult>(pipeline_capacity);

    let mut member_count = 0_u64;
    let mut total_output = 0_u64;
    let mut current_bit = first_deflate_bit;
    let mut next_to_schedule = initial_task_window;
    let mut next_to_emit = 0_usize;
    let mut pending = BTreeMap::new();
    let mut prepared_total_output = 0_u64;
    let mut next_resolve_sequence = 0_usize;
    let mut next_resolve_to_emit = 0_usize;
    let mut outstanding_resolves = 0_usize;
    let mut resolve_pending = BTreeMap::new();

    let scoped_result = thread::scope(|scope| -> Result<(), DecodeError> {
        let _stop_on_exit = SignalledStopGuard {
            stopped: &stopped,
            work_signal: &work_signal.1,
            limit_signal: &adaptive_workers.limit_signal,
        };
        let mut sender_template = Some(sender);
        let mut resolve_sender_template = Some(resolve_sender);
        let mut spawned_workers = 0_usize;

        let mut window = Window::empty();
        let mut accounting = MemberAccounting::new();
        let mut footer_offset = None;
        let mut bridge_compressed = Vec::new();

        'decode: loop {
            let spawn_target = adaptive_workers.current_limit().min(worker_pool_count);
            while spawned_workers < spawn_target {
                spawn_estimated_worker(
                    scope,
                    spawned_workers,
                    source,
                    &tasks,
                    maximum_output,
                    Arc::clone(&task_queue),
                    Arc::clone(&resolve_queue),
                    sender_template
                        .as_ref()
                        .expect("worker sender remains while calibration can grow")
                        .clone(),
                    resolve_sender_template
                        .as_ref()
                        .expect("resolve sender remains while calibration can grow")
                        .clone(),
                    Arc::clone(&stopped),
                    Arc::clone(&available_decode_tasks),
                    Arc::clone(&available_resolve_tasks),
                    Arc::clone(&work_signal),
                    Arc::clone(&adaptive_workers),
                    Arc::clone(&output_free_list),
                );
                spawned_workers += 1;
            }
            if !adaptive_workers.is_calibrating() {
                drop(sender_template.take());
                drop(resolve_sender_template.take());
            }

            if let Some(offset) = footer_offset.take() {
                drain_native_resolutions(
                    &resolve_receiver,
                    &mut resolve_pending,
                    &mut next_resolve_to_emit,
                    &mut outstanding_resolves,
                    cancelled,
                    current_bit,
                    config,
                    output,
                    &mut accounting,
                    &mut total_output,
                    &mut index_builder,
                    &output_free_list,
                )?;
                let actual_footer = validate_footer(
                    &mut frame_cursor,
                    offset,
                    member_count,
                    &accounting,
                    config.crc32_enabled,
                )?;
                member_count += 1;
                if actual_footer.saturating_add(8) == frame_cursor.length() {
                    break 'decode;
                }

                let header = parse_member_header(&mut frame_cursor, false)?;
                current_bit = header.deflate_start.saturating_mul(8);
                window = Window::empty();
                accounting = MemberAccounting::new();
                // History resets at each member; empty-window seek point.
                index_builder.checkpoint_at(current_bit, total_output, None, true);

                // Use the first file-wide grid point strictly after this
                // member header. The exact bridge chunk ends at the same
                // independently discovered boundary where that regular task
                // begins. Workers can therefore keep useful later-member
                // tasks in flight while framing and history reset here.
                let target_index = usize::try_from(
                    current_bit
                        .saturating_sub(first_deflate_bit)
                        .div_euclid(spacing_bits)
                        .saturating_add(1),
                )
                .unwrap_or(usize::MAX);
                if target_index >= tasks.len() {
                    footer_offset = Some(inflate_from_block(
                        source,
                        config,
                        cancelled,
                        output,
                        current_bit,
                        &window,
                        &mut accounting,
                        &mut total_output,
                        &mut index_builder,
                    )?);
                    prepared_total_output = total_output;
                    continue 'decode;
                }

                pending.retain(|index, _| *index >= target_index);
                next_to_emit = target_index;
                if next_to_schedule < target_index {
                    next_to_schedule = target_index;
                }
                let task_window = adaptive_workers.current_limit().min(pipeline_capacity);
                let schedule_end = target_index.saturating_add(task_window).min(tasks.len());
                if next_to_schedule < schedule_end {
                    let (lock, signal) = &*work_signal;
                    let _guard = lock.lock().expect("estimated work mutex poisoned");
                    while next_to_schedule < schedule_end {
                        task_queue.push(next_to_schedule);
                        available_decode_tasks.fetch_add(1, Ordering::Release);
                        next_to_schedule += 1;
                    }
                    signal.notify_all();
                }

                let bridge_stop = tasks[target_index].search_start_bit;
                let bridge = EstimatedTask {
                    search_start_bit: current_bit,
                    search_end_bit: current_bit,
                    estimated_stop_bit: bridge_stop,
                    read_end_bit: bridge_stop
                        .saturating_add(LOOKAHEAD_BYTES.saturating_mul(8))
                        .min(length_bits),
                    exact_start: true,
                };
                let mut bridge_marked = Vec::new();
                let mut bridge_clean = Vec::new();
                let mut bridge_inflater = None;
                let bridge_result = run_estimated_task(
                    source,
                    &bridge,
                    maximum_output,
                    &mut bridge_compressed,
                    &mut bridge_marked,
                    &mut bridge_clean,
                    &mut bridge_inflater,
                );
                match bridge_result {
                    Ok(chunk) if chunk.start_bit as u64 == current_bit => {
                        let reached_stream_end = enqueue_native_resolution(
                            chunk,
                            config,
                            &mut current_bit,
                            &mut window,
                            &mut prepared_total_output,
                            &mut next_resolve_sequence,
                            &mut outstanding_resolves,
                            &resolve_queue,
                            &available_resolve_tasks,
                            &work_signal,
                            &mut index_builder,
                        )?;
                        let resolve_window =
                            adaptive_workers.current_limit().min(pipeline_capacity);
                        if outstanding_resolves >= resolve_window {
                            let parts = wait_for_resolved(
                                &resolve_receiver,
                                &mut resolve_pending,
                                next_resolve_to_emit,
                                cancelled,
                                current_bit,
                            )?;
                            emit_resolved_parts(
                                parts,
                                config,
                                output,
                                &mut accounting,
                                &mut total_output,
                                &mut index_builder,
                                &output_free_list,
                            )?;
                            next_resolve_to_emit += 1;
                            outstanding_resolves -= 1;
                        }
                        if reached_stream_end {
                            footer_offset = Some(current_bit / 8);
                        }
                    }
                    Ok(_) | Err(_) => {
                        footer_offset = Some(inflate_from_block(
                            source,
                            config,
                            cancelled,
                            output,
                            current_bit,
                            &window,
                            &mut accounting,
                            &mut total_output,
                            &mut index_builder,
                        )?);
                        prepared_total_output = total_output;
                    }
                }
                continue 'decode;
            }

            if cancelled.load(Ordering::Relaxed) {
                return Err(DecodeError::Cancelled);
            }
            let result = loop {
                if let Some(result) = pending.remove(&next_to_emit) {
                    break result;
                }
                match receiver.recv_timeout(COORDINATOR_RECV_TIMEOUT) {
                    Ok(result) if result.index < next_to_emit => {}
                    Ok(result) => {
                        pending.insert(result.index, result.result);
                    }
                    Err(RecvTimeoutError::Timeout) => {
                        if cancelled.load(Ordering::Relaxed) {
                            return Err(DecodeError::Cancelled);
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => {
                        return Err(DecodeError::WorkerPanicked);
                    }
                }
            };

            match result {
                Ok(chunk) if chunk.start_bit as u64 == current_bit => {
                    let reached_stream_end = enqueue_native_resolution(
                        chunk,
                        config,
                        &mut current_bit,
                        &mut window,
                        &mut prepared_total_output,
                        &mut next_resolve_sequence,
                        &mut outstanding_resolves,
                        &resolve_queue,
                        &available_resolve_tasks,
                        &work_signal,
                        &mut index_builder,
                    )?;
                    let resolve_window = adaptive_workers.current_limit().min(pipeline_capacity);
                    if outstanding_resolves >= resolve_window {
                        let parts = wait_for_resolved(
                            &resolve_receiver,
                            &mut resolve_pending,
                            next_resolve_to_emit,
                            cancelled,
                            current_bit,
                        )?;
                        emit_resolved_parts(
                            parts,
                            config,
                            output,
                            &mut accounting,
                            &mut total_output,
                            &mut index_builder,
                            &output_free_list,
                        )?;
                        next_resolve_to_emit += 1;
                        outstanding_resolves -= 1;
                    }
                    next_to_emit += 1;
                    let task_window = adaptive_workers.current_limit().min(pipeline_capacity);
                    let schedule_end = next_to_emit.saturating_add(task_window).min(tasks.len());
                    if next_to_schedule < schedule_end {
                        let (lock, signal) = &*work_signal;
                        let _guard = lock.lock().expect("estimated work mutex poisoned");
                        while next_to_schedule < schedule_end {
                            task_queue.push(next_to_schedule);
                            available_decode_tasks.fetch_add(1, Ordering::Release);
                            next_to_schedule += 1;
                        }
                        signal.notify_all();
                    }
                    if reached_stream_end {
                        footer_offset = Some(current_bit / 8);
                    } else if next_to_emit >= tasks.len() {
                        drain_native_resolutions(
                            &resolve_receiver,
                            &mut resolve_pending,
                            &mut next_resolve_to_emit,
                            &mut outstanding_resolves,
                            cancelled,
                            current_bit,
                            config,
                            output,
                            &mut accounting,
                            &mut total_output,
                            &mut index_builder,
                            &output_free_list,
                        )?;
                        footer_offset = Some(inflate_from_block(
                            source,
                            config,
                            cancelled,
                            output,
                            current_bit,
                            &window,
                            &mut accounting,
                            &mut total_output,
                            &mut index_builder,
                        )?);
                        prepared_total_output = total_output;
                    }
                }
                Ok(_) | Err(_) => {
                    drain_native_resolutions(
                        &resolve_receiver,
                        &mut resolve_pending,
                        &mut next_resolve_to_emit,
                        &mut outstanding_resolves,
                        cancelled,
                        current_bit,
                        config,
                        output,
                        &mut accounting,
                        &mut total_output,
                        &mut index_builder,
                        &output_free_list,
                    )?;
                    footer_offset = Some(inflate_from_block(
                        source,
                        config,
                        cancelled,
                        output,
                        current_bit,
                        &window,
                        &mut accounting,
                        &mut total_output,
                        &mut index_builder,
                    )?);
                    prepared_total_output = total_output;
                }
            }
        }

        stopped.store(true, Ordering::Relaxed);
        work_signal.1.notify_all();
        Ok(())
    });
    stopped.store(true, Ordering::Relaxed);
    work_signal.1.notify_all();
    scoped_result?;

    let final_length = source
        .len()
        .map_err(|error| DecodeError::input_io(frame_cursor.position(), error))?;
    if final_length != frame_cursor.length() {
        return Err(DecodeError::input_io(
            frame_cursor.position(),
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during decoding",
            ),
        ));
    }
    Ok(finish_report(
        config,
        frame_cursor.position(),
        total_output,
        member_count,
        index_builder,
    ))
}

#[derive(Clone, Copy, Debug)]
struct CompressedRange {
    start: u64,
    end: u64,
}

#[derive(Clone, Copy, Debug)]
struct BgzfRange {
    start: u64,
    deflate_start: u64,
    end: u64,
}

fn index_bgzf<R: ReadAt + ?Sized>(
    source: &R,
    page_size: usize,
) -> Result<Option<Vec<BgzfRange>>, DecodeError> {
    let mut cursor = SourceCursor::new(source, page_size)?;
    if cursor.at_end() {
        return Ok(None);
    }
    let first = parse_member_header(&mut cursor, true)?;
    let Some(first_size) = first.bgzf_block_size else {
        return Ok(None);
    };

    let mut ranges = Vec::new();
    let mut header = first;
    let mut block_size = first_size;
    loop {
        let end = header.start.checked_add(u64::from(block_size) + 1).ok_or(
            DecodeError::InvalidGzip {
                offset: header.start,
                reason: GzipErrorKind::Truncated,
            },
        )?;
        if end > cursor.length() || end < header.deflate_start.saturating_add(8) {
            return Ok(None);
        }
        ranges.push(BgzfRange {
            start: header.start,
            deflate_start: header.deflate_start,
            end,
        });
        if end == cursor.length() {
            break;
        }
        cursor.seek(end)?;
        header = match parse_member_header(&mut cursor, false) {
            Ok(header) => header,
            Err(DecodeError::InvalidGzip { .. }) => return Ok(None),
            Err(error) => return Err(error),
        };
        let Some(next_size) = header.bgzf_block_size else {
            return Ok(None);
        };
        block_size = next_size;
    }
    Ok(Some(ranges))
}

struct BgzfResult {
    index: usize,
    result: Result<BgzfDecoded, DecodeError>,
}

/// Decodes one BGZF block, appending to `output`. Returns the produced length.
///
/// Stream lifecycle (reset) and the one-shot inflate go through
/// [`InflateBackend`] monomorphized to [`RawInflater`]. The extra spare byte
/// beyond the footer ISIZE still distinguishes exact-size success from
/// output-buffer exhaustion (including empty EOF blocks).
fn decode_bgzf_block_into<R: ReadAt + ?Sized>(
    source: &R,
    range: BgzfRange,
    member: u64,
    compressed: &mut Vec<u8>,
    output: &mut Vec<u8>,
    inflater: &mut RawInflater,
    crc32_enabled: bool,
) -> Result<usize, DecodeError> {
    const MAX_BGZF_OUTPUT: usize = 64 * 1024;
    let compressed_length =
        usize::try_from(range.end.saturating_sub(range.start)).map_err(|_| {
            DecodeError::InvalidGzip {
                offset: range.start,
                reason: GzipErrorKind::Truncated,
            }
        })?;
    read_range_reuse(source, range.start, compressed_length, compressed)?;
    let local_deflate =
        usize::try_from(range.deflate_start.saturating_sub(range.start)).map_err(|_| {
            DecodeError::InvalidGzip {
                offset: range.start,
                reason: GzipErrorKind::Truncated,
            }
        })?;
    let footer_start = compressed_length
        .checked_sub(8)
        .ok_or(DecodeError::InvalidGzip {
            offset: range.start,
            reason: GzipErrorKind::Truncated,
        })?;
    let payload = compressed
        .get(local_deflate..footer_start)
        .ok_or(DecodeError::InvalidGzip {
            offset: range.start,
            reason: GzipErrorKind::Truncated,
        })?;
    let footer = &compressed[footer_start..];
    let expected_crc = u32::from_le_bytes(footer[..4].try_into().expect("four bytes"));
    let expected_size = u32::from_le_bytes(footer[4..].try_into().expect("four bytes"));
    let decoded_length = usize::try_from(expected_size).map_err(|_| DecodeError::InvalidGzip {
        offset: range.start,
        reason: GzipErrorKind::Truncated,
    })?;
    if decoded_length > MAX_BGZF_OUTPUT {
        return Err(DecodeError::InvalidGzip {
            offset: range.start,
            reason: GzipErrorKind::Truncated,
        });
    }

    let bit_offset = range.deflate_start.saturating_mul(8);
    InflateBackend::reset(inflater, bit_offset)?;
    let old_length = output.len();
    // Reserve ISIZE + 1 so Finish can report STREAM_END with exact produced
    // length instead of looking like buffer exhaustion.
    let writable = decoded_length.saturating_add(1);
    output.reserve(writable);
    let step = InflateBackend::inflate(inflater, payload, output, InflateFlush::Finish)?;
    if step.status != inflate_status::STREAM_END
        || step.consumed != payload.len()
        || step.produced != decoded_length
    {
        return Err(DecodeError::InvalidDeflate {
            bit_offset,
            reason: if step.status == inflate_status::DATA_ERROR {
                DeflateErrorKind::InvalidData
            } else {
                DeflateErrorKind::BackendStatus(step.status)
            },
        });
    }
    if crc32_enabled {
        let mut crc = Crc32::new();
        crc.update(&output[old_length..]);
        let actual_crc = crc.finish();
        if actual_crc != expected_crc {
            return Err(DecodeError::ChecksumMismatch {
                member,
                expected: expected_crc,
                actual: actual_crc,
            });
        }
    }
    Ok(step.produced)
}

struct BgzfDecoded {
    bytes: Vec<u8>,
    /// Uncompressed size of each block in this task, in emission order.
    block_sizes: Vec<usize>,
}

fn send_bgzf_result(
    sender: &mpsc::SyncSender<BgzfResult>,
    stopped: &AtomicBool,
    mut result: BgzfResult,
) {
    loop {
        if stopped.load(Ordering::Relaxed) {
            return;
        }
        match sender.try_send(result) {
            Ok(()) | Err(TrySendError::Disconnected(_)) => return,
            Err(TrySendError::Full(returned)) => {
                result = returned;
                thread::park_timeout(RESULT_CHANNEL_FULL_PARK);
            }
        }
    }
}

fn decode_bgzf_parallel<R, O>(
    source: &R,
    config: &Config,
    cancelled: &AtomicBool,
    output: &mut O,
    ranges: &[BgzfRange],
) -> Result<DecodeReport, DecodeError>
where
    R: ReadAt + ?Sized,
    O: Output,
{
    const BLOCKS_PER_TASK: usize = 8;
    let task_count = ranges.len().div_ceil(BLOCKS_PER_TASK);
    let worker_count = config.decoder_threads.min(task_count);
    let task_queue = Arc::new(Injector::new());
    let stopped = Arc::new(AtomicBool::new(false));
    let available_tasks = Arc::new(AtomicUsize::new(0));
    let work_signal = Arc::new((Mutex::new(()), Condvar::new()));
    let task_window = worker_count;
    let pending_limit = config.in_flight_chunks.max(worker_count);
    for index in 0..task_window {
        task_queue.push(index);
    }
    available_tasks.store(task_window, Ordering::Release);
    let (sender, receiver) = mpsc::sync_channel::<BgzfResult>(pending_limit);
    let mut next_to_schedule = task_window;
    let mut next_to_emit = 0;
    let mut running = task_window;
    let mut reordered = BTreeMap::new();
    let mut total_output = 0_u64;
    let mut index_builder = new_index_builder(config);

    let crc32_enabled = config.crc32_enabled;
    let scoped_result = thread::scope(|scope| -> Result<(), DecodeError> {
        let _stop_on_exit = StopGuard(&stopped);
        for _ in 0..worker_count {
            let queue = Arc::clone(&task_queue);
            let worker_stopped = Arc::clone(&stopped);
            let available_tasks = Arc::clone(&available_tasks);
            let work_signal = Arc::clone(&work_signal);
            let sender = sender.clone();
            scope.spawn(move || {
                // Compressed page and inflater reuse across blocks/tasks.
                // Decoded/block_sizes scratches keep capacity after failed tasks;
                // successful results transfer ownership to the coordinator.
                let mut compressed = Vec::new();
                let mut decoded_scratch = Vec::new();
                let mut block_sizes_scratch = Vec::new();
                let mut inflater = None;

                while !worker_stopped.load(Ordering::Relaxed) && !cancelled.load(Ordering::Relaxed)
                {
                    let index = loop {
                        match queue.steal() {
                            Steal::Success(index) => {
                                available_tasks.fetch_sub(1, Ordering::AcqRel);
                                break index;
                            }
                            Steal::Retry => std::hint::spin_loop(),
                            Steal::Empty => {
                                if worker_stopped.load(Ordering::Relaxed)
                                    || cancelled.load(Ordering::Relaxed)
                                {
                                    return;
                                }
                                let (lock, signal) = &*work_signal;
                                let guard = lock.lock().expect("BGZF work mutex poisoned");
                                let _ = signal
                                    .wait_timeout_while(guard, WORKER_IDLE_PARK, |_| {
                                        available_tasks.load(Ordering::Acquire) == 0
                                            && !worker_stopped.load(Ordering::Relaxed)
                                            && !cancelled.load(Ordering::Relaxed)
                                    })
                                    .expect("BGZF work mutex poisoned");
                            }
                        }
                    };
                    let first_block = index * BLOCKS_PER_TASK;
                    let past_last_block = (first_block + BLOCKS_PER_TASK).min(ranges.len());
                    let block_count = past_last_block - first_block;
                    decoded_scratch.clear();
                    block_sizes_scratch.clear();
                    let needed = block_count.saturating_mul(64 * 1024);
                    if decoded_scratch.capacity() < needed {
                        decoded_scratch.reserve(needed - decoded_scratch.capacity());
                    }
                    if block_sizes_scratch.capacity() < block_count {
                        block_sizes_scratch.reserve(block_count - block_sizes_scratch.capacity());
                    }
                    let result = (|| {
                        if inflater.is_none() {
                            inflater = Some(<RawInflater as InflateBackend>::create()?);
                        }
                        let inflater = inflater
                            .as_mut()
                            .expect("the inflater was initialized immediately above");
                        for (range_index, &range) in ranges
                            .iter()
                            .enumerate()
                            .take(past_last_block)
                            .skip(first_block)
                        {
                            if cancelled.load(Ordering::Relaxed) {
                                return Err(DecodeError::Cancelled);
                            }
                            let produced = decode_bgzf_block_into(
                                source,
                                range,
                                range_index as u64,
                                &mut compressed,
                                &mut decoded_scratch,
                                inflater,
                                crc32_enabled,
                            )?;
                            block_sizes_scratch.push(produced);
                        }
                        Ok(BgzfDecoded {
                            bytes: std::mem::take(&mut decoded_scratch),
                            block_sizes: std::mem::take(&mut block_sizes_scratch),
                        })
                    })();
                    send_bgzf_result(&sender, &worker_stopped, BgzfResult { index, result });
                }
            });
        }
        drop(sender);

        while next_to_emit < task_count {
            if cancelled.load(Ordering::Relaxed) {
                stopped.store(true, Ordering::Relaxed);
                return Err(DecodeError::Cancelled);
            }
            let result = match receiver.recv_timeout(COORDINATOR_RECV_TIMEOUT) {
                Ok(result) => {
                    running = running.saturating_sub(1);
                    result
                }
                Err(RecvTimeoutError::Timeout) => continue,
                Err(RecvTimeoutError::Disconnected) => {
                    stopped.store(true, Ordering::Relaxed);
                    return Err(DecodeError::WorkerPanicked);
                }
            };
            reordered.insert(result.index, result.result);

            while let Some(result) = reordered.remove(&next_to_emit) {
                let decoded = match result {
                    Ok(decoded) => decoded,
                    Err(error) => {
                        stopped.store(true, Ordering::Relaxed);
                        return Err(error);
                    }
                };
                let next_total = total_output.checked_add(decoded.bytes.len() as u64).ok_or(
                    DecodeError::OutputLimitExceeded {
                        limit: config.output_limit.unwrap_or(u64::MAX),
                    },
                )?;
                if config.output_limit.is_some_and(|limit| next_total > limit) {
                    stopped.store(true, Ordering::Relaxed);
                    return Err(DecodeError::OutputLimitExceeded {
                        limit: config.output_limit.expect("checked as some"),
                    });
                }
                // Each BGZF block is an independent inflate with empty history.
                if index_builder.tracks_output() {
                    let first_block = next_to_emit * BLOCKS_PER_TASK;
                    let mut offset = 0_usize;
                    for (i, &size) in decoded.block_sizes.iter().enumerate() {
                        let range = ranges[first_block + i];
                        if index_builder.enabled() {
                            index_builder
                                .force_checkpoint(range.deflate_start.saturating_mul(8), true);
                        }
                        index_builder.push_output(&decoded.bytes[offset..offset + size]);
                        offset += size;
                    }
                }
                total_output = next_total;
                if !decoded.bytes.is_empty() {
                    output.emit(decoded.bytes)?;
                }
                next_to_emit += 1;
            }

            while running < worker_count
                && next_to_schedule < task_count
                && reordered.len() < pending_limit
            {
                let (lock, signal) = &*work_signal;
                let _guard = lock.lock().expect("BGZF work mutex poisoned");
                task_queue.push(next_to_schedule);
                available_tasks.fetch_add(1, Ordering::Release);
                next_to_schedule += 1;
                running += 1;
                signal.notify_one();
            }
        }
        stopped.store(true, Ordering::Relaxed);
        Ok(())
    });
    stopped.store(true, Ordering::Relaxed);
    scoped_result?;

    let final_length = source.len().map_err(|error| {
        DecodeError::input_io(ranges.last().map_or(0, |range| range.end), error)
    })?;
    let compressed_bytes = ranges.last().map_or(0, |range| range.end);
    if final_length != compressed_bytes {
        return Err(DecodeError::input_io(
            compressed_bytes,
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "compressed source length changed during decoding",
            ),
        ));
    }

    Ok(finish_report(
        config,
        compressed_bytes,
        total_output,
        ranges.len() as u64,
        index_builder,
    ))
}

#[cfg(test)]
mod tests {
    use super::{
        INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES, MemberAccounting, MemberHeader, SourceCursor,
        Window, batch_independent_headers, find_gzip_magic, find_gzip_magic_scalar,
        independent_member_task_candidate_limit, inflate_tail, validate_footer,
    };

    fn header(start: u64) -> MemberHeader {
        MemberHeader {
            start,
            deflate_start: start + 10,
            bgzf_block_size: None,
        }
    }

    #[test]
    fn independent_member_tasks_are_bounded_by_span_and_candidate_count() {
        let headers = [
            header(0),
            header(100),
            header(999),
            header(1_000),
            header(1_100),
        ];
        let tasks =
            batch_independent_headers(&headers, 1_000, INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES);
        assert_eq!(tasks.len(), 2);
        assert_eq!(tasks[0].headers().len(), 3);
        assert_eq!(tasks[1].headers().len(), 2);

        let headers = (0..=INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES)
            .map(|offset| header(offset as u64))
            .collect::<Vec<_>>();
        let tasks =
            batch_independent_headers(&headers, usize::MAX, INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES);
        assert_eq!(tasks.len(), 2);
        assert_eq!(
            tasks[0].headers().len(),
            INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES
        );
        assert_eq!(tasks[1].headers().len(), 1);

        assert_eq!(
            independent_member_task_candidate_limit(4 * 1024, 1024 * 1024),
            INDEPENDENT_MEMBER_TASK_MAX_CANDIDATES
        );
        assert_eq!(
            independent_member_task_candidate_limit(4 * 1024 + 1, 1024 * 1024),
            1
        );
    }

    #[test]
    fn dispatched_gzip_magic_scan_matches_scalar_across_vector_edges() {
        let mut bytes = vec![0_u8; 128];
        for offset in [0, 1, 15, 31, 32, 63, 64, 95, 96, 127] {
            bytes[offset] = 0x1F;
        }
        let mut scalar = Vec::new();
        let mut dispatched = Vec::new();
        find_gzip_magic_scalar(&bytes, 97, &mut scalar);
        find_gzip_magic(&bytes, 97, &mut dispatched);
        assert_eq!(dispatched, scalar);
    }

    #[test]
    fn zlib_rs_tail_stops_at_exact_stored_block_boundary() {
        let encoded = [
            0, 5, 0, 250, 255, b'h', b'e', b'l', b'l', b'o', 1, 5, 0, 250, 255, b'w', b'o', b'r',
            b'l', b'd',
        ];
        let first_block_end = 10 * 8;
        let mut inflater = None;
        let mut output = Vec::new();
        let tail = inflate_tail(
            &encoded,
            0,
            first_block_end,
            &Window::empty(),
            1024,
            true,
            &mut inflater,
            &mut output,
        )
        .unwrap();
        assert_eq!(tail.output, b"hello");
        assert_eq!(tail.end_bit, first_block_end);
        assert!(!tail.reached_stream_end);
    }

    #[test]
    fn zlib_rs_tail_recognizes_final_z_block_boundary() {
        // Empty final fixed-Huffman block. Z_BLOCK reaches its end-of-block
        // before a subsequent inflate call would report Z_STREAM_END.
        let mut inflater = None;
        let mut output = Vec::new();
        let tail = inflate_tail(
            &[0x03, 0x00],
            0,
            1,
            &Window::empty(),
            1024,
            false,
            &mut inflater,
            &mut output,
        )
        .unwrap();
        assert!(tail.output.is_empty());
        assert_eq!(tail.end_bit, 16);
        assert!(tail.reached_stream_end);
    }

    #[test]
    fn footer_validation_recovers_backend_read_ahead() {
        let mut accounting = MemberAccounting::new();
        accounting.crc.update(b"hello");
        accounting.size = 5;

        let mut encoded = Vec::new();
        encoded.extend_from_slice(&accounting.crc.finish().to_le_bytes());
        encoded.extend_from_slice(&accounting.size.to_le_bytes());
        encoded.extend_from_slice(&[1, 2, 3, 4, 5, 6]);

        let mut cursor = SourceCursor::new(encoded.as_slice(), 4).unwrap();
        assert_eq!(
            validate_footer(&mut cursor, 6, 0, &accounting, true).unwrap(),
            0
        );
        assert_eq!(cursor.position(), 8);
    }

    #[test]
    fn zlib_adler_footer_validation_recovers_backend_read_ahead() {
        use super::validate_zlib_adler_footer;

        let mut accounting = MemberAccounting::new();
        accounting.adler.update(b"hello");

        let mut encoded = Vec::new();
        encoded.extend_from_slice(&accounting.adler.finish().to_be_bytes());
        encoded.extend_from_slice(&[1, 2, 3, 4, 5, 6]);

        let mut cursor = SourceCursor::new(encoded.as_slice(), 4).unwrap();
        assert_eq!(
            validate_zlib_adler_footer(&mut cursor, 6, 0, &accounting, true).unwrap(),
            0
        );
        assert_eq!(cursor.position(), 4);
    }
}
