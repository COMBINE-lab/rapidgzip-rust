//! Bounded structural analysis of DEFLATE streams.
//!
//! Analysis walks every block in causal order and records framing, Huffman,
//! symbol, and predecessor-window facts. It shares the decoder's container
//! parsers and native Huffman primitives, but intentionally remains
//! single-threaded: a block's history depends on every preceding block in its
//! stream.
//!
//! Memory does not scale with decompressed size. The walker keeps one bounded
//! linear history/output buffer, the bounded result collections, and the
//! caller-configured number of detailed back-reference records.

use crate::backend::resolve_cursor_format;
use crate::crc32::Crc32;
use crate::gzip::{
    DetailedMemberHeader, InputCursor, SourceCursor, StreamCursor, parse_member_header_detailed,
};
use crate::parallel::deflate::{
    self, DISTANCE_BASE, DISTANCE_EXTRA, DeflateBits, END_OF_BLOCK, Huffman, LENGTH_BASE,
    LENGTH_EXTRA, dynamic_trees_with_lengths, fixed_trees,
};
use crate::zlib::Adler32;
use crate::{
    AnalysisCounter, AnalysisErrorKind, AnalysisResource, DecodeError, DeflateErrorKind, Format,
    GzipErrorKind, ReadAt, ZlibErrorKind,
};
use std::io::Read;

const WINDOW_SIZE: usize = 32 * 1024;
const CHECKSUM_BUFFER_SIZE: usize = 8 * 1024;
const DEFAULT_MAXIMUM_STREAMS: usize = 100_000;
const DEFAULT_MAXIMUM_BLOCKS: usize = 100_000;
const DEFAULT_MAXIMUM_HEADER_BYTES: usize = 1024 * 1024;

/// Limits controlling the memory retained by structural analysis.
///
/// Defaults accept 100,000 streams, 100,000 blocks, and 1 MiB of optional gzip
/// metadata across the complete input. Individual back-reference records are omitted by
/// default; exact counts, length histograms, reach, and window coverage are
/// always collected regardless of that retention budget.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct AnalyzeOptions {
    maximum_streams: usize,
    maximum_blocks: usize,
    maximum_header_bytes: usize,
    maximum_retained_backreferences: usize,
}

impl Default for AnalyzeOptions {
    fn default() -> Self {
        Self {
            maximum_streams: DEFAULT_MAXIMUM_STREAMS,
            maximum_blocks: DEFAULT_MAXIMUM_BLOCKS,
            maximum_header_bytes: DEFAULT_MAXIMUM_HEADER_BYTES,
            maximum_retained_backreferences: 0,
        }
    }
}

impl AnalyzeOptions {
    /// Sets the maximum number of gzip members or other framed streams.
    #[must_use]
    pub const fn maximum_streams(mut self, maximum: usize) -> Self {
        self.maximum_streams = maximum;
        self
    }

    /// Sets the maximum number of DEFLATE blocks across the input.
    #[must_use]
    pub const fn maximum_blocks(mut self, maximum: usize) -> Self {
        self.maximum_blocks = maximum;
        self
    }

    /// Sets the input-wide maximum for retained optional gzip metadata.
    ///
    /// The budget covers the extra field, original name, and comment. Fixed
    /// header fields do not consume it. Extra fields, names, and comments from
    /// all members share this budget.
    #[must_use]
    pub const fn maximum_header_bytes(mut self, maximum: usize) -> Self {
        self.maximum_header_bytes = maximum;
        self
    }

    /// Sets the input-wide budget for detailed predecessor-window references.
    ///
    /// Summaries remain exact after this many records have been retained.
    /// Each affected block reports how many of its records were omitted.
    #[must_use]
    pub const fn maximum_retained_backreferences(mut self, maximum: usize) -> Self {
        self.maximum_retained_backreferences = maximum;
        self
    }

    /// Returns the configured stream limit.
    #[must_use]
    pub const fn stream_limit(self) -> usize {
        self.maximum_streams
    }

    /// Returns the configured block limit.
    #[must_use]
    pub const fn block_limit(self) -> usize {
        self.maximum_blocks
    }

    /// Returns the input-wide optional gzip-metadata limit.
    #[must_use]
    pub const fn header_byte_limit(self) -> usize {
        self.maximum_header_bytes
    }

    /// Returns the input-wide detailed-reference retention budget.
    #[must_use]
    pub const fn retained_backreference_limit(self) -> usize {
        self.maximum_retained_backreferences
    }
}

/// Encoding selected by one DEFLATE block.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum BlockType {
    /// Stored bytes with no Huffman coding.
    #[default]
    Uncompressed,
    /// RFC 1951's fixed literal/length and distance alphabets.
    FixedHuffman,
    /// Alphabets declared in the block header.
    DynamicHuffman,
}

/// Shape of one alphabet declared by a dynamic-Huffman block.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct AlphabetShape {
    /// Code length for every symbol in alphabet order, including zero lengths.
    pub code_lengths: Vec<u8>,
    /// Number of code lengths physically declared by the header.
    pub declared_count: usize,
}

impl AlphabetShape {
    /// Returns the number of symbols with non-zero code lengths.
    #[must_use]
    pub fn used_count(&self) -> usize {
        self.code_lengths
            .iter()
            .filter(|&&length| length != 0)
            .count()
    }

    /// Returns the shortest and longest non-zero code lengths.
    #[must_use]
    pub fn length_range(&self) -> Option<(u8, u8)> {
        let mut lengths = self
            .code_lengths
            .iter()
            .copied()
            .filter(|&length| length != 0);
        let first = lengths.next()?;
        Some(lengths.fold((first, first), |(minimum, maximum), length| {
            (minimum.min(length), maximum.max(length))
        }))
    }

    /// Returns symbol counts grouped by code length, shortest first.
    #[must_use]
    pub fn counts_by_length(&self) -> Vec<(u8, usize)> {
        let mut counts = [0_usize; 16];
        for &length in &self.code_lengths {
            counts[usize::from(length).min(15)] += 1;
        }
        counts
            .into_iter()
            .enumerate()
            .filter(|&(_, count)| count != 0)
            .map(|(length, count)| (length as u8, count))
            .collect()
    }
}

/// One retained reference into the predecessor window.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Backreference {
    /// Distance before the current block's first output byte.
    pub distance: u16,
    /// Reference length as reported by rapidgzip, capped at the copy distance.
    pub length: u16,
}

/// Structural facts for one DEFLATE block.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct BlockAnalysis {
    /// Zero-based index of the containing stream.
    pub stream_index: u64,
    /// Zero-based index within the containing stream.
    pub index_in_stream: u64,
    /// Whether this is the stream's final block.
    pub is_final: bool,
    /// Block encoding.
    pub block_type: BlockType,
    /// Absolute compressed bit offset of the three-bit block header.
    pub compressed_offset_in_bits: u64,
    /// Absolute compressed bit offset after stored-length or Huffman metadata.
    pub compressed_data_offset_in_bits: u64,
    /// Absolute decompressed byte offset of the block's first output byte.
    pub uncompressed_offset_in_bytes: u64,
    /// Exact compressed block size, including its header.
    pub compressed_size_in_bits: u64,
    /// Exact number of output bytes produced by the block.
    pub uncompressed_size_in_bytes: u64,
    /// Dynamic precode alphabet.
    pub precode: Option<AlphabetShape>,
    /// Dynamic distance alphabet.
    pub distance: Option<AlphabetShape>,
    /// Dynamic literal/length alphabet.
    pub literal: Option<AlphabetShape>,
    /// Literal symbols decoded from the block.
    pub literal_symbols: u64,
    /// Length/distance symbols decoded from the block.
    pub backreference_symbols: u64,
    /// Output bytes copied by length/distance symbols.
    pub copied_bytes: u64,
    /// Farthest reach before this block's first output byte.
    pub farthest_backreference: u64,
    /// References whose source begins before this block's output.
    pub window_backreference_count: u64,
    /// Deterministic interval-union count for those references.
    pub merged_window_backreference_count: u64,
    /// Covered predecessor-window bytes for blocks producing at least 32 KiB.
    pub used_window_symbols: Option<u64>,
    /// Detailed references retained within [`AnalyzeOptions`]' global budget.
    pub retained_backreferences: Vec<Backreference>,
    /// Detailed references omitted after the global budget was exhausted.
    pub omitted_backreference_count: u64,
}

/// Complete gzip header metadata for one member.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct GzipHeaderFields {
    /// RFC 1952 flag byte.
    pub flags: u8,
    /// Modification time, or zero when unspecified.
    pub modification_time: u32,
    /// Compressor hint byte (`XFL`).
    pub extra_flags: u8,
    /// Originating operating-system code.
    pub operating_system: u8,
    /// Original file name without its terminating zero.
    pub file_name: Option<Vec<u8>>,
    /// Comment without its terminating zero.
    pub comment: Option<Vec<u8>>,
    /// Complete extra-field payload.
    pub extra: Option<Vec<u8>>,
    /// Stored and verified optional header CRC16.
    pub header_crc16: Option<u16>,
    /// BGZF `BC` block-size value when a well-formed subfield was present.
    pub bgzf_block_size: Option<u16>,
}

/// RFC 1950 header fields for one zlib stream.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub struct ZlibHeaderFields {
    /// Declared LZ77 window size in bytes.
    pub window_size: u32,
    /// Two-bit compressor level hint.
    pub compression_level: u8,
    /// Preset dictionary identifier. This decoder currently rejects FDICT, so
    /// accepted analyses report `None`.
    pub dictionary_id: Option<u32>,
}

/// Container header beginning one analyzed stream.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StreamHeader {
    /// gzip member header.
    Gzip(GzipHeaderFields),
    /// zlib header.
    Zlib(ZlibHeaderFields),
    /// Raw DEFLATE has no header.
    RawDeflate,
}

/// Verified container trailer ending one analyzed stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum StreamFooter {
    /// gzip CRC32 and modulo-2^32 output size.
    Gzip {
        /// Stored and verified checksum.
        crc32: u32,
        /// Stored and verified modulo output size.
        uncompressed_size: u32,
    },
    /// zlib Adler-32.
    Zlib {
        /// Stored and verified checksum.
        adler32: u32,
    },
    /// Raw DEFLATE has no trailer.
    None,
}

/// One gzip member, zlib stream, or raw-DEFLATE stream.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct StreamAnalysis {
    /// Zero-based stream index.
    pub index: u64,
    /// Parsed container header.
    pub header: StreamHeader,
    /// Absolute compressed bit offset of the container header.
    pub header_offset_in_bits: u64,
    /// Absolute compressed bit offset of the first DEFLATE block.
    pub deflate_offset_in_bits: u64,
    /// Absolute compressed bit offset of the container trailer.
    pub footer_offset_in_bits: u64,
    /// Absolute decompressed byte offset where this stream begins.
    pub uncompressed_offset_in_bytes: u64,
    /// Verified container trailer.
    pub footer: StreamFooter,
    /// Container size including header and trailer.
    pub compressed_size_in_bits: u64,
    /// Output bytes produced by this stream.
    pub uncompressed_size_in_bytes: u64,
    /// Index of this stream's first entry in [`Analysis::blocks`].
    pub first_block_index: usize,
    /// Number of entries belonging to this stream.
    pub block_count: usize,
}

/// Complete deterministic structural analysis of an input.
#[derive(Clone, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct Analysis {
    /// Resolved container format.
    pub format: Format,
    /// Streams in input order.
    pub streams: Vec<StreamAnalysis>,
    /// Blocks in input order across every stream.
    pub blocks: Vec<BlockAnalysis>,
    /// Total decompressed size.
    pub uncompressed_size_in_bytes: u64,
    /// Total consumed compressed size.
    pub compressed_size_in_bytes: u64,
    /// Exact input-wide counts indexed by reference length from 0 through 258.
    pub backreference_length_counts: [u64; 259],
}

impl Analysis {
    /// Returns non-zero block-type counts in stable stored/fixed/dynamic order.
    #[must_use]
    pub fn block_type_counts(&self) -> Vec<(BlockType, u64)> {
        let mut counts = [0_u64; 3];
        for block in &self.blocks {
            let index = match block.block_type {
                BlockType::Uncompressed => 0,
                BlockType::FixedHuffman => 1,
                BlockType::DynamicHuffman => 2,
            };
            counts[index] += 1;
        }
        [
            BlockType::Uncompressed,
            BlockType::FixedHuffman,
            BlockType::DynamicHuffman,
        ]
        .into_iter()
        .zip(counts)
        .filter(|&(_, count)| count != 0)
        .collect()
    }

    /// Returns whether every detailed predecessor-window reference was kept.
    #[must_use]
    pub fn has_complete_backreference_details(&self) -> bool {
        self.blocks
            .iter()
            .all(|block| block.omitted_backreference_count == 0)
    }
}

struct AnalysisCursor<C> {
    inner: C,
    bits: u64,
    buffered_bits: u8,
    bit_position: u64,
    scratch: [u8; 8],
    serving_buffer: bool,
    position_overflowed: bool,
}

impl<C: InputCursor> AnalysisCursor<C> {
    fn new(inner: C) -> Result<Self, DecodeError> {
        let bit_position = inner
            .position()
            .checked_mul(8)
            .ok_or(DecodeError::Analysis {
                reason: AnalysisErrorKind::CounterOverflow {
                    counter: AnalysisCounter::CompressedBits,
                },
            })?;
        Ok(Self {
            inner,
            bits: 0,
            buffered_bits: 0,
            bit_position,
            scratch: [0; 8],
            serving_buffer: false,
            position_overflowed: false,
        })
    }

    const fn bit_position(&self) -> u64 {
        self.bit_position
    }

    #[cold]
    #[inline(never)]
    fn refill_bits(&mut self, wanted: u8) -> Result<(), DecodeError> {
        self.check_position()?;
        while self.buffered_bits < wanted {
            let available = self.inner.available()?;
            let capacity = usize::from((56 - self.buffered_bits) / 8);
            let count = capacity.min(available.len());
            if count == 0 {
                break;
            }
            let word = if available.len() >= std::mem::size_of::<u64>() {
                // SAFETY: `available.len() >= 8` proves that the unaligned
                // eight-byte load is wholly inside the initialized slice.
                // `read_unaligned` has no alignment requirement, and `to_le`
                // normalizes the word before DEFLATE's least-significant-bit
                // extraction.
                unsafe { available.as_ptr().cast::<u64>().read_unaligned() }.to_le()
            } else {
                let mut tail = [0_u8; 8];
                tail[..count].copy_from_slice(&available[..count]);
                u64::from_le_bytes(tail)
            };
            let refill_bits = count * 8;
            let mask = (1_u64 << refill_bits) - 1;
            self.bits |= (word & mask) << self.buffered_bits;
            self.buffered_bits += u8::try_from(count * 8).expect("at most seven bytes fit");
            self.inner.advance(count);
        }
        Ok(())
    }

    #[inline(always)]
    fn fill_to(&mut self, wanted: u8) -> Result<(), DecodeError> {
        if self.buffered_bits >= wanted && !self.position_overflowed {
            Ok(())
        } else {
            self.refill_bits(wanted)
        }
    }

    fn check_position(&self) -> Result<(), DecodeError> {
        if self.position_overflowed {
            Err(DecodeError::Analysis {
                reason: AnalysisErrorKind::CounterOverflow {
                    counter: AnalysisCounter::CompressedBits,
                },
            })
        } else {
            Ok(())
        }
    }

    #[inline(always)]
    fn consume_buffered(&mut self, count: u8) -> Result<(), DecodeError> {
        if count > self.buffered_bits {
            return Err(self.deflate_error(deflate::Error::UnexpectedEof));
        }
        self.bits >>= count;
        self.buffered_bits -= count;
        self.bit_position =
            self.bit_position
                .checked_add(u64::from(count))
                .ok_or(DecodeError::Analysis {
                    reason: AnalysisErrorKind::CounterOverflow {
                        counter: AnalysisCounter::CompressedBits,
                    },
                })?;
        Ok(())
    }

    fn align_to_byte(&mut self) -> Result<(), DecodeError> {
        let padding = ((8 - self.bit_position % 8) % 8) as u8;
        if padding != 0 {
            self.read_bits(padding)?;
        }
        Ok(())
    }

    fn deflate_error(&self, error: deflate::Error) -> DecodeError {
        let reason = match error {
            deflate::Error::UnexpectedEof => DeflateErrorKind::Truncated,
            _ => DeflateErrorKind::InvalidData,
        };
        DecodeError::InvalidDeflate {
            bit_offset: self.bit_position,
            reason,
        }
    }
}

impl<C: InputCursor> DeflateBits for AnalysisCursor<C> {
    type Error = DecodeError;

    #[inline(always)]
    fn read_bits(&mut self, count: u8) -> Result<u32, Self::Error> {
        debug_assert!(count <= 24);
        self.fill_to(count)?;
        if self.buffered_bits < count {
            return Err(self.deflate_error(deflate::Error::UnexpectedEof));
        }
        let mask = if count == 0 { 0 } else { (1_u64 << count) - 1 };
        let value = (self.bits & mask) as u32;
        self.consume_buffered(count)?;
        Ok(value)
    }

    #[inline(always)]
    fn peek_bits_padded(&mut self, count: u8) -> Result<(u32, u8), Self::Error> {
        self.fill_to(count)?;
        let available = self.buffered_bits.min(count);
        let mask = if count == 0 { 0 } else { (1_u64 << count) - 1 };
        Ok(((self.bits & mask) as u32, available))
    }

    #[inline(always)]
    fn advance_bits(&mut self, count: u8) -> Result<(), Self::Error> {
        self.consume_buffered(count)
    }

    #[inline(always)]
    fn error(&self, error: deflate::Error) -> Self::Error {
        self.deflate_error(error)
    }
}

impl<C: InputCursor> InputCursor for AnalysisCursor<C> {
    fn position(&self) -> u64 {
        self.bit_position / 8
    }

    fn is_at_end(&mut self) -> Result<bool, DecodeError> {
        self.check_position()?;
        if self.buffered_bits >= 8 {
            return Ok(false);
        }
        self.inner.is_at_end()
    }

    fn available(&mut self) -> Result<&[u8], DecodeError> {
        self.check_position()?;
        debug_assert_eq!(self.bit_position % 8, 0);
        if self.buffered_bits >= 8 {
            let byte_count = usize::from(self.buffered_bits / 8);
            for (index, byte) in self.scratch[..byte_count].iter_mut().enumerate() {
                *byte = (self.bits >> (index * 8)) as u8;
            }
            self.serving_buffer = true;
            return Ok(&self.scratch[..byte_count]);
        }
        self.serving_buffer = false;
        self.inner.available()
    }

    fn advance(&mut self, count: usize) {
        if self.serving_buffer {
            let bits = u8::try_from(count.saturating_mul(8))
                .expect("the analysis scratch buffer contains at most eight bytes");
            debug_assert!(bits <= self.buffered_bits);
            self.bits >>= bits;
            self.buffered_bits -= bits;
        } else {
            self.inner.advance(count);
        }
        let additional = u64::try_from(count)
            .ok()
            .and_then(|count| count.checked_mul(8));
        match additional.and_then(|additional| self.bit_position.checked_add(additional)) {
            Some(position) => self.bit_position = position,
            None => self.position_overflowed = true,
        }
        self.serving_buffer = false;
    }

    fn verify_source_unchanged(&self) -> Result<(), DecodeError> {
        self.check_position()?;
        self.inner.verify_source_unchanged()
    }

    fn peek_two(&mut self) -> Result<Option<[u8; 2]>, DecodeError> {
        self.check_position()?;
        debug_assert_eq!(self.bit_position % 8, 0);
        self.fill_to(16)?;
        Ok((self.buffered_bits >= 16).then_some([self.bits as u8, (self.bits >> 8) as u8]))
    }
}

enum StreamChecksum {
    Gzip(Crc32),
    Zlib(Adler32),
    None,
}

struct OutputState {
    // The prefix is predecessor history and the tail is new output awaiting a
    // checksum update. Keeping both in one linear allocation lets literals and
    // matches be written exactly once. When the tail fills, the checksum is
    // advanced and the newest 32 KiB is compacted back to the prefix.
    bytes: [u8; WINDOW_SIZE + CHECKSUM_BUFFER_SIZE],
    history_length: usize,
    write_position: usize,
    checksum_start: usize,
    maximum_distance: usize,
    checksum: StreamChecksum,
    stream_size: u64,
}

impl OutputState {
    fn new(maximum_distance: usize, checksum: StreamChecksum) -> Self {
        Self {
            bytes: [0; WINDOW_SIZE + CHECKSUM_BUFFER_SIZE],
            history_length: 0,
            write_position: 0,
            checksum_start: 0,
            maximum_distance,
            checksum,
            stream_size: 0,
        }
    }

    #[inline(always)]
    fn prepare_output<const CHECK_CONFIGURED_LIMITS: bool>(
        &mut self,
        total_output: &mut u64,
        additional: usize,
        config: &crate::config::Config,
    ) -> Result<(), DecodeError> {
        let actual = if CHECK_CONFIGURED_LIMITS {
            config.checked_output_total(*total_output, additional)?
        } else {
            total_output
                .checked_add(additional as u64)
                .ok_or(DecodeError::OutputLimitExceeded { limit: u64::MAX })?
        };
        *total_output = actual;
        // A stream's output is a subset of total output. The checked total
        // addition above therefore proves that this addition cannot overflow.
        self.stream_size += additional as u64;
        Ok(())
    }

    #[inline(always)]
    fn ensure_space(&mut self, additional: usize) {
        debug_assert!(additional <= 258);
        if self.write_position + additional > self.bytes.len() {
            self.roll_buffer();
        }
        debug_assert!(self.write_position + additional <= self.bytes.len());
    }

    #[inline(always)]
    fn emit(&mut self, byte: u8) {
        self.ensure_space(1);
        self.bytes[self.write_position] = byte;
        self.write_position += 1;
        self.history_length = (self.history_length + 1).min(WINDOW_SIZE);
    }

    fn append_bytes(&mut self, bytes: &[u8]) {
        let mut offset = 0;
        while offset < bytes.len() {
            if self.write_position == self.bytes.len() {
                self.roll_buffer();
            }
            let count = (bytes.len() - offset).min(self.bytes.len() - self.write_position);
            self.bytes[self.write_position..self.write_position + count]
                .copy_from_slice(&bytes[offset..offset + count]);
            self.write_position += count;
            self.history_length = (self.history_length + count).min(WINDOW_SIZE);
            offset += count;
        }
    }

    #[inline]
    fn copy_match(&mut self, distance: usize, length: usize) {
        debug_assert!(distance != 0);
        debug_assert!(distance <= self.maximum_distance);
        debug_assert!(distance <= self.history_length);
        self.ensure_space(length);
        let destination = self.write_position;
        let source = destination - distance;
        if distance == 1 {
            let byte = self.bytes[source];
            self.bytes[destination..destination + length].fill(byte);
        } else if length <= distance {
            self.bytes.copy_within(source..source + length, destination);
        } else {
            self.bytes
                .copy_within(source..source + distance, destination);
            let mut produced = distance;
            while produced < length {
                let count = produced.min(length - produced);
                self.bytes
                    .copy_within(destination..destination + count, destination + produced);
                produced += count;
            }
        }
        self.write_position += length;
        self.history_length = (self.history_length + length).min(WINDOW_SIZE);
    }

    fn flush_checksum(&mut self) {
        let bytes = &self.bytes[self.checksum_start..self.write_position];
        match &mut self.checksum {
            StreamChecksum::Gzip(checksum) => checksum.update(bytes),
            StreamChecksum::Zlib(checksum) => checksum.update(bytes),
            StreamChecksum::None => {}
        }
        self.checksum_start = self.write_position;
    }

    #[cold]
    fn roll_buffer(&mut self) {
        self.flush_checksum();
        let history_start = self.write_position - self.history_length;
        self.bytes
            .copy_within(history_start..self.write_position, 0);
        self.write_position = self.history_length;
        self.checksum_start = self.write_position;
    }

    fn finish_checksum(mut self) -> (StreamChecksum, u64) {
        self.flush_checksum();
        (self.checksum, self.stream_size)
    }
}

struct AnalyzeState<'a> {
    config: &'a crate::config::Config,
    options: AnalyzeOptions,
    analysis: Analysis,
    total_output: u64,
    remaining_backreferences: usize,
    retained_header_bytes: usize,
}

impl AnalyzeState<'_> {
    fn reserve_stream(&mut self) -> Result<(), DecodeError> {
        reserve_item(
            &mut self.analysis.streams,
            self.options.maximum_streams,
            AnalysisResource::Streams,
        )
    }

    fn reserve_block(&mut self) -> Result<(), DecodeError> {
        reserve_item(
            &mut self.analysis.blocks,
            self.options.maximum_blocks,
            AnalysisResource::Blocks,
        )
    }
}

fn reserve_item<T>(
    values: &mut Vec<T>,
    limit: usize,
    resource: AnalysisResource,
) -> Result<(), DecodeError> {
    if values.len() >= limit {
        return Err(DecodeError::Analysis {
            reason: AnalysisErrorKind::ResourceLimit { resource, limit },
        });
    }
    values.try_reserve(1).map_err(|_| DecodeError::Analysis {
        reason: AnalysisErrorKind::AllocationFailed {
            resource,
            additional: 1,
        },
    })
}

fn alphabet_shape(bytes: &[u8], declared_count: usize) -> Result<AlphabetShape, DecodeError> {
    let mut code_lengths = Vec::new();
    code_lengths
        .try_reserve_exact(bytes.len())
        .map_err(|_| DecodeError::Analysis {
            reason: AnalysisErrorKind::AllocationFailed {
                resource: AnalysisResource::AlphabetCodeLengths,
                additional: bytes.len(),
            },
        })?;
    code_lengths.extend_from_slice(bytes);
    Ok(AlphabetShape {
        code_lengths,
        declared_count,
    })
}

fn record_window_reference(
    block: &mut BlockAnalysis,
    coverage: &mut [bool; WINDOW_SIZE],
    global_lengths: &mut [u64; 259],
    remaining: &mut usize,
    distance: usize,
    length: usize,
) -> Result<(), DecodeError> {
    let reference = Backreference {
        distance: u16::try_from(distance).expect("DEFLATE distances fit u16"),
        length: u16::try_from(length).expect("DEFLATE lengths fit u16"),
    };
    // Every count below is bounded by the number of decoded output symbols.
    // `prepare_output` has already proved that total output fits in `u64`.
    block.window_backreference_count += 1;
    block.farthest_backreference = block.farthest_backreference.max(distance as u64);
    let length_count = global_lengths
        .get_mut(length)
        .expect("DEFLATE reference lengths do not exceed 258");
    *length_count += 1;

    let begin = WINDOW_SIZE - distance;
    let end = begin.saturating_add(length).min(WINDOW_SIZE);
    coverage[begin..end].fill(true);

    if *remaining == 0 {
        block.omitted_backreference_count += 1;
        return Ok(());
    }
    block
        .retained_backreferences
        .try_reserve(1)
        .map_err(|_| DecodeError::Analysis {
            reason: AnalysisErrorKind::AllocationFailed {
                resource: AnalysisResource::Backreferences,
                additional: 1,
            },
        })?;
    block.retained_backreferences.push(reference);
    *remaining -= 1;
    Ok(())
}

fn coverage_groups(coverage: &[bool; WINDOW_SIZE]) -> u64 {
    coverage
        .iter()
        .copied()
        .fold((false, 0_u64), |(inside, groups), covered| {
            (covered, groups + u64::from(covered && !inside))
        })
        .1
}

fn analyze_compressed_symbols<C: InputCursor, const CHECK_CONFIGURED_LIMITS: bool>(
    cursor: &mut AnalysisCursor<C>,
    trees: (&Huffman, &Huffman),
    output: &mut OutputState,
    state: &mut AnalyzeState<'_>,
    block: &mut BlockAnalysis,
    block_output_start: u64,
    coverage: &mut [bool; WINDOW_SIZE],
) -> Result<(), DecodeError> {
    let (literal_tree, distance_tree) = trees;
    loop {
        match literal_tree.decode(cursor)? {
            symbol @ 0..=255 => {
                output.prepare_output::<CHECK_CONFIGURED_LIMITS>(
                    &mut state.total_output,
                    1,
                    state.config,
                )?;
                // This is bounded by total output, whose increment was checked.
                block.literal_symbols += 1;
                output.emit(symbol as u8);
            }
            END_OF_BLOCK => return Ok(()),
            symbol @ 257..=285 => {
                let length_index = symbol - 257;
                let length = LENGTH_BASE[length_index]
                    + cursor.read_bits(LENGTH_EXTRA[length_index])? as usize;
                let distance_symbol = distance_tree.decode(cursor)?;
                if distance_symbol >= DISTANCE_BASE.len() {
                    return Err(cursor.deflate_error(deflate::Error::InvalidDistance));
                }
                let distance = DISTANCE_BASE[distance_symbol]
                    + cursor.read_bits(DISTANCE_EXTRA[distance_symbol])? as usize;
                if distance == 0
                    || distance > output.maximum_distance
                    || distance > output.history_length
                {
                    return Err(cursor.deflate_error(deflate::Error::InvalidDistance));
                }
                let position_in_block = state.total_output - block_output_start;
                output.prepare_output::<CHECK_CONFIGURED_LIMITS>(
                    &mut state.total_output,
                    length,
                    state.config,
                )?;
                // Both counters are bounded by checked total output.
                block.backreference_symbols += 1;
                block.copied_bytes += length as u64;
                if distance as u64 > position_in_block {
                    let preceding_distance = distance as u64 - position_in_block;
                    let reported_length = length.min(distance);
                    record_window_reference(
                        block,
                        coverage,
                        &mut state.analysis.backreference_length_counts,
                        &mut state.remaining_backreferences,
                        preceding_distance as usize,
                        reported_length,
                    )?;
                }

                output.copy_match(distance, length);
            }
            _ => return Err(cursor.deflate_error(deflate::Error::InvalidSymbol)),
        }
    }
}

fn analyze_block<C: InputCursor, const CHECK_CONFIGURED_LIMITS: bool>(
    cursor: &mut AnalysisCursor<C>,
    output: &mut OutputState,
    state: &mut AnalyzeState<'_>,
    stream_index: u64,
    index_in_stream: u64,
) -> Result<BlockAnalysis, DecodeError> {
    let block_start = cursor.bit_position();
    let block_output_start = state.total_output;
    let is_final = cursor.read_bits(1)? != 0;
    let encoding = cursor.read_bits(2)?;
    let mut block = BlockAnalysis {
        stream_index,
        index_in_stream,
        is_final,
        compressed_offset_in_bits: block_start,
        uncompressed_offset_in_bytes: block_output_start,
        ..BlockAnalysis::default()
    };
    let mut coverage = [false; WINDOW_SIZE];

    match encoding {
        0 => {
            block.block_type = BlockType::Uncompressed;
            cursor.align_to_byte()?;
            let length = cursor.read_bits(16)? as u16;
            let complement = cursor.read_bits(16)? as u16;
            if length != !complement {
                return Err(cursor.deflate_error(deflate::Error::InvalidStoredLength));
            }
            block.compressed_data_offset_in_bits = cursor.bit_position();
            output.prepare_output::<CHECK_CONFIGURED_LIMITS>(
                &mut state.total_output,
                usize::from(length),
                state.config,
            )?;
            let mut remaining = usize::from(length);
            while remaining != 0 {
                let available = cursor.available()?;
                if available.is_empty() {
                    return Err(cursor.deflate_error(deflate::Error::UnexpectedEof));
                }
                let count = remaining.min(available.len());
                output.append_bytes(&available[..count]);
                cursor.advance(count);
                remaining -= count;
            }
        }
        1 => {
            block.block_type = BlockType::FixedHuffman;
            block.compressed_data_offset_in_bits = cursor.bit_position();
            let (literal, distance) = fixed_trees();
            analyze_compressed_symbols::<_, CHECK_CONFIGURED_LIMITS>(
                cursor,
                (literal, distance),
                output,
                state,
                &mut block,
                block_output_start,
                &mut coverage,
            )?;
        }
        2 => {
            block.block_type = BlockType::DynamicHuffman;
            let (literal_tree, distance_tree, declared) = dynamic_trees_with_lengths(cursor)?;
            block.compressed_data_offset_in_bits = cursor.bit_position();
            block.precode = Some(alphabet_shape(&declared.precode, declared.precode_count)?);
            let literal_end = declared.literal_count;
            let distance_end = literal_end + declared.distance_count;
            block.literal = Some(alphabet_shape(
                &declared.lengths[..literal_end],
                declared.literal_count,
            )?);
            block.distance = Some(alphabet_shape(
                &declared.lengths[literal_end..distance_end],
                declared.distance_count,
            )?);
            analyze_compressed_symbols::<_, CHECK_CONFIGURED_LIMITS>(
                cursor,
                (&literal_tree, &distance_tree),
                output,
                state,
                &mut block,
                block_output_start,
                &mut coverage,
            )?;
        }
        _ => return Err(cursor.deflate_error(deflate::Error::InvalidBlockType)),
    }
    cursor.check_position()?;

    block.compressed_size_in_bits =
        cursor
            .bit_position()
            .checked_sub(block_start)
            .ok_or(DecodeError::Analysis {
                reason: AnalysisErrorKind::CounterOverflow {
                    counter: AnalysisCounter::CompressedBits,
                },
            })?;
    block.uncompressed_size_in_bytes =
        state
            .total_output
            .checked_sub(block_output_start)
            .ok_or(DecodeError::Analysis {
                reason: AnalysisErrorKind::CounterOverflow {
                    counter: AnalysisCounter::DecompressedBytes,
                },
            })?;
    block.merged_window_backreference_count = coverage_groups(&coverage);
    if block.uncompressed_size_in_bytes >= WINDOW_SIZE as u64 {
        block.used_window_symbols = Some(coverage.iter().filter(|&&used| used).count() as u64);
    }
    Ok(block)
}

fn gzip_header(details: DetailedMemberHeader) -> StreamHeader {
    StreamHeader::Gzip(GzipHeaderFields {
        flags: details.flags,
        modification_time: details.modification_time,
        extra_flags: details.extra_flags,
        operating_system: details.operating_system,
        file_name: details.file_name,
        comment: details.comment,
        extra: details.extra,
        header_crc16: details.header_crc16,
        bgzf_block_size: details.member.bgzf_block_size,
    })
}

fn read_zlib_header<C: InputCursor>(
    cursor: &mut AnalysisCursor<C>,
) -> Result<(StreamHeader, usize), DecodeError> {
    let offset = cursor.position();
    let mut bytes = [0_u8; 2];
    for byte in &mut bytes {
        let Some(&value) = cursor.available()?.first() else {
            return Err(DecodeError::InvalidZlib {
                offset,
                reason: ZlibErrorKind::Truncated,
            });
        };
        cursor.advance(1);
        *byte = value;
    }
    let window_bits = crate::zlib::parse_header(bytes, offset)?;
    Ok((
        StreamHeader::Zlib(ZlibHeaderFields {
            window_size: 1_u32 << window_bits,
            compression_level: bytes[1] >> 6,
            dictionary_id: None,
        }),
        1_usize << window_bits,
    ))
}

fn read_zlib_footer<C: InputCursor>(
    cursor: &mut AnalysisCursor<C>,
) -> Result<[u8; 4], DecodeError> {
    let offset = cursor.position();
    let mut bytes = [0_u8; 4];
    for byte in &mut bytes {
        let Some(&value) = cursor.available()?.first() else {
            return Err(DecodeError::InvalidZlib {
                offset,
                reason: ZlibErrorKind::Truncated,
            });
        };
        cursor.advance(1);
        *byte = value;
    }
    Ok(bytes)
}

fn analyze_one_stream<C: InputCursor, const CHECK_CONFIGURED_LIMITS: bool>(
    cursor: &mut AnalysisCursor<C>,
    state: &mut AnalyzeState<'_>,
    format: Format,
) -> Result<(), DecodeError> {
    state.reserve_stream()?;
    let stream_index = state.analysis.streams.len() as u64;
    let header_offset = cursor.bit_position();
    let stream_output_start = state.total_output;
    let first_block_index = state.analysis.blocks.len();

    let (header, maximum_distance, checksum) = match format {
        Format::Gzip => {
            let details = parse_member_header_detailed(
                cursor,
                stream_index == 0,
                state.options.maximum_header_bytes,
                state.retained_header_bytes,
            )?;
            state.retained_header_bytes = details.retained_metadata_bytes;
            (
                gzip_header(details),
                WINDOW_SIZE,
                StreamChecksum::Gzip(Crc32::new()),
            )
        }
        Format::Zlib => {
            let (header, maximum_distance) = read_zlib_header(cursor)?;
            (
                header,
                maximum_distance,
                StreamChecksum::Zlib(Adler32::new()),
            )
        }
        Format::RawDeflate => (StreamHeader::RawDeflate, WINDOW_SIZE, StreamChecksum::None),
    };
    let deflate_offset = cursor.bit_position();
    let mut output = OutputState::new(maximum_distance, checksum);
    let mut block_index = 0_u64;
    loop {
        state.reserve_block()?;
        let block = analyze_block::<_, CHECK_CONFIGURED_LIMITS>(
            cursor,
            &mut output,
            state,
            stream_index,
            block_index,
        )?;
        let final_block = block.is_final;
        state.analysis.blocks.push(block);
        block_index = block_index.checked_add(1).ok_or(DecodeError::Analysis {
            reason: AnalysisErrorKind::CounterOverflow {
                counter: AnalysisCounter::StructuralItems,
            },
        })?;
        if final_block {
            break;
        }
    }
    cursor.align_to_byte()?;
    let footer_offset = cursor.bit_position();
    let (checksum, stream_size) = output.finish_checksum();

    let footer = match (format, checksum) {
        (Format::Gzip, StreamChecksum::Gzip(checksum)) => {
            let bytes = cursor.read_exact::<8>(cursor.position())?;
            let expected_crc = u32::from_le_bytes(bytes[..4].try_into().expect("four bytes"));
            let expected_size = u32::from_le_bytes(bytes[4..].try_into().expect("four bytes"));
            let actual_crc = checksum.finish();
            if expected_crc != actual_crc {
                return Err(DecodeError::ChecksumMismatch {
                    member: stream_index,
                    expected: expected_crc,
                    actual: actual_crc,
                });
            }
            if expected_size != stream_size as u32 {
                return Err(DecodeError::SizeMismatch {
                    member: stream_index,
                    expected: expected_size,
                    actual_mod32: stream_size as u32,
                });
            }
            StreamFooter::Gzip {
                crc32: expected_crc,
                uncompressed_size: expected_size,
            }
        }
        (Format::Zlib, StreamChecksum::Zlib(checksum)) => {
            let expected = u32::from_be_bytes(read_zlib_footer(cursor)?);
            let actual = checksum.finish();
            if expected != actual {
                return Err(DecodeError::InvalidZlib {
                    offset: footer_offset / 8,
                    reason: ZlibErrorKind::ChecksumMismatch { expected, actual },
                });
            }
            StreamFooter::Zlib { adler32: expected }
        }
        (Format::RawDeflate, StreamChecksum::None) => StreamFooter::None,
        _ => unreachable!("the stream checksum follows the selected format"),
    };
    let stream_end = cursor.bit_position();
    let compressed_size_in_bits =
        stream_end
            .checked_sub(header_offset)
            .ok_or(DecodeError::Analysis {
                reason: AnalysisErrorKind::CounterOverflow {
                    counter: AnalysisCounter::CompressedBits,
                },
            })?;
    let block_count = state.analysis.blocks.len() - first_block_index;
    state.analysis.streams.push(StreamAnalysis {
        index: stream_index,
        header,
        header_offset_in_bits: header_offset,
        deflate_offset_in_bits: deflate_offset,
        footer_offset_in_bits: footer_offset,
        uncompressed_offset_in_bytes: stream_output_start,
        footer,
        compressed_size_in_bits,
        uncompressed_size_in_bytes: stream_size,
        first_block_index,
        block_count,
    });
    Ok(())
}

fn analyze_cursor_mode<C: InputCursor, const CHECK_CONFIGURED_LIMITS: bool>(
    cursor: C,
    config: &crate::config::Config,
    options: AnalyzeOptions,
) -> Result<Analysis, DecodeError> {
    let mut cursor = AnalysisCursor::new(cursor)?;
    let format = resolve_cursor_format(&mut cursor, config.format)?;
    let mut state = AnalyzeState {
        config,
        options,
        analysis: Analysis {
            format,
            streams: Vec::new(),
            blocks: Vec::new(),
            uncompressed_size_in_bytes: 0,
            compressed_size_in_bytes: 0,
            backreference_length_counts: [0; 259],
        },
        total_output: 0,
        remaining_backreferences: options.maximum_retained_backreferences,
        retained_header_bytes: 0,
    };

    match format {
        Format::Gzip => {
            if cursor.is_at_end()? {
                return Err(DecodeError::InvalidGzip {
                    offset: 0,
                    reason: GzipErrorKind::BadMagic,
                });
            }
            while !cursor.is_at_end()? {
                analyze_one_stream::<_, CHECK_CONFIGURED_LIMITS>(&mut cursor, &mut state, format)?;
            }
        }
        Format::Zlib | Format::RawDeflate => {
            analyze_one_stream::<_, CHECK_CONFIGURED_LIMITS>(&mut cursor, &mut state, format)?;
            if !cursor.is_at_end()? {
                return Err(match format {
                    Format::Zlib => DecodeError::InvalidZlib {
                        offset: cursor.position(),
                        reason: ZlibErrorKind::TrailingGarbage,
                    },
                    Format::RawDeflate => DecodeError::InvalidDeflate {
                        bit_offset: cursor.bit_position(),
                        reason: DeflateErrorKind::TrailingGarbage,
                    },
                    Format::Gzip => unreachable!(),
                });
            }
        }
    }

    cursor.verify_source_unchanged()?;
    config.verify_expected_output(state.total_output)?;
    state.analysis.uncompressed_size_in_bytes = state.total_output;
    state.analysis.compressed_size_in_bytes = cursor.position();
    Ok(state.analysis)
}

fn analyze_cursor<C: InputCursor>(
    cursor: C,
    config: &crate::config::Config,
    options: AnalyzeOptions,
) -> Result<Analysis, DecodeError> {
    // Specializing once per operation keeps the common unconstrained walk
    // free of two Option checks for every decoded symbol. The constrained
    // version retains the exact configured-limit error precedence.
    if config.output_limit.is_some() || config.expected_uncompressed_size.is_some() {
        analyze_cursor_mode::<_, true>(cursor, config, options)
    } else {
        analyze_cursor_mode::<_, false>(cursor, config, options)
    }
}

pub(crate) fn analyze_source<R: ReadAt + ?Sized>(
    source: &R,
    config: &crate::config::Config,
    options: AnalyzeOptions,
) -> Result<Analysis, DecodeError> {
    let cursor = SourceCursor::new(source, config.input_page_size)?;
    if cursor.length() > u64::MAX / 8 {
        return Err(DecodeError::Analysis {
            reason: AnalysisErrorKind::CounterOverflow {
                counter: AnalysisCounter::CompressedBits,
            },
        });
    }
    analyze_cursor(cursor, config, options)
}

pub(crate) fn analyze_stream<R: Read>(
    source: R,
    config: &crate::config::Config,
    options: AnalyzeOptions,
) -> Result<Analysis, DecodeError> {
    analyze_cursor(
        StreamCursor::new(source, config.input_page_size),
        config,
        options,
    )
}

#[cfg(test)]
mod tests {
    use super::{OutputState, StreamChecksum, WINDOW_SIZE};

    fn ordered_history(output: &OutputState) -> Vec<u8> {
        let start = output.write_position - output.history_length;
        output.bytes[start..output.write_position].to_vec()
    }

    #[test]
    fn bulk_match_copy_matches_naive_overlap_across_buffer_rolls() {
        let mut expected: Vec<u8> = (0..WINDOW_SIZE + 173)
            .map(|index| (index.wrapping_mul(37) >> 3) as u8)
            .collect();
        let mut output = OutputState::new(WINDOW_SIZE, StreamChecksum::None);
        output.append_bytes(&expected);

        for (distance, length) in [
            (1, 258),
            (3, 257),
            (257, 258),
            (258, 17),
            (WINDOW_SIZE - 1, 258),
            (WINDOW_SIZE, 258),
        ] {
            for _ in 0..length {
                let byte = expected[expected.len() - distance];
                expected.push(byte);
            }
            output.copy_match(distance, length);
            let suffix = &expected[expected.len() - WINDOW_SIZE..];
            assert_eq!(ordered_history(&output), suffix);
        }
    }
}
