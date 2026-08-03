//! Random access to decompressed data through a [`DeflateIndex`].

mod window;

use crate::crc32::Crc32;
use crate::gzip::{SourceCursor, parse_member_header};
use crate::index::{Checkpoint, CheckpointKind, DeflateIndex, IndexError, IndexKind, WINDOW_SIZE};
use crate::inflate::RawInflater;
use crate::zlib::{self, Adler32};
use crate::{DecodeError, ReadAt};
use libz_rs_sys as z;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read, Seek, SeekFrom};
use std::sync::Arc;
use window::{DEFAULT_BUDGET, WindowCache};

const INPUT_PAGE: usize = 128 * 1024;
const OUTPUT_STEP: usize = 128 * 1024;

/// Failure while opening an [`IndexedReader`].
#[derive(Clone, Debug)]
#[non_exhaustive]
pub enum IndexedReaderError {
    /// The supplied index is internally invalid.
    Index(IndexError),
    /// The raw inflate backend could not be initialized.
    Decode(DecodeError),
    /// The positional source could not be inspected.
    Io(Arc<io::Error>),
}

impl Display for IndexedReaderError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Index(error) => write!(formatter, "invalid DEFLATE index: {error}"),
            Self::Decode(error) => {
                write!(formatter, "could not initialize indexed decode: {error}")
            }
            Self::Io(error) => write!(formatter, "could not inspect indexed source: {error}"),
        }
    }
}

impl Error for IndexedReaderError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::Index(error) => Some(error),
            Self::Decode(error) => Some(error),
            Self::Io(error) => Some(error.as_ref()),
        }
    }
}

impl From<IndexError> for IndexedReaderError {
    fn from(error: IndexError) -> Self {
        Self::Index(error)
    }
}

impl From<DecodeError> for IndexedReaderError {
    fn from(error: DecodeError) -> Self {
        Self::Decode(error)
    }
}

enum Verification {
    Gzip { crc: Crc32, output_size: u32 },
    Zlib(Adler32),
}

impl Verification {
    const fn gzip() -> Self {
        Self::Gzip {
            crc: Crc32::new(),
            output_size: 0,
        }
    }

    const fn zlib() -> Self {
        Self::Zlib(Adler32::new())
    }

    fn update(&mut self, bytes: &[u8]) {
        match self {
            Self::Gzip { crc, output_size } => {
                crc.update(bytes);
                *output_size = output_size.wrapping_add(bytes.len() as u32);
            }
            Self::Zlib(checksum) => checksum.update(bytes),
        }
    }
}

/// A [`Read`] and [`Seek`] view of decompressed bytes described by an index.
///
/// Seeking resumes at the nearest preceding checkpoint and discards output up
/// to the requested byte. Gzip-member and zlib-header checkpoints permit full
/// checksum verification, including discarded bytes. An interior DEFLATE
/// checkpoint cannot authenticate the skipped prefix because the index does
/// not store its checksum state. Raw DEFLATE has no container checksum.
///
/// The index is validated and a known compressed size is compared with the
/// source before construction succeeds. Callers remain responsible for pairing
/// indexes without a recorded size with the source from which they were built.
pub struct IndexedReader<R: ReadAt> {
    source: R,
    index: DeflateIndex,
    source_length: u64,
    inflater: RawInflater,
    windows: WindowCache,
    input: Vec<u8>,
    input_position: usize,
    next_input: u64,
    decoded: Vec<u8>,
    decoded_position: usize,
    position: u64,
    state: State,
    verification: Option<Verification>,
    window_bits: u8,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    NeedsResume,
    Running,
    Ended,
}

impl<R: ReadAt> IndexedReader<R> {
    /// Opens `source` for random access through `index`.
    ///
    /// # Errors
    ///
    /// Returns an error when the index violates its invariants, the source
    /// length cannot be read or disagrees with a known index size, or the raw
    /// inflate backend cannot be initialized.
    pub fn new(source: R, index: DeflateIndex) -> Result<Self, IndexedReaderError> {
        index.validate()?;
        let source_length = source
            .len()
            .map_err(|error| IndexedReaderError::Io(Arc::new(error)))?;
        if let Some(index_size) = index.compressed_size() {
            if index_size != source_length {
                return Err(IndexedReaderError::Index(IndexError::ArchiveSizeMismatch {
                    index_size,
                    archive_size: source_length,
                }));
            }
        }
        let window_bits = if index.kind() == IndexKind::Zlib {
            let header = read_exact_from_source::<2, _>(&source, 0)
                .map_err(|error| IndexedReaderError::Io(Arc::new(error)))?;
            zlib::parse_header(header, 0)?
        } else {
            15
        };
        Ok(Self {
            source,
            index,
            source_length,
            inflater: RawInflater::new_with_window_bits(window_bits)?,
            windows: WindowCache::new(DEFAULT_BUDGET),
            input: Vec::new(),
            input_position: 0,
            next_input: 0,
            decoded: Vec::new(),
            decoded_position: 0,
            position: 0,
            state: State::NeedsResume,
            verification: None,
            window_bits,
        })
    }

    /// Sets the expanded-window cache budget in bytes.
    #[must_use]
    pub fn with_window_cache_bytes(mut self, bytes: usize) -> Self {
        self.windows = WindowCache::new(bytes);
        self
    }

    /// Returns the index backing this reader.
    #[must_use]
    pub const fn index(&self) -> &DeflateIndex {
        &self.index
    }

    /// Returns the decompressed offset of the next byte a read returns.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Returns the source and index, discarding decoding state.
    pub fn into_inner(self) -> (R, DeflateIndex) {
        (self.source, self.index)
    }

    /// Seeks to the first byte of zero-based `line` and returns its byte offset.
    ///
    /// The reader resumes from the nearest preceding annotated checkpoint and
    /// scans forward only far enough to find the requested newline. Seeking
    /// beyond the recorded lines positions the reader at decoded EOF.
    ///
    /// # Errors
    ///
    /// Returns [`io::ErrorKind::Unsupported`] unless the index carries a total
    /// line count and a line offset for every checkpoint. Other errors have the
    /// same meaning as [`Read`] and [`Seek`] errors from this reader.
    pub fn seek_to_line(&mut self, line: u64) -> io::Result<u64> {
        let Some(checkpoint) = self.index.checkpoint_at_or_before_line(line) else {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the index does not carry complete line metadata",
            ));
        };
        let checkpoint_line = checkpoint
            .line_offset
            .expect("line checkpoint lookup requires complete metadata");
        let mut remaining = line - checkpoint_line;
        let start = checkpoint.uncompressed_offset_in_bytes;
        self.seek(SeekFrom::Start(start))?;
        if remaining == 0 {
            return Ok(start);
        }

        let mut scratch = [0_u8; 64 * 1024];
        loop {
            let read = self.read(&mut scratch)?;
            if read == 0 {
                return Ok(self.position);
            }
            let mut consumed = 0_usize;
            for (index, &byte) in scratch[..read].iter().enumerate() {
                if byte != b'\n' {
                    continue;
                }
                remaining -= 1;
                if remaining == 0 {
                    consumed = index + 1;
                    break;
                }
            }
            if remaining == 0 {
                let position = self.position - (read - consumed) as u64;
                self.seek(SeekFrom::Start(position))?;
                return Ok(position);
            }
        }
    }

    fn buffered(&self) -> usize {
        self.decoded.len() - self.decoded_position
    }

    fn discard_buffers(&mut self) {
        self.decoded.clear();
        self.decoded_position = 0;
        self.input.clear();
        self.input_position = 0;
    }

    fn read_input_page(&mut self, offset: u64) -> io::Result<()> {
        let remaining = self.source_length.saturating_sub(offset);
        let length = usize::try_from(remaining)
            .unwrap_or(usize::MAX)
            .min(INPUT_PAGE);
        self.input.clear();
        self.input.resize(length, 0);
        let mut filled = 0;
        while filled < length {
            let read = self
                .source
                .read_at(offset + filled as u64, &mut self.input[filled..])?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "positional source ended before its reported length",
                ));
            }
            filled += read;
        }
        self.input_position = 0;
        self.next_input = offset + filled as u64;
        Ok(())
    }

    fn read_exact_at<const N: usize>(&self, offset: u64) -> io::Result<[u8; N]> {
        let mut bytes = [0_u8; N];
        let mut filled = 0;
        while filled < N {
            let read = self
                .source
                .read_at(offset + filled as u64, &mut bytes[filled..])?;
            if read == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated compressed-stream framing",
                ));
            }
            filled += read;
        }
        Ok(bytes)
    }

    fn parse_member_header(&self, offset: u64) -> io::Result<u64> {
        let mut cursor =
            SourceCursor::new(&self.source, INPUT_PAGE).map_err(|error| error.to_io_error())?;
        cursor.seek(offset).map_err(|error| error.to_io_error())?;
        let header =
            parse_member_header(&mut cursor, offset == 0).map_err(|error| error.to_io_error())?;
        Ok(header.deflate_start)
    }

    fn resume(&mut self) -> io::Result<()> {
        self.discard_buffers();
        let checkpoint = self
            .index
            .checkpoint_at_or_before(self.position)
            .copied()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "the index has no checkpoint at or before the requested offset",
                )
            })?;
        let bit_offset = checkpoint.compressed_offset_in_bits;
        let byte_offset = bit_offset / 8;
        if byte_offset > self.source_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint points past the end of the source",
            ));
        }

        self.inflater
            .reset_with_window_bits(self.window_bits, bit_offset)
            .map_err(|error| error.to_io_error())?;
        let mut start = byte_offset;
        let window = self.expanded_window(&checkpoint)?;
        self.verification = match checkpoint.kind {
            CheckpointKind::GzipMemberHeader => {
                start = self.parse_member_header(byte_offset)?;
                Some(Verification::gzip())
            }
            CheckpointKind::GzipMemberDeflate {
                header_offset_in_bytes,
            } => {
                let parsed_start = self.parse_member_header(header_offset_in_bytes)?;
                if parsed_start != byte_offset {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "member checkpoint does not match the parsed gzip header",
                    ));
                }
                Some(Verification::gzip())
            }
            CheckpointKind::ZlibHeader => {
                let header = self.read_exact_at::<2>(byte_offset)?;
                let parsed_window =
                    zlib::parse_header(header, byte_offset).map_err(|error| error.to_io_error())?;
                if parsed_window != self.window_bits {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "zlib header window changed after reader construction",
                    ));
                }
                start = byte_offset + 2;
                Some(Verification::zlib())
            }
            CheckpointKind::RawDeflateStart => None,
            CheckpointKind::DeflateBlock => {
                let remainder = (bit_offset % 8) as u8;
                if remainder != 0 {
                    let straddled = self.read_exact_at::<1>(byte_offset)?[0];
                    self.inflater
                        .prime(8 - remainder, straddled >> remainder, bit_offset)
                        .map_err(|error| error.to_io_error())?;
                    start += 1;
                }
                None
            }
        };
        if !window.is_empty() {
            let allowed = 1_usize << self.window_bits;
            let window = &window[window.len().saturating_sub(allowed)..];
            self.inflater
                .set_dictionary_bytes(window, bit_offset)
                .map_err(|error| error.to_io_error())?;
        }
        self.read_input_page(start)?;
        self.state = State::Running;

        let mut remaining = self.position - checkpoint.uncompressed_offset_in_bytes;
        while remaining > 0 {
            if self.buffered() == 0 && !self.fill()? {
                return Ok(());
            }
            let skipped = remaining.min(self.buffered() as u64);
            self.decoded_position += skipped as usize;
            remaining -= skipped;
        }
        Ok(())
    }

    fn expanded_window(&mut self, checkpoint: &Checkpoint) -> io::Result<Vec<u8>> {
        let key = checkpoint.compressed_offset_in_bits;
        if let Some(cached) = self.windows.get(key) {
            return Ok(cached.to_vec());
        }
        let Some(stored) = self.index.windows().get(key) else {
            return Ok(Vec::new());
        };
        let expanded = stored
            .decompressed()
            .map_err(io::Error::other)?
            .into_owned();
        if expanded.len() != WINDOW_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stored predecessor window is not exactly 32768 bytes",
            ));
        }
        self.windows.insert(key, expanded.clone());
        Ok(expanded)
    }

    fn fill(&mut self) -> io::Result<bool> {
        loop {
            match self.state {
                State::Ended => return Ok(false),
                State::NeedsResume => {
                    self.resume()?;
                    if self.buffered() > 0 {
                        return Ok(true);
                    }
                    if self.state == State::Ended {
                        return Ok(false);
                    }
                }
                State::Running => {}
            }

            if self.input_position >= self.input.len() {
                if self.next_input >= self.source_length {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "truncated DEFLATE stream",
                    ));
                }
                self.read_input_page(self.next_input)?;
            }
            if self.decoded_position == self.decoded.len() {
                self.decoded.clear();
                self.decoded_position = 0;
            }
            let produced = self.inflate_step()?;
            if produced > 0 {
                return Ok(true);
            }
            if self.state == State::Ended {
                return Ok(false);
            }
        }
    }

    fn inflate_step(&mut self) -> io::Result<usize> {
        let output_start = self.decoded.len();
        self.decoded.reserve(OUTPUT_STEP);
        let output_capacity = (self.decoded.capacity() - output_start).min(u32::MAX as usize);
        let input = &self.input[self.input_position..];
        let input_length = input.len().min(u32::MAX as usize);

        self.inflater.stream.next_in = input.as_ptr();
        self.inflater.stream.avail_in = input_length as u32;
        self.inflater.stream.next_out = self.decoded.spare_capacity_mut().as_mut_ptr().cast();
        self.inflater.stream.avail_out = output_capacity as u32;
        let input_before = self.inflater.stream.avail_in;
        let output_before = self.inflater.stream.avail_out;
        // SAFETY: input and output point to live, non-overlapping allocations
        // for this call, and the initialized inflater is uniquely borrowed.
        let status = unsafe { z::inflate(&mut self.inflater.stream, z::Z_NO_FLUSH) };
        let consumed = (input_before - self.inflater.stream.avail_in) as usize;
        let produced = (output_before - self.inflater.stream.avail_out) as usize;
        self.inflater.stream.next_in = std::ptr::null();
        self.inflater.stream.avail_in = 0;
        self.inflater.stream.next_out = std::ptr::null_mut();
        self.inflater.stream.avail_out = 0;
        self.input_position += consumed;
        // SAFETY: zlib initialized exactly `produced` bytes in the supplied
        // spare capacity and cannot report more than its `avail_out` bound.
        unsafe { self.decoded.set_len(output_start + produced) };
        if let Some(verification) = self.verification.as_mut() {
            verification.update(&self.decoded[output_start..]);
        }

        match status {
            z::Z_OK if consumed != 0 || produced != 0 => Ok(produced),
            z::Z_BUF_ERROR if consumed != 0 || produced != 0 => Ok(produced),
            z::Z_OK | z::Z_BUF_ERROR if self.next_input >= self.source_length => Err(
                io::Error::new(io::ErrorKind::UnexpectedEof, "truncated DEFLATE stream"),
            ),
            z::Z_OK | z::Z_BUF_ERROR => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "DEFLATE decoder made no progress",
            )),
            z::Z_STREAM_END => {
                self.finish_stream()?;
                Ok(produced)
            }
            z::Z_DATA_ERROR => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                self.inflater
                    .message()
                    .unwrap_or_else(|| "invalid DEFLATE data".to_owned()),
            )),
            other => Err(io::Error::other(format!(
                "unexpected DEFLATE backend status {other}"
            ))),
        }
    }

    fn finish_stream(&mut self) -> io::Result<()> {
        let trailer_offset = self.next_input - (self.input.len() - self.input_position) as u64;
        match self.index.kind() {
            IndexKind::Gzip | IndexKind::Bgzf => self.finish_gzip_member(trailer_offset),
            IndexKind::Zlib => self.finish_zlib_stream(trailer_offset),
            IndexKind::RawDeflate => {
                if trailer_offset != self.source_length {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "trailing data after raw DEFLATE stream",
                    ));
                }
                self.verification = None;
                self.state = State::Ended;
                Ok(())
            }
        }
    }

    fn finish_gzip_member(&mut self, footer_offset: u64) -> io::Result<()> {
        let footer = self.read_exact_at::<8>(footer_offset)?;
        if let Some(Verification::Gzip { crc, output_size }) = self.verification.take() {
            let expected_crc = u32::from_le_bytes(footer[..4].try_into().expect("four bytes"));
            let expected_size = u32::from_le_bytes(footer[4..].try_into().expect("four bytes"));
            let actual_crc = crc.finish();
            if expected_crc != actual_crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "gzip CRC32 mismatch: expected {expected_crc:#010x}, got {actual_crc:#010x}"
                    ),
                ));
            }
            if expected_size != output_size {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "gzip ISIZE mismatch: expected {expected_size}, got {}",
                        output_size
                    ),
                ));
            }
        }

        let next_member = footer_offset.checked_add(8).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "gzip footer offset overflow")
        })?;
        if next_member == self.source_length {
            self.state = State::Ended;
            return Ok(());
        }
        if next_member > self.source_length {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "truncated gzip member footer",
            ));
        }
        let deflate_start = self.parse_member_header(next_member)?;
        self.inflater
            .reset(deflate_start.saturating_mul(8))
            .map_err(|error| error.to_io_error())?;
        self.verification = Some(Verification::gzip());
        self.read_input_page(deflate_start)?;
        self.state = State::Running;
        Ok(())
    }

    fn finish_zlib_stream(&mut self, trailer_offset: u64) -> io::Result<()> {
        let trailer = self.read_exact_at::<4>(trailer_offset)?;
        if let Some(Verification::Zlib(checksum)) = self.verification.take() {
            let expected = u32::from_be_bytes(trailer);
            let actual = checksum.finish();
            if expected != actual {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "zlib Adler-32 mismatch: expected {expected:#010x}, got {actual:#010x}"
                    ),
                ));
            }
        }
        let end = trailer_offset.checked_add(4).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "zlib trailer offset overflow")
        })?;
        if end != self.source_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "trailing data after zlib stream",
            ));
        }
        self.state = State::Ended;
        Ok(())
    }
}

fn read_exact_from_source<const N: usize, R: ReadAt>(
    source: &R,
    offset: u64,
) -> io::Result<[u8; N]> {
    let mut bytes = [0_u8; N];
    let mut filled = 0;
    while filled < N {
        let read = source.read_at(offset + filled as u64, &mut bytes[filled..])?;
        if read == 0 {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "compressed source ended before the requested framing bytes",
            ));
        }
        filled += read;
    }
    Ok(bytes)
}

impl<R: ReadAt> Read for IndexedReader<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        if output.is_empty() {
            return Ok(0);
        }
        loop {
            if self.buffered() > 0 {
                let count = self.buffered().min(output.len());
                let start = self.decoded_position;
                output[..count].copy_from_slice(&self.decoded[start..start + count]);
                self.decoded_position += count;
                self.position += count as u64;
                return Ok(count);
            }
            if !self.fill()? {
                return Ok(0);
            }
        }
    }
}

impl<R: ReadAt> Seek for IndexedReader<R> {
    fn seek(&mut self, target: SeekFrom) -> io::Result<u64> {
        let position = match target {
            SeekFrom::Start(offset) => offset,
            SeekFrom::Current(delta) => add_offset(self.position, delta)?,
            SeekFrom::End(delta) => add_offset(
                self.index.uncompressed_size().ok_or_else(|| {
                    io::Error::new(
                        io::ErrorKind::Unsupported,
                        "the index does not record the decompressed size",
                    )
                })?,
                delta,
            )?,
        };

        if position == self.position && self.state != State::NeedsResume {
            return Ok(position);
        }
        if position > self.position && self.state == State::Running {
            let ahead = position - self.position;
            if ahead <= self.buffered() as u64 {
                self.decoded_position += ahead as usize;
                self.position = position;
                return Ok(position);
            }
        }
        self.position = position;
        self.state = State::NeedsResume;
        self.verification = None;
        self.discard_buffers();
        Ok(position)
    }

    fn stream_position(&mut self) -> io::Result<u64> {
        Ok(self.position)
    }
}

fn add_offset(base: u64, delta: i64) -> io::Result<u64> {
    let result = if delta >= 0 {
        base.checked_add(delta as u64)
    } else {
        base.checked_sub(delta.unsigned_abs())
    };
    result.ok_or_else(|| {
        io::Error::new(
            io::ErrorKind::InvalidInput,
            "seek position is outside the decompressed stream",
        )
    })
}
