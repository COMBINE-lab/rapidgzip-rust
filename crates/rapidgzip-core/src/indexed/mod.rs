//! Random access to decompressed data through a [`GzipIndex`].
//!
//! [`IndexedReader`] resumes inflate at the checkpoint nearest before a
//! requested decompressed offset, installs that checkpoint's predecessor
//! window as the inflate dictionary, and discards output up to the target. It
//! is single-threaded on purpose: throughput remains the job of
//! [`crate::DecoderReader`], while this type serves scattered reads without
//! decoding everything before them.

mod window;

use crate::gzip::{SourceCursor, parse_member_header};
use crate::index::{Checkpoint, GzipIndex, WINDOW_SIZE};
use crate::inflate::RawInflater;
use crate::{DecodeError, ReadAt};
use libz_rs_sys as z;
use std::io::{self, Read, Seek, SeekFrom};
use window::{DEFAULT_BUDGET, WindowCache};

/// Compressed bytes read per positional request.
const INPUT_PAGE: usize = 128 * 1024;

/// Decompressed bytes produced per inflate call.
const OUTPUT_STEP: usize = 128 * 1024;

/// A [`Read`] and [`Seek`] view of the decompressed data behind an index.
///
/// Seeking positions the reader in the decompressed stream. Reads after a seek
/// resume from the nearest checkpoint at or before that position, so the cost
/// of a seek is bounded by the index's checkpoint spacing rather than by the
/// distance from the start of the file.
///
/// The index must describe the source actually supplied; an index built from
/// different data produces decoding errors rather than silent corruption only
/// insofar as DEFLATE itself detects them.
pub struct IndexedReader<R: ReadAt> {
    source: R,
    index: GzipIndex,
    source_length: u64,
    inflater: RawInflater,
    windows: WindowCache,
    input: Vec<u8>,
    input_position: usize,
    next_input: u64,
    decoded: Vec<u8>,
    decoded_position: usize,
    /// Decompressed offset of the next byte a read returns.
    position: u64,
    state: State,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum State {
    /// The inflater must be resumed before the next read.
    NeedsResume,
    /// The inflater is positioned at `position` plus the buffered bytes.
    Running,
    /// The last member ended and no member follows.
    Ended,
}

impl<R: ReadAt> IndexedReader<R> {
    /// Opens `source` for random access through `index`.
    ///
    /// The index is validated before use.
    pub fn new(source: R, index: GzipIndex) -> Result<Self, DecodeError> {
        index
            .validate()
            .map_err(|error| DecodeError::input_io(0, io::Error::other(error)))?;
        let source_length = source
            .len()
            .map_err(|error| DecodeError::input_io(0, error))?;
        Ok(Self {
            source,
            index,
            source_length,
            inflater: RawInflater::new()?,
            windows: WindowCache::new(DEFAULT_BUDGET),
            input: Vec::new(),
            input_position: 0,
            next_input: 0,
            decoded: Vec::new(),
            decoded_position: 0,
            position: 0,
            state: State::NeedsResume,
        })
    }

    /// Sets the expanded-window cache budget in bytes.
    ///
    /// The default holds eight full windows. A budget smaller than one window
    /// disables caching.
    #[must_use]
    pub fn with_window_cache_bytes(mut self, bytes: usize) -> Self {
        self.windows = WindowCache::new(bytes);
        self
    }

    /// Returns the index backing this reader.
    #[must_use]
    pub const fn index(&self) -> &GzipIndex {
        &self.index
    }

    /// Returns the decompressed offset of the next byte a read returns.
    #[must_use]
    pub const fn position(&self) -> u64 {
        self.position
    }

    /// Returns the source and index, discarding decoding state.
    pub fn into_inner(self) -> (R, GzipIndex) {
        (self.source, self.index)
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

    /// Reads `length` bytes at `offset` into `self.input`.
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
                break;
            }
            filled += read;
        }
        self.input.truncate(filled);
        self.input_position = 0;
        self.next_input = offset + filled as u64;
        Ok(())
    }

    /// Positions the inflater so that the next produced byte is at
    /// `self.position`.
    fn resume(&mut self) -> io::Result<()> {
        self.discard_buffers();

        let Some(checkpoint) = self.index.checkpoint_at_or_before(self.position).copied() else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "the index has no checkpoint at or before the requested offset",
            ));
        };

        let window = self.expanded_window(&checkpoint)?;
        let bit_offset = checkpoint.compressed_offset_in_bits;
        let byte_offset = bit_offset / 8;
        let remainder = (bit_offset % 8) as u8;

        if byte_offset > self.source_length {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "checkpoint points past the end of the source",
            ));
        }

        self.inflater.reset(bit_offset).map_err(io::Error::other)?;

        let mut start = byte_offset;
        if remainder != 0 {
            let mut straddled = [0u8; 1];
            let read = self.source.read_at(byte_offset, &mut straddled)?;
            if read != 1 {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "source ended inside a checkpoint's straddled byte",
                ));
            }
            self.inflater
                .prime(8 - remainder, straddled[0] >> remainder, bit_offset)
                .map_err(io::Error::other)?;
            start += 1;
        } else if window.is_empty() {
            // A byte-aligned checkpoint with no history is usually a member
            // boundary, whose gzip header must be skipped before inflating.
            // It is not always one: indexed_gzip records its first point at
            // the DEFLATE start instead, so the header is detected rather
            // than assumed.
            start = self.skip_member_header_if_present(byte_offset)?;
        }

        if !window.is_empty() {
            self.inflater
                .set_dictionary_bytes(&window, bit_offset)
                .map_err(io::Error::other)?;
        }

        self.read_input_page(start)?;
        self.state = State::Running;

        // Discard the decompressed bytes between the checkpoint and the target.
        let mut remaining = self.position - checkpoint.uncompressed_offset_in_bytes;
        while remaining > 0 {
            if self.buffered() == 0 && !self.fill()? {
                return Ok(());
            }
            let available = self.buffered() as u64;
            let skipped = remaining.min(available);
            self.decoded_position += skipped as usize;
            remaining -= skipped;
        }
        Ok(())
    }

    /// Returns the expanded predecessor window for `checkpoint`.
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
        if expanded.len() > WINDOW_SIZE {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "stored window is larger than the DEFLATE history",
            ));
        }
        if !expanded.is_empty() {
            self.windows.insert(key, expanded.clone());
        }
        Ok(expanded)
    }

    /// Returns where DEFLATE data starts at `offset`, skipping a gzip header
    /// when one is actually there.
    ///
    /// A checkpoint with no predecessor window can sit either on a member
    /// header or directly on the DEFLATE stream, depending on which tool wrote
    /// the index, so the magic bytes decide. A header that starts with the
    /// magic but fails to parse is treated as DEFLATE data that happens to
    /// begin with those two bytes.
    fn skip_member_header_if_present(&mut self, offset: u64) -> io::Result<u64> {
        let mut magic = [0u8; 2];
        let mut filled = 0;
        while filled < magic.len() {
            let read = self
                .source
                .read_at(offset + filled as u64, &mut magic[filled..])?;
            if read == 0 {
                break;
            }
            filled += read;
        }
        if filled < 2 || magic != [0x1f, 0x8b] {
            return Ok(offset);
        }
        Ok(self.skip_member_header(offset).unwrap_or(offset))
    }

    /// Parses the gzip header at `offset` and returns where its DEFLATE data
    /// starts.
    fn skip_member_header(&mut self, offset: u64) -> io::Result<u64> {
        let mut cursor = SourceCursor::new(&self.source, INPUT_PAGE).map_err(io::Error::other)?;
        cursor.seek(offset).map_err(io::Error::other)?;
        let header = parse_member_header(&mut cursor, offset == 0).map_err(io::Error::other)?;
        Ok(header.deflate_start)
    }

    /// Produces more decompressed bytes, returning false at the end of the
    /// last member.
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
                    // Input exhausted without a final block: treat as the end
                    // of the stream rather than looping forever.
                    self.state = State::Ended;
                    return Ok(false);
                }
                let next = self.next_input;
                self.read_input_page(next)?;
                if self.input.is_empty() {
                    self.state = State::Ended;
                    return Ok(false);
                }
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

    /// Runs one inflate call, handling member ends, and returns the number of
    /// decompressed bytes appended.
    fn inflate_step(&mut self) -> io::Result<usize> {
        let output_start = self.decoded.len();
        self.decoded.reserve(OUTPUT_STEP);
        let output_capacity = (self.decoded.capacity() - output_start).min(u32::MAX as usize);
        let input = &self.input[self.input_position..];
        let input_length = input.len().min(u32::MAX as usize);

        self.inflater.stream.next_in = input.as_ptr();
        self.inflater.stream.avail_in = input_length as u32;
        self.inflater.stream.next_out = self.decoded.spare_capacity_mut().as_mut_ptr().cast::<u8>();
        self.inflater.stream.avail_out = output_capacity as u32;
        let input_before = self.inflater.stream.avail_in;
        let output_before = self.inflater.stream.avail_out;

        // SAFETY:
        // - the inflater is initialized and uniquely borrowed for this call;
        // - `next_in` covers `input`, which stays live and unmoved until
        //   `inflate` returns;
        // - `next_out` covers only uniquely owned spare capacity of `decoded`,
        //   whose length is extended below by exactly the count zlib reports.
        let status = unsafe { z::inflate(&mut self.inflater.stream, z::Z_NO_FLUSH) };

        let consumed = (input_before - self.inflater.stream.avail_in) as usize;
        let produced = (output_before - self.inflater.stream.avail_out) as usize;
        self.input_position += consumed;
        // SAFETY: zlib initialized exactly `produced` bytes of the spare
        // capacity supplied above and cannot report more than that capacity.
        unsafe { self.decoded.set_len(output_start + produced) };

        match status {
            z::Z_OK => {
                if consumed == 0 && produced == 0 && self.next_input >= self.source_length {
                    self.state = State::Ended;
                }
                Ok(produced)
            }
            z::Z_STREAM_END => {
                self.finish_member()?;
                Ok(produced)
            }
            z::Z_BUF_ERROR => {
                if consumed == 0 && produced == 0 && self.next_input >= self.source_length {
                    self.state = State::Ended;
                }
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

    /// Steps over a member footer and starts the next member, if any.
    fn finish_member(&mut self) -> io::Result<()> {
        // The absolute offset of the first unconsumed input byte, which is the
        // start of the eight-byte member footer.
        let footer = self.next_input - (self.input.len() - self.input_position) as u64;
        let next_member = footer.saturating_add(8);
        if next_member >= self.source_length {
            self.state = State::Ended;
            return Ok(());
        }

        let deflate_start = self.skip_member_header(next_member)?;
        self.inflater
            .reset(deflate_start.saturating_mul(8))
            .map_err(io::Error::other)?;
        self.read_input_page(deflate_start)?;
        if self.input.is_empty() {
            self.state = State::Ended;
        }
        Ok(())
    }
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
            SeekFrom::End(delta) => {
                let length = self.index.uncompressed_size_in_bytes;
                if length == u64::MAX || length == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "the index does not record the decompressed size",
                    ));
                }
                add_offset(length, delta)?
            }
        };

        if position == self.position && self.state != State::NeedsResume {
            return Ok(position);
        }

        // A forward seek inside the buffered bytes needs no resume.
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
