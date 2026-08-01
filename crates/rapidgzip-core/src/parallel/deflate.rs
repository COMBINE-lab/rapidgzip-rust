//! Native block decoder used by speculative rapidgzip tasks.
//!
//! This is a clean Rust implementation of RFC 1951.  A task beginning at an
//! internal block boundary seeds its 32 KiB history with marker symbols. LZ77
//! copies retain those symbols, so successful work is resolved rather than
//! decoded again once the real predecessor window becomes available.

use super::marker::{MarkerBuffer, Symbol, WINDOW_SIZE, Window};
use std::array;
use std::sync::OnceLock;

const MAX_BITS: usize = 15;
const END_OF_BLOCK: usize = 256;

const LENGTH_BASE: [usize; 29] = [
    3, 4, 5, 6, 7, 8, 9, 10, 11, 13, 15, 17, 19, 23, 27, 31, 35, 43, 51, 59, 67, 83, 99, 115, 131,
    163, 195, 227, 258,
];
const LENGTH_EXTRA: [u8; 29] = [
    0, 0, 0, 0, 0, 0, 0, 0, 1, 1, 1, 1, 2, 2, 2, 2, 3, 3, 3, 3, 4, 4, 4, 4, 5, 5, 5, 5, 0,
];
const DISTANCE_BASE: [usize; 30] = [
    1, 2, 3, 4, 5, 7, 9, 13, 17, 25, 33, 49, 65, 97, 129, 193, 257, 385, 513, 769, 1025, 1537,
    2049, 3073, 4097, 6145, 8193, 12_289, 16_385, 24_577,
];
const DISTANCE_EXTRA: [u8; 30] = [
    0, 0, 0, 0, 1, 1, 2, 2, 3, 3, 4, 4, 5, 5, 6, 6, 7, 7, 8, 8, 9, 9, 10, 10, 11, 11, 12, 12, 13,
    13,
];
const PRECODE_ORDER: [usize; 19] = [
    16, 17, 18, 0, 8, 7, 9, 6, 10, 5, 11, 4, 12, 3, 13, 2, 14, 1, 15,
];

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum Error {
    UnexpectedEof,
    InvalidBlockType,
    InvalidStoredLength,
    InvalidHuffmanTree,
    InvalidCodeLengths,
    InvalidSymbol,
    InvalidDistance,
    OutputLimit,
    BoundaryMismatch,
}

#[derive(Clone)]
struct BitReader<'a> {
    bytes: &'a [u8],
    bit_offset: usize,
}

impl<'a> BitReader<'a> {
    #[inline]
    fn at(bytes: &'a [u8], bit_offset: usize) -> Result<Self, Error> {
        if bit_offset > bytes.len().saturating_mul(8) {
            return Err(Error::UnexpectedEof);
        }
        Ok(Self { bytes, bit_offset })
    }

    const fn position(&self) -> usize {
        self.bit_offset
    }

    #[inline(always)]
    fn word_at(&self, byte_offset: usize) -> u64 {
        word_at(self.bytes, byte_offset)
    }

    #[inline(always)]
    fn read_bits(&mut self, count: u8) -> Result<u32, Error> {
        debug_assert!(count <= 24);
        let byte_offset = self.bit_offset / 8;
        let shift = self.bit_offset % 8;
        if self.bytes.len().saturating_sub(byte_offset) >= 8 {
            // SAFETY: the length check proves that eight initialized bytes are
            // available. With `shift <= 7` and `count <= 24`, every requested
            // bit is contained in that word, so advancing cannot pass EOF.
            let word = unsafe {
                std::ptr::read_unaligned(self.bytes.as_ptr().add(byte_offset).cast::<u64>())
            }
            .to_le();
            self.bit_offset += usize::from(count);
            let mask = if count == 0 { 0 } else { (1_u64 << count) - 1 };
            return Ok(((word >> shift) & mask) as u32);
        }
        if self
            .bit_offset
            .checked_add(usize::from(count))
            .is_none_or(|end| end > self.bytes.len().saturating_mul(8))
        {
            return Err(Error::UnexpectedEof);
        }
        let word = self.word_at(byte_offset);
        self.bit_offset += usize::from(count);
        let mask = if count == 0 { 0 } else { (1_u64 << count) - 1 };
        Ok(((word >> shift) & mask) as u32)
    }

    #[inline(always)]
    fn peek_bits_padded(&self, count: u8) -> (u32, u8) {
        let byte_offset = self.bit_offset / 8;
        let shift = self.bit_offset % 8;
        if self.bytes.len().saturating_sub(byte_offset) >= 8 {
            // SAFETY: the length check proves that the unaligned eight-byte
            // load is within `bytes`. Huffman peeks request at most 15 bits,
            // which fit in the word even at the largest seven-bit shift.
            let word = unsafe {
                std::ptr::read_unaligned(self.bytes.as_ptr().add(byte_offset).cast::<u64>())
            }
            .to_le();
            let mask = if count == 0 { 0 } else { (1_u64 << count) - 1 };
            return (((word >> shift) & mask) as u32, count);
        }
        let available = self
            .bytes
            .len()
            .saturating_mul(8)
            .saturating_sub(self.bit_offset)
            .min(usize::from(count));
        let word = self.word_at(byte_offset);
        let mask = if count == 0 { 0 } else { (1_u64 << count) - 1 };
        (((word >> shift) & mask) as u32, available as u8)
    }

    #[inline(always)]
    fn align_to_byte(&mut self) {
        self.bit_offset = self.bit_offset.saturating_add(7) & !7;
    }
}

#[inline(always)]
fn word_at(bytes: &[u8], byte_offset: usize) -> u64 {
    if bytes.len().saturating_sub(byte_offset) >= 8 {
        // SAFETY: the preceding length check proves that eight initialized
        // bytes beginning at `byte_offset` are within `bytes`.
        // `read_unaligned` imposes no alignment requirement and the loaded
        // integer is normalized to little endian before bit extraction.
        return unsafe { std::ptr::read_unaligned(bytes.as_ptr().add(byte_offset).cast::<u64>()) }
            .to_le();
    }
    let mut word = 0_u64;
    for (index, &byte) in bytes[byte_offset..].iter().take(8).enumerate() {
        word |= u64::from(byte) << (index * 8);
    }
    word
}

#[derive(Clone)]
struct Huffman {
    // Packed as `(bit_length << 9) | symbol`, indexed by the next
    // `maximum_length` stream bits.
    table: Vec<u16>,
    maximum_length: u8,
}

fn reverse_low_bits(value: u16, count: usize) -> u16 {
    value.reverse_bits() >> (u16::BITS as usize - count)
}

impl Huffman {
    fn from_lengths(lengths: &[u8]) -> Result<Self, Error> {
        let mut counts = [0_u16; MAX_BITS + 1];
        for &length in lengths {
            if usize::from(length) > MAX_BITS {
                return Err(Error::InvalidHuffmanTree);
            }
            if length != 0 {
                counts[usize::from(length)] += 1;
            }
        }
        if counts.iter().skip(1).all(|&count| count == 0) {
            return Err(Error::InvalidHuffmanTree);
        }

        let mut remaining = 1_i32;
        for &count in counts.iter().skip(1) {
            remaining = remaining * 2 - i32::from(count);
            if remaining < 0 {
                return Err(Error::InvalidHuffmanTree);
            }
        }
        let symbol_count: usize = counts.iter().skip(1).map(|&count| usize::from(count)).sum();
        if remaining != 0 && !(symbol_count == 1 && counts[1] == 1) {
            return Err(Error::InvalidHuffmanTree);
        }

        let mut next_code = [0_u16; MAX_BITS + 1];
        let mut code = 0_u16;
        for bits in 1..=MAX_BITS {
            code = (code + counts[bits - 1]) << 1;
            next_code[bits] = code;
        }

        let mut maximum_length = 0;
        for &length in lengths {
            maximum_length = maximum_length.max(length);
        }
        let mut table = vec![u16::MAX; 1_usize << maximum_length];
        for (symbol, &length) in lengths.iter().enumerate() {
            if length == 0 {
                continue;
            }
            let length_index = usize::from(length);
            let canonical = next_code[length_index];
            next_code[length_index] += 1;
            let reversed = usize::from(reverse_low_bits(canonical, length_index));
            let packed = (u16::from(length) << 9) | symbol as u16;
            let suffix_count = 1_usize << (usize::from(maximum_length) - length_index);
            for suffix in 0..suffix_count {
                let index = reversed | (suffix << length_index);
                if table[index] != u16::MAX {
                    return Err(Error::InvalidHuffmanTree);
                }
                table[index] = packed;
            }
        }
        Ok(Self {
            table,
            maximum_length,
        })
    }

    #[inline(always)]
    fn decode(&self, reader: &mut BitReader<'_>) -> Result<usize, Error> {
        let (bits, available) = reader.peek_bits_padded(self.maximum_length);
        let packed = self.table[bits as usize];
        if packed == u16::MAX {
            return Err(Error::InvalidSymbol);
        }
        let length = (packed >> 9) as u8;
        if length > available {
            return Err(Error::UnexpectedEof);
        }
        reader.bit_offset += usize::from(length);
        Ok(usize::from(packed & 0x01FF))
    }
}

struct History {
    symbols: [u16; WINDOW_SIZE],
    length: usize,
    next: usize,
    marker_count: usize,
}

impl History {
    fn unknown() -> Self {
        Self {
            symbols: array::from_fn(|index| (WINDOW_SIZE + index) as u16),
            length: WINDOW_SIZE,
            next: 0,
            marker_count: WINDOW_SIZE,
        }
    }

    const fn empty() -> Self {
        Self {
            symbols: [0; WINDOW_SIZE],
            length: 0,
            next: 0,
            marker_count: 0,
        }
    }

    #[allow(dead_code)]
    fn from_window(window: &Window) -> Self {
        let mut result = Self::empty();
        for &byte in window.as_slice() {
            result.push(Symbol::literal(byte));
        }
        result
    }

    #[inline(always)]
    fn push(&mut self, symbol: Symbol) {
        if self.length == WINDOW_SIZE && self.symbols[self.next] >= WINDOW_SIZE as u16 {
            self.marker_count -= 1;
        }
        self.symbols[self.next] = symbol.encoded();
        if symbol.encoded() >= WINDOW_SIZE as u16 {
            self.marker_count += 1;
        }
        self.next = (self.next + 1) & (WINDOW_SIZE - 1);
        self.length = (self.length + 1).min(WINDOW_SIZE);
    }

    #[inline(always)]
    const fn contains_markers(&self) -> bool {
        self.marker_count != 0
    }

    #[inline(always)]
    fn push_clean(&mut self, byte: u8) {
        debug_assert_eq!(self.marker_count, 0);
        self.symbols[self.next] = u16::from(byte);
        self.next = (self.next + 1) & (WINDOW_SIZE - 1);
        self.length = (self.length + 1).min(WINDOW_SIZE);
    }

    /// Bulk-append marker-free literals into the ring (clean path only).
    #[inline(always)]
    fn push_clean_bytes(&mut self, bytes: &[u8]) {
        debug_assert_eq!(self.marker_count, 0);
        if bytes.is_empty() {
            return;
        }
        // Fast path: the whole run fits before the ring wraps.
        let space = WINDOW_SIZE - self.next;
        if bytes.len() <= space {
            for (offset, &byte) in bytes.iter().enumerate() {
                self.symbols[self.next + offset] = u16::from(byte);
            }
            self.next = (self.next + bytes.len()) & (WINDOW_SIZE - 1);
            self.length = (self.length + bytes.len()).min(WINDOW_SIZE);
            return;
        }
        // General path: may wrap (DEFLATE matches are ≤258, so usually once).
        let mut remaining = bytes;
        while !remaining.is_empty() {
            let space = WINDOW_SIZE - self.next;
            let chunk = remaining.len().min(space);
            for (offset, &byte) in remaining[..chunk].iter().enumerate() {
                self.symbols[self.next + offset] = u16::from(byte);
            }
            self.next = (self.next + chunk) & (WINDOW_SIZE - 1);
            remaining = &remaining[chunk..];
        }
        self.length = (self.length + bytes.len()).min(WINDOW_SIZE);
    }

    /// Bulk-append already-decoded symbols into the ring (marked path).
    #[inline(always)]
    fn push_symbols(&mut self, symbols: &[Symbol]) {
        for &symbol in symbols {
            self.push(symbol);
        }
    }

    /// Single-symbol ring lookup retained for test oracles (bulk paths no longer
    /// walk matches byte-by-byte).
    #[cfg(test)]
    fn get_distance(&self, distance: usize) -> Result<Symbol, Error> {
        if distance == 0 || distance > self.length {
            return Err(Error::InvalidDistance);
        }
        let index = self.next.wrapping_sub(distance) & (WINDOW_SIZE - 1);
        Ok(Symbol::from_encoded(self.symbols[index]))
    }

    #[cfg(test)]
    fn get_distance_byte(&self, distance: usize) -> Result<u8, Error> {
        self.get_distance(distance)?
            .as_literal()
            .ok_or(Error::InvalidDistance)
    }

    /// Append `count` consecutive history bytes beginning `distance` behind the
    /// write head. Used for the pre-clean portion of a clean-path match.
    ///
    /// `count` must not exceed `distance` (caller copies only the
    /// non-overlapping first period segment from the ring).
    fn append_bytes_at_distance(
        &self,
        distance: usize,
        count: usize,
        out: &mut Vec<u8>,
    ) -> Result<(), Error> {
        if distance == 0 || distance > self.length {
            return Err(Error::InvalidDistance);
        }
        debug_assert!(count <= distance);
        debug_assert_eq!(self.marker_count, 0);
        if count == 0 {
            return Ok(());
        }
        let start = self.next.wrapping_sub(distance) & (WINDOW_SIZE - 1);
        out.reserve(count);
        let first = count.min(WINDOW_SIZE - start);
        for &encoded in &self.symbols[start..start + first] {
            debug_assert!(encoded <= u8::MAX as u16);
            out.push(encoded as u8);
        }
        if first < count {
            for &encoded in &self.symbols[..count - first] {
                debug_assert!(encoded <= u8::MAX as u16);
                out.push(encoded as u8);
            }
        }
        Ok(())
    }

    /// Append `count` consecutive history symbols beginning `distance` behind
    /// the write head (marked path pre-output portion of a match).
    fn append_symbols_at_distance(
        &self,
        distance: usize,
        count: usize,
        out: &mut Vec<Symbol>,
    ) -> Result<(), Error> {
        if distance == 0 || distance > self.length {
            return Err(Error::InvalidDistance);
        }
        debug_assert!(count <= distance);
        if count == 0 {
            return Ok(());
        }
        let start = self.next.wrapping_sub(distance) & (WINDOW_SIZE - 1);
        out.reserve(count);
        let first = count.min(WINDOW_SIZE - start);
        for &encoded in &self.symbols[start..start + first] {
            out.push(Symbol::from_encoded(encoded));
        }
        if first < count {
            for &encoded in &self.symbols[..count - first] {
                out.push(Symbol::from_encoded(encoded));
            }
        }
        Ok(())
    }

    fn literal_window(&self) -> Window {
        debug_assert!(!self.contains_markers());
        let mut bytes = Vec::with_capacity(self.length);
        let start = if self.length == WINDOW_SIZE {
            self.next
        } else {
            0
        };
        for index in 0..self.length {
            let encoded = self.symbols[(start + index) & (WINDOW_SIZE - 1)];
            bytes.push(
                Symbol::from_encoded(encoded)
                    .as_literal()
                    .expect("marker-free history contains only literals"),
            );
        }
        Window::new(bytes).expect("DEFLATE history never exceeds 32 KiB")
    }
}

fn fixed_trees() -> &'static (Huffman, Huffman) {
    static TREES: OnceLock<(Huffman, Huffman)> = OnceLock::new();
    TREES.get_or_init(|| {
        let mut literal_lengths = [0_u8; 288];
        literal_lengths[..144].fill(8);
        literal_lengths[144..256].fill(9);
        literal_lengths[256..280].fill(7);
        literal_lengths[280..].fill(8);
        let distance_lengths = [5_u8; 32];
        (
            Huffman::from_lengths(&literal_lengths).expect("RFC 1951 fixed literal tree is valid"),
            Huffman::from_lengths(&distance_lengths)
                .expect("RFC 1951 fixed distance tree is valid"),
        )
    })
}

fn dynamic_trees(reader: &mut BitReader<'_>) -> Result<(Huffman, Huffman), Error> {
    let literal_count = 257 + reader.read_bits(5)? as usize;
    let distance_count = 1 + reader.read_bits(5)? as usize;
    let precode_count = 4 + reader.read_bits(4)? as usize;
    if literal_count > 286 || distance_count > 32 {
        return Err(Error::InvalidCodeLengths);
    }

    let mut precode_lengths = [0_u8; 19];
    for &symbol in PRECODE_ORDER.iter().take(precode_count) {
        precode_lengths[symbol] = reader.read_bits(3)? as u8;
    }
    let precode = Huffman::from_lengths(&precode_lengths)?;

    let target_count = literal_count + distance_count;
    let mut lengths = Vec::with_capacity(target_count);
    while lengths.len() < target_count {
        match precode.decode(reader)? {
            value @ 0..=15 => lengths.push(value as u8),
            16 => {
                let previous = *lengths.last().ok_or(Error::InvalidCodeLengths)?;
                let repetitions = 3 + reader.read_bits(2)? as usize;
                if lengths.len().saturating_add(repetitions) > target_count {
                    return Err(Error::InvalidCodeLengths);
                }
                lengths.extend(std::iter::repeat_n(previous, repetitions));
            }
            17 => {
                let repetitions = 3 + reader.read_bits(3)? as usize;
                if lengths.len().saturating_add(repetitions) > target_count {
                    return Err(Error::InvalidCodeLengths);
                }
                lengths.extend(std::iter::repeat_n(0, repetitions));
            }
            18 => {
                let repetitions = 11 + reader.read_bits(7)? as usize;
                if lengths.len().saturating_add(repetitions) > target_count {
                    return Err(Error::InvalidCodeLengths);
                }
                lengths.extend(std::iter::repeat_n(0, repetitions));
            }
            _ => return Err(Error::InvalidSymbol),
        }
    }
    if lengths[END_OF_BLOCK] == 0 {
        return Err(Error::InvalidCodeLengths);
    }
    if distance_count > 30
        && lengths[literal_count + 30..]
            .iter()
            .any(|&length| length != 0)
    {
        return Err(Error::InvalidCodeLengths);
    }

    let literal = Huffman::from_lengths(&lengths[..literal_count])?;
    let distance = Huffman::from_lengths(&lengths[literal_count..])?;
    Ok((literal, distance))
}

struct DecodedBuffer {
    marked: Vec<Symbol>,
    clean: Vec<u8>,
}

impl DecodedBuffer {
    const fn new() -> Self {
        Self {
            marked: Vec::new(),
            clean: Vec::new(),
        }
    }

    #[cfg(test)]
    fn with_capacity(capacity: usize) -> Self {
        Self {
            marked: Vec::with_capacity(capacity),
            clean: Vec::new(),
        }
    }

    #[inline(always)]
    fn len(&self) -> usize {
        self.marked.len() + self.clean.len()
    }

    fn finish(self) -> ChunkOutput {
        ChunkOutput {
            marked: MarkerBuffer::new(self.marked),
            clean: self.clean,
            backend_tail: Vec::new(),
        }
    }

    fn from_marked(marked: Vec<Symbol>) -> Self {
        Self {
            marked,
            clean: Vec::new(),
        }
    }
}

/// Appends an LZ77 match to speculative output without maintaining a second
/// 32 KiB history ring.
///
/// Output before the chunk start is represented by the same marker values as
/// [`History::unknown`]. Once a match points into output produced by this
/// chunk, `Vec::extend_from_within` copies whole runs. The final doubling loop
/// preserves DEFLATE overlap semantics while reducing short-distance matches
/// to logarithmically many bulk copies.
fn copy_match_unknown(
    output: &mut Vec<Symbol>,
    distance: usize,
    length: usize,
    output_limit: usize,
) -> Result<(), Error> {
    if distance == 0 || distance > WINDOW_SIZE {
        return Err(Error::InvalidDistance);
    }
    if length > output_limit.saturating_sub(output.len()) {
        return Err(Error::OutputLimit);
    }

    output.reserve(length);
    let match_start = output.len();
    let mut copied = 0;

    if distance > match_start {
        let from_window = (distance - match_start).min(length);
        let first_index = WINDOW_SIZE + match_start - distance;
        output.extend(
            (0..from_window)
                .map(|offset| Symbol::from_encoded((WINDOW_SIZE + first_index + offset) as u16)),
        );
        copied = from_window;
    }

    let first_period = distance.min(length);
    if copied < first_period {
        let count = first_period - copied;
        let source = output.len() - distance;
        output.extend_from_within(source..source + count);
        copied += count;
    }

    while copied < length {
        let count = copied.min(length - copied);
        output.extend_from_within(match_start..match_start + count);
        copied += count;
    }
    Ok(())
}

fn decode_compressed_block_unknown(
    reader: &mut BitReader<'_>,
    literal: &Huffman,
    distance: &Huffman,
    output: &mut Vec<Symbol>,
    output_limit: usize,
) -> Result<(), Error> {
    loop {
        let symbol = literal.decode(reader)?;
        match symbol {
            0..=255 => {
                if output.len() >= output_limit {
                    return Err(Error::OutputLimit);
                }
                output.push(Symbol::literal(symbol as u8));
            }
            END_OF_BLOCK => return Ok(()),
            257..=285 => {
                let length_index = symbol - 257;
                let length = LENGTH_BASE[length_index]
                    + reader.read_bits(LENGTH_EXTRA[length_index])? as usize;
                let distance_symbol = distance.decode(reader)?;
                if distance_symbol >= DISTANCE_BASE.len() {
                    return Err(Error::InvalidDistance);
                }
                let copy_distance = DISTANCE_BASE[distance_symbol]
                    + reader.read_bits(DISTANCE_EXTRA[distance_symbol])? as usize;
                copy_match_unknown(output, copy_distance, length, output_limit)?;
            }
            _ => return Err(Error::InvalidSymbol),
        }
    }
}

fn decode_stored_block_unknown(
    reader: &mut BitReader<'_>,
    output: &mut Vec<Symbol>,
    output_limit: usize,
) -> Result<(), Error> {
    reader.align_to_byte();
    let length = reader.read_bits(16)? as u16;
    let complement = reader.read_bits(16)? as u16;
    if length != !complement {
        return Err(Error::InvalidStoredLength);
    }
    if usize::from(length) > output_limit.saturating_sub(output.len()) {
        return Err(Error::OutputLimit);
    }
    output.reserve(usize::from(length));
    for _ in 0..length {
        output.push(Symbol::literal(reader.read_bits(8)? as u8));
    }
    Ok(())
}

fn marker_free_window(output: &[Symbol]) -> Option<Window> {
    let window = output.get(output.len().checked_sub(WINDOW_SIZE)?..)?;
    if window.iter().any(|symbol| symbol.as_literal().is_none()) {
        return None;
    }
    let bytes = window
        .iter()
        .map(|symbol| symbol.as_literal().expect("window was checked as literal"))
        .collect();
    Some(Window::new(bytes).expect("DEFLATE window has exactly 32 KiB"))
}

fn decode_to_estimated_boundary_unknown(
    bytes: &[u8],
    start_bit: usize,
    estimated_stop_bit: usize,
    maximum_output: usize,
    marked: &mut Vec<Symbol>,
) -> Result<Chunk, Error> {
    if estimated_stop_bit <= start_bit {
        return Err(Error::BoundaryMismatch);
    }
    let mut reader = BitReader::at(bytes, start_bit)?;
    // Reuse capacity across failed structural candidates in the same task.
    marked.clear();

    loop {
        let reached_stream_end = reader.read_bits(1)? != 0;
        match reader.read_bits(2)? {
            0 => decode_stored_block_unknown(&mut reader, marked, maximum_output)?,
            1 => {
                let (literal, distance) = fixed_trees();
                decode_compressed_block_unknown(
                    &mut reader,
                    literal,
                    distance,
                    marked,
                    maximum_output,
                )?;
            }
            2 => {
                let (literal, distance) = dynamic_trees(&mut reader)?;
                decode_compressed_block_unknown(
                    &mut reader,
                    &literal,
                    &distance,
                    marked,
                    maximum_output,
                )?;
            }
            _ => return Err(Error::InvalidBlockType),
        }

        if reached_stream_end {
            reader.align_to_byte();
            return Ok(Chunk {
                start_bit,
                end_bit: reader.position(),
                output: DecodedBuffer::from_marked(std::mem::take(marked)).finish(),
                reached_stream_end: true,
                backend_continuation: None,
            });
        }
        if reader.position() >= estimated_stop_bit {
            return Ok(Chunk {
                start_bit,
                end_bit: reader.position(),
                output: DecodedBuffer::from_marked(std::mem::take(marked)).finish(),
                reached_stream_end: false,
                backend_continuation: None,
            });
        }
        if let Some(window) = marker_free_window(marked) {
            return Ok(Chunk {
                start_bit,
                end_bit: reader.position(),
                output: DecodedBuffer::from_marked(std::mem::take(marked)).finish(),
                reached_stream_end: false,
                backend_continuation: Some(window),
            });
        }
    }
}

#[inline(always)]
fn emit_marked(
    symbol: Symbol,
    history: &mut History,
    output: &mut DecodedBuffer,
    output_limit: usize,
) -> Result<(), Error> {
    if output.len() >= output_limit {
        return Err(Error::OutputLimit);
    }
    history.push(symbol);
    output.marked.push(symbol);
    Ok(())
}

/// LZ77 match copy on the marked (Symbol) path with bulk copies.
///
/// Same overlap-aware strategy as [`copy_match_unknown`]: the first period is
/// filled from history and/or `output.marked`, then geometric doubling via
/// `extend_from_within` expands overlapping matches. History is bulk-updated
/// once after the match is fully materialised in the output buffer.
#[inline(always)]
fn copy_match_marked(
    length: usize,
    distance: usize,
    history: &mut History,
    output: &mut DecodedBuffer,
    output_limit: usize,
) -> Result<(), Error> {
    if distance == 0 || distance > WINDOW_SIZE || distance > history.length {
        return Err(Error::InvalidDistance);
    }
    if length > output_limit.saturating_sub(output.len()) {
        return Err(Error::OutputLimit);
    }
    if length == 0 {
        return Ok(());
    }

    let marked = &mut output.marked;
    marked.reserve(length);
    let match_start = marked.len();
    let mut copied = 0;

    // Portion that still lives only in the ring (predecessor markers or
    // pre-marked-buffer history), not yet present in `marked`.
    if distance > match_start {
        let from_history = (distance - match_start).min(length);
        history.append_symbols_at_distance(distance, from_history, marked)?;
        copied = from_history;
    }

    // Complete the first non-overlapping period from already-emitted output.
    let first_period = distance.min(length);
    if copied < first_period {
        let count = first_period - copied;
        let source = marked.len() - distance;
        marked.extend_from_within(source..source + count);
        copied += count;
    }

    // Geometric doubling preserves DEFLATE overlap semantics (e.g. d=1 RLE).
    while copied < length {
        let count = copied.min(length - copied);
        marked.extend_from_within(match_start..match_start + count);
        copied += count;
    }

    history.push_symbols(&marked[match_start..]);
    Ok(())
}

/// LZ77 match copy on the marker-free clean path with bulk copies.
///
/// When the match source lies entirely in `output.clean` (`distance <=
/// clean.len()`), this is pure `extend_from_within` plus geometric doubling —
/// the same strategy as [`copy_match_unknown`]. When the match reaches into the
/// 32 KiB history ring (clean is only the post-marker-drain suffix), the first
/// period segment is bulk-copied from the ring, then the same doubling path
/// takes over. History is updated once from the newly appended clean bytes.
#[inline(always)]
fn copy_match_clean(
    length: usize,
    distance: usize,
    history: &mut History,
    output: &mut DecodedBuffer,
    output_limit: usize,
) -> Result<(), Error> {
    debug_assert!(!history.contains_markers());
    if distance == 0 || distance > WINDOW_SIZE || distance > history.length {
        return Err(Error::InvalidDistance);
    }
    if length > output_limit.saturating_sub(output.len()) {
        return Err(Error::OutputLimit);
    }
    if length == 0 {
        return Ok(());
    }

    let clean = &mut output.clean;
    clean.reserve(length);
    let match_start = clean.len();
    let mut copied = 0;

    // Clean only holds post-drain bytes; longer distances pull from the ring.
    if distance > match_start {
        let from_history = (distance - match_start).min(length);
        history.append_bytes_at_distance(distance, from_history, clean)?;
        copied = from_history;
    }

    let first_period = distance.min(length);
    if copied < first_period {
        let count = first_period - copied;
        let source = clean.len() - distance;
        clean.extend_from_within(source..source + count);
        copied += count;
    }

    while copied < length {
        let count = copied.min(length - copied);
        clean.extend_from_within(match_start..match_start + count);
        copied += count;
    }

    history.push_clean_bytes(&clean[match_start..]);
    Ok(())
}

fn decode_compressed_block(
    reader: &mut BitReader<'_>,
    literal: &Huffman,
    distance: &Huffman,
    history: &mut History,
    output: &mut DecodedBuffer,
    output_limit: usize,
) -> Result<(), Error> {
    if !history.contains_markers() {
        return decode_compressed_block_clean(
            reader,
            literal,
            distance,
            history,
            output,
            output_limit,
        );
    }
    loop {
        let symbol = literal.decode(reader)?;
        match symbol {
            0..=255 => emit_marked(Symbol::literal(symbol as u8), history, output, output_limit)?,
            END_OF_BLOCK => return Ok(()),
            257..=285 => {
                let length_index = symbol - 257;
                let length = LENGTH_BASE[length_index]
                    + reader.read_bits(LENGTH_EXTRA[length_index])? as usize;
                let distance_symbol = distance.decode(reader)?;
                if distance_symbol >= DISTANCE_BASE.len() {
                    return Err(Error::InvalidDistance);
                }
                let copy_distance = DISTANCE_BASE[distance_symbol]
                    + reader.read_bits(DISTANCE_EXTRA[distance_symbol])? as usize;
                copy_match_marked(length, copy_distance, history, output, output_limit)?;
            }
            _ => return Err(Error::InvalidSymbol),
        }
        if !history.contains_markers() {
            return decode_compressed_block_clean(
                reader,
                literal,
                distance,
                history,
                output,
                output_limit,
            );
        }
    }
}

#[inline(always)]
fn emit_clean_unchecked(byte: u8, history: &mut History, output: &mut DecodedBuffer) {
    output.clean.push(byte);
    history.push_clean(byte);
}

fn decode_compressed_block_clean(
    reader: &mut BitReader<'_>,
    literal: &Huffman,
    distance: &Huffman,
    history: &mut History,
    output: &mut DecodedBuffer,
    output_limit: usize,
) -> Result<(), Error> {
    debug_assert!(!history.contains_markers());
    loop {
        let symbol = literal.decode(reader)?;
        match symbol {
            0..=255 => {
                if output.len() >= output_limit {
                    return Err(Error::OutputLimit);
                }
                emit_clean_unchecked(symbol as u8, history, output);
            }
            END_OF_BLOCK => return Ok(()),
            257..=285 => {
                let length_index = symbol - 257;
                let length = LENGTH_BASE[length_index]
                    + reader.read_bits(LENGTH_EXTRA[length_index])? as usize;
                let distance_symbol = distance.decode(reader)?;
                if distance_symbol >= DISTANCE_BASE.len() {
                    return Err(Error::InvalidDistance);
                }
                let copy_distance = DISTANCE_BASE[distance_symbol]
                    + reader.read_bits(DISTANCE_EXTRA[distance_symbol])? as usize;
                copy_match_clean(length, copy_distance, history, output, output_limit)?;
            }
            _ => return Err(Error::InvalidSymbol),
        }
    }
}

fn decode_stored_block(
    reader: &mut BitReader<'_>,
    history: &mut History,
    output: &mut DecodedBuffer,
    output_limit: usize,
) -> Result<(), Error> {
    reader.align_to_byte();
    let length = reader.read_bits(16)? as u16;
    let complement = reader.read_bits(16)? as u16;
    if length != !complement {
        return Err(Error::InvalidStoredLength);
    }
    for _ in 0..length {
        let byte = reader.read_bits(8)? as u8;
        if history.contains_markers() {
            emit_marked(Symbol::literal(byte), history, output, output_limit)?;
        } else {
            if output.len() >= output_limit {
                return Err(Error::OutputLimit);
            }
            emit_clean_unchecked(byte, history, output);
        }
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) enum InitialHistory<'a> {
    Unknown,
    #[cfg(test)]
    Empty,
    #[allow(dead_code)]
    Known(&'a Window),
}

#[derive(Debug)]
pub(crate) struct ChunkOutput {
    marked: MarkerBuffer,
    clean: Vec<u8>,
    backend_tail: Vec<u8>,
}

pub(crate) type ResolvedParts = (Vec<u8>, Vec<u8>, Vec<u8>);

impl ChunkOutput {
    pub(crate) fn from_clean(bytes: Vec<u8>) -> Self {
        Self {
            marked: MarkerBuffer::new(Vec::new()),
            clean: bytes,
            backend_tail: Vec::new(),
        }
    }

    /// Resolves marked symbols into bytes and returns emptied symbol capacity.
    ///
    /// The `Vec<Symbol>` is always cleared and returned (success or error) so
    /// the resolving worker can recycle it into local scratch. Only call after
    /// the worker has exclusive ownership of this [`ChunkOutput`] (stolen from
    /// the resolve queue) — never while the buffer is still enqueued.
    pub(crate) fn resolve_parts(
        self,
        window: &Window,
    ) -> (
        Result<ResolvedParts, super::marker::MarkerError>,
        Vec<Symbol>,
    ) {
        let (marked, emptied_symbols) = self.marked.resolve_with_symbols(window);
        (
            marked.map(|bytes| (bytes, self.clean, self.backend_tail)),
            emptied_symbols,
        )
    }

    #[cfg(test)]
    pub(crate) fn resolve(self, window: &Window) -> Result<Vec<u8>, super::marker::MarkerError> {
        let (parts, _emptied_symbols) = self.resolve_parts(window);
        let (mut result, clean, backend_tail) = parts?;
        result.extend_from_slice(&clean);
        result.extend_from_slice(&backend_tail);
        Ok(result)
    }

    pub(crate) fn len(&self) -> usize {
        self.marked.symbols().len() + self.clean.len() + self.backend_tail.len()
    }

    pub(crate) fn window_after(
        &self,
        predecessor: &Window,
    ) -> Result<Window, super::marker::MarkerError> {
        let total = self.len();
        let skip = total.saturating_sub(WINDOW_SIZE);
        let marked_end = self.marked.len();
        let clean_end = marked_end + self.clean.len();
        let mut suffix = Vec::with_capacity(total.min(WINDOW_SIZE));

        if skip < marked_end {
            self.marked
                .append_resolved_range(skip..marked_end, &mut suffix, predecessor)?;
        }
        if skip < clean_end {
            let clean_start = skip.saturating_sub(marked_end);
            suffix.extend_from_slice(&self.clean[clean_start..]);
        }
        let backend_start = skip.saturating_sub(clean_end);
        suffix.extend_from_slice(&self.backend_tail[backend_start..]);
        Ok(predecessor.advanced_by(&suffix))
    }

    pub(crate) fn append_clean(&mut self, bytes: Vec<u8>) {
        debug_assert!(
            self.backend_tail.is_empty(),
            "a chunk can have only one backend continuation"
        );
        self.backend_tail = bytes;
    }

    /// Splits owned buffers out for worker-local capacity recycling.
    ///
    /// Successful chunks transfer ownership to the coordinator/resolve path; the
    /// worker must not reuse those allocations until they are fully consumed.
    /// This helper is only for failed attempts that already built a
    /// [`ChunkOutput`] (for example a tail inflate that fails after a native
    /// decode success).
    pub(crate) fn into_recycle_parts(self) -> (Vec<Symbol>, Vec<u8>, Vec<u8>) {
        (self.marked.into_symbols(), self.clean, self.backend_tail)
    }
}

#[derive(Debug)]
pub(crate) struct Chunk {
    pub(crate) start_bit: usize,
    pub(crate) end_bit: usize,
    pub(crate) output: ChunkOutput,
    pub(crate) reached_stream_end: bool,
    pub(crate) backend_continuation: Option<Window>,
}

#[cfg(test)]
fn decode_chunk(
    bytes: &[u8],
    start_bit: usize,
    initial_history: InitialHistory<'_>,
    target_output: usize,
    maximum_output: usize,
) -> Result<Chunk, Error> {
    let mut reader = BitReader::at(bytes, start_bit)?;
    let mut history = match initial_history {
        InitialHistory::Unknown => History::unknown(),
        #[cfg(test)]
        InitialHistory::Empty => History::empty(),
        InitialHistory::Known(window) => History::from_window(window),
    };
    let mut output = DecodedBuffer::with_capacity(target_output.min(maximum_output));
    let mut reached_stream_end;

    loop {
        reached_stream_end = reader.read_bits(1)? != 0;
        match reader.read_bits(2)? {
            0 => decode_stored_block(&mut reader, &mut history, &mut output, maximum_output)?,
            1 => {
                let (literal, distance) = fixed_trees();
                decode_compressed_block(
                    &mut reader,
                    literal,
                    distance,
                    &mut history,
                    &mut output,
                    maximum_output,
                )?;
            }
            2 => {
                let (literal, distance) = dynamic_trees(&mut reader)?;
                decode_compressed_block(
                    &mut reader,
                    &literal,
                    &distance,
                    &mut history,
                    &mut output,
                    maximum_output,
                )?;
            }
            _ => return Err(Error::InvalidBlockType),
        }
        if reached_stream_end || output.len() >= target_output {
            break;
        }
    }

    if reached_stream_end {
        reader.align_to_byte();
    }
    Ok(Chunk {
        start_bit,
        end_bit: reader.position(),
        output: output.finish(),
        reached_stream_end,
        backend_continuation: None,
    })
}

/// Finds the next structurally validated non-final dynamic-Huffman boundary.
///
/// Stored blocks are handled by the dedicated stored-stream path. Treating a
/// chance LEN/NLEN match as an independent speculative boundary has a much
/// higher false-positive rate than a fully validated dynamic tree.
pub(crate) fn find_next_structural_candidate(
    bytes: &[u8],
    start_bit: usize,
    end_bit: usize,
) -> Option<usize> {
    let end = end_bit.min(bytes.len().saturating_mul(8));
    let first_byte = start_bit / 8;
    let last_byte = end.div_ceil(8).min(bytes.len());
    for byte_offset in first_byte..last_byte {
        let low = u16::from(bytes[byte_offset]);
        let high = bytes
            .get(byte_offset + 1)
            .map_or(0, |byte| u16::from(*byte));
        let header_window = low | (high << 8);
        for bit_in_byte in 0..8 {
            let offset = byte_offset * 8 + bit_in_byte;
            if offset < start_bit {
                continue;
            }
            let header = (header_window >> bit_in_byte) & 0b111;
            let structurally_valid = match header {
                // Non-final dynamic block.
                0b100 if offset.saturating_add(13) < end => {
                    let mut fields =
                        BitReader::at(bytes, offset + 3).expect("offset was range checked");
                    let literal_delta = fields.read_bits(5).unwrap_or(31);
                    let distance_delta = fields.read_bits(5).unwrap_or(31);
                    literal_delta <= 29
                        && distance_delta <= 29
                        && valid_precode_shape(bytes, offset)
                        && {
                            let mut tree =
                                BitReader::at(bytes, offset + 3).expect("offset was range checked");
                            dynamic_trees(&mut tree).is_ok()
                        }
                }
                _ => false,
            };
            if structurally_valid {
                return Some(offset);
            }
        }
    }
    None
}

/// Finds strongly validated, non-final dynamic-Huffman block candidates.
#[cfg(test)]
pub(crate) fn find_dynamic_candidates(
    bytes: &[u8],
    start_bit: usize,
    end_bit: usize,
) -> Vec<usize> {
    let mut candidates = Vec::new();
    let end = end_bit.min(bytes.len().saturating_mul(8));
    let first_byte = start_bit / 8;
    let last_byte = end.div_ceil(8).min(bytes.len());
    for byte_offset in first_byte..last_byte {
        let low = u16::from(bytes[byte_offset]);
        let high = bytes
            .get(byte_offset + 1)
            .map_or(0, |byte| u16::from(*byte));
        let header_window = low | (high << 8);
        for bit_in_byte in 0..8 {
            let offset = byte_offset * 8 + bit_in_byte;
            if offset < start_bit || offset.saturating_add(13) >= end {
                continue;
            }
            // 0b100 means BFINAL=0 followed by BTYPE=10 in stream order.
            if ((header_window >> bit_in_byte) & 0b111) != 0b100 {
                continue;
            }
            let mut header = BitReader::at(bytes, offset + 3).expect("offset was range checked");
            let literal_delta = header.read_bits(5).unwrap_or(31);
            let distance_delta = header.read_bits(5).unwrap_or(31);
            if literal_delta > 29 || distance_delta > 29 || !valid_precode_shape(bytes, offset) {
                continue;
            }
            // Reparse the complete tree. Canonical-tree validation makes
            // surviving random false positives rare without decoding and then
            // discarding an arbitrary output prefix.
            let mut validation =
                BitReader::at(bytes, offset + 3).expect("offset was range checked");
            if dynamic_trees(&mut validation).is_ok() {
                candidates.push(offset);
            }
        }
    }
    candidates
}

fn valid_precode_shape(bytes: &[u8], block_offset: usize) -> bool {
    const PRECODE_BITS: usize = 4 + 19 * 3;
    let Some(precode_offset) = block_offset.checked_add(13) else {
        return false;
    };
    if precode_offset
        .checked_add(PRECODE_BITS)
        .is_none_or(|end| end > bytes.len().saturating_mul(8))
    {
        return false;
    }
    let byte_offset = precode_offset / 8;
    let shift = precode_offset % 8;
    let low = word_at(bytes, byte_offset);
    let bits = if shift == 0 {
        low
    } else {
        let high = u64::from(bytes.get(byte_offset + 8).copied().unwrap_or(0));
        (low >> shift) | (high << (u64::BITS as usize - shift))
    };
    let precode_count = 4 + (bits & 0b1111) as usize;
    let code_lengths = bits >> 4;
    let mut counts = [0_u8; 8];
    let mut used = 0_u8;
    for index in 0..precode_count {
        let length = ((code_lengths >> (index * 3)) & 0b111) as usize;
        if length != 0 {
            counts[length] += 1;
            used += 1;
        }
    }
    if used == 0 {
        return false;
    }
    let mut remaining = 1_i16;
    for count in counts.iter().skip(1) {
        remaining = remaining * 2 - i16::from(*count);
        if remaining < 0 {
            return false;
        }
    }
    // RFC 1951 code-length alphabets must be complete. Accept the degenerate
    // one-symbol form defensively; the complete parser remains authoritative.
    remaining == 0 || used == 1
}

/// Decodes complete blocks until reaching the first boundary at or beyond an
/// estimated compressed offset.
///
/// This is the rapidgzip partitioning primitive: adjacent workers search from
/// the same estimated grid point, so the predecessor's end boundary is the
/// successor's independently discovered start boundary.
pub(crate) fn decode_to_estimated_boundary(
    bytes: &[u8],
    start_bit: usize,
    estimated_stop_bit: usize,
    initial_history: InitialHistory<'_>,
    maximum_output: usize,
    marked_scratch: &mut Vec<Symbol>,
) -> Result<Chunk, Error> {
    if matches!(initial_history, InitialHistory::Unknown) {
        return decode_to_estimated_boundary_unknown(
            bytes,
            start_bit,
            estimated_stop_bit,
            maximum_output,
            marked_scratch,
        );
    }
    if estimated_stop_bit <= start_bit {
        return Err(Error::BoundaryMismatch);
    }
    let mut reader = BitReader::at(bytes, start_bit)?;
    let mut history = match initial_history {
        InitialHistory::Unknown => History::unknown(),
        #[cfg(test)]
        InitialHistory::Empty => History::empty(),
        InitialHistory::Known(window) => History::from_window(window),
    };
    let mut output = DecodedBuffer::new();

    loop {
        let reached_stream_end = reader.read_bits(1)? != 0;
        match reader.read_bits(2)? {
            0 => decode_stored_block(&mut reader, &mut history, &mut output, maximum_output)?,
            1 => {
                let (literal, distance) = fixed_trees();
                decode_compressed_block(
                    &mut reader,
                    literal,
                    distance,
                    &mut history,
                    &mut output,
                    maximum_output,
                )?;
            }
            2 => {
                let (literal, distance) = dynamic_trees(&mut reader)?;
                decode_compressed_block(
                    &mut reader,
                    &literal,
                    &distance,
                    &mut history,
                    &mut output,
                    maximum_output,
                )?;
            }
            _ => return Err(Error::InvalidBlockType),
        }

        if reached_stream_end {
            reader.align_to_byte();
            return Ok(Chunk {
                start_bit,
                end_bit: reader.position(),
                output: output.finish(),
                reached_stream_end: true,
                backend_continuation: None,
            });
        }
        if reader.position() >= estimated_stop_bit {
            return Ok(Chunk {
                start_bit,
                end_bit: reader.position(),
                output: output.finish(),
                reached_stream_end: false,
                backend_continuation: None,
            });
        }
        if !history.contains_markers() {
            return Ok(Chunk {
                start_bit,
                end_bit: reader.position(),
                output: output.finish(),
                reached_stream_end: false,
                backend_continuation: Some(history.literal_window()),
            });
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{
        ChunkOutput, DecodedBuffer, Error, History, InitialHistory, Symbol, WINDOW_SIZE,
        copy_match_clean, copy_match_marked, copy_match_unknown, decode_chunk,
        find_dynamic_candidates,
    };
    use crate::parallel::{MarkerBuffer, Window};

    /// Seed history with `history_only` (not in clean) followed by `clean`.
    fn seed_clean_state(history_only: &[u8], clean: &[u8]) -> (History, DecodedBuffer) {
        let mut history = History::empty();
        for &byte in history_only {
            history.push_clean(byte);
        }
        for &byte in clean {
            history.push_clean(byte);
        }
        let output = DecodedBuffer {
            marked: Vec::new(),
            clean: clean.to_vec(),
        };
        (history, output)
    }

    /// Naive byte-by-byte oracle matching the old clean-path loop.
    fn naive_copy_match_clean(
        length: usize,
        distance: usize,
        history: &mut History,
        output: &mut DecodedBuffer,
        output_limit: usize,
    ) -> Result<(), Error> {
        if length > output_limit.saturating_sub(output.len()) {
            return Err(Error::OutputLimit);
        }
        for _ in 0..length {
            let copied = history.get_distance_byte(distance)?;
            output.clean.push(copied);
            history.push_clean(copied);
        }
        Ok(())
    }

    /// Naive marked-path oracle.
    fn naive_copy_match_marked(
        length: usize,
        distance: usize,
        history: &mut History,
        output: &mut DecodedBuffer,
        output_limit: usize,
    ) -> Result<(), Error> {
        if length > output_limit.saturating_sub(output.len()) {
            return Err(Error::OutputLimit);
        }
        for _ in 0..length {
            let copied = history.get_distance(distance)?;
            history.push(copied);
            output.marked.push(copied);
        }
        Ok(())
    }

    fn hex(text: &str) -> Vec<u8> {
        text.as_bytes()
            .chunks_exact(2)
            .map(|pair| {
                let pair = std::str::from_utf8(pair).unwrap();
                u8::from_str_radix(pair, 16).unwrap()
            })
            .collect()
    }

    #[test]
    fn decodes_stored_block_from_empty_member_window() {
        let encoded = [1, 5, 0, 250, 255, b'h', b'e', b'l', b'l', b'o'];
        let chunk = decode_chunk(&encoded, 0, InitialHistory::Empty, 1, 1024).unwrap();
        assert!(chunk.reached_stream_end);
        assert_eq!(chunk.start_bit, 0);
        assert_eq!(chunk.output.resolve(&Window::empty()).unwrap(), b"hello");
    }

    #[test]
    fn decodes_final_dynamic_block() {
        let encoded = hex(
            "edc3410900000804b06c870f0b5cff2c82393658661b5555555555555555555555555555555555555555555555555555555555555555555555555555f51f",
        );
        let chunk = decode_chunk(&encoded, 0, InitialHistory::Empty, usize::MAX, 50_000).unwrap();
        assert!(chunk.reached_stream_end);
        assert_eq!(
            chunk.output.resolve(&Window::empty()).unwrap(),
            b"ACGT".repeat(10_000)
        );
    }

    #[test]
    fn finds_and_decodes_nonfinal_dynamic_candidate() {
        let encoded = hex(
            "ecc3410900000804b06c870f0b5cff2c82393658661b5555555555555555555555555555555555555555555555555555555555555555555555555555f51f000000ffffedc3310d00000803306d640706f0af856336daa4b7195555555555555555555555555555555555555555555555555555555555555555555555555555b51f",
        );
        let candidates = find_dynamic_candidates(&encoded, 0, encoded.len() * 8);
        assert_eq!(candidates.first(), Some(&0));
        let chunk =
            decode_chunk(&encoded, candidates[0], InitialHistory::Unknown, 1, 50_000).unwrap();
        assert!(!chunk.reached_stream_end);
        assert!(chunk.end_bit > chunk.start_bit);
        let predecessor = Window::new((0..WINDOW_SIZE).map(|index| index as u8).collect()).unwrap();
        assert_eq!(
            chunk.output.resolve(&predecessor).unwrap(),
            b"ACGT".repeat(10_000)
        );
    }

    #[test]
    fn known_history_supports_overlapping_copies() {
        // Fixed-Huffman final block for "hellohellohello".
        let encoded = hex("cb48cdc9c9cf801300");
        let empty = Window::empty();
        let chunk = decode_chunk(&encoded, 0, InitialHistory::Known(&empty), 1, 1024).unwrap();
        assert_eq!(chunk.output.resolve(&empty).unwrap(), b"hellohellohello");
    }

    #[test]
    fn bulk_unknown_match_matches_a_naive_window_for_overlap_and_wraparound() {
        let prefixes = [0, 1, 7, 257, WINDOW_SIZE - 1, WINDOW_SIZE, WINDOW_SIZE + 19];
        let lengths = [1, 2, 7, 31, 258];
        for prefix_length in prefixes {
            let prefix: Vec<_> = (0..prefix_length)
                .map(|index| Symbol::literal(index as u8))
                .collect();
            for distance in [1, 2, 7, 31, 257, 4096, WINDOW_SIZE] {
                for length in lengths {
                    let mut expected_history: Vec<_> = (0..WINDOW_SIZE)
                        .map(|index| Symbol::from_encoded((WINDOW_SIZE + index) as u16))
                        .chain(prefix.iter().copied())
                        .collect();
                    for _ in 0..length {
                        let source = expected_history.len() - distance;
                        let symbol = expected_history[source];
                        expected_history.push(symbol);
                    }
                    let expected = &expected_history[WINDOW_SIZE..];

                    let mut actual = prefix.clone();
                    copy_match_unknown(&mut actual, distance, length, usize::MAX).unwrap();
                    assert_eq!(
                        actual, expected,
                        "prefix={prefix_length} d={distance} l={length}"
                    );
                }
            }
        }
    }

    #[test]
    fn bulk_clean_match_matches_naive_for_overlap_history_and_ring_wrap() {
        // history_only lengths exercise: fully in clean, straddling history+clean,
        // fully in history (empty clean), and distances near the 32 KiB ring edge.
        let history_only_lens = [0, 1, 7, 31, 257, 4096, WINDOW_SIZE - 8, WINDOW_SIZE];
        let clean_lens = [0, 1, 7, 31, 258, 1024, 4096];
        let distances = [1, 2, 3, 7, 31, 100, 257, 4096, WINDOW_SIZE - 1, WINDOW_SIZE];
        let lengths = [1, 2, 3, 7, 31, 100, 258];

        for history_only_len in history_only_lens {
            for clean_len in clean_lens {
                let total = history_only_len + clean_len;
                if total == 0 {
                    continue;
                }
                // Cap combined seed so history ring semantics stay well-defined.
                if total > WINDOW_SIZE {
                    continue;
                }
                let history_only: Vec<u8> = (0..history_only_len)
                    .map(|i| (i.wrapping_mul(13) + 1) as u8)
                    .collect();
                let clean: Vec<u8> = (0..clean_len)
                    .map(|i| (i.wrapping_mul(17) + 3) as u8)
                    .collect();

                for distance in distances {
                    if distance > total {
                        continue;
                    }
                    for length in lengths {
                        let (mut expected_hist, mut expected_out) =
                            seed_clean_state(&history_only, &clean);
                        let (mut actual_hist, mut actual_out) =
                            seed_clean_state(&history_only, &clean);

                        naive_copy_match_clean(
                            length,
                            distance,
                            &mut expected_hist,
                            &mut expected_out,
                            usize::MAX,
                        )
                        .unwrap();
                        copy_match_clean(
                            length,
                            distance,
                            &mut actual_hist,
                            &mut actual_out,
                            usize::MAX,
                        )
                        .unwrap();

                        assert_eq!(
                            actual_out.clean, expected_out.clean,
                            "hist_only={history_only_len} clean={clean_len} d={distance} l={length}"
                        );
                        // History write heads and contents must match.
                        assert_eq!(actual_hist.next, expected_hist.next);
                        assert_eq!(actual_hist.length, expected_hist.length);
                        assert_eq!(actual_hist.symbols, expected_hist.symbols);
                    }
                }
            }
        }
    }

    #[test]
    fn clean_match_distance_one_run_length_expansion() {
        let (mut history, mut output) = seed_clean_state(&[], b"A");
        copy_match_clean(100, 1, &mut history, &mut output, usize::MAX).unwrap();
        assert_eq!(output.clean, vec![b'A'; 101]);
        assert_eq!(history.length, 101);
        assert_eq!(history.get_distance_byte(1).unwrap(), b'A');
    }

    #[test]
    fn clean_match_rejects_invalid_distance_and_output_limit() {
        let (mut history, mut output) = seed_clean_state(b"abcd", &[]);
        assert_eq!(
            copy_match_clean(3, 0, &mut history, &mut output, usize::MAX),
            Err(Error::InvalidDistance)
        );
        assert_eq!(
            copy_match_clean(3, 5, &mut history, &mut output, usize::MAX),
            Err(Error::InvalidDistance)
        );
        assert_eq!(
            copy_match_clean(3, 2, &mut history, &mut output, 2),
            Err(Error::OutputLimit)
        );
    }

    #[test]
    fn clean_match_ring_wrap_source_segment() {
        // Fill the ring so the write head is near zero and a long-distance match
        // source wraps across the end of the symbols array.
        let mut history = History::empty();
        for i in 0..WINDOW_SIZE {
            history.push_clean((i % 251) as u8);
        }
        // Advance so next is small but non-zero: push a few more.
        for i in 0..17 {
            history.push_clean((200 + i) as u8);
        }
        assert_eq!(history.next, 17);
        assert_eq!(history.length, WINDOW_SIZE);

        let mut output = DecodedBuffer {
            marked: Vec::new(),
            clean: Vec::new(),
        };
        let distance = WINDOW_SIZE; // entire window
        let length = 40;
        let mut expected_hist = History {
            symbols: history.symbols,
            length: history.length,
            next: history.next,
            marker_count: history.marker_count,
        };
        let mut expected_out = DecodedBuffer {
            marked: Vec::new(),
            clean: Vec::new(),
        };
        naive_copy_match_clean(
            length,
            distance,
            &mut expected_hist,
            &mut expected_out,
            usize::MAX,
        )
        .unwrap();
        copy_match_clean(length, distance, &mut history, &mut output, usize::MAX).unwrap();
        assert_eq!(output.clean, expected_out.clean);
        assert_eq!(history.symbols, expected_hist.symbols);
        assert_eq!(history.next, expected_hist.next);
    }

    #[test]
    fn bulk_marked_match_matches_naive_for_overlap_and_markers() {
        let prefixes = [0, 1, 7, 31, 257, 1024];
        let distances = [1, 2, 7, 31, 257, 1000, WINDOW_SIZE];
        let lengths = [1, 2, 7, 31, 100, 258];

        for prefix_len in prefixes {
            for distance in distances {
                // Unknown history is full of markers; only distances into that
                // window or into the prefix are valid.
                if distance > WINDOW_SIZE + prefix_len {
                    continue;
                }
                for length in lengths {
                    let mut expected_hist = History::unknown();
                    let mut expected_out = DecodedBuffer {
                        marked: (0..prefix_len).map(|i| Symbol::literal(i as u8)).collect(),
                        clean: Vec::new(),
                    };
                    for &symbol in &expected_out.marked {
                        expected_hist.push(symbol);
                    }

                    let mut actual_hist = History::unknown();
                    let mut actual_out = DecodedBuffer {
                        marked: (0..prefix_len).map(|i| Symbol::literal(i as u8)).collect(),
                        clean: Vec::new(),
                    };
                    for &symbol in &actual_out.marked {
                        actual_hist.push(symbol);
                    }

                    // Distance may still exceed available history only if prefix
                    // is empty and we somehow broke the unknown window — skip
                    // invalid combos the naive path would also reject.
                    let naive = naive_copy_match_marked(
                        length,
                        distance,
                        &mut expected_hist,
                        &mut expected_out,
                        usize::MAX,
                    );
                    let bulk = copy_match_marked(
                        length,
                        distance,
                        &mut actual_hist,
                        &mut actual_out,
                        usize::MAX,
                    );
                    assert_eq!(
                        bulk, naive,
                        "prefix={prefix_len} d={distance} l={length} result"
                    );
                    if naive.is_ok() {
                        assert_eq!(
                            actual_out.marked, expected_out.marked,
                            "prefix={prefix_len} d={distance} l={length}"
                        );
                        assert_eq!(actual_hist.next, expected_hist.next);
                        assert_eq!(actual_hist.length, expected_hist.length);
                        assert_eq!(actual_hist.marker_count, expected_hist.marker_count);
                        assert_eq!(actual_hist.symbols, expected_hist.symbols);
                    }
                }
            }
        }
    }

    #[test]
    fn unresolved_suffix_produces_the_same_window_as_full_resolution() {
        let predecessor = Window::new(
            (0..WINDOW_SIZE)
                .map(|index| index.wrapping_mul(17) as u8)
                .collect(),
        )
        .unwrap();
        for (marked_len, clean_len, backend_len) in [
            (127, 0, 0),
            (40_000, 0, 0),
            (40_000, 9000, 0),
            (9000, 9000, 20_000),
        ] {
            let make_output = || {
                let marked = (0..marked_len)
                    .map(|index| {
                        if index % 3 == 0 {
                            Symbol::marker(index % WINDOW_SIZE).unwrap()
                        } else {
                            Symbol::literal(index as u8)
                        }
                    })
                    .collect();
                ChunkOutput {
                    marked: MarkerBuffer::new(marked),
                    clean: (0..clean_len).map(|index| (index * 3) as u8).collect(),
                    backend_tail: (0..backend_len).map(|index| (index * 5) as u8).collect(),
                }
            };

            let actual = make_output().window_after(&predecessor).unwrap();
            let (parts, emptied_symbols) = make_output().resolve_parts(&predecessor);
            let (mut resolved, clean, backend) = parts.unwrap();
            assert!(emptied_symbols.is_empty());
            resolved.extend_from_slice(&clean);
            resolved.extend_from_slice(&backend);
            assert_eq!(actual, predecessor.advanced_by(&resolved));
        }
    }
}
