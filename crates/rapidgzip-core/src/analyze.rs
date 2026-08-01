//! Structural analysis of a compressed stream, block by block.
//!
//! Analysis walks every DEFLATE block and records what it is made of: where it
//! starts to the bit, how its Huffman alphabets are shaped, how much of its
//! output came from literals against back-references, and how far its
//! back-references reach into the preceding window. It is what explains a file
//! that decodes correctly but behaves oddly.
//!
//! The walk is sequential by nature, since every block's statistics depend on
//! the window the previous blocks produced. It decodes with the crate's native
//! DEFLATE decoder rather than zlib, because zlib reports none of this.
//!
//! This module owns the data. Presenting it is the caller's business; the
//! command-line tool prints it in the layout rapidgzip uses.

use crate::index::WINDOW_SIZE;
use crate::parallel::deflate::{
    BitReader, DISTANCE_BASE, DISTANCE_EXTRA, END_OF_BLOCK, Huffman, LENGTH_BASE, LENGTH_EXTRA,
    dynamic_trees_with_lengths, fixed_trees,
};
use crate::{DecodeError, DeflateErrorKind, Format, GzipErrorKind, ReadAt, ZlibErrorKind};

/// How a DEFLATE block encodes its data.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub enum BlockType {
    /// Stored, meaning copied verbatim.
    #[default]
    Uncompressed,
    /// Encoded with the fixed Huffman alphabets from RFC 1951.
    FixedHuffman,
    /// Encoded with alphabets the block declares itself.
    DynamicHuffman,
}

impl BlockType {
    /// Returns the name rapidgzip prints for this type.
    #[must_use]
    pub const fn name(self) -> &'static str {
        match self {
            Self::Uncompressed => "Uncompressed",
            Self::FixedHuffman => "Fixed Huffman",
            Self::DynamicHuffman => "Dynamic Huffman",
        }
    }
}

/// Shape of one Huffman alphabet a block declared.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct AlphabetShape {
    /// Code lengths as declared, including the zeros.
    pub code_lengths: Vec<u8>,
    /// How many lengths the header carried, which for the precode is fewer
    /// than the vector holds.
    pub declared_count: usize,
}

impl AlphabetShape {
    /// Returns the number of symbols with a non-zero code length.
    #[must_use]
    pub fn used_count(&self) -> usize {
        self.code_lengths
            .iter()
            .filter(|&&length| length != 0)
            .count()
    }

    /// Returns the shortest and longest non-zero code length.
    #[must_use]
    pub fn length_range(&self) -> Option<(u8, u8)> {
        let mut lengths = self
            .code_lengths
            .iter()
            .copied()
            .filter(|&length| length != 0);
        let first = lengths.next()?;
        let mut minimum = first;
        let mut maximum = first;
        for length in lengths {
            minimum = minimum.min(length);
            maximum = maximum.max(length);
        }
        Some((minimum, maximum))
    }

    /// Returns how many symbols carry each code length, shortest first.
    #[must_use]
    pub fn counts_by_length(&self) -> Vec<(u8, usize)> {
        let mut counts = [0_usize; 16];
        for &length in &self.code_lengths {
            counts[usize::from(length).min(15)] += 1;
        }
        counts
            .iter()
            .enumerate()
            .filter(|&(_, &count)| count != 0)
            .map(|(length, &count)| (length as u8, count))
            .collect()
    }
}

/// One DEFLATE block, as analysis found it.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct BlockAnalysis {
    /// Whether the block is the final one of its stream.
    pub is_final: bool,
    /// How the block encodes its data.
    pub block_type: BlockType,
    /// Absolute compressed bit offset of the block header.
    pub compressed_offset_in_bits: u64,
    /// Absolute compressed bit offset of the data after the header.
    pub compressed_data_offset_in_bits: u64,
    /// Absolute decompressed byte offset of the block's first byte.
    pub uncompressed_offset_in_bytes: u64,
    /// Compressed size of the block in bits, header included.
    pub compressed_size_in_bits: u64,
    /// Decompressed size of the block in bytes.
    pub uncompressed_size_in_bytes: u64,
    /// One-based index of the block within its stream.
    pub index_in_stream: u64,
    /// Precode alphabet, present only for a dynamic block.
    pub precode: Option<AlphabetShape>,
    /// Distance alphabet, present only for a dynamic block.
    pub distance: Option<AlphabetShape>,
    /// Literal and length alphabet, present only for a dynamic block.
    pub literal: Option<AlphabetShape>,
    /// Literal symbols the block emitted.
    pub literal_symbols: u64,
    /// Back-reference symbols the block emitted.
    pub backreference_symbols: u64,
    /// Bytes the block produced by copying rather than by literal.
    pub copied_bytes: u64,
    /// Farthest a back-reference reached before the block's own start.
    pub farthest_backreference: u64,
    /// Back-references reaching before the block's own start.
    pub window_backreference_count: u64,
    /// The same references after merging overlapping and adjacent ones.
    pub merged_window_backreference_count: u64,
    /// Window bytes some back-reference reached, when the block produced at
    /// least a full window of output.
    pub used_window_symbols: Option<u64>,
    /// Lengths of every back-reference reaching before the block's start.
    pub backreference_lengths: Vec<u64>,
}

/// Header fields of one gzip member.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GzipHeaderFields {
    /// Modification time as stored, zero when unset.
    pub modification_time: u32,
    /// Operating system code.
    pub operating_system: u8,
    /// Extra flags byte.
    pub extra_flags: u8,
    /// Original file name, when the header carried one.
    pub file_name: Option<Vec<u8>>,
    /// Comment, when the header carried one.
    pub comment: Option<Vec<u8>>,
    /// Extra field payload, when the header carried one.
    pub extra: Option<Vec<u8>>,
    /// Header CRC16, when the header carried one.
    pub header_crc16: Option<u16>,
}

/// Header fields of one zlib stream.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct ZlibHeaderFields {
    /// Window size the header declares, in bytes.
    pub window_size: u32,
    /// Compression level code.
    pub compression_level: u8,
    /// Preset dictionary identifier, zero when absent.
    pub dictionary_id: u32,
}

/// Container header of one stream.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum StreamHeader {
    /// A gzip member header.
    Gzip(GzipHeaderFields),
    /// A zlib stream header.
    Zlib(ZlibHeaderFields),
    /// Raw DEFLATE, which has no header.
    RawDeflate,
}

/// Container trailer of one stream.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StreamFooter {
    /// gzip's CRC32 and size modulo 2^32.
    Gzip {
        /// CRC32 of the decompressed member.
        crc32: u32,
        /// Decompressed size modulo 2^32.
        uncompressed_size: u32,
    },
    /// zlib's Adler-32.
    Zlib {
        /// Adler-32 of the decompressed stream.
        adler32: u32,
    },
    /// Raw DEFLATE, which has no trailer.
    None,
}

/// One complete stream inside the analyzed input.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StreamAnalysis {
    /// One-based index of this stream in the input.
    pub index: u64,
    /// Container header, with whatever fields it carried.
    pub header: StreamHeader,
    /// Absolute compressed bit offset of the header.
    pub header_offset_in_bits: u64,
    /// Absolute decompressed byte offset where this stream's output begins.
    pub uncompressed_offset_in_bytes: u64,
    /// Container trailer.
    pub footer: StreamFooter,
    /// Compressed size of the stream in bits, header and trailer included.
    pub compressed_size_in_bits: u64,
    /// Decompressed size of the stream in bytes.
    pub uncompressed_size_in_bytes: u64,
}

/// The complete result of analyzing an input.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Analysis {
    /// Container the input turned out to hold.
    pub format: Format,
    /// Every stream, in file order.
    pub streams: Vec<StreamAnalysis>,
    /// Every block, in file order, across all streams.
    pub blocks: Vec<BlockAnalysis>,
    /// Total decompressed size in bytes.
    pub uncompressed_size_in_bytes: u64,
    /// Total compressed size in bytes.
    pub compressed_size_in_bytes: u64,
}

impl Analysis {
    /// Returns how many blocks used each encoding, in a stable order.
    #[must_use]
    pub fn block_type_counts(&self) -> Vec<(BlockType, u64)> {
        let mut counts = [0_u64; 3];
        for block in &self.blocks {
            let slot = match block.block_type {
                BlockType::Uncompressed => 0,
                BlockType::FixedHuffman => 1,
                BlockType::DynamicHuffman => 2,
            };
            counts[slot] += 1;
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
}

/// Analyzes `source`, which is read into memory in full.
///
/// Analysis is inherently sequential and touches every bit of the input, so it
/// works on a single buffer rather than the positional cursor the decoder
/// uses.
pub(crate) fn analyze_source<R: ReadAt + ?Sized>(
    source: &R,
    format: Format,
) -> Result<Analysis, DecodeError> {
    let length = source
        .len()
        .map_err(|error| DecodeError::input_io(0, error))?;
    let mut bytes = vec![0_u8; usize::try_from(length).unwrap_or(usize::MAX)];
    let mut filled = 0;
    while filled < bytes.len() {
        let read = source
            .read_at(filled as u64, &mut bytes[filled..])
            .map_err(|error| DecodeError::input_io(filled as u64, error))?;
        if read == 0 {
            break;
        }
        filled += read;
    }
    bytes.truncate(filled);
    analyze_bytes(&bytes, format)
}

/// Analyzes a complete compressed buffer.
fn analyze_bytes(bytes: &[u8], format: Format) -> Result<Analysis, DecodeError> {
    let format = match format {
        Format::Auto => crate::format::detect(bytes).unwrap_or(Format::Gzip),
        concrete => concrete,
    };
    let mut analysis = Analysis {
        format,
        compressed_size_in_bytes: bytes.len() as u64,
        ..Analysis::default()
    };

    let mut output: Vec<u8> = Vec::new();
    let mut byte_offset = 0_usize;
    let mut stream_index = 0_u64;

    loop {
        if byte_offset >= bytes.len() {
            break;
        }
        stream_index += 1;
        let header_offset = byte_offset;
        let uncompressed_stream_start = output.len() as u64;
        let header = match format {
            Format::Gzip => StreamHeader::Gzip(parse_gzip_header(bytes, &mut byte_offset)?),
            Format::Zlib => StreamHeader::Zlib(parse_zlib_header(bytes, &mut byte_offset)?),
            _ => StreamHeader::RawDeflate,
        };

        let mut bit_offset = byte_offset * 8;
        let mut block_index_in_stream = 0_u64;
        loop {
            block_index_in_stream += 1;
            let block = analyze_block(
                bytes,
                &mut bit_offset,
                &mut output,
                block_index_in_stream,
                uncompressed_stream_start,
            )?;
            let is_final = block.is_final;
            analysis.blocks.push(block);
            if is_final {
                break;
            }
        }

        // A container trailer is byte aligned after the final block.
        byte_offset = bit_offset.div_ceil(8);
        let footer = match format {
            Format::Gzip => {
                let footer = read_gzip_footer(bytes, byte_offset)?;
                byte_offset += 8;
                footer
            }
            Format::Zlib => {
                let footer = read_zlib_footer(bytes, byte_offset)?;
                byte_offset += 4;
                footer
            }
            _ => StreamFooter::None,
        };

        analysis.streams.push(StreamAnalysis {
            index: stream_index,
            header,
            header_offset_in_bits: header_offset as u64 * 8,
            uncompressed_offset_in_bytes: uncompressed_stream_start,
            footer,
            compressed_size_in_bits: (byte_offset - header_offset) as u64 * 8,
            uncompressed_size_in_bytes: output.len() as u64 - uncompressed_stream_start,
        });

        if format != Format::Gzip {
            break;
        }
    }

    analysis.uncompressed_size_in_bytes = output.len() as u64;
    Ok(analysis)
}

/// Reads one gzip member header, keeping every field it carries.
fn parse_gzip_header(bytes: &[u8], offset: &mut usize) -> Result<GzipHeaderFields, DecodeError> {
    let start = *offset;
    let need = |count: usize, at: usize| -> Result<(), DecodeError> {
        if at + count > bytes.len() {
            return Err(DecodeError::InvalidGzip {
                offset: at as u64,
                reason: GzipErrorKind::Truncated,
            });
        }
        Ok(())
    };
    need(10, start)?;
    if bytes[start] != 0x1f || bytes[start + 1] != 0x8b {
        return Err(DecodeError::InvalidGzip {
            offset: start as u64,
            reason: GzipErrorKind::BadMagic,
        });
    }
    if bytes[start + 2] != 8 {
        return Err(DecodeError::InvalidGzip {
            offset: start as u64 + 2,
            reason: GzipErrorKind::UnsupportedCompressionMethod(bytes[start + 2]),
        });
    }
    let flags = bytes[start + 3];
    let mut header = GzipHeaderFields {
        modification_time: u32::from_le_bytes(
            bytes[start + 4..start + 8].try_into().expect("four bytes"),
        ),
        extra_flags: bytes[start + 8],
        operating_system: bytes[start + 9],
        ..GzipHeaderFields::default()
    };
    let mut at = start + 10;

    if flags & 0b0000_0100 != 0 {
        need(2, at)?;
        let length = u16::from_le_bytes(bytes[at..at + 2].try_into().expect("two bytes")) as usize;
        at += 2;
        need(length, at)?;
        header.extra = Some(bytes[at..at + length].to_vec());
        at += length;
    }
    for (bit, field) in [(0b0000_1000_u8, 0_usize), (0b0001_0000_u8, 1_usize)] {
        if flags & bit == 0 {
            continue;
        }
        let end =
            bytes[at..]
                .iter()
                .position(|&byte| byte == 0)
                .ok_or(DecodeError::InvalidGzip {
                    offset: at as u64,
                    reason: GzipErrorKind::Truncated,
                })?;
        let value = bytes[at..at + end].to_vec();
        if field == 0 {
            header.file_name = Some(value);
        } else {
            header.comment = Some(value);
        }
        at += end + 1;
    }
    if flags & 0b0000_0010 != 0 {
        need(2, at)?;
        header.header_crc16 = Some(u16::from_le_bytes(
            bytes[at..at + 2].try_into().expect("two bytes"),
        ));
        at += 2;
    }

    *offset = at;
    Ok(header)
}

/// Reads one zlib header.
fn parse_zlib_header(bytes: &[u8], offset: &mut usize) -> Result<ZlibHeaderFields, DecodeError> {
    let start = *offset;
    if start + 2 > bytes.len() {
        return Err(DecodeError::InvalidZlib {
            offset: start as u64,
            reason: ZlibErrorKind::Truncated,
        });
    }
    let cmf = bytes[start];
    let flg = bytes[start + 1];
    crate::zlib::validate_header(cmf, flg, start as u64)?;
    let mut at = start + 2;
    let dictionary_id = if flg & 0b0010_0000 == 0 {
        0
    } else {
        if at + 4 > bytes.len() {
            return Err(DecodeError::InvalidZlib {
                offset: at as u64,
                reason: ZlibErrorKind::Truncated,
            });
        }
        let value = u32::from_be_bytes(bytes[at..at + 4].try_into().expect("four bytes"));
        at += 4;
        value
    };
    *offset = at;
    Ok(ZlibHeaderFields {
        window_size: 1 << (u32::from(cmf >> 4) + 8),
        compression_level: flg >> 6,
        dictionary_id,
    })
}

fn read_gzip_footer(bytes: &[u8], offset: usize) -> Result<StreamFooter, DecodeError> {
    if offset + 8 > bytes.len() {
        return Err(DecodeError::InvalidGzip {
            offset: offset as u64,
            reason: GzipErrorKind::Truncated,
        });
    }
    Ok(StreamFooter::Gzip {
        crc32: u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("four bytes")),
        uncompressed_size: u32::from_le_bytes(
            bytes[offset + 4..offset + 8]
                .try_into()
                .expect("four bytes"),
        ),
    })
}

fn read_zlib_footer(bytes: &[u8], offset: usize) -> Result<StreamFooter, DecodeError> {
    if offset + 4 > bytes.len() {
        return Err(DecodeError::InvalidZlib {
            offset: offset as u64,
            reason: ZlibErrorKind::Truncated,
        });
    }
    Ok(StreamFooter::Zlib {
        adler32: u32::from_be_bytes(bytes[offset..offset + 4].try_into().expect("four bytes")),
    })
}

/// A back-reference that reached before the current block's first byte.
#[derive(Clone, Copy)]
struct WindowReference {
    /// Distance back from the block's first byte.
    distance: u64,
    /// How many bytes the reference copied.
    length: u64,
}

/// Decodes one block, appending its output and recording its statistics.
fn analyze_block(
    bytes: &[u8],
    bit_offset: &mut usize,
    output: &mut Vec<u8>,
    index_in_stream: u64,
    uncompressed_stream_start: u64,
) -> Result<BlockAnalysis, DecodeError> {
    let block_start_bits = *bit_offset;
    let block_output_start = output.len();
    let mut reader = BitReader::at(bytes, block_start_bits).map_err(native)?;

    let is_final = reader.read_bits(1).map_err(native)? == 1;
    let encoding = reader.read_bits(2).map_err(native)?;

    let mut block = BlockAnalysis {
        is_final,
        compressed_offset_in_bits: block_start_bits as u64,
        uncompressed_offset_in_bytes: block_output_start as u64,
        index_in_stream,
        ..BlockAnalysis::default()
    };
    let _ = uncompressed_stream_start;

    let mut references: Vec<WindowReference> = Vec::new();

    match encoding {
        0 => {
            block.block_type = BlockType::Uncompressed;
            reader.align_to_byte();
            let at = reader.position() / 8;
            if at + 4 > bytes.len() {
                return Err(truncated(at));
            }
            let length =
                u16::from_le_bytes(bytes[at..at + 2].try_into().expect("two bytes")) as usize;
            let complement =
                u16::from_le_bytes(bytes[at + 2..at + 4].try_into().expect("two bytes"));
            if complement != !(length as u16) {
                return Err(DecodeError::InvalidDeflate {
                    bit_offset: (at as u64) * 8,
                    reason: DeflateErrorKind::InvalidData,
                });
            }
            let data = at + 4;
            if data + length > bytes.len() {
                return Err(truncated(data));
            }
            block.compressed_data_offset_in_bits = (data as u64) * 8;
            output.extend_from_slice(&bytes[data..data + length]);
            *bit_offset = (data + length) * 8;
        }
        1 | 2 => {
            let (literal_tree, distance_tree) = if encoding == 1 {
                block.block_type = BlockType::FixedHuffman;
                let (literal, distance) = fixed_trees();
                (literal, distance)
            } else {
                block.block_type = BlockType::DynamicHuffman;
                let (literal, distance, declared) =
                    dynamic_trees_with_lengths(&mut reader).map_err(native)?;
                block.precode = Some(AlphabetShape {
                    code_lengths: declared.precode.to_vec(),
                    declared_count: declared.precode_read,
                });
                block.distance = Some(AlphabetShape {
                    declared_count: declared.distance.len(),
                    code_lengths: declared.distance,
                });
                block.literal = Some(AlphabetShape {
                    declared_count: declared.literal.len(),
                    code_lengths: declared.literal,
                });
                return finish_symbols(
                    bytes,
                    reader,
                    &literal,
                    &distance,
                    output,
                    block,
                    block_output_start,
                    bit_offset,
                    &mut references,
                );
            };
            block.compressed_data_offset_in_bits = reader.position() as u64;
            return finish_symbols(
                bytes,
                reader,
                literal_tree,
                distance_tree,
                output,
                block,
                block_output_start,
                bit_offset,
                &mut references,
            );
        }
        _ => {
            return Err(DecodeError::InvalidDeflate {
                bit_offset: block_start_bits as u64,
                reason: DeflateErrorKind::InvalidData,
            });
        }
    }

    finalize(
        &mut block,
        output,
        block_output_start,
        &references,
        *bit_offset,
    );
    Ok(block)
}

/// Decodes the symbol stream of a compressed block.
#[allow(clippy::too_many_arguments)]
fn finish_symbols(
    _bytes: &[u8],
    mut reader: BitReader<'_>,
    literal_tree: &Huffman,
    distance_tree: &Huffman,
    output: &mut Vec<u8>,
    mut block: BlockAnalysis,
    block_output_start: usize,
    bit_offset: &mut usize,
    references: &mut Vec<WindowReference>,
) -> Result<BlockAnalysis, DecodeError> {
    if block.compressed_data_offset_in_bits == 0 {
        block.compressed_data_offset_in_bits = reader.position() as u64;
    }
    loop {
        let symbol = literal_tree.decode(&mut reader).map_err(native)?;
        match symbol {
            0..=255 => {
                block.literal_symbols += 1;
                output.push(symbol as u8);
            }
            END_OF_BLOCK => break,
            257..=285 => {
                let length_index = symbol - 257;
                let length = LENGTH_BASE[length_index]
                    + reader
                        .read_bits(LENGTH_EXTRA[length_index])
                        .map_err(native)? as usize;
                let distance_symbol = distance_tree.decode(&mut reader).map_err(native)?;
                if distance_symbol >= DISTANCE_BASE.len() {
                    return Err(native(crate::parallel::deflate::Error::InvalidDistance));
                }
                let distance = DISTANCE_BASE[distance_symbol]
                    + reader
                        .read_bits(DISTANCE_EXTRA[distance_symbol])
                        .map_err(native)? as usize;
                if distance > output.len() {
                    return Err(native(crate::parallel::deflate::Error::InvalidDistance));
                }
                block.backreference_symbols += 1;
                block.copied_bytes += length as u64;

                // A reference is interesting to analysis when it reaches
                // before this block's own first byte, since that is what the
                // preceding window has to supply.
                let position_in_block = output.len() - block_output_start;
                if distance > position_in_block {
                    let before_block = (distance - position_in_block) as u64;
                    references.push(WindowReference {
                        distance: before_block,
                        length: length as u64,
                    });
                    block.farthest_backreference = block.farthest_backreference.max(before_block);
                }

                let start = output.len() - distance;
                for index in 0..length {
                    let byte = output[start + index];
                    output.push(byte);
                }
            }
            _ => return Err(native(crate::parallel::deflate::Error::InvalidSymbol)),
        }
    }
    *bit_offset = reader.position();
    finalize(
        &mut block,
        output,
        block_output_start,
        references,
        *bit_offset,
    );
    Ok(block)
}

/// Fills in the sizes and window statistics once a block is decoded.
fn finalize(
    block: &mut BlockAnalysis,
    output: &[u8],
    block_output_start: usize,
    references: &[WindowReference],
    end_bits: usize,
) {
    block.compressed_size_in_bits = end_bits as u64 - block.compressed_offset_in_bits;
    block.uncompressed_size_in_bytes = (output.len() - block_output_start) as u64;
    block.window_backreference_count = references.len() as u64;
    block.backreference_lengths = references.iter().map(|entry| entry.length).collect();
    block.merged_window_backreference_count = merged_count(references);

    if block.uncompressed_size_in_bytes >= WINDOW_SIZE as u64 {
        let mut used = vec![false; WINDOW_SIZE];
        for entry in references {
            let begin = if entry.distance >= WINDOW_SIZE as u64 {
                0
            } else {
                WINDOW_SIZE - entry.distance as usize
            };
            let end = (begin + entry.length as usize).min(WINDOW_SIZE);
            used[begin..end].fill(true);
        }
        block.used_window_symbols = Some(used.iter().filter(|&&flag| flag).count() as u64);
    }
}

/// Counts references after merging overlapping and adjacent ones.
fn merged_count(references: &[WindowReference]) -> u64 {
    if references.is_empty() {
        return 0;
    }
    let mut sorted: Vec<WindowReference> = references.to_vec();
    sorted.sort_by_key(|entry| entry.distance);
    let mut current = 0_usize;
    for index in 1..sorted.len() {
        if sorted[current].distance + sorted[current].length >= sorted[index].distance {
            sorted[current].length =
                sorted[index].distance + sorted[index].length - sorted[current].distance;
        } else {
            current += 1;
            sorted[current] = sorted[index];
        }
    }
    (current + 1).min(references.len()) as u64
}

fn native(error: crate::parallel::deflate::Error) -> DecodeError {
    let _ = error;
    DecodeError::InvalidDeflate {
        bit_offset: 0,
        reason: DeflateErrorKind::InvalidData,
    }
}

fn truncated(offset: usize) -> DecodeError {
    DecodeError::InvalidDeflate {
        bit_offset: (offset as u64) * 8,
        reason: DeflateErrorKind::Truncated,
    }
}
