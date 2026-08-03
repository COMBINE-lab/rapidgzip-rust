//! rapidgzip 0.16.0-compatible presentation of structured analysis.
//!
//! Deterministic sections intentionally follow the reference's labels,
//! spacing, order, and number formatting. Locally measured timings have the
//! same shape but cannot have the same values. The merged-reference count uses
//! a deterministic interval union instead of the reference's unstable-sort
//! defect.

use crate::cxx_format::{Histogram, bits, bytes, general};
use rapidgzip_core::{
    AlphabetShape, Analysis, Backreference, BlockAnalysis, BlockType, StreamAnalysis, StreamFooter,
    StreamHeader,
};
use std::collections::HashSet;
use std::io::{self, Write};
use std::time::Duration;

/// Locally measured phases displayed by the compatibility report.
#[derive(Clone, Copy, Debug, Default)]
pub struct Timings {
    /// Time attributed to parsing dynamic Huffman headers.
    pub read_dynamic_header: Duration,
    /// Time attributed to walking block symbol data.
    pub read_data: Duration,
}

/// Writes the complete compatibility report without buffering it in full.
pub fn write_report<W: Write>(
    output: &mut W,
    analysis: &Analysis,
    timings: Timings,
    verbose_references: bool,
) -> io::Result<()> {
    let mut total_block_count = 0_u64;
    for stream in &analysis.streams {
        write_stream_header(output, stream)?;
        let end = stream.first_block_index + stream.block_count;
        for block in &analysis.blocks[stream.first_block_index..end] {
            total_block_count += 1;
            write_block(output, block, stream, total_block_count, verbose_references)?;
        }
        write_stream_footer(output, stream)?;
    }

    writeln!(
        output,
        "Bit reader EOF reached at {}",
        bits(analysis.compressed_size_in_bytes.saturating_mul(8))
    )?;
    write_benchmark_profile(output, timings)?;
    write_alphabet_statistics(output, analysis)?;
    write_distributions(output, analysis)?;
    write_type_counts(output, analysis)
}

fn write_stream_header<W: Write>(output: &mut W, stream: &StreamAnalysis) -> io::Result<()> {
    match &stream.header {
        StreamHeader::Gzip(header) => {
            writeln!(output, "Gzip header:")?;
            writeln!(output, "    Gzip Stream Count   : {}", stream.index + 1)?;
            writeln!(
                output,
                "    Compressed Offset   : {}",
                bits(stream.header_offset_in_bits)
            )?;
            writeln!(
                output,
                "    Uncompressed Offset : {} B",
                stream.uncompressed_offset_in_bytes
            )?;
            if let Some(name) = &header.file_name {
                writeln!(
                    output,
                    "    File Name           : {}",
                    String::from_utf8_lossy(name)
                )?;
            }
            writeln!(
                output,
                "    Modification Time   : {}",
                header.modification_time
            )?;
            writeln!(
                output,
                "    OS                  : {}",
                operating_system_name(header.operating_system)
            )?;
            writeln!(
                output,
                "    Flags               : {}",
                extra_flags_description(header.extra_flags)
            )?;
            if let Some(comment) = &header.comment {
                writeln!(
                    output,
                    "    Comment             : {}",
                    String::from_utf8_lossy(comment)
                )?;
            }
            if let Some(extra) = &header.extra {
                write!(output, "    Extra               : {} B: ", extra.len())?;
                for &value in extra {
                    if value.is_ascii_graphic() || value == b' ' {
                        write!(output, "{}", value as char)?;
                    } else {
                        write!(output, "\\x{value:02x}")?;
                    }
                }
                writeln!(output)?;
            }
            if let Some(crc16) = header.header_crc16 {
                writeln!(output, "    CRC16               : 0x{crc16:016x}")?;
            }
            writeln!(output)?;
        }
        StreamHeader::Zlib(header) => {
            writeln!(output, "Zlib header:")?;
            writeln!(output, "    Gzip Stream Count   : {}", stream.index + 1)?;
            writeln!(
                output,
                "    Compressed Offset   : {}",
                bits(stream.header_offset_in_bits)
            )?;
            writeln!(
                output,
                "    Uncompressed Offset : {} B",
                stream.uncompressed_offset_in_bytes
            )?;
            writeln!(output, "    Window Size         : {}", header.window_size)?;
            writeln!(
                output,
                "    Compression Level   : {}",
                compression_level_name(header.compression_level)
            )?;
            writeln!(
                output,
                "    Dictionary ID       : {}",
                header.dictionary_id.unwrap_or(0)
            )?;
            writeln!(output)?;
        }
        StreamHeader::RawDeflate => {}
        _ => {}
    }
    Ok(())
}

fn write_stream_footer<W: Write>(output: &mut W, stream: &StreamAnalysis) -> io::Result<()> {
    match stream.footer {
        StreamFooter::Gzip {
            crc32,
            uncompressed_size,
        } => {
            writeln!(output, "Gzip footer:")?;
            writeln!(
                output,
                "    Decompressed Size % 2^32  : {uncompressed_size}"
            )?;
            writeln!(output, "    CRC32                     : 0x{crc32:08x}")?;
        }
        StreamFooter::Zlib { adler32 } => {
            writeln!(output, "Zlib footer:")?;
            writeln!(output, "    Adler32 : 0x{adler32:08x}")?;
        }
        StreamFooter::None => {}
        _ => {}
    }
    Ok(())
}

fn write_block<W: Write>(
    output: &mut W,
    block: &BlockAnalysis,
    stream: &StreamAnalysis,
    total_block_count: u64,
    verbose_references: bool,
) -> io::Result<()> {
    writeln!(output, "Deflate block:")?;
    writeln!(
        output,
        "    Final Block                : {}",
        if block.is_final { "True" } else { "False" }
    )?;
    writeln!(
        output,
        "    Compression Type           : {}",
        block_type_name(block.block_type)
    )?;
    writeln!(output, "    File Statistics:")?;
    writeln!(
        output,
        "        Total Block Count      : {total_block_count}"
    )?;
    writeln!(
        output,
        "        Compressed Offset      : {}",
        bits(block.compressed_offset_in_bits)
    )?;
    writeln!(
        output,
        "        Uncompressed Offset    : {} B",
        block.uncompressed_offset_in_bytes
    )?;
    writeln!(
        output,
        "        Compressed Data Offset : {}",
        bits(block.compressed_data_offset_in_bits)
    )?;
    writeln!(output, "    Gzip Stream Statistics:")?;
    writeln!(
        output,
        "        Block Count            : {}",
        block.index_in_stream + 1
    )?;
    writeln!(
        output,
        "        Compressed Offset      : {}",
        bits(block.compressed_offset_in_bits - stream.header_offset_in_bits)
    )?;
    writeln!(
        output,
        "        Uncompressed Offset    : {} B",
        block.uncompressed_offset_in_bytes - stream.uncompressed_offset_in_bytes
    )?;
    writeln!(
        output,
        "    Farthest Backreference     : {}",
        bytes(block.farthest_backreference)
    )?;
    writeln!(
        output,
        "    Compressed Size            : {}",
        bits(block.compressed_size_in_bits)
    )?;
    writeln!(
        output,
        "    Uncompressed Size          : {} B",
        block.uncompressed_size_in_bytes
    )?;
    let ratio =
        block.uncompressed_size_in_bytes as f64 / (block.compressed_size_in_bits as f64 / 8.0);
    writeln!(
        output,
        "    Compression Ratio          : {}",
        general(ratio)
    )?;

    if let (Some(precode), Some(distance), Some(literal)) =
        (&block.precode, &block.distance, &block.literal)
    {
        writeln!(output, "    Huffman Alphabets:")?;
        writeln!(output, "        Precode  : {}", alphabet_line(precode))?;
        writeln!(output, "        Distance : {}", alphabet_line(distance))?;
        writeln!(output, "        Literals : {}", alphabet_line(literal))?;
    }

    if block.uncompressed_size_in_bytes > 0 && block.block_type != BlockType::Uncompressed {
        let symbols = block.literal_symbols + block.backreference_symbols;
        writeln!(output, "    Symbol Types:")?;
        writeln!(
            output,
            "        Literal         : {} ({} %)",
            block.literal_symbols,
            general(percentage(block.literal_symbols, symbols))
        )?;
        writeln!(
            output,
            "        Back-References : {} ({} %)",
            block.backreference_symbols,
            general(percentage(block.backreference_symbols, symbols))
        )?;
        writeln!(
            output,
            "        Copied Symbols  : {} ({} %)",
            block.copied_bytes,
            general(percentage(
                block.copied_bytes,
                block.uncompressed_size_in_bytes
            ))
        )?;
    }

    if verbose_references && block.uncompressed_size_in_bytes > 0 {
        write!(output, "    Back-references to the preceding window:")?;
        for reference in &block.retained_backreferences {
            write!(output, " {}@{}", reference.length, reference.distance)?;
        }
        if block.omitted_backreference_count != 0 {
            write!(
                output,
                " [{} omitted by retention limit]",
                block.omitted_backreference_count
            )?;
        }
        writeln!(output)?;

        write!(output, "    Merged back-references to preceding window:")?;
        if block.omitted_backreference_count == 0 {
            for (distance, length) in merged_references(&block.retained_backreferences)? {
                write!(output, " {length}@{distance}")?;
            }
        } else {
            write!(output, " [details incomplete]")?;
        }
        writeln!(output)?;
    }

    writeln!(
        output,
        "    Number of back-references        : {}",
        block.window_backreference_count
    )?;
    writeln!(
        output,
        "    Number of merged back-references : {}",
        block.merged_window_backreference_count
    )?;
    if let Some(used) = block.used_window_symbols {
        writeln!(
            output,
            "    Used window symbols              : {used} ({} %)",
            general(percentage(used, 32768))
        )?;
    }
    writeln!(output)
}

fn merged_references(references: &[Backreference]) -> io::Result<Vec<(u16, u16)>> {
    let mut intervals = Vec::new();
    intervals
        .try_reserve(references.len())
        .map_err(io::Error::other)?;
    intervals.extend(
        references
            .iter()
            .map(|reference| (reference.distance, reference.length)),
    );
    merge_intervals(&intervals)
}

fn merge_intervals(references: &[(u16, u16)]) -> io::Result<Vec<(u16, u16)>> {
    let mut merged = Vec::new();
    merged
        .try_reserve(references.len())
        .map_err(io::Error::other)?;
    merged.extend_from_slice(references);
    merged.sort_unstable();
    let mut result: Vec<(u16, u16)> = Vec::new();
    result.try_reserve(merged.len()).map_err(io::Error::other)?;
    for (distance, length) in merged {
        let Some(previous) = result.last_mut() else {
            result.push((distance, length));
            continue;
        };
        let previous_end = u32::from(previous.0) + u32::from(previous.1);
        if u32::from(distance) <= previous_end {
            let end = previous_end.max(u32::from(distance) + u32::from(length));
            previous.1 = (end - u32::from(previous.0)) as u16;
        } else {
            result.push((distance, length));
        }
    }
    Ok(result)
}

fn percentage(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        0.0
    } else {
        part as f64 * 100.0 / whole as f64
    }
}

fn alphabet_line(shape: &AlphabetShape) -> String {
    let used = shape.used_count();
    let (minimum, maximum) = shape.length_range().unwrap_or((0, 0));
    let mut line = format!(
        "{used} CLs in [{minimum}, {maximum}] out of {}: CL:Count, ",
        shape.declared_count
    );
    for (length, count) in shape.counts_by_length() {
        line.push_str(&format!("{length}:{count}, "));
    }
    line
}

fn write_benchmark_profile<W: Write>(output: &mut W, timings: Timings) -> io::Result<()> {
    let header = timings.read_dynamic_header.as_secs_f64();
    let data = timings.read_data.as_secs_f64();
    let total = header + data;
    let categorized = |value: f64| {
        format!(
            "{} s ({} %)",
            general(value),
            general(if total > 0.0 {
                value / total * 100.0
            } else {
                0.0
            })
        )
    };
    let of_header = |value: f64| {
        format!(
            "{} s ({} %)",
            general(value),
            general(if header > 0.0 {
                value / header * 100.0
            } else {
                0.0
            })
        )
    };

    writeln!(output, "\n\n== Benchmark Profile (Cumulative Times) ==\n")?;
    writeln!(output, "readDynamicHuffmanCoding : {}", categorized(header))?;
    writeln!(output, "readData                 : {}", categorized(data))?;
    writeln!(output, "Dynamic Huffman Initialization in Detail:")?;
    for label in [
        "Read precode      ",
        "Create precode HC ",
        "Apply precode HC  ",
        "Create distance HC",
        "Create literal HC ",
    ] {
        writeln!(output, "    {label} : {}", of_header(0.0))?;
    }
    writeln!(output, "\n")
}

fn distinct_alphabets(
    analysis: &Analysis,
    select: fn(&BlockAnalysis) -> Option<&AlphabetShape>,
) -> io::Result<(usize, usize)> {
    let mut seen: HashSet<&[u8]> = HashSet::new();
    seen.try_reserve(analysis.blocks.len())
        .map_err(io::Error::other)?;
    let mut total = 0;
    let mut duplicates = 0;
    for block in &analysis.blocks {
        let Some(shape) = select(block) else {
            continue;
        };
        total += 1;
        if !seen.insert(&shape.code_lengths) {
            duplicates += 1;
        }
    }
    Ok((duplicates, total))
}

fn write_alphabet_statistics<W: Write>(output: &mut W, analysis: &Analysis) -> io::Result<()> {
    let precode = distinct_alphabets(analysis, |block| block.precode.as_ref())?;
    let distance = distinct_alphabets(analysis, |block| block.distance.as_ref())?;
    let literal = distinct_alphabets(analysis, |block| block.literal.as_ref())?;
    let distinct = |(duplicates, total): (usize, usize)| total - duplicates;
    if distinct(precode) <= 1 && distinct(distance) <= 1 && distinct(literal) <= 1 {
        return Ok(());
    }
    let render = |(duplicates, total): (usize, usize)| {
        format!(
            "{duplicates} duplicates out of {total} ({} %)",
            general(percentage(duplicates as u64, total as u64))
        )
    };
    writeln!(output, "== Alphabet Statistics ==\n")?;
    writeln!(output, "Precode  : {}", render(precode))?;
    writeln!(output, "Distance : {}", render(distance))?;
    writeln!(output, "Literals : {}\n", render(literal))
}

fn collect_u64(
    analysis: &Analysis,
    select: impl Fn(&BlockAnalysis) -> Option<u64>,
) -> io::Result<Vec<u64>> {
    let mut values = Vec::new();
    values
        .try_reserve(analysis.blocks.len())
        .map_err(io::Error::other)?;
    values.extend(analysis.blocks.iter().filter_map(select));
    Ok(values)
}

fn write_distributions<W: Write>(output: &mut W, analysis: &Analysis) -> io::Result<()> {
    for (title, values) in [
        (
            "Precode Code Length Count Distribution",
            collect_u64(analysis, |block| {
                block
                    .precode
                    .as_ref()
                    .map(|shape| shape.declared_count as u64)
            })?,
        ),
        (
            "Distance Code Length Count Distribution",
            collect_u64(analysis, |block| {
                block
                    .distance
                    .as_ref()
                    .map(|shape| shape.declared_count as u64)
            })?,
        ),
        (
            "Literal Code Length Count Distribution",
            collect_u64(analysis, |block| {
                block
                    .literal
                    .as_ref()
                    .map(|shape| shape.declared_count as u64)
            })?,
        ),
    ] {
        if values.len() <= 1 {
            continue;
        }
        write!(
            output,
            "== {title} ==\n\n{}\n",
            Histogram::integers(&values, 8, "").plot()
        )?;
        if title.starts_with("Literal") {
            writeln!(output)?;
        }
    }

    let farthest = collect_u64(analysis, |block| Some(block.farthest_backreference))?;
    if farthest.len() > 1 {
        write!(
            output,
            "\n== Farthest Backreferences Distribution ==\n\n{}\n",
            Histogram::integers(&farthest, 8, "Bytes").plot()
        )?;
    }

    writeln!(output, "Counts for each length in the range [3,258]:")?;
    for count in analysis.backreference_length_counts {
        write!(output, " {count}")?;
    }
    writeln!(output)?;

    if analysis.blocks.len() > 1 {
        let encoded = collect_u64(analysis, |block| Some(block.compressed_size_in_bits))?;
        let decoded = collect_u64(analysis, |block| Some(block.uncompressed_size_in_bytes))?;
        let mut ratios = Vec::new();
        ratios
            .try_reserve(analysis.blocks.len())
            .map_err(io::Error::other)?;
        ratios.extend(analysis.blocks.iter().map(|block| {
            block.uncompressed_size_in_bytes as f64 / (block.compressed_size_in_bits as f64 / 8.0)
        }));
        write!(
            output,
            "\n\n== Encoded Block Size Distribution ==\n\n{}\n\n== Decoded Block Size Distribution ==\n\n{}\n\n== Compression Ratio Distribution ==\n\n{}\n",
            Histogram::integers(&encoded, 8, "bits").plot(),
            Histogram::integers(&decoded, 8, "Bytes").plot(),
            Histogram::reals(&ratios, 8, "Bytes").plot()
        )?;
    }

    if analysis.streams.len() > 1 {
        let mut encoded = Vec::new();
        let mut decoded = Vec::new();
        encoded
            .try_reserve(analysis.streams.len())
            .map_err(io::Error::other)?;
        decoded
            .try_reserve(analysis.streams.len())
            .map_err(io::Error::other)?;
        encoded.extend(
            analysis
                .streams
                .iter()
                .map(|stream| stream.compressed_size_in_bits),
        );
        decoded.extend(
            analysis
                .streams
                .iter()
                .map(|stream| stream.uncompressed_size_in_bytes),
        );
        write!(
            output,
            "\n== Compressed Stream Sizes for {} streams ==\n\n{}\n\n== Decompressed Stream Sizes for {} streams ==\n\n{}\n",
            encoded.len(),
            Histogram::integers(&encoded, 8, "Bytes").plot(),
            decoded.len(),
            Histogram::integers(&decoded, 8, "Bytes").plot()
        )?;
    }
    Ok(())
}

fn write_type_counts<W: Write>(output: &mut W, analysis: &Analysis) -> io::Result<()> {
    writeln!(output, "== Deflate Block Compression Types ==\n")?;
    for (block_type, count) in analysis.block_type_counts() {
        writeln!(output, "{:>10} : {count}", block_type_name(block_type))?;
    }
    writeln!(output)
}

fn block_type_name(block_type: BlockType) -> &'static str {
    match block_type {
        BlockType::Uncompressed => "Uncompressed",
        BlockType::FixedHuffman => "Fixed Huffman",
        BlockType::DynamicHuffman => "Dynamic Huffman",
        _ => "Unknown",
    }
}

fn operating_system_name(code: u8) -> String {
    match code {
        0 => "FAT filesystem (MS-DOS, OS/2, NT/Win32)".to_owned(),
        1 => "Amiga".to_owned(),
        2 => "VMS (or OpenVMS)".to_owned(),
        3 => "Unix".to_owned(),
        4 => "VM/CMS".to_owned(),
        5 => "Atari TOS".to_owned(),
        6 => "HPFS filesystem (OS/2, NT)".to_owned(),
        7 => "Macintosh".to_owned(),
        8 => "Z-System".to_owned(),
        9 => "CP/M".to_owned(),
        10 => "TOPS-20".to_owned(),
        11 => "NTFS filesystem (NT)".to_owned(),
        12 => "QDOS".to_owned(),
        13 => "Acorn RISCOS".to_owned(),
        255 => "unknown".to_owned(),
        other => format!("undefined ({other})"),
    }
}

fn extra_flags_description(code: u8) -> String {
    match code {
        0 => "none".to_owned(),
        2 => "compressor used maximum compression, slowest algorithm".to_owned(),
        4 => "compressor used fastest algorithm".to_owned(),
        other => format!("undefined ({other})"),
    }
}

fn compression_level_name(code: u8) -> &'static str {
    match code {
        0 => "fastest algorithm",
        1 => "fast algorithm",
        2 => "default algorithm",
        _ => "maximum compression, slowest algorithm",
    }
}

#[cfg(test)]
mod tests {
    use super::merge_intervals;

    #[test]
    fn merged_reference_union_never_shrinks_a_containing_interval() {
        let merged = merge_intervals(&[(10, 20), (10, 2), (30, 5)]).unwrap();
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0], (10, 25));
    }
}
