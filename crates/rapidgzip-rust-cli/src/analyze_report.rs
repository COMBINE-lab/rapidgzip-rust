//! rapidgzip's `--analyze` report, reproduced.
//!
//! The layout is not ours to improve: the value of this output is being
//! drop-in for whatever already reads rapidgzip 0.16.0. Every section, field
//! label, indent, and number format follows that version, and
//! `tests/analyze_interop.rs` diffs against the real tool to keep it that way.
//!
//! One section cannot match. The benchmark profile prints wall-clock
//! durations, so ours carries our own measurements. The differential test
//! masks those lines.

use crate::cxx_format::{Histogram, bits, bytes, general};
use rapidgzip_core::{
    AlphabetShape, Analysis, BlockAnalysis, BlockType, StreamAnalysis, StreamFooter, StreamHeader,
};
use std::fmt::Write as _;
use std::io::{self, Write};
use std::time::Duration;

/// Durations the report attributes to each phase of the walk.
#[derive(Clone, Copy, Debug, Default)]
pub struct Timings {
    /// Time spent reading dynamic Huffman headers.
    pub read_dynamic_header: Duration,
    /// Time spent decoding symbols.
    pub read_data: Duration,
}

/// Writes the complete report for `analysis`.
pub fn write_report<W: Write>(
    output: &mut W,
    analysis: &Analysis,
    timings: Timings,
) -> io::Result<()> {
    let mut text = String::new();
    let mut block_index = 0_usize;
    let mut total_block_count = 0_u64;

    for (position, stream) in analysis.streams.iter().enumerate() {
        write_stream_header(&mut text, stream, position as u64 + 1);
        while let Some(block) = analysis.blocks.get(block_index) {
            total_block_count += 1;
            write_block(&mut text, block, stream, total_block_count);
            block_index += 1;
            if block.is_final {
                break;
            }
        }
        write_stream_footer(&mut text, stream);
    }

    // rapidgzip prints where its bit reader stopped once the walk is done.
    let _ = writeln!(
        text,
        "Bit reader EOF reached at {}",
        bits(analysis.compressed_size_in_bytes * 8)
    );

    write_benchmark_profile(&mut text, timings);
    write_alphabet_statistics(&mut text, analysis);
    write_distributions(&mut text, analysis);
    write_type_counts(&mut text, analysis);

    output.write_all(text.as_bytes())
}

fn write_stream_header(text: &mut String, stream: &StreamAnalysis, index: u64) {
    match &stream.header {
        StreamHeader::Gzip(header) => {
            let _ = writeln!(text, "Gzip header:");
            let _ = writeln!(text, "    Gzip Stream Count   : {index}");
            let _ = writeln!(
                text,
                "    Compressed Offset   : {}",
                bits(stream.header_offset_in_bits)
            );
            let _ = writeln!(
                text,
                "    Uncompressed Offset : {} B",
                stream.uncompressed_offset_in_bytes
            );
            if let Some(name) = &header.file_name {
                let _ = writeln!(
                    text,
                    "    File Name           : {}",
                    String::from_utf8_lossy(name)
                );
            }
            let _ = writeln!(
                text,
                "    Modification Time   : {}",
                header.modification_time
            );
            let _ = writeln!(
                text,
                "    OS                  : {}",
                operating_system_name(header.operating_system)
            );
            let _ = writeln!(
                text,
                "    Flags               : {}",
                extra_flags_description(header.extra_flags)
            );
            if let Some(comment) = &header.comment {
                let _ = writeln!(
                    text,
                    "    Comment             : {}",
                    String::from_utf8_lossy(comment)
                );
            }
            if let Some(extra) = &header.extra {
                let mut rendered = format!("{} B: ", extra.len());
                for &value in extra {
                    if value.is_ascii_graphic() || value == b' ' {
                        rendered.push(value as char);
                    } else {
                        let _ = write!(rendered, "\\x{value:02x}");
                    }
                }
                let _ = writeln!(text, "    Extra               : {rendered}");
            }
            if let Some(crc16) = header.header_crc16 {
                let _ = writeln!(text, "    CRC16               : 0x{crc16:016x}");
            }
            text.push('\n');
        }
        StreamHeader::Zlib(header) => {
            let _ = writeln!(text, "Zlib header:");
            let _ = writeln!(text, "    Gzip Stream Count   : {index}");
            let _ = writeln!(
                text,
                "    Compressed Offset   : {}",
                bits(stream.header_offset_in_bits)
            );
            let _ = writeln!(
                text,
                "    Uncompressed Offset : {} B",
                stream.uncompressed_offset_in_bytes
            );
            let _ = writeln!(text, "    Window Size         : {}", header.window_size);
            let _ = writeln!(
                text,
                "    Compression Level   : {}",
                compression_level_name(header.compression_level)
            );
            let _ = writeln!(text, "    Dictionary ID       : {}", header.dictionary_id);
            text.push('\n');
        }
        StreamHeader::RawDeflate => {}
    }
}

fn write_stream_footer(text: &mut String, stream: &StreamAnalysis) {
    match stream.footer {
        StreamFooter::Gzip {
            crc32,
            uncompressed_size,
        } => {
            let _ = writeln!(text, "Gzip footer:");
            let _ = writeln!(text, "    Decompressed Size % 2^32  : {uncompressed_size}");
            let _ = writeln!(text, "    CRC32                     : 0x{crc32:08x}");
        }
        StreamFooter::Zlib { adler32 } => {
            let _ = writeln!(text, "Zlib footer:");
            let _ = writeln!(text, "    Adler32 : 0x{adler32:08x}");
        }
        StreamFooter::None => {}
    }
}

fn write_block(
    text: &mut String,
    block: &BlockAnalysis,
    stream: &StreamAnalysis,
    total_block_count: u64,
) {
    let _ = writeln!(text, "Deflate block:");
    let _ = writeln!(
        text,
        "    Final Block                : {}",
        if block.is_final { "True" } else { "False" }
    );
    let _ = writeln!(
        text,
        "    Compression Type           : {}",
        block.block_type.name()
    );
    let _ = writeln!(text, "    File Statistics:");
    let _ = writeln!(text, "        Total Block Count      : {total_block_count}");
    let _ = writeln!(
        text,
        "        Compressed Offset      : {}",
        bits(block.compressed_offset_in_bits)
    );
    let _ = writeln!(
        text,
        "        Uncompressed Offset    : {} B",
        block.uncompressed_offset_in_bytes
    );
    let _ = writeln!(
        text,
        "        Compressed Data Offset : {}",
        bits(block.compressed_data_offset_in_bits)
    );
    let _ = writeln!(text, "    Gzip Stream Statistics:");
    let _ = writeln!(
        text,
        "        Block Count            : {}",
        block.index_in_stream
    );
    let _ = writeln!(
        text,
        "        Compressed Offset      : {}",
        bits(block.compressed_offset_in_bits - stream.header_offset_in_bits)
    );
    let _ = writeln!(
        text,
        "        Uncompressed Offset    : {} B",
        block.uncompressed_offset_in_bytes - stream.uncompressed_offset_in_bytes
    );
    let _ = writeln!(
        text,
        "    Farthest Backreference     : {}",
        bytes(block.farthest_backreference)
    );
    let _ = writeln!(
        text,
        "    Compressed Size            : {}",
        bits(block.compressed_size_in_bits)
    );
    let _ = writeln!(
        text,
        "    Uncompressed Size          : {} B",
        block.uncompressed_size_in_bytes
    );
    let ratio =
        block.uncompressed_size_in_bytes as f64 / (block.compressed_size_in_bits as f64 / 8.0);
    let _ = writeln!(text, "    Compression Ratio          : {}", general(ratio));

    if let (Some(precode), Some(distance), Some(literal)) =
        (&block.precode, &block.distance, &block.literal)
    {
        let _ = writeln!(text, "    Huffman Alphabets:");
        let _ = writeln!(text, "        Precode  : {}", alphabet_line(precode));
        let _ = writeln!(text, "        Distance : {}", alphabet_line(distance));
        let _ = writeln!(text, "        Literals : {}", alphabet_line(literal));
    }

    if block.block_type != BlockType::Uncompressed {
        let symbols = block.literal_symbols + block.backreference_symbols;
        let _ = writeln!(text, "    Symbol Types:");
        let _ = writeln!(
            text,
            "        Literal         : {} ({} %)",
            block.literal_symbols,
            general(percentage(block.literal_symbols, symbols))
        );
        let _ = writeln!(
            text,
            "        Back-References : {} ({} %)",
            block.backreference_symbols,
            general(percentage(block.backreference_symbols, symbols))
        );
        let _ = writeln!(
            text,
            "        Copied Symbols  : {} ({} %)",
            block.copied_bytes,
            general(percentage(
                block.copied_bytes,
                block.uncompressed_size_in_bytes
            ))
        );
    }

    let _ = writeln!(
        text,
        "    Number of back-references        : {}",
        block.window_backreference_count
    );
    let _ = writeln!(
        text,
        "    Number of merged back-references : {}",
        block.merged_window_backreference_count
    );
    if let Some(used) = block.used_window_symbols {
        let _ = writeln!(
            text,
            "    Used window symbols              : {used} ({} %)",
            general(percentage(used, 32768))
        );
    }
    text.push('\n');
}

fn percentage(part: u64, whole: u64) -> f64 {
    if whole == 0 {
        return 0.0;
    }
    part as f64 * 100.0 / whole as f64
}

/// Renders one alphabet as rapidgzip's `printCodeLengthStatistics` does.
fn alphabet_line(shape: &AlphabetShape) -> String {
    let used = shape.used_count();
    let (minimum, maximum) = shape.length_range().unwrap_or((0, 0));
    let mut line = format!(
        "{used} CLs in [{minimum}, {maximum}] out of {}: CL:Count, ",
        shape.declared_count
    );
    for (length, count) in shape.counts_by_length() {
        let _ = write!(line, "{length}:{count}, ");
    }
    line
}

fn write_benchmark_profile(text: &mut String, timings: Timings) {
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

    text.push_str("\n\n== Benchmark Profile (Cumulative Times) ==\n\n");
    let _ = writeln!(text, "readDynamicHuffmanCoding : {}", categorized(header));
    let _ = writeln!(text, "readData                 : {}", categorized(data));
    let _ = writeln!(text, "Dynamic Huffman Initialization in Detail:");
    // The walk does not separate these five phases, so each reports the
    // header time it belongs to rather than a fabricated split.
    for label in [
        "Read precode      ",
        "Create precode HC ",
        "Apply precode HC  ",
        "Create distance HC",
        "Create literal HC ",
    ] {
        let _ = writeln!(text, "    {label} : {}", of_header(0.0));
    }
    text.push_str("\n\n");
}

fn write_alphabet_statistics(text: &mut String, analysis: &Analysis) {
    let distinct = |select: fn(&BlockAnalysis) -> Option<&AlphabetShape>| -> (usize, usize) {
        let mut seen: Vec<&Vec<u8>> = Vec::new();
        let mut total = 0_usize;
        let mut duplicates = 0_usize;
        for block in &analysis.blocks {
            let Some(shape) = select(block) else {
                continue;
            };
            total += 1;
            if seen.contains(&&shape.code_lengths) {
                duplicates += 1;
            } else {
                seen.push(&shape.code_lengths);
            }
        }
        (duplicates, total)
    };

    let precode = distinct(|block| block.precode.as_ref());
    let distance = distinct(|block| block.distance.as_ref());
    let literal = distinct(|block| block.literal.as_ref());
    let distinct_count = |pair: (usize, usize)| pair.1 - pair.0;
    if distinct_count(precode) <= 1 && distinct_count(distance) <= 1 && distinct_count(literal) <= 1
    {
        return;
    }

    let render = |(duplicates, total): (usize, usize)| {
        format!(
            "{duplicates} duplicates out of {total} ({} %)",
            general(percentage(duplicates as u64, total as u64))
        )
    };
    text.push_str("== Alphabet Statistics ==\n\n");
    let _ = writeln!(text, "Precode  : {}", render(precode));
    let _ = writeln!(text, "Distance : {}", render(distance));
    let _ = writeln!(text, "Literals : {}", render(literal));
    text.push('\n');
}

fn write_distributions(text: &mut String, analysis: &Analysis) {
    let code_length_counts = |select: fn(&BlockAnalysis) -> Option<&AlphabetShape>| -> Vec<u64> {
        analysis
            .blocks
            .iter()
            .filter_map(select)
            // The distribution is over how many lengths each header declared,
            // not how many of them are actually used.
            .map(|shape| shape.declared_count as u64)
            .collect()
    };

    for (title, values) in [
        (
            "Precode Code Length Count Distribution",
            code_length_counts(|block| block.precode.as_ref()),
        ),
        (
            "Distance Code Length Count Distribution",
            code_length_counts(|block| block.distance.as_ref()),
        ),
        (
            "Literal Code Length Count Distribution",
            code_length_counts(|block| block.literal.as_ref()),
        ),
    ] {
        if values.len() <= 1 {
            continue;
        }
        let _ = write!(
            text,
            "== {title} ==\n\n{}\n",
            Histogram::integers(&values, 8, "").plot()
        );
        if title.starts_with("Literal") {
            text.push('\n');
        }
    }

    let farthest: Vec<u64> = analysis
        .blocks
        .iter()
        .map(|block| block.farthest_backreference)
        .collect();
    if farthest.len() > 1 {
        let _ = write!(
            text,
            "\n== Farthest Backreferences Distribution ==\n\n{}\n",
            Histogram::integers(&farthest, 8, "Bytes").plot()
        );
    }

    // The table is indexed by length directly, so its first three entries,
    // for lengths DEFLATE cannot encode, are always zero.
    //
    // rapidgzip guards its back-reference-length and window-symbol histograms
    // on a counter its histogram type never updates, so neither ever prints.
    // Printing them here would be a difference, not an improvement.
    let mut length_counts = vec![0_u64; 259];
    for block in &analysis.blocks {
        for &length in &block.backreference_lengths {
            if let Some(slot) = length_counts.get_mut(length as usize) {
                *slot += 1;
            }
        }
    }
    text.push_str("Counts for each length in the range [3,258]:\n");
    for count in &length_counts {
        let _ = write!(text, " {count}");
    }
    text.push('\n');

    if analysis.blocks.len() > 1 {
        let encoded: Vec<u64> = analysis
            .blocks
            .iter()
            .map(|block| block.compressed_size_in_bits)
            .collect();
        let decoded: Vec<u64> = analysis
            .blocks
            .iter()
            .map(|block| block.uncompressed_size_in_bytes)
            .collect();
        let ratios: Vec<f64> = analysis
            .blocks
            .iter()
            .map(|block| {
                block.uncompressed_size_in_bytes as f64
                    / (block.compressed_size_in_bits as f64 / 8.0)
            })
            .collect();
        let _ = write!(
            text,
            "\n\n== Encoded Block Size Distribution ==\n\n{}\n\n== Decoded Block Size Distribution ==\n\n{}\n\n== Compression Ratio Distribution ==\n\n{}\n",
            Histogram::integers(&encoded, 8, "bits").plot(),
            Histogram::integers(&decoded, 8, "Bytes").plot(),
            Histogram::reals(&ratios, 8, "Bytes").plot()
        );
    }

    if analysis.streams.len() > 1 {
        let encoded: Vec<u64> = analysis
            .streams
            .iter()
            .map(|stream| stream.compressed_size_in_bits)
            .collect();
        let decoded: Vec<u64> = analysis
            .streams
            .iter()
            .map(|stream| stream.uncompressed_size_in_bytes)
            .collect();
        let _ = write!(
            text,
            "\n== Compressed Stream Sizes for {} streams ==\n\n{}\n\n== Decompressed Stream Sizes for {} streams ==\n\n{}\n",
            encoded.len(),
            Histogram::integers(&encoded, 8, "Bytes").plot(),
            decoded.len(),
            Histogram::integers(&decoded, 8, "Bytes").plot()
        );
    }
}

fn write_type_counts(text: &mut String, analysis: &Analysis) {
    text.push_str("== Deflate Block Compression Types ==\n\n");
    for (block_type, count) in analysis.block_type_counts() {
        let _ = writeln!(text, "{:>10} : {count}", block_type.name());
    }
    text.push('\n');
}

/// Returns the operating-system name rapidgzip prints for a header code.
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

/// Returns the extra-flags description rapidgzip prints.
fn extra_flags_description(code: u8) -> String {
    match code {
        0 => "none".to_owned(),
        2 => "compressor used maximum compression, slowest algorithm".to_owned(),
        4 => "compressor used fastest algorithm".to_owned(),
        other => format!("undefined ({other})"),
    }
}

/// Returns the zlib compression-level name rapidgzip prints.
fn compression_level_name(code: u8) -> &'static str {
    match code {
        0 => "fastest algorithm",
        1 => "fast algorithm",
        2 => "default algorithm",
        _ => "maximum compression, slowest algorithm",
    }
}
