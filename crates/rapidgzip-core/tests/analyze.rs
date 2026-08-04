//! Structural analysis invariants and resource-bound behavior.

mod common;

use common::{bgzf, corpus, gzip, raw_deflate, zlib};
use rapidgzip_core::{
    Analysis, AnalysisErrorKind, AnalysisResource, AnalyzeOptions, BlockType, DecodeError, Decoder,
    Format, GzipHeaderFields, ReadAt, StreamFooter, StreamHeader,
};
use std::io::{self, Cursor, Read};
use std::sync::atomic::{AtomicUsize, Ordering};

fn decoder(format: Format) -> Decoder {
    Decoder::builder()
        .format(format)
        .input_page_size(257)
        .build()
        .expect("valid decoder")
}

fn assert_consistent(analysis: &Analysis, decoded_size: usize) {
    assert_eq!(analysis.uncompressed_size_in_bytes, decoded_size as u64);
    assert_eq!(
        analysis
            .blocks
            .iter()
            .map(|block| block.uncompressed_size_in_bytes)
            .sum::<u64>(),
        decoded_size as u64,
    );
    assert_eq!(
        analysis
            .streams
            .iter()
            .map(|stream| stream.uncompressed_size_in_bytes)
            .sum::<u64>(),
        decoded_size as u64,
    );

    let mut previous_compressed = None;
    let mut previous_output = 0;
    for (index, block) in analysis.blocks.iter().enumerate() {
        if let Some(previous) = previous_compressed {
            assert!(block.compressed_offset_in_bits > previous);
        }
        assert!(block.uncompressed_offset_in_bytes >= previous_output);
        assert!(block.compressed_data_offset_in_bits >= block.compressed_offset_in_bits);
        assert!(
            block.compressed_data_offset_in_bits
                <= block.compressed_offset_in_bits + block.compressed_size_in_bits
        );
        if block.block_type != BlockType::Uncompressed {
            assert_eq!(
                block.literal_symbols + block.copied_bytes,
                block.uncompressed_size_in_bytes,
                "block {index} output is literals plus copied bytes",
            );
        }
        assert!(block.merged_window_backreference_count <= block.window_backreference_count);
        assert_eq!(
            block.retained_backreferences.len() as u64 + block.omitted_backreference_count,
            block.window_backreference_count,
        );
        previous_compressed = Some(block.compressed_offset_in_bits);
        previous_output = block.uncompressed_offset_in_bytes;
    }

    let final_blocks = analysis
        .blocks
        .iter()
        .filter(|block| block.is_final)
        .count();
    assert_eq!(final_blocks, analysis.streams.len());
    for stream in &analysis.streams {
        let range = stream.first_block_index..stream.first_block_index + stream.block_count;
        assert!(!range.is_empty());
        assert!(analysis.blocks[range.end - 1].is_final);
        assert!(analysis.blocks[range.clone()].iter().all(|block| {
            block.stream_index == stream.index
                && block.compressed_offset_in_bits >= stream.deflate_offset_in_bits
        }));
    }
}

#[test]
fn analyzes_every_supported_framing_without_retaining_output() {
    let plain = corpus(2 * 1024 * 1024);
    for (compressed, format) in [
        (gzip(&plain, 6), Format::Gzip),
        (zlib(&plain, 6), Format::Zlib),
        (raw_deflate(&plain, 6), Format::RawDeflate),
    ] {
        let analysis = decoder(format).analyze(&compressed).expect("analyze");
        assert_eq!(analysis.format, format);
        assert_eq!(analysis.compressed_size_in_bytes, compressed.len() as u64);
        assert_consistent(&analysis, plain.len());
    }
}

#[test]
fn concatenated_empty_and_bgzf_members_remain_distinct_streams() {
    let first = corpus(200 * 1024);
    let second = corpus(100 * 1024);
    let mut concatenated = gzip(&first, 6);
    concatenated.extend_from_slice(&gzip(&[], 6));
    concatenated.extend_from_slice(&gzip(&second, 1));
    let analysis = decoder(Format::Gzip)
        .analyze(&concatenated)
        .expect("concatenated analysis");
    assert_eq!(analysis.streams.len(), 3);
    assert_eq!(analysis.streams[1].uncompressed_size_in_bytes, 0);
    assert_eq!(
        analysis.streams[2].uncompressed_offset_in_bytes,
        first.len() as u64,
    );
    assert_consistent(&analysis, first.len() + second.len());

    let plain = corpus(512 * 1024);
    let compressed = bgzf(&plain, 32 * 1024);
    let analysis = decoder(Format::Gzip)
        .analyze(&compressed)
        .expect("BGZF analysis");
    assert!(analysis.streams.len() > 2);
    assert!(analysis.streams.iter().all(|stream| matches!(
        stream.header,
        StreamHeader::Gzip(GzipHeaderFields {
            bgzf_block_size: Some(_),
            ..
        })
    )));
    assert_consistent(&analysis, plain.len());
}

#[test]
fn block_encodings_and_dynamic_alphabets_are_reported() {
    let stored = decoder(Format::Gzip)
        .analyze(&gzip(&corpus(256 * 1024), 0))
        .expect("stored");
    assert!(
        stored
            .blocks
            .iter()
            .any(|block| block.block_type == BlockType::Uncompressed)
    );

    let dynamic = decoder(Format::Gzip)
        .analyze(&gzip(&corpus(1024 * 1024), 6))
        .expect("dynamic");
    let block = dynamic
        .blocks
        .iter()
        .find(|block| block.block_type == BlockType::DynamicHuffman)
        .expect("dynamic block");
    let precode = block.precode.as_ref().expect("precode");
    let literal = block.literal.as_ref().expect("literal alphabet");
    let distance = block.distance.as_ref().expect("distance alphabet");
    assert_eq!(precode.code_lengths.len(), 19);
    assert!((4..=19).contains(&precode.declared_count));
    assert!((257..=286).contains(&literal.declared_count));
    assert!((1..=32).contains(&distance.declared_count));
    assert!(literal.used_count() > 0);
    assert!(
        literal
            .length_range()
            .is_some_and(|(_, maximum)| maximum <= 15)
    );
    assert_eq!(
        literal
            .counts_by_length()
            .into_iter()
            .map(|(_, count)| count)
            .sum::<usize>(),
        literal.code_lengths.len(),
    );
}

#[test]
fn detailed_backreferences_are_budgeted_but_summaries_stay_exact() {
    let plain = corpus(4 * 1024 * 1024);
    let compressed = gzip(&plain, 6);
    let options = AnalyzeOptions::default().maximum_retained_backreferences(3);
    let analysis = decoder(Format::Gzip)
        .analyze_with_options(&compressed, options)
        .expect("analyze");
    let total: u64 = analysis
        .blocks
        .iter()
        .map(|block| block.window_backreference_count)
        .sum();
    let retained: usize = analysis
        .blocks
        .iter()
        .map(|block| block.retained_backreferences.len())
        .sum();
    let omitted: u64 = analysis
        .blocks
        .iter()
        .map(|block| block.omitted_backreference_count)
        .sum();
    assert!(total > 3, "fixture should reach into predecessor windows");
    assert_eq!(retained, 3);
    assert_eq!(retained as u64 + omitted, total);
    assert_eq!(
        analysis.backreference_length_counts.iter().sum::<u64>(),
        total
    );
    assert!(!analysis.has_complete_backreference_details());
    assert!(analysis.blocks.iter().all(|block| {
        block.farthest_backreference <= 32 * 1024
            && block
                .used_window_symbols
                .is_none_or(|used| used <= 32 * 1024)
    }));
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut value = u32::MAX;
    for &byte in bytes {
        value ^= u32::from(byte);
        for _ in 0..8 {
            value = (value >> 1) ^ (0xEDB_88320 & 0_u32.wrapping_sub(value & 1));
        }
    }
    !value
}

fn optional_header_member(plain: &[u8]) -> Vec<u8> {
    let ordinary = gzip(plain, 0);
    let deflate_and_footer = &ordinary[10..];
    const FLAGS: u8 = 0x02 | 0x04 | 0x08 | 0x10;
    let mut header = vec![0x1f, 0x8b, 8, FLAGS, 1, 2, 3, 4, 2, 3];
    header.extend_from_slice(&6_u16.to_le_bytes());
    header.extend_from_slice(b"XY\x02\x00ok");
    header.extend_from_slice(b"reads.fastq\0");
    header.extend_from_slice(b"analysis fixture\0");
    header.extend_from_slice(&(crc32(&header) as u16).to_le_bytes());
    header.extend_from_slice(deflate_and_footer);
    header
}

#[test]
fn shared_gzip_parser_retains_and_bounds_optional_metadata() {
    let compressed = optional_header_member(b"metadata\n");
    let analysis = decoder(Format::Gzip)
        .analyze(&compressed)
        .expect("optional header");
    let StreamHeader::Gzip(header) = &analysis.streams[0].header else {
        panic!("gzip header")
    };
    assert_eq!(header.flags, 0x1e);
    assert_eq!(header.modification_time, 0x0403_0201);
    assert_eq!(header.extra_flags, 2);
    assert_eq!(header.operating_system, 3);
    assert_eq!(header.extra.as_deref(), Some(b"XY\x02\x00ok".as_slice()));
    assert_eq!(header.file_name.as_deref(), Some(b"reads.fastq".as_slice()));
    assert_eq!(
        header.comment.as_deref(),
        Some(b"analysis fixture".as_slice())
    );
    assert!(header.header_crc16.is_some());

    let error = decoder(Format::Gzip)
        .analyze_with_options(
            &compressed,
            AnalyzeOptions::default().maximum_header_bytes(4),
        )
        .expect_err("metadata limit");
    assert!(matches!(
        error,
        DecodeError::Analysis {
            reason: AnalysisErrorKind::ResourceLimit {
                resource: AnalysisResource::HeaderBytes,
                limit: 4,
            }
        }
    ));

    let mut two_members = compressed;
    two_members.extend_from_slice(&optional_header_member(b"second\n"));
    let error = decoder(Format::Gzip)
        .analyze_with_options(
            &two_members,
            AnalyzeOptions::default().maximum_header_bytes(40),
        )
        .expect_err("metadata limit is shared by all members");
    assert!(matches!(
        error,
        DecodeError::Analysis {
            reason: AnalysisErrorKind::ResourceLimit {
                resource: AnalysisResource::HeaderBytes,
                limit: 40,
            }
        }
    ));
}

#[test]
fn stream_and_block_limits_are_typed() {
    let mut two_members = gzip(b"one", 6);
    two_members.extend_from_slice(&gzip(b"two", 6));
    let stream_error = decoder(Format::Gzip)
        .analyze_with_options(&two_members, AnalyzeOptions::default().maximum_streams(1))
        .expect_err("stream limit");
    assert!(matches!(
        stream_error,
        DecodeError::Analysis {
            reason: AnalysisErrorKind::ResourceLimit {
                resource: AnalysisResource::Streams,
                limit: 1,
            }
        }
    ));

    let block_error = decoder(Format::Gzip)
        .analyze_with_options(
            &gzip(&corpus(2 * 1024 * 1024), 6),
            AnalyzeOptions::default().maximum_blocks(1),
        )
        .expect_err("block limit");
    assert!(matches!(
        block_error,
        DecodeError::Analysis {
            reason: AnalysisErrorKind::ResourceLimit {
                resource: AnalysisResource::Blocks,
                limit: 1,
            }
        }
    ));
}

struct OneByteReads<R>(R);

impl<R: Read> Read for OneByteReads<R> {
    fn read(&mut self, output: &mut [u8]) -> io::Result<usize> {
        let length = output.len().min(1);
        self.0.read(&mut output[..length])
    }
}

#[test]
fn streaming_analysis_handles_one_byte_reads_and_auto_detection() {
    let plain = corpus(128 * 1024);
    let compressed = zlib(&plain, 6);
    let decoder = Decoder::builder()
        .auto_detect_format()
        .input_page_size(1)
        .build()
        .unwrap();
    let analysis = decoder
        .analyze_stream(OneByteReads(Cursor::new(compressed.clone())))
        .expect("stream analysis");
    assert_eq!(analysis.format, Format::Zlib);
    assert_eq!(analysis.compressed_size_in_bytes, compressed.len() as u64);
    assert_consistent(&analysis, plain.len());
}

#[test]
fn checksums_trailing_data_and_output_contracts_are_verified() {
    let plain = corpus(64 * 1024);
    let mut corrupt = gzip(&plain, 6);
    let footer = corrupt.len() - 8;
    corrupt[footer] ^= 0xff;
    assert!(matches!(
        decoder(Format::Gzip).analyze(&corrupt),
        Err(DecodeError::ChecksumMismatch { .. })
    ));

    let mut trailing = raw_deflate(&plain, 6);
    trailing.push(0);
    assert!(matches!(
        decoder(Format::RawDeflate).analyze(&trailing),
        Err(DecodeError::InvalidDeflate { .. })
    ));

    let limited = Decoder::builder()
        .format(Format::Gzip)
        .output_limit(Some(plain.len() as u64 - 1))
        .build()
        .unwrap();
    assert!(matches!(
        limited.analyze(&gzip(&plain, 6)),
        Err(DecodeError::OutputLimitExceeded { .. })
    ));
}

struct ObservedReadAt {
    bytes: Vec<u8>,
    largest_request: AtomicUsize,
}

impl ReadAt for ObservedReadAt {
    fn len(&self) -> io::Result<u64> {
        Ok(self.bytes.len() as u64)
    }

    fn read_at(&self, offset: u64, output: &mut [u8]) -> io::Result<usize> {
        self.largest_request
            .fetch_max(output.len(), Ordering::Relaxed);
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        if start >= self.bytes.len() {
            return Ok(0);
        }
        let count = output.len().min(self.bytes.len() - start);
        output[..count].copy_from_slice(&self.bytes[start..start + count]);
        Ok(count)
    }
}

#[test]
fn positional_input_is_paged_instead_of_materialized() {
    let plain = corpus(1024 * 1024);
    let source = ObservedReadAt {
        bytes: gzip(&plain, 6),
        largest_request: AtomicUsize::new(0),
    };
    let decoder = Decoder::builder()
        .format(Format::Gzip)
        .input_page_size(257)
        .build()
        .unwrap();
    decoder.analyze(&source).expect("analysis");
    assert!(source.largest_request.load(Ordering::Relaxed) <= 257);
}

#[test]
fn footer_values_are_exposed_only_after_validation() {
    let plain = b"verified footer";
    let gzip_analysis = decoder(Format::Gzip)
        .analyze(&gzip(plain, 6))
        .expect("gzip");
    assert!(matches!(
        gzip_analysis.streams[0].footer,
        StreamFooter::Gzip {
            uncompressed_size,
            ..
        } if uncompressed_size == plain.len() as u32
    ));
    let zlib_analysis = decoder(Format::Zlib)
        .analyze(&zlib(plain, 6))
        .expect("zlib");
    assert!(matches!(
        zlib_analysis.streams[0].footer,
        StreamFooter::Zlib { .. }
    ));
}
