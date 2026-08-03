//! End-to-end tests for the installed command-line surface.

use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

const BINARY: &str = env!("CARGO_BIN_EXE_rapidgzip-rust");

fn workspace(name: &str) -> PathBuf {
    let directory =
        std::env::temp_dir().join(format!("rapidgzip-rust-cli-{}-{name}", std::process::id()));
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).expect("create test directory");
    directory
}

fn corpus(lines: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for line in 0..lines {
        bytes.extend_from_slice(format!("{line:06} the quick brown fox jumps over\n").as_bytes());
    }
    bytes
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut value = u32::MAX;
    for &byte in bytes {
        value ^= u32::from(byte);
        for _ in 0..8 {
            value = (value >> 1) ^ (0xedb8_8320 & 0_u32.wrapping_sub(value & 1));
        }
    }
    !value
}

fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
    encoded.extend_from_slice(&raw_deflate(bytes));
    encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    encoded
}

fn raw_deflate(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    let chunks: Vec<&[u8]> = if bytes.is_empty() {
        vec![&[]]
    } else {
        bytes.chunks(u16::MAX as usize).collect()
    };
    for (index, chunk) in chunks.iter().enumerate() {
        encoded.push(u8::from(index + 1 == chunks.len()));
        let length = chunk.len() as u16;
        encoded.extend_from_slice(&length.to_le_bytes());
        encoded.extend_from_slice(&(!length).to_le_bytes());
        encoded.extend_from_slice(chunk);
    }
    encoded
}

fn adler32(bytes: &[u8]) -> u32 {
    const MODULUS: u32 = 65_521;
    let mut first = 1_u32;
    let mut second = 0_u32;
    for &byte in bytes {
        first = (first + u32::from(byte)) % MODULUS;
        second = (second + first) % MODULUS;
    }
    (second << 16) | first
}

fn zlib(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = vec![0x78, 0x01];
    encoded.extend_from_slice(&raw_deflate(bytes));
    encoded.extend_from_slice(&adler32(bytes).to_be_bytes());
    encoded
}

fn bgzf(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    for chunk in bytes.chunks(60 * 1024) {
        let mut deflate = Vec::with_capacity(chunk.len() + 5);
        deflate.push(1);
        let length = chunk.len() as u16;
        deflate.extend_from_slice(&length.to_le_bytes());
        deflate.extend_from_slice(&(!length).to_le_bytes());
        deflate.extend_from_slice(chunk);
        let total = 18 + deflate.len() + 8;
        let mut block = b"\x1f\x8b\x08\x04\0\0\0\0\x00\xff\x06\x00BC\x02\x00".to_vec();
        block.extend_from_slice(&((total - 1) as u16).to_le_bytes());
        block.extend_from_slice(&deflate);
        block.extend_from_slice(&crc32(chunk).to_le_bytes());
        block.extend_from_slice(&(chunk.len() as u32).to_le_bytes());
        encoded.extend_from_slice(&block);
    }
    encoded.extend_from_slice(&[
        31, 139, 8, 4, 0, 0, 0, 0, 0, 255, 6, 0, 66, 67, 2, 0, 27, 0, 3, 0, 0, 0, 0, 0, 0, 0, 0, 0,
    ]);
    encoded
}

fn write(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).expect("write fixture");
}

fn run(arguments: &[&str]) -> Output {
    Command::new(BINARY)
        .args(arguments)
        .stdin(Stdio::null())
        .output()
        .expect("run CLI")
}

fn stdout(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_ok(output: &Output) {
    assert!(
        output.status.success(),
        "expected success; stderr: {}",
        stderr(output)
    );
}

fn assert_failed(output: &Output) {
    assert!(
        !output.status.success(),
        "expected failure; stdout: {}",
        stdout(output)
    );
}

#[test]
fn decodes_to_stdout_and_an_explicit_file() {
    let directory = workspace("basic-output");
    let plain = corpus(200);
    let archive = directory.join("data.gz");
    let target = directory.join("decoded.txt");
    write(&archive, &gzip(&plain));

    let output = run(&["-c", archive.to_str().expect("path")]);
    assert_ok(&output);
    assert_eq!(output.stdout, plain);

    assert_ok(&run(&[
        "-o",
        target.to_str().expect("path"),
        archive.to_str().expect("path"),
    ]));
    assert_eq!(fs::read(&target).expect("read output"), plain);
}

#[test]
fn existing_output_requires_force() {
    let directory = workspace("force");
    let plain = corpus(30);
    let archive = directory.join("data.gz");
    let target = directory.join("decoded.txt");
    write(&archive, &gzip(&plain));
    write(&target, b"preserve me");

    let output = run(&[
        "-o",
        target.to_str().expect("path"),
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(stderr(&output).contains("--force"));
    assert_eq!(fs::read(&target).expect("read"), b"preserve me");

    assert_ok(&run(&[
        "--force",
        "--output",
        target.to_str().expect("path"),
        archive.to_str().expect("path"),
    ]));
    assert_eq!(fs::read(&target).expect("read"), plain);
}

#[test]
fn output_and_index_paths_cannot_destroy_the_input_or_each_other() {
    let directory = workspace("path-aliases");
    let plain = corpus(30);
    let archive = directory.join("data.gz");
    let original = gzip(&plain);
    write(&archive, &original);

    let output = run(&[
        "--force",
        "--output",
        archive.to_str().expect("path"),
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert_eq!(fs::read(&archive).expect("input survives"), original);

    let output = run(&[
        "--force",
        "--export-index",
        archive.to_str().expect("path"),
        "--test",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert_eq!(fs::read(&archive).expect("input survives"), original);

    let shared = directory.join("shared.out");
    let output = run(&[
        "--force",
        "--output",
        shared.to_str().expect("path"),
        "--export-index",
        shared.to_str().expect("path"),
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(!shared.exists());
}

#[cfg(unix)]
#[test]
fn hard_link_and_symlink_aliases_cannot_destroy_the_input() {
    use std::os::unix::fs::symlink;

    let directory = workspace("filesystem-aliases");
    let plain = corpus(30);
    let archive = directory.join("data.gz");
    let hard_link = directory.join("hard-link.gz");
    let symbolic_link = directory.join("symbolic-link.gz");
    let original = gzip(&plain);
    write(&archive, &original);
    fs::hard_link(&archive, &hard_link).expect("hard link");
    symlink(&archive, &symbolic_link).expect("symbolic link");

    for alias in [&hard_link, &symbolic_link] {
        let output = run(&[
            "--force",
            "--output",
            alias.to_str().expect("path"),
            archive.to_str().expect("path"),
        ]);
        assert_failed(&output);
        assert_eq!(fs::read(&archive).expect("input survives"), original);
    }
}

#[cfg(windows)]
#[test]
fn windows_hard_link_alias_cannot_destroy_the_input() {
    let directory = workspace("windows-hard-link-alias");
    let plain = corpus(30);
    let archive = directory.join("data.gz");
    let hard_link = directory.join("hard-link.gz");
    let original = gzip(&plain);
    write(&archive, &original);
    fs::hard_link(&archive, &hard_link).expect("hard link");

    let output = run(&[
        "--force",
        "--output",
        hard_link.to_str().expect("path"),
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert_eq!(fs::read(&archive).expect("input survives"), original);
}

#[test]
fn an_existing_index_requires_force() {
    let directory = workspace("index-force");
    let plain = corpus(30);
    let archive = directory.join("data.gz");
    let index = directory.join("data.rgzidx");
    write(&archive, &gzip(&plain));
    write(&index, b"preserve me");

    let output = run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--index-format",
        "native",
        "--test",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert_eq!(fs::read(&index).expect("preserved"), b"preserve me");

    assert_ok(&run(&[
        "--force",
        "--export-index",
        index.to_str().expect("path"),
        "--index-format",
        "native",
        "--test",
        archive.to_str().expect("path"),
    ]));
    assert!(fs::read(&index).expect("replaced").starts_with(b"RGZIDX01"));
}

#[test]
fn test_count_and_line_count_are_complete_stream_actions() {
    let directory = workspace("report-actions");
    let plain = corpus(300);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    let output = run(&["--test", archive.to_str().expect("path")]);
    assert_ok(&output);
    assert!(output.stdout.is_empty());
    assert!(stderr(&output).contains("ok"));

    let output = run(&["--count", archive.to_str().expect("path")]);
    assert_ok(&output);
    assert_eq!(stdout(&output).trim(), plain.len().to_string());

    let output = run(&["--count-lines", archive.to_str().expect("path")]);
    assert_ok(&output);
    assert_eq!(stdout(&output).trim(), "300");
}

#[test]
fn payload_and_counts_share_one_decode_without_mixing_stdout() {
    let directory = workspace("combined-actions");
    let plain = corpus(300);
    let archive = directory.join("data.gz");
    let target = directory.join("decoded.txt");
    write(&archive, &gzip(&plain));

    let output = run(&[
        "--stdout",
        "--count",
        "--count-lines",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    assert_eq!(output.stdout, plain);
    let diagnostic = stderr(&output);
    assert!(
        diagnostic
            .lines()
            .any(|line| line == plain.len().to_string())
    );
    assert!(diagnostic.lines().any(|line| line == "300"));

    let output = run(&[
        "--output",
        target.to_str().expect("path"),
        "--count-lines",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    assert_eq!(fs::read(target).expect("decoded file"), plain);
    assert_eq!(stdout(&output).trim(), "300");
}

#[test]
fn quiet_and_verbose_control_success_diagnostics() {
    let directory = workspace("volume");
    let plain = corpus(40);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    let output = run(&["--quiet", "--test", archive.to_str().expect("path")]);
    assert_ok(&output);
    assert!(output.stderr.is_empty());

    let output = run(&["--verbose", "--test", archive.to_str().expect("path")]);
    assert_ok(&output);
    let diagnostic = stderr(&output);
    assert!(diagnostic.contains("throughput"), "{diagnostic}");
    assert!(
        diagnostic.contains(&plain.len().to_string()),
        "{diagnostic}"
    );

    let missing = directory.join("missing.gz");
    let output = run(&["--quiet", "--test", missing.to_str().expect("path")]);
    assert_failed(&output);
    assert!(stderr(&output).contains("rapidgzip-rust:"));
}

#[test]
fn indexes_round_trip_through_byte_ranges_and_full_decode() {
    let directory = workspace("index-round-trip");
    let plain = corpus(20_000);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    for format in ["indexed_gzip", "gztool", "native"] {
        let index = directory.join(format!("data-{format}.idx"));
        assert_ok(&run(&[
            "--export-index",
            index.to_str().expect("path"),
            "--index-format",
            format,
            "--test",
            archive.to_str().expect("path"),
        ]));
        assert!(fs::metadata(&index).expect("index").len() > 0);

        let output = run(&[
            "--import-index",
            index.to_str().expect("path"),
            "--ranges",
            "64@1024",
            "--stdout",
            archive.to_str().expect("path"),
        ]);
        assert_ok(&output);
        assert_eq!(output.stdout, &plain[1024..1088], "format {format}");

        let output = run(&[
            "--import-index",
            index.to_str().expect("path"),
            "--stdout",
            archive.to_str().expect("path"),
        ]);
        assert_ok(&output);
        assert_eq!(output.stdout, plain, "full indexed decode for {format}");
    }
}

#[test]
fn native_index_is_the_format_neutral_default() {
    let directory = workspace("native-default");
    let plain = corpus(20_000);
    for (name, format, compressed) in [
        ("zlib", "zlib", zlib(&plain)),
        ("raw", "raw-deflate", raw_deflate(&plain)),
    ] {
        let archive = directory.join(format!("data.{name}"));
        let index = directory.join(format!("data.{name}.idx"));
        write(&archive, &compressed);
        assert_ok(&run(&[
            "--format",
            format,
            "--export-index",
            index.to_str().expect("path"),
            "--test",
            archive.to_str().expect("path"),
        ]));
        assert!(fs::read(&index).expect("index").starts_with(b"RGZIDX01"));
        let output = run(&[
            "--format",
            format,
            "--import-index",
            index.to_str().expect("path"),
            "--stdout",
            archive.to_str().expect("path"),
        ]);
        assert_ok(&output);
        assert_eq!(output.stdout, plain, "format {name}");
    }
}

#[test]
fn failed_index_conversion_never_damages_the_destination() {
    let directory = workspace("transactional-index");
    let plain = corpus(1_000);
    let archive = directory.join("data.gz");
    let existing = directory.join("existing.gzi");
    let absent = directory.join("absent.gzi");
    write(&archive, &gzip(&plain));
    write(&existing, b"preserve me");

    for destination in [&existing, &absent] {
        let output = run(&[
            "--force",
            "--export-index",
            destination.to_str().expect("path"),
            "--index-format",
            "gzi",
            "--test",
            archive.to_str().expect("path"),
        ]);
        assert_failed(&output);
    }
    assert_eq!(fs::read(existing).expect("preserved"), b"preserve me");
    assert!(!absent.exists());
    assert!(fs::read_dir(directory).expect("directory").all(|entry| {
        !entry
            .expect("entry")
            .file_name()
            .to_string_lossy()
            .contains("rapidgzip-rust-index")
    }));
}

#[test]
fn imported_indexes_reject_trailing_bytes_and_ambiguous_gzi_fallback() {
    let directory = workspace("strict-index-file");
    let plain = corpus(2_000);
    let archive = directory.join("data.gz");
    let index = directory.join("data.idx");
    write(&archive, &gzip(&plain));
    assert_ok(&run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--test",
        archive.to_str().expect("path"),
    ]));

    let mut bytes = fs::read(&index).expect("index");
    bytes.extend_from_slice(b"trailing");
    write(&index, &bytes);
    let output = run(&[
        "--import-index",
        index.to_str().expect("path"),
        "--test",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(stderr(&output).contains("trailing bytes"));

    write(&index, &[0; 8]);
    fs::OpenOptions::new()
        .append(true)
        .open(&index)
        .expect("open")
        .write_all(b"garbage")
        .expect("append");
    let output = run(&[
        "--import-index",
        index.to_str().expect("path"),
        "--test",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(stderr(&output).contains("unrecognized index format"));
}

#[test]
fn sparse_hostile_gzi_is_rejected_before_checkpoint_allocation() {
    let directory = workspace("bounded-gzi");
    let plain = corpus(10);
    let archive = directory.join("data.gz");
    let index = directory.join("hostile.gzi");
    write(&archive, &gzip(&plain));
    let count = 4_u64 * 1024 * 1024 + 1;
    let file = fs::File::create(&index).expect("create sparse index");
    file.set_len(8 + count * 16).expect("make sparse");
    drop(file);
    let mut file = fs::OpenOptions::new()
        .write(true)
        .open(&index)
        .expect("open sparse");
    file.write_all(&count.to_le_bytes()).expect("write count");
    drop(file);

    let output = run(&[
        "--import-index",
        index.to_str().expect("path"),
        "--test",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(stderr(&output).contains("excessive BGZF index pair count"));
}

#[test]
fn imported_ranges_reject_ignored_decoder_options_without_verify() {
    let directory = workspace("range-options");
    let plain = corpus(20_000);
    let archive = directory.join("data.gz");
    let index = directory.join("data.idx");
    write(&archive, &gzip(&plain));
    assert_ok(&run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--test",
        archive.to_str().expect("path"),
    ]));

    for option in [
        &["--threads", "2"][..],
        &["--chunk-size", "64"][..],
        &["--expected-size", "720000"][..],
        &["--format", "gzip"][..],
    ] {
        let mut arguments = option.to_vec();
        arguments.extend([
            "--import-index",
            index.to_str().expect("path"),
            "--ranges",
            "16@32",
            "--stdout",
            archive.to_str().expect("path"),
        ]);
        let output = run(&arguments);
        assert_failed(&output);
        assert!(stderr(&output).contains("--verify"));
    }

    let output = run(&[
        "--verify",
        "--threads",
        "2",
        "--format",
        "gzip",
        "--expected-size",
        &plain.len().to_string(),
        "--import-index",
        index.to_str().expect("path"),
        "--ranges",
        "16@32",
        "--stdout",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    assert_eq!(output.stdout, &plain[32..48]);
}

#[test]
fn imported_range_verify_authenticates_bytes_skipped_by_random_access() {
    let directory = workspace("range-verification");
    let first = corpus(128 * 1024);
    let second = corpus(128 * 1024);
    let mut plain = first.clone();
    plain.extend_from_slice(&second);
    let archive = directory.join("data.gz");
    let index = directory.join("data.idx");
    let mut compressed = gzip(&first);
    compressed.extend_from_slice(&gzip(&second));
    write(&archive, &compressed);
    assert_ok(&run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--test",
        archive.to_str().expect("path"),
    ]));

    // Stored-DEFLATE bytes map directly to output. Change a byte in the first
    // member, then seek from the authenticated second-member checkpoint. The
    // random read can verify the second member but cannot authenticate the
    // skipped first one without the explicit complete pass.
    compressed[100] ^= 1;
    write(&archive, &compressed);
    let offset = first.len() + 32;
    let specification = format!("32@{offset}");

    let partial = run(&[
        "--import-index",
        index.to_str().expect("path"),
        "--ranges",
        &specification,
        "--stdout",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&partial);
    assert_eq!(partial.stdout, &plain[offset..offset + 32]);

    let verified = run(&[
        "--verify",
        "--import-index",
        index.to_str().expect("path"),
        "--ranges",
        &specification,
        "--stdout",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&verified);
    assert!(stderr(&verified).contains("CRC32 mismatch"));
    assert!(verified.stdout.is_empty());
}

#[test]
fn bgzf_gzi_round_trips_through_full_indexed_decode() {
    let directory = workspace("gzi-round-trip");
    let plain = corpus(10_000);
    let archive = directory.join("data.bgz");
    let index = directory.join("data.bgz.gzi");
    write(&archive, &bgzf(&plain));

    assert_ok(&run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--index-format",
        "gzi",
        "--test",
        archive.to_str().expect("path"),
    ]));
    let output = run(&[
        "--import-index",
        index.to_str().expect("path"),
        "--stdout",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    assert_eq!(output.stdout, plain);
}

#[test]
fn malformed_imported_index_is_not_silently_ignored() {
    let directory = workspace("strict-index");
    let plain = corpus(20_000);
    let archive = directory.join("data.gz");
    let index = directory.join("data.rgzidx");
    write(&archive, &gzip(&plain));
    assert_ok(&run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--index-format",
        "native",
        "--test",
        archive.to_str().expect("path"),
    ]));

    let mut bytes = fs::read(&index).expect("read index");
    // Native v1's first checkpoint begins at byte 52. Moving its compressed
    // boundary by one bit keeps the file parseable but makes it source-wrong.
    let original = u64::from_le_bytes(bytes[52..60].try_into().expect("offset"));
    bytes[52..60].copy_from_slice(&original.saturating_add(1).to_le_bytes());
    write(&index, &bytes);

    let output = run(&[
        "--import-index",
        index.to_str().expect("path"),
        "--stdout",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(output.stdout.is_empty());
}

#[test]
fn byte_line_mixed_and_overlapping_ranges_preserve_order() {
    let directory = workspace("ranges");
    let plain = corpus(10_000);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    let output = run(&[
        "--ranges",
        "8@0,8@4,inf@319960",
        "--stdout",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    let mut expected = Vec::new();
    expected.extend_from_slice(&plain[0..8]);
    expected.extend_from_slice(&plain[4..12]);
    expected.extend_from_slice(&plain[319_960..]);
    assert_eq!(output.stdout, expected);

    let output = run(&[
        "--ranges",
        "2L@3L",
        "--stdout",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    let line_bytes = plain.iter().position(|&byte| byte == b'\n').expect("line") + 1;
    assert_eq!(output.stdout, &plain[3 * line_bytes..5 * line_bytes]);
}

#[test]
fn line_aware_gztool_index_round_trips_and_requires_counting() {
    let directory = workspace("gztool-lines");
    let plain = corpus(8_000);
    let archive = directory.join("data.gz");
    let index = directory.join("data.gzi");
    write(&archive, &gzip(&plain));

    let output = run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--index-format",
        "gztool-with-lines",
        "--test",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(stderr(&output).contains("--count-lines"));

    let output = run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--index-format",
        "gztool-with-lines",
        "--count-lines",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    assert_eq!(stdout(&output).trim(), "8000");

    let output = run(&[
        "--import-index",
        index.to_str().expect("path"),
        "--ranges",
        "2L@25L",
        "--stdout",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    let line_bytes = plain.iter().position(|&byte| byte == b'\n').expect("line") + 1;
    assert_eq!(output.stdout, &plain[25 * line_bytes..27 * line_bytes]);
}

#[test]
fn line_ranges_reject_an_import_without_line_metadata() {
    let directory = workspace("missing-lines");
    let plain = corpus(2_000);
    let archive = directory.join("data.gz");
    let index = directory.join("data.gzidx");
    write(&archive, &gzip(&plain));
    assert_ok(&run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--test",
        archive.to_str().expect("path"),
    ]));

    let output = run(&[
        "--import-index",
        index.to_str().expect("path"),
        "--ranges",
        "1L@1L",
        "--stdout",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(stderr(&output).contains("line metadata"));
}

#[test]
fn stdin_is_decoded_sequentially() {
    let plain = corpus(100);
    let archive = gzip(&plain);
    let mut child = Command::new(BINARY)
        .args(["--stdout", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    child
        .stdin
        .as_mut()
        .expect("stdin")
        .write_all(&archive)
        .expect("write stdin");
    let output = child.wait_with_output().expect("wait");
    assert_ok(&output);
    assert_eq!(output.stdout, plain);
}

#[test]
fn closed_output_pipe_is_a_successful_early_consumer_exit() {
    let directory = workspace("broken-pipe");
    let plain = corpus(500_000);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));
    let mut child = Command::new(BINARY)
        .args(["--stdout", archive.to_str().expect("path")])
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn");
    let mut pipe = child.stdout.take().expect("stdout pipe");
    let mut byte = [0_u8; 1];
    pipe.read_exact(&mut byte).expect("first output byte");
    drop(pipe);
    let output = child.wait_with_output().expect("wait");
    assert_ok(&output);
    assert!(output.stderr.is_empty(), "stderr: {}", stderr(&output));
}

#[test]
fn compatibility_aliases_are_honored_and_semantic_gaps_are_rejected() {
    let directory = workspace("compatibility");
    let plain = corpus(20);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    for argument in ["--keep", "--decompress", "--verify", "--no-sparse-windows"] {
        let output = run(&[argument, "--stdout", archive.to_str().expect("path")]);
        assert_ok(&output);
        assert_eq!(output.stdout, plain, "argument {argument}");
    }
    let output = run(&[
        "--io-read-method",
        "pread",
        "--stdout",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    assert_eq!(output.stdout, plain);

    for arguments in [
        vec!["--no-verify"],
        vec!["--sparse-windows"],
        vec!["--io-read-method", "sequential"],
        vec!["--io-read-method", "locked-read"],
    ] {
        let mut complete = arguments;
        complete.extend(["--stdout", archive.to_str().expect("path")]);
        let output = run(&complete);
        assert_failed(&output);
        assert!(output.stdout.is_empty());
    }
}

#[test]
fn attributions_do_not_require_an_input() {
    let output = run(&["--oss-attributions"]);
    assert_ok(&output);
    assert!(stdout(&output).contains("zlib-rs"));

    let output = run(&["--oss-attributions-yaml"]);
    assert_ok(&output);
    assert!(stdout(&output).contains("license:"));
}

#[test]
fn corrupt_input_and_invalid_configuration_fail() {
    let directory = workspace("failures");
    let plain = corpus(50);
    let archive = directory.join("data.gz");
    let mut compressed = gzip(&plain);
    let last = compressed.len() - 1;
    compressed[last] ^= 0xff;
    write(&archive, &compressed);
    assert_failed(&run(&["--test", archive.to_str().expect("path")]));

    let valid = directory.join("valid.gz");
    write(&valid, &gzip(&plain));
    let output = run(&["--chunk-size", "0", "--test", valid.to_str().expect("path")]);
    assert_failed(&output);
    assert!(stderr(&output).contains("decoded_chunk_size"));
}
