//! End-to-end tests driving the built binary.
//!
//! These run the real executable through `std::process::Command`, so they
//! cover argument wiring, exit codes, and file effects. Pure logic such as
//! range parsing is unit-tested next to the code instead.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Output, Stdio};

const BINARY: &str = env!("CARGO_BIN_EXE_rapidgzip-rust");

/// Returns a fresh directory for one test.
fn workspace(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!("rapidgzip-cli-{name}"));
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).expect("create the workspace");
    directory
}

/// Text with a known size and newline count.
fn corpus(lines: usize) -> Vec<u8> {
    let mut bytes = Vec::new();
    for index in 0..lines {
        bytes.extend_from_slice(format!("{index:06} the quick brown fox jumps over\n").as_bytes());
    }
    bytes
}

fn crc32(bytes: &[u8]) -> u32 {
    let mut value = u32::MAX;
    for &byte in bytes {
        value ^= u32::from(byte);
        for _ in 0..8 {
            value = (value >> 1) ^ (0xEDB8_8320 & 0_u32.wrapping_sub(value & 1));
        }
    }
    !value
}

/// Wraps `bytes` in a single gzip member of stored DEFLATE blocks.
///
/// Building the archive here rather than shelling out to gzip keeps the tests
/// independent of what is installed.
fn gzip(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
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
    encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    encoded
}

fn write(path: &Path, bytes: &[u8]) {
    fs::write(path, bytes).expect("write the file");
}

fn run(arguments: &[&str]) -> Output {
    Command::new(BINARY)
        .args(arguments)
        .stdin(Stdio::null())
        .output()
        .expect("run the binary")
}

fn stdout_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stdout).into_owned()
}

fn stderr_of(output: &Output) -> String {
    String::from_utf8_lossy(&output.stderr).into_owned()
}

fn assert_ok(output: &Output) {
    assert!(
        output.status.success(),
        "expected success, stderr was: {}",
        stderr_of(output)
    );
}

fn assert_failed(output: &Output) {
    assert!(
        !output.status.success(),
        "expected failure, stdout was: {}",
        stdout_of(output)
    );
}

#[test]
fn decompresses_to_stdout() {
    let directory = workspace("stdout");
    let plain = corpus(200);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    let output = run(&["-c", archive.to_str().expect("path")]);
    assert_ok(&output);
    assert_eq!(output.stdout, plain);
}

#[test]
fn decompresses_to_a_named_file() {
    let directory = workspace("named-output");
    let plain = corpus(200);
    let archive = directory.join("data.gz");
    let target = directory.join("out.txt");
    write(&archive, &gzip(&plain));

    assert_ok(&run(&[
        "-o",
        target.to_str().expect("path"),
        archive.to_str().expect("path"),
    ]));
    assert_eq!(fs::read(&target).expect("read"), plain);
}

#[test]
fn an_existing_output_file_is_refused_without_force() {
    let directory = workspace("overwrite");
    let plain = corpus(50);
    let archive = directory.join("data.gz");
    let target = directory.join("out.txt");
    write(&archive, &gzip(&plain));
    write(&target, b"do not lose me");

    let output = run(&[
        "-o",
        target.to_str().expect("path"),
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(
        stderr_of(&output).contains("--force"),
        "the message should say how to proceed: {}",
        stderr_of(&output)
    );
    assert_eq!(fs::read(&target).expect("read"), b"do not lose me");

    assert_ok(&run(&[
        "-f",
        "-o",
        target.to_str().expect("path"),
        archive.to_str().expect("path"),
    ]));
    assert_eq!(fs::read(&target).expect("read"), plain);
}

#[test]
fn testing_verifies_without_writing() {
    let directory = workspace("test-action");
    let plain = corpus(100);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    let output = run(&["-t", archive.to_str().expect("path")]);
    assert_ok(&output);
    assert!(output.stdout.is_empty(), "--test writes no payload");
    assert!(stderr_of(&output).contains("ok"));
}

#[test]
fn quiet_suppresses_the_result_but_not_the_failure() {
    let directory = workspace("quiet");
    let plain = corpus(10);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    let output = run(&["-q", "-t", archive.to_str().expect("path")]);
    assert_ok(&output);
    assert!(stderr_of(&output).is_empty(), "--test line should be quiet");

    let missing = directory.join("absent.gz");
    let output = run(&["-q", "-t", missing.to_str().expect("path")]);
    assert_failed(&output);
    assert!(
        stderr_of(&output).contains("rapidgzip-rust:"),
        "a failure is never non-critical"
    );
}

#[test]
fn verbose_reports_the_container_and_sizes() {
    let directory = workspace("verbose");
    let plain = corpus(100);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    let output = run(&["-v", "-c", archive.to_str().expect("path")]);
    assert_ok(&output);
    let stderr = stderr_of(&output);
    assert!(stderr.contains("format"), "{stderr}");
    assert!(stderr.contains(&plain.len().to_string()), "{stderr}");
}

#[test]
fn counting_reports_size_and_lines() {
    let directory = workspace("counts");
    let plain = corpus(300);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    let output = run(&["--count", archive.to_str().expect("path")]);
    assert_ok(&output);
    assert_eq!(stdout_of(&output).trim(), plain.len().to_string());

    let output = run(&["-l", archive.to_str().expect("path")]);
    assert_ok(&output);
    assert_eq!(stdout_of(&output).trim(), "300");
}

#[test]
fn every_index_format_round_trips_through_ranges() {
    let directory = workspace("index-formats");
    let plain = corpus(20_000);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    for format in ["indexed_gzip", "gztool", "native"] {
        let index = directory.join(format!("data.{format}.idx"));
        assert_ok(&run(&[
            "--export-index",
            index.to_str().expect("path"),
            "--index-format",
            format,
            "-t",
            archive.to_str().expect("path"),
        ]));
        assert!(
            fs::metadata(&index).expect("index exists").len() > 0,
            "{format} produced an empty index"
        );

        let output = run(&[
            "--import-index",
            index.to_str().expect("path"),
            "--ranges",
            "64@1024",
            "-c",
            archive.to_str().expect("path"),
        ]);
        assert_ok(&output);
        assert_eq!(output.stdout, &plain[1024..1088], "for format {format}");
    }
}

#[test]
fn the_line_aware_gztool_format_needs_counted_lines() {
    let directory = workspace("gztool-lines");
    let plain = corpus(5_000);
    let archive = directory.join("data.gz");
    let index = directory.join("data.gzi");
    write(&archive, &gzip(&plain));

    let output = run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--index-format",
        "gztool-with-lines",
        "-t",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(
        stderr_of(&output).contains("--count-lines"),
        "the message should name the missing option: {}",
        stderr_of(&output)
    );

    assert_ok(&run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--index-format",
        "gztool-with-lines",
        "--count-lines",
        archive.to_str().expect("path"),
    ]));
    assert!(fs::metadata(&index).expect("index exists").len() > 0);
}

#[test]
fn ranges_extract_bytes_and_lines() {
    let directory = workspace("ranges");
    let plain = corpus(10_000);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    // Byte ranges, in the order given, including an overlap.
    let output = run(&[
        "--ranges",
        "8@0,8@4,inf@319960",
        "-c",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    let mut expected = Vec::new();
    expected.extend_from_slice(&plain[0..8]);
    expected.extend_from_slice(&plain[4..12]);
    expected.extend_from_slice(&plain[319_960..]);
    assert_eq!(output.stdout, expected);

    // Line ranges, which need the index to carry line offsets.
    let output = run(&["--ranges", "2L@3L", "-c", archive.to_str().expect("path")]);
    assert_ok(&output);
    let line_length = plain
        .iter()
        .position(|&byte| byte == b'\n')
        .expect("the corpus has newlines")
        + 1;
    let line_start = 3 * line_length;
    assert_eq!(
        output.stdout,
        &plain[line_start..line_start + 2 * line_length]
    );
}

#[test]
fn a_line_range_needs_an_index_with_line_offsets() {
    let directory = workspace("range-no-lines");
    let plain = corpus(2_000);
    let archive = directory.join("data.gz");
    let index = directory.join("data.idx");
    write(&archive, &gzip(&plain));

    assert_ok(&run(&[
        "--export-index",
        index.to_str().expect("path"),
        "--index-format",
        "indexed_gzip",
        "-t",
        archive.to_str().expect("path"),
    ]));

    let output = run(&[
        "--import-index",
        index.to_str().expect("path"),
        "--ranges",
        "1L@1L",
        "-c",
        archive.to_str().expect("path"),
    ]);
    assert_failed(&output);
    assert!(
        stderr_of(&output).contains("line offsets"),
        "the message should explain the gap: {}",
        stderr_of(&output)
    );
}

#[test]
fn standard_input_is_decoded() {
    let directory = workspace("stdin");
    let plain = corpus(100);
    let archive = gzip(&plain);
    let _ = directory;

    let mut child = Command::new(BINARY)
        .args(["-c", "-"])
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
        .expect("write");
    let output = child.wait_with_output().expect("wait");
    assert_ok(&output);
    assert_eq!(output.stdout, plain);
}

#[test]
fn compatibility_no_ops_are_accepted() {
    let directory = workspace("no-ops");
    let plain = corpus(20);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    for argument in [
        "-k",
        "-d",
        "--verify",
        "--sparse-windows",
        "--no-sparse-windows",
    ] {
        let output = run(&[argument, "-c", archive.to_str().expect("path")]);
        assert_ok(&output);
        assert_eq!(output.stdout, plain, "for {argument}");
    }
    for method in ["pread", "sequential", "locked-read"] {
        let output = run(&[
            "--io-read-method",
            method,
            "-c",
            archive.to_str().expect("path"),
        ]);
        assert_ok(&output);
        assert_eq!(output.stdout, plain, "for {method}");
    }
}

#[test]
fn no_verify_is_rejected_rather_than_ignored() {
    let directory = workspace("no-verify");
    let plain = corpus(20);
    let archive = directory.join("data.gz");
    write(&archive, &gzip(&plain));

    let output = run(&["--no-verify", "-c", archive.to_str().expect("path")]);
    assert_failed(&output);
    let stderr = stderr_of(&output);
    assert!(stderr.contains("--no-verify"), "{stderr}");
    assert!(
        output.stdout.is_empty(),
        "nothing should be decoded before refusing"
    );
}

#[test]
fn corrupt_input_fails_with_a_nonzero_status() {
    let directory = workspace("corrupt");
    let plain = corpus(200);
    let archive = directory.join("data.gz");
    let mut bytes = gzip(&plain);
    let last = bytes.len() - 1;
    bytes[last] ^= 0xff;
    write(&archive, &bytes);

    assert_failed(&run(&["-t", archive.to_str().expect("path")]));
}

#[test]
fn a_missing_input_fails_cleanly() {
    let output = run(&["-t", "/nonexistent/rapidgzip/input.gz"]);
    assert_failed(&output);
    assert!(stderr_of(&output).contains("rapidgzip-rust:"));
}

#[test]
fn attributions_print_without_an_input() {
    let output = run(&["--oss-attributions"]);
    assert_ok(&output);
    assert!(stdout_of(&output).contains("zlib-rs"));

    let output = run(&["--oss-attributions-yaml"]);
    assert_ok(&output);
    assert!(stdout_of(&output).contains("license:"));
}

#[test]
fn an_imported_index_accelerates_a_plain_decompression() {
    let directory = workspace("import-without-ranges");
    let plain = corpus(500);
    let archive = directory.join("data.gz");
    let index = directory.join("data.idx");
    write(&archive, &gzip(&plain));
    assert_ok(&run(&[
        "--export-index",
        index.to_str().expect("path"),
        "-t",
        archive.to_str().expect("path"),
    ]));

    // The index is no longer only for seeking: it gives every worker a resume
    // point, so an ordinary decompression uses it and must still be exact.
    let output = run(&[
        "--import-index",
        index.to_str().expect("path"),
        "-c",
        archive.to_str().expect("path"),
    ]);
    assert_ok(&output);
    assert_eq!(output.stdout, plain);
}
