//! Differential checks against rapidgzip 0.16.0's `--analyze` report.
//!
//! Timings are local measurements. The merged-reference line is also masked:
//! the reference combines an unstable equal-distance sort with a merge that
//! can shorten a containing interval, whereas the Rust implementation reports
//! a deterministic interval union. All remaining lines compare byte for byte.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const BINARY: &str = env!("CARGO_BIN_EXE_rapidgzip-rust");

fn workspace(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!(
        "rapidgzip-rust-analyze-{}-{name}",
        std::process::id()
    ));
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).expect("create test directory");
    directory
}

fn reference_available() -> bool {
    Command::new("rapidgzip")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

fn corpus(bytes: usize) -> Vec<u8> {
    let mut output = Vec::with_capacity(bytes + 64);
    let mut state = 0x243f_6a88_u32;
    while output.len() < bytes {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        output.extend_from_slice(
            format!("record {} value {}\n", output.len(), state >> 16).as_bytes(),
        );
    }
    output.truncate(bytes);
    output
}

fn gzip_file(directory: &Path, name: &str, plain: &[u8], level: &str) -> PathBuf {
    let source = directory.join(name);
    fs::write(&source, plain).expect("write corpus");
    let status = Command::new("gzip")
        .args([level, "-k", "-f"])
        .arg(&source)
        .status()
        .expect("run gzip");
    assert!(status.success(), "gzip failed");
    directory.join(format!("{name}.gz"))
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

fn stored_gzip_file(directory: &Path, name: &str, plain: &[u8]) -> PathBuf {
    let path = directory.join(name);
    let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
    let chunks = plain.chunks(u16::MAX as usize);
    let count = chunks.len();
    for (index, chunk) in chunks.enumerate() {
        encoded.push(u8::from(index + 1 == count));
        let length = chunk.len() as u16;
        encoded.extend_from_slice(&length.to_le_bytes());
        encoded.extend_from_slice(&(!length).to_le_bytes());
        encoded.extend_from_slice(chunk);
    }
    encoded.extend_from_slice(&crc32(plain).to_le_bytes());
    encoded.extend_from_slice(&(plain.len() as u32).to_le_bytes());
    fs::write(&path, encoded).expect("write stored gzip");
    path
}

fn masked(report: &str) -> Vec<&str> {
    const PREFIXES: [&str; 9] = [
        "readDynamicHuffmanCoding",
        "readData",
        "    Read precode",
        "    Create precode HC",
        "    Apply precode HC",
        "    Create distance HC",
        "    Create literal HC",
        "    Merged back-references to preceding window",
        "    Number of merged back-references",
    ];
    report
        .lines()
        .filter(|line| !PREFIXES.iter().any(|prefix| line.starts_with(prefix)))
        .collect()
}

fn assert_reports_match(archive: &Path, verbose: bool) {
    if !reference_available() {
        eprintln!("skipped: rapidgzip 0.16.0 is not installed");
        return;
    }
    let mut ours_command = Command::new(BINARY);
    ours_command.arg("--analyze");
    if verbose {
        ours_command.arg("--verbose");
    }
    let ours = ours_command
        .arg(archive)
        .output()
        .expect("run Rust analyzer");
    assert!(
        ours.status.success(),
        "Rust analyzer failed: {}",
        String::from_utf8_lossy(&ours.stderr)
    );
    let mut reference_command = Command::new("rapidgzip");
    reference_command.arg("--analyze");
    if verbose {
        reference_command.arg("--verbose");
    }
    let reference = reference_command
        .arg(archive)
        .output()
        .expect("run reference analyzer");
    assert!(reference.status.success(), "reference analyzer failed");

    let ours_text = String::from_utf8_lossy(&ours.stdout);
    let reference_text = String::from_utf8_lossy(&reference.stdout);
    let ours = masked(&ours_text);
    let reference = masked(&reference_text);
    for (line, (expected, actual)) in reference.iter().zip(&ours).enumerate() {
        assert_eq!(
            expected,
            actual,
            "line {} differs for {}",
            line + 1,
            archive.display()
        );
    }
    assert_eq!(reference.len(), ours.len(), "report length differs");
}

#[test]
#[ignore = "requires rapidgzip 0.16.0"]
fn single_and_tiny_member_reports_match() {
    let directory = workspace("single");
    let large = gzip_file(&directory, "corpus.txt", &corpus(4 * 1024 * 1024), "-6");
    assert_reports_match(&large, false);
    let tiny = gzip_file(&directory, "tiny.txt", b"hello\n", "-6");
    assert_reports_match(&tiny, false);
}

#[test]
#[ignore = "requires rapidgzip 0.16.0"]
fn concatenated_report_matches() {
    let directory = workspace("concatenated");
    let first = gzip_file(&directory, "first.txt", &corpus(768 * 1024), "-6");
    let second = gzip_file(&directory, "second.txt", &corpus(512 * 1024), "-9");
    let combined = directory.join("combined.gz");
    let mut bytes = fs::read(&first).expect("read first");
    bytes.extend_from_slice(&fs::read(&second).expect("read second"));
    fs::File::create(&combined)
        .expect("create combined")
        .write_all(&bytes)
        .expect("write combined");
    assert_reports_match(&combined, false);
}

#[test]
#[ignore = "requires rapidgzip 0.16.0"]
fn stored_report_matches() {
    let directory = workspace("stored");
    let archive = stored_gzip_file(&directory, "corpus.txt.gz", &corpus(512 * 1024));
    assert_reports_match(&archive, false);
}

#[test]
#[ignore = "requires rapidgzip 0.16.0"]
fn verbose_reference_details_match() {
    let directory = workspace("verbose");
    let archive = gzip_file(&directory, "corpus.txt", &corpus(4 * 1024 * 1024), "-6");
    assert_reports_match(&archive, true);
}
