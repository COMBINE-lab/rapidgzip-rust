//! Diffs `--analyze` against the real rapidgzip 0.16.0.
//!
//! Ignored by default because it needs `rapidgzip` on the PATH. The interop CI
//! job installs it from PyPI.
//!
//! Two things are masked, both stated in `README.md`:
//!
//! - the benchmark profile, which prints wall-clock durations;
//! - `Number of merged back-references`, whose reference implementation sorts
//!   with `std::sort` and then merges pairwise in a way that can shorten the
//!   current run, so its value depends on how that unstable sort ordered equal
//!   distances. Ours is a plain interval union and does not.
//!
//! Everything else must match byte for byte.

use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const BINARY: &str = env!("CARGO_BIN_EXE_rapidgzip-rust");

fn workspace(name: &str) -> PathBuf {
    let directory = std::env::temp_dir().join(format!("rapidgzip-analyze-{name}"));
    let _ = fs::remove_dir_all(&directory);
    fs::create_dir_all(&directory).expect("create the workspace");
    directory
}

fn rapidgzip_available() -> bool {
    Command::new("rapidgzip")
        .arg("--version")
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .is_ok_and(|status| status.success())
}

/// Text that compresses into several dynamic blocks with real back-references.
fn corpus(bytes: usize) -> Vec<u8> {
    let mut out = Vec::with_capacity(bytes + 64);
    let mut state = 0x243f_6a88_u32;
    while out.len() < bytes {
        state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
        out.extend_from_slice(format!("record {} value {}\n", out.len(), state >> 16).as_bytes());
    }
    out.truncate(bytes);
    out
}

/// Compresses with the system gzip, which is what the reference expects to see.
fn gzip_file(directory: &Path, name: &str, plain: &[u8], level: &str) -> PathBuf {
    let source = directory.join(name);
    fs::write(&source, plain).expect("write the corpus");
    let status = Command::new("gzip")
        .args([level, "-k", "-f"])
        .arg(&source)
        .status()
        .expect("run gzip");
    assert!(status.success(), "gzip failed");
    directory.join(format!("{name}.gz"))
}

/// Drops the lines that cannot match, and normalizes trailing blank lines.
fn masked(report: &str) -> Vec<String> {
    const MASKED_PREFIXES: [&str; 8] = [
        "readDynamicHuffmanCoding",
        "readData",
        "    Read precode",
        "    Create precode HC",
        "    Apply precode HC",
        "    Create distance HC",
        "    Create literal HC",
        "    Number of merged back-references",
    ];
    report
        .lines()
        .filter(|line| {
            !MASKED_PREFIXES
                .iter()
                .any(|prefix| line.starts_with(prefix))
        })
        .map(str::to_owned)
        .collect()
}

fn assert_reports_match(archive: &Path) {
    let ours = Command::new(BINARY)
        .arg("--analyze")
        .arg(archive)
        .output()
        .expect("run our binary");
    assert!(
        ours.status.success(),
        "our analyze failed: {}",
        String::from_utf8_lossy(&ours.stderr)
    );
    let theirs = Command::new("rapidgzip")
        .arg("--analyze")
        .arg(archive)
        .output()
        .expect("run rapidgzip");
    assert!(theirs.status.success(), "reference analyze failed");

    let ours = masked(&String::from_utf8_lossy(&ours.stdout));
    let theirs = masked(&String::from_utf8_lossy(&theirs.stdout));

    for (number, (left, right)) in theirs.iter().zip(ours.iter()).enumerate() {
        assert_eq!(
            left,
            right,
            "line {} differs for {}\n  reference: {left:?}\n  ours     : {right:?}",
            number + 1,
            archive.display()
        );
    }
    assert_eq!(
        theirs.len(),
        ours.len(),
        "report length differs for {}",
        archive.display()
    );
}

#[test]
#[ignore = "requires rapidgzip"]
fn a_single_member_report_matches() {
    if !rapidgzip_available() {
        println!("skipped: rapidgzip is not installed");
        return;
    }
    let directory = workspace("single");
    let archive = gzip_file(&directory, "corpus.txt", &corpus(4 * 1024 * 1024), "-6");
    assert_reports_match(&archive);
}

#[test]
#[ignore = "requires rapidgzip"]
fn a_stored_member_report_matches() {
    if !rapidgzip_available() {
        println!("skipped: rapidgzip is not installed");
        return;
    }
    let directory = workspace("stored");
    let archive = gzip_file(&directory, "corpus.txt", &corpus(512 * 1024), "-1");
    assert_reports_match(&archive);
}

#[test]
#[ignore = "requires rapidgzip"]
fn a_concatenated_report_matches() {
    if !rapidgzip_available() {
        println!("skipped: rapidgzip is not installed");
        return;
    }
    let directory = workspace("concatenated");
    let first = gzip_file(&directory, "first.txt", &corpus(768 * 1024), "-6");
    let second = gzip_file(&directory, "second.txt", &corpus(512 * 1024), "-9");
    let combined = directory.join("combined.gz");
    let mut bytes = fs::read(&first).expect("read");
    bytes.extend_from_slice(&fs::read(&second).expect("read"));
    fs::File::create(&combined)
        .expect("create")
        .write_all(&bytes)
        .expect("write");
    assert_reports_match(&combined);
}

#[test]
#[ignore = "requires rapidgzip"]
fn a_tiny_member_report_matches() {
    if !rapidgzip_available() {
        println!("skipped: rapidgzip is not installed");
        return;
    }
    let directory = workspace("tiny");
    let archive = gzip_file(&directory, "corpus.txt", b"hello\n", "-6");
    assert_reports_match(&archive);
}
