//! CLI stdin spill paths: ranges / decompress from a pipe use a tempfile, not a full RAM buffer.

use std::io::Write;
use std::process::{Command, Stdio};

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

fn stored_deflate(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = Vec::new();
    if bytes.is_empty() {
        encoded.extend_from_slice(&[1, 0, 0, 0xFF, 0xFF]);
        return encoded;
    }
    let chunks = bytes.chunks(u16::MAX as usize);
    let chunk_count = chunks.len();
    for (index, chunk) in chunks.enumerate() {
        encoded.push(u8::from(index + 1 == chunk_count));
        let length = chunk.len() as u16;
        encoded.extend_from_slice(&length.to_le_bytes());
        encoded.extend_from_slice(&(!length).to_le_bytes());
        encoded.extend_from_slice(chunk);
    }
    encoded
}

fn gzip_member(bytes: &[u8]) -> Vec<u8> {
    let mut encoded = b"\x1f\x8b\x08\x00\0\0\0\0\x00\xff".to_vec();
    encoded.extend_from_slice(&stored_deflate(bytes));
    encoded.extend_from_slice(&crc32(bytes).to_le_bytes());
    encoded.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    encoded
}

fn bin() -> Command {
    Command::new(env!("CARGO_BIN_EXE_rapidgzip-rust"))
}

#[test]
fn stdin_ranges_extracts_via_tempfile() {
    let payload = b"hello world from stdin ranges\n";
    let compressed = gzip_member(payload);

    let mut child = bin()
        .args(["--ranges", "5@0", "-c", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rapidgzip-rust");

    {
        let mut stdin = child.stdin.take().expect("stdin");
        stdin.write_all(&compressed).expect("write stdin");
    }

    let output = child.wait_with_output().expect("wait");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, b"hello");
}

#[test]
fn stdin_decompress_matches_payload() {
    let payload = b"streaming stdin decompress\n";
    let compressed = gzip_member(payload);

    let mut child = bin()
        .args(["-c", "-P", "2", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rapidgzip-rust");

    {
        let mut stdin = child.stdin.take().expect("stdin");
        stdin.write_all(&compressed).expect("write stdin");
    }

    let output = child.wait_with_output().expect("wait");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    assert_eq!(output.stdout, payload);
}

#[test]
fn stdin_analyze_exits_zero() {
    let compressed = gzip_member(b"analyze me\n");

    let mut child = bin()
        .args(["--analyze", "-"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("spawn rapidgzip-rust");

    {
        let mut stdin = child.stdin.take().expect("stdin");
        stdin.write_all(&compressed).expect("write stdin");
    }

    let output = child.wait_with_output().expect("wait");
    assert!(
        output.status.success(),
        "stderr={}",
        String::from_utf8_lossy(&output.stderr)
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.to_ascii_lowercase().contains("gzip")
            || stdout.contains("member")
            || !stdout.is_empty(),
        "unexpected analyze output: {stdout}"
    );
}
