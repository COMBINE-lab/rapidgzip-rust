//! Optional interoperability tests against the reference index tools.
//!
//! Run these on a machine with `bgzip`, `gztool`, and Python's
//! `indexed_gzip` package using:
//!
//! ```text
//! cargo test -p rapidgzip-core --test index_interop -- --ignored --nocapture
//! ```

mod common;

use common::{bgzf, corpus, gzip};
use rapidgzip_core::index::WithLines;
use rapidgzip_core::{Decoder, DeflateIndex, IndexOptions, IndexedReader};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

fn workspace(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("rapidgzip-interop-{name}"));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("create test directory");
    path
}

fn available(tool: &str, arguments: &[&str]) -> bool {
    Command::new(tool)
        .args(arguments)
        .output()
        .map(|output| output.status.success() || !output.stderr.is_empty())
        .unwrap_or(false)
}

fn indexed_gzip_available() -> bool {
    Command::new("python3")
        .args(["-c", "import indexed_gzip"])
        .status()
        .is_ok_and(|status| status.success())
}

fn run(command: &mut Command) -> Vec<u8> {
    let output = command.output().expect("run reference tool");
    assert!(
        output.status.success(),
        "{command:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn write(path: &Path, bytes: &[u8]) {
    fs::File::create(path)
        .expect("create fixture")
        .write_all(bytes)
        .expect("write fixture");
}

fn built_index(path: &Path) -> DeflateIndex {
    let decoder = Decoder::builder()
        .decoder_threads(4)
        .build()
        .expect("builder");
    let source = fs::File::open(path).expect("open archive");
    decoder
        .decode_with_index(&source, &mut std::io::sink(), IndexOptions::default())
        .expect("indexed decode")
        .index
}

/// Builds an archive whose second authenticated member guarantees a portable
/// checkpoint without depending on timing-sensitive marker-path admission.
fn concatenated_gzip(plain: &[u8]) -> Vec<u8> {
    let split = plain.len() / 2;
    let mut compressed = gzip(&plain[..split], 6);
    compressed.extend_from_slice(&gzip(&plain[split..], 6));
    compressed
}

fn assert_seeks_match(compressed: &Path, index: DeflateIndex, plain: &[u8], targets: &[usize]) {
    let source = fs::File::open(compressed).expect("open archive");
    let mut reader = IndexedReader::new(source, index).expect("indexed reader");
    for &target in targets {
        reader.seek(SeekFrom::Start(target as u64)).expect("seek");
        let length = 1024.min(plain.len() - target);
        let mut buffer = vec![0; length];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(buffer, &plain[target..target + length], "at {target}");
    }
}

#[test]
#[ignore = "requires bgzip"]
fn reads_the_gzi_written_by_bgzip() {
    if !available("bgzip", &["--version"]) {
        println!("skipped: bgzip is not installed");
        return;
    }
    let directory = workspace("read-bgzip");
    let plain = corpus(4 * 1024 * 1024);
    let plain_path = directory.join("corpus.txt");
    write(&plain_path, &plain);
    run(Command::new("bgzip").args(["-i", "-f"]).arg(&plain_path));

    let archive = directory.join("corpus.txt.gz");
    let bytes = fs::read(directory.join("corpus.txt.gz.gzi")).expect("read gzi");
    let archive_size = fs::metadata(&archive).expect("stat archive").len();
    let index =
        DeflateIndex::read_gzi(&mut bytes.as_slice(), Some(archive_size)).expect("gzi import");
    assert_seeks_match(
        &archive,
        index,
        &plain,
        &[0, 1000, 2_000_000, plain.len() - 1024],
    );
}

#[test]
#[ignore = "requires bgzip"]
fn bgzip_reads_the_gzi_we_write() {
    if !available("bgzip", &["--version"]) {
        println!("skipped: bgzip is not installed");
        return;
    }
    let directory = workspace("write-bgzip");
    let plain = corpus(2 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    write(&archive, &bgzf(&plain, 48 * 1024));
    let index = built_index(&archive);
    let mut bytes = Vec::new();
    index.write_gzi(&mut bytes).expect("gzi export");
    write(&directory.join("corpus.gz.gzi"), &bytes);

    let extracted = run(Command::new("bgzip")
        .args(["-b", "1500000", "-s", "1024", "-c"])
        .arg(&archive));
    assert_eq!(extracted, &plain[1_500_000..1_501_024]);
}

#[test]
#[ignore = "requires Python indexed_gzip"]
fn reads_the_gzidx_written_by_indexed_gzip() {
    if !indexed_gzip_available() {
        println!("skipped: indexed_gzip is not installed");
        return;
    }
    let directory = workspace("read-indexed-gzip");
    let plain = corpus(8 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    let exported = directory.join("corpus.gzidx");
    write(&archive, &gzip(&plain, 6));
    run(Command::new("python3").args([
        "-c",
        &format!(
            "import indexed_gzip\n\
             f = indexed_gzip.IndexedGzipFile('{archive}', spacing=1048576)\n\
             f.build_full_index()\n\
             f.export_index('{exported}')\n",
            archive = archive.display(),
            exported = exported.display(),
        ),
    ]));

    let bytes = fs::read(&exported).expect("read GZIDX");
    let archive_size = fs::metadata(&archive).expect("stat archive").len();
    let index =
        DeflateIndex::read_gzidx(&mut bytes.as_slice(), Some(archive_size)).expect("GZIDX import");
    assert_seeks_match(
        &archive,
        index,
        &plain,
        &[0, 4_000_000, 7_000_000, plain.len() - 1024],
    );
}

#[test]
#[ignore = "requires Python indexed_gzip"]
fn indexed_gzip_reads_the_gzidx_we_write() {
    if !indexed_gzip_available() {
        println!("skipped: indexed_gzip is not installed");
        return;
    }
    let directory = workspace("write-indexed-gzip");
    let plain = corpus(8 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    let exported = directory.join("ours.gzidx");
    write(&archive, &concatenated_gzip(&plain));
    let index = built_index(&archive);
    assert!(index.checkpoint_count() > 1);
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("GZIDX export");
    write(&exported, &bytes);

    let output = run(Command::new("python3").args([
        "-c",
        &format!(
            "import indexed_gzip, sys\n\
             f = indexed_gzip.IndexedGzipFile('{archive}')\n\
             f.import_index('{exported}')\n\
             f.seek(6000000)\n\
             sys.stdout.buffer.write(f.read(1024))\n",
            archive = archive.display(),
            exported = exported.display(),
        ),
    ]));
    assert_eq!(output, &plain[6_000_000..6_001_024]);
}

#[test]
#[ignore = "requires gztool"]
fn reads_the_index_written_by_gztool() {
    if !available("gztool", &["-h"]) {
        println!("skipped: gztool is not installed");
        return;
    }
    let directory = workspace("read-gztool");
    let plain = corpus(8 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    write(&archive, &gzip(&plain, 6));
    run(Command::new("gztool").args(["-i", "-s", "1"]).arg(&archive));

    let bytes = fs::read(directory.join("corpus.gzi")).expect("read gztool index");
    let archive_size = fs::metadata(&archive).expect("stat archive").len();
    let index = DeflateIndex::read_gztool(&mut bytes.as_slice(), Some(archive_size))
        .expect("gztool import");
    assert_seeks_match(
        &archive,
        index,
        &plain,
        &[0, 3_000_000, 7_000_000, plain.len() - 1024],
    );
}

#[test]
#[ignore = "requires gztool"]
fn gztool_reads_the_index_we_write() {
    if !available("gztool", &["-h"]) {
        println!("skipped: gztool is not installed");
        return;
    }
    let directory = workspace("write-gztool");
    let plain = corpus(8 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    write(&archive, &concatenated_gzip(&plain));
    let index = built_index(&archive);
    assert!(index.checkpoint_count() > 1);
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::No)
        .expect("gztool export");
    write(&directory.join("corpus.gzi"), &bytes);

    let extracted = run(Command::new("gztool").args(["-b", "5000000"]).arg(&archive));
    assert_eq!(&extracted[..1024], &plain[5_000_000..5_001_024]);
}

#[test]
#[ignore = "requires gztool"]
fn gztool_extracts_lines_from_the_index_we_write() {
    if !available("gztool", &["-h"]) {
        println!("skipped: gztool is not installed");
        return;
    }
    let directory = workspace("write-gztool-lines");
    let plain = corpus(8 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    write(&archive, &concatenated_gzip(&plain));

    let decoder = Decoder::builder()
        .decoder_threads(4)
        .count_lines(true)
        .build()
        .expect("builder");
    let source = fs::File::open(&archive).expect("open archive");
    let index = decoder
        .decode_with_index(&source, &mut std::io::sink(), IndexOptions::default())
        .expect("indexed decode")
        .index;
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::Yes)
        .expect("gztool line export");
    write(&directory.join("corpus.gzi"), &bytes);

    let external_line = 50_000_usize;
    let extracted = run(Command::new("gztool")
        .args(["-L", &external_line.to_string()])
        .arg(&archive));
    let internal_line = external_line - 1;
    let start = plain
        .iter()
        .enumerate()
        .filter(|&(_, &byte)| byte == b'\n')
        .nth(internal_line - 1)
        .map(|(offset, _)| offset + 1)
        .expect("target line exists");
    assert_eq!(&extracted[..512], &plain[start..start + 512]);

    let mut reader = IndexedReader::new(source, index).expect("indexed reader");
    assert_eq!(
        reader
            .seek_to_line(internal_line as u64)
            .expect("seek line"),
        start as u64
    );
}
