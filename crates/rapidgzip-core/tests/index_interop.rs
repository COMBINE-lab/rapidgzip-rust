//! Interoperability with the reference indexing tools.
//!
//! These tests are `#[ignore]` because they need `bgzip`, `gztool`, or a
//! `python3` with `indexed_gzip` installed. Run them with:
//!
//! ```text
//! cargo test -p rapidgzip-core --test index_interop -- --ignored
//! ```
//!
//! A test whose tool is missing reports success after printing why it did
//! nothing, so the suite stays usable on a machine with only some of them.

mod common;

use common::{bgzf, corpus, gzip};
use rapidgzip_core::index::WithLines;
use rapidgzip_core::{Decoder, GzipIndex, IndexedReader};
use std::fs;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

/// Creates a private directory for one test, replacing any previous run.
fn workspace(name: &str) -> PathBuf {
    let path = std::env::temp_dir().join(format!("rapidgzip-interop-{name}"));
    let _ = fs::remove_dir_all(&path);
    fs::create_dir_all(&path).expect("create the test directory");
    path
}

/// Returns whether `tool` can be executed.
fn available(tool: &str, arguments: &[&str]) -> bool {
    Command::new(tool)
        .args(arguments)
        .output()
        .map(|output| output.status.success() || !output.stderr.is_empty())
        .unwrap_or(false)
}

/// Returns whether this machine has a `python3` with `indexed_gzip`.
fn indexed_gzip_available() -> bool {
    Command::new("python3")
        .args(["-c", "import indexed_gzip"])
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn run(command: &mut Command) -> Vec<u8> {
    let output = command.output().expect("run the tool");
    assert!(
        output.status.success(),
        "{command:?} failed: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    output.stdout
}

fn write(path: &Path, bytes: &[u8]) {
    fs::File::create(path)
        .expect("create the file")
        .write_all(bytes)
        .expect("write the file");
}

/// Decodes `path` fully and returns the index the decoder collected.
fn built_index(path: &Path) -> GzipIndex {
    let decoder = Decoder::builder()
        .build_index(true)
        .build()
        .expect("builder");
    let mut reader = decoder.open(path).expect("open");
    std::io::copy(&mut reader, &mut std::io::sink()).expect("decode");
    reader
        .finish()
        .expect("report")
        .index
        .expect("index was requested")
}

fn assert_seeks_match(compressed: &Path, index: GzipIndex, plain: &[u8], targets: &[usize]) {
    let file = fs::File::open(compressed).expect("open the archive");
    let mut reader = IndexedReader::new(file, index).expect("indexed reader");
    for &target in targets {
        reader.seek(SeekFrom::Start(target as u64)).expect("seek");
        let length = 1024.min(plain.len() - target);
        let mut buffer = vec![0u8; length];
        reader.read_exact(&mut buffer).expect("read");
        assert_eq!(buffer, &plain[target..target + length], "at {target}");
    }
}

#[test]
#[ignore = "requires bgzip"]
fn we_read_the_gzi_written_by_bgzip() {
    if !available("bgzip", &["--version"]) {
        println!("skipped: bgzip is not installed");
        return;
    }
    let directory = workspace("bgzip-reads-ours");
    let plain = corpus(4 * 1024 * 1024);
    let plain_path = directory.join("corpus.txt");
    write(&plain_path, &plain);

    // `-i` writes corpus.txt.gz plus its corpus.txt.gz.gzi index.
    run(Command::new("bgzip").args(["-i", "-f"]).arg(&plain_path));
    let archive = directory.join("corpus.txt.gz");
    let gzi = directory.join("corpus.txt.gz.gzi");

    let bytes = fs::read(&gzi).expect("read the bgzip index");
    let archive_size = fs::metadata(&archive).expect("stat").len();
    let index = GzipIndex::read_gzi(&mut bytes.as_slice(), Some(archive_size)).expect("import");
    assert!(index.checkpoint_count() > 1, "bgzip wrote a trivial index");

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
    let directory = workspace("bgzip-reads-theirs");
    let plain = corpus(2 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    write(&archive, &bgzf(&plain, 48 * 1024));

    let index = built_index(&archive);
    let mut bytes = Vec::new();
    index.write_gzi(&mut bytes).expect("gzi export");
    write(&directory.join("corpus.gz.gzi"), &bytes);

    // `-b` seeks to a decompressed offset using the .gzi we just wrote.
    let extracted = run(Command::new("bgzip")
        .args(["-b", "1500000", "-s", "1024", "-c"])
        .arg(&archive));
    assert_eq!(extracted, &plain[1_500_000..1_501_024]);
}

#[test]
#[ignore = "requires python3 with indexed_gzip"]
fn we_read_the_gzidx_written_by_indexed_gzip() {
    if !indexed_gzip_available() {
        println!("skipped: python3 has no indexed_gzip module");
        return;
    }
    let directory = workspace("igzip-reads-ours");
    let plain = corpus(8 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    write(&archive, &gzip(&plain, 6));
    let exported = directory.join("corpus.gzidx");

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

    let bytes = fs::read(&exported).expect("read the exported index");
    let archive_size = fs::metadata(&archive).expect("stat").len();
    let index = GzipIndex::read_gzidx(&mut bytes.as_slice(), Some(archive_size)).expect("import");
    assert!(
        index.checkpoint_count() > 1,
        "indexed_gzip wrote a trivial index"
    );

    assert_seeks_match(
        &archive,
        index,
        &plain,
        &[0, 4_000_000, 7_000_000, plain.len() - 1024],
    );
}

#[test]
#[ignore = "requires python3 with indexed_gzip"]
fn indexed_gzip_reads_the_gzidx_we_write() {
    if !indexed_gzip_available() {
        println!("skipped: python3 has no indexed_gzip module");
        return;
    }
    let directory = workspace("igzip-reads-theirs");
    let plain = corpus(8 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    write(&archive, &gzip(&plain, 6));

    let index = built_index(&archive);
    assert!(
        index.checkpoint_count() > 1,
        "the decoder produced no interior checkpoints to test with"
    );
    let exported = directory.join("ours.gzidx");
    let mut bytes = Vec::new();
    index.write_gzidx(&mut bytes).expect("gzidx export");
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
fn we_read_the_index_written_by_gztool() {
    if !available("gztool", &["-h"]) {
        println!("skipped: gztool is not installed");
        return;
    }
    let directory = workspace("gztool-reads-ours");
    let plain = corpus(8 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    write(&archive, &gzip(&plain, 6));

    // `-i` writes corpus.gzi next to the archive; `-s 1` asks for a point
    // every megabyte instead of gztool's ten-megabyte default.
    run(Command::new("gztool").args(["-i", "-s", "1"]).arg(&archive));
    let produced = directory.join("corpus.gzi");

    let bytes = fs::read(&produced).expect("read the gztool index");
    let archive_size = fs::metadata(&archive).expect("stat").len();
    let index = GzipIndex::read_gztool(&mut bytes.as_slice(), Some(archive_size)).expect("import");
    assert!(index.checkpoint_count() > 1, "gztool wrote a trivial index");

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
    let directory = workspace("gztool-reads-theirs");
    let plain = corpus(8 * 1024 * 1024);
    let archive = directory.join("corpus.gz");
    write(&archive, &gzip(&plain, 6));

    let index = built_index(&archive);
    assert!(
        index.checkpoint_count() > 1,
        "the decoder produced no interior checkpoints to test with"
    );
    let exported = directory.join("corpus.gzi");
    let mut bytes = Vec::new();
    index
        .write_gztool(&mut bytes, WithLines::No)
        .expect("gztool export");
    write(&exported, &bytes);

    // `-b` extracts from a decompressed byte offset using corpus.gzi.
    let extracted = run(Command::new("gztool").args(["-b", "5000000"]).arg(&archive));
    assert_eq!(&extracted[..1024], &plain[5_000_000..5_001_024]);
}
