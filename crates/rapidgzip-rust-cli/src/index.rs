//! Import and export of supported random-access index formats.

use clap::ValueEnum;
use rapidgzip_core::DeflateIndex;
use rapidgzip_core::index::WithLines;
use std::fs::OpenOptions;
use std::io::{BufReader, BufWriter, Write};
use std::path::Path;

/// On-disk index format selected by the CLI.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum IndexFormat {
    /// indexed_gzip's `GZIDX` version 1 format.
    #[value(name = "indexed_gzip")]
    IndexedGzip,
    /// gztool version 0 without line counters.
    #[value(name = "gztool")]
    Gztool,
    /// gztool version 1 with complete line counters.
    #[value(name = "gztool-with-lines")]
    GztoolWithLines,
    /// rapidgzip-rust's versioned, lossless index format.
    #[value(name = "native")]
    Native,
    /// htslib's BGZF `.gzi` format.
    #[value(name = "gzi")]
    Gzi,
}

impl IndexFormat {
    /// Returns whether this format requires complete line metadata.
    pub const fn needs_line_counts(self) -> bool {
        matches!(self, Self::GztoolWithLines)
    }
}

/// Writes `index` to a newly truncated `path` in `format`.
pub fn export(
    index: &DeflateIndex,
    path: &Path,
    format: IndexFormat,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut options = OpenOptions::new();
    options.write(true);
    if force {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    let file = options.open(path).map_err(|error| {
        if error.kind() == std::io::ErrorKind::AlreadyExists {
            format!(
                "{} already exists; pass --force to overwrite it",
                path.display()
            )
            .into()
        } else {
            Box::new(error) as Box<dyn std::error::Error>
        }
    })?;
    let mut writer = BufWriter::new(file);
    match format {
        IndexFormat::IndexedGzip => index.write_gzidx(&mut writer)?,
        IndexFormat::Gztool => index.write_gztool(&mut writer, WithLines::No)?,
        IndexFormat::GztoolWithLines => index.write_gztool(&mut writer, WithLines::Yes)?,
        IndexFormat::Native => index.write_native(&mut writer)?,
        IndexFormat::Gzi => index.write_gzi(&mut writer)?,
    }
    writer.flush()?;
    Ok(())
}

/// Reads an index, detecting self-identifying formats by magic bytes.
///
/// The headerless BGZF `.gzi` representation is the fallback. `archive_size`
/// supplies source-size metadata to formats that do not store it.
pub fn import(
    path: &Path,
    archive_size: Option<u64>,
) -> Result<DeflateIndex, Box<dyn std::error::Error>> {
    let bytes = std::fs::read(path)?;
    let mut reader = BufReader::new(bytes.as_slice());
    if bytes.starts_with(b"RGZIDX01") {
        return Ok(DeflateIndex::read_native(&mut reader)?);
    }
    if bytes.starts_with(b"GZIDX") {
        return Ok(DeflateIndex::read_gzidx(&mut reader, archive_size)?);
    }
    if bytes.len() >= 16 && matches!(&bytes[8..16], b"gzipindx" | b"gzipindX") {
        return Ok(DeflateIndex::read_gztool(&mut reader, archive_size)?);
    }
    Ok(DeflateIndex::read_gzi(&mut reader, archive_size)?)
}
