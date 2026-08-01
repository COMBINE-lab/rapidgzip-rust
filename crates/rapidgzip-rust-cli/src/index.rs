//! Reading and writing random-access indexes in the supported formats.

use clap::ValueEnum;
use rapidgzip_core::GzipIndex;
use rapidgzip_core::index::WithLines;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

/// On-disk index format.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
pub enum IndexFormat {
    /// indexed_gzip's `GZIDX`, the rapidgzip default.
    #[value(name = "indexed_gzip")]
    IndexedGzip,
    /// gztool's format without line counters.
    #[value(name = "gztool")]
    Gztool,
    /// gztool's format with line counters.
    #[value(name = "gztool-with-lines")]
    GztoolWithLines,
    /// This crate's own versioned format.
    ///
    /// The only format that round-trips every field, including compressed
    /// window payloads and line offsets together.
    #[value(name = "native")]
    Native,
    /// htslib's BGZF `.gzi`.
    #[value(name = "gzi")]
    Gzi,
}

impl IndexFormat {
    /// Returns whether writing this format requires counted lines.
    pub const fn needs_line_counts(self) -> bool {
        matches!(self, Self::GztoolWithLines)
    }
}

/// Writes `index` to `path` in `format`.
pub fn export(
    index: &GzipIndex,
    path: &Path,
    format: IndexFormat,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut writer = BufWriter::new(File::create(path)?);
    match format {
        IndexFormat::IndexedGzip => index.write_gzidx(&mut writer)?,
        IndexFormat::Gztool => index.write_gztool(&mut writer, WithLines::No)?,
        IndexFormat::GztoolWithLines => index.write_gztool(&mut writer, WithLines::Yes)?,
        IndexFormat::Native => index.write_native(&mut writer)?,
        IndexFormat::Gzi => index.write_gzi(&mut writer)?,
    }
    Ok(())
}

/// Reads an index from `path`, detecting its format from the leading bytes.
///
/// `archive_size` is the compressed size, which the formats that do not record
/// their own offsets need in order to be interpreted. `.gzi` carries no magic
/// at all, so it is the fallback rather than something that can be detected.
pub fn import(
    path: &Path,
    archive_size: Option<u64>,
) -> Result<GzipIndex, Box<dyn std::error::Error>> {
    let bytes = std::fs::read(path)?;
    let mut reader = BufReader::new(bytes.as_slice());
    if bytes.starts_with(b"RGZIDX01") {
        return Ok(GzipIndex::read_native(&mut reader)?);
    }
    if bytes.starts_with(b"GZIDX") {
        return Ok(GzipIndex::read_gzidx(&mut reader, archive_size)?);
    }
    // gztool writes eight zero bytes before its magic.
    if bytes.len() >= 16 && (&bytes[8..16] == b"gzipindx" || &bytes[8..16] == b"gzipindX") {
        return Ok(GzipIndex::read_gztool(&mut reader, archive_size)?);
    }
    Ok(GzipIndex::read_gzi(&mut reader, archive_size)?)
}
