//! Import and export of supported random-access index formats.

use clap::ValueEnum;
use rapidgzip_core::DeflateIndex;
use rapidgzip_core::index::WithLines;
use std::ffi::OsStr;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

static TEMPORARY_SEQUENCE: AtomicU64 = AtomicU64::new(0);

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

/// A temporary same-directory file removed unless it is committed.
struct PendingFile {
    path: Option<PathBuf>,
}

impl PendingFile {
    fn new(path: PathBuf) -> Self {
        Self { path: Some(path) }
    }

    fn path(&self) -> &Path {
        self.path.as_deref().expect("pending path")
    }

    fn committed(&mut self) {
        self.path = None;
    }
}

impl Drop for PendingFile {
    fn drop(&mut self) {
        if let Some(path) = self.path.take() {
            let _ = std::fs::remove_file(path);
        }
    }
}

fn temporary_file(path: &Path, role: &str) -> io::Result<(File, PathBuf)> {
    let parent = path.parent().unwrap_or_else(|| Path::new("."));
    let basename = path.file_name().unwrap_or_else(|| OsStr::new("index"));
    for _ in 0..128 {
        let sequence = TEMPORARY_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let mut name = basename.to_os_string();
        name.push(format!(
            ".rapidgzip-rust-{role}-{}-{sequence}",
            std::process::id()
        ));
        let candidate = parent.join(name);
        match OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .open(&candidate)
        {
            Ok(file) => return Ok((file, candidate)),
            Err(error) if error.kind() == io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
    }
    Err(io::Error::new(
        io::ErrorKind::AlreadyExists,
        "could not reserve a unique temporary index path",
    ))
}

fn already_exists(path: &Path) -> Box<dyn std::error::Error> {
    format!(
        "{} already exists; pass --force to overwrite it",
        path.display()
    )
    .into()
}

fn commit_new(
    pending: &mut PendingFile,
    destination: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::hard_link(pending.path(), destination).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            already_exists(destination)
        } else {
            Box::new(error) as Box<dyn std::error::Error>
        }
    })?;
    std::fs::remove_file(pending.path())?;
    pending.committed();
    Ok(())
}

#[cfg(unix)]
fn commit_replace(
    pending: &mut PendingFile,
    destination: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::rename(pending.path(), destination)?;
    pending.committed();
    Ok(())
}

#[cfg(windows)]
fn commit_replace(
    pending: &mut PendingFile,
    destination: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if !destination.try_exists()? {
        std::fs::rename(pending.path(), destination)?;
        pending.committed();
        return Ok(());
    }

    // `std::fs::rename` does not replace a destination on Windows. Preserve
    // the old file under a same-directory name and restore it if installing
    // the completed index fails. This is transactional with respect to
    // failures, although Windows does not expose a portable atomic-replace
    // primitive through the standard library.
    let (reservation, backup) = temporary_file(destination, "backup")?;
    drop(reservation);
    std::fs::remove_file(&backup)?;
    std::fs::rename(destination, &backup)?;
    match std::fs::rename(pending.path(), destination) {
        Ok(()) => {
            pending.committed();
            let _ = std::fs::remove_file(backup);
            Ok(())
        }
        Err(error) => {
            let _ = std::fs::rename(&backup, destination);
            Err(Box::new(error))
        }
    }
}

#[cfg(not(any(unix, windows)))]
fn commit_replace(
    pending: &mut PendingFile,
    destination: &Path,
) -> Result<(), Box<dyn std::error::Error>> {
    if destination.try_exists()? {
        return Err("atomic replacement is unsupported on this platform".into());
    }
    std::fs::rename(pending.path(), destination)?;
    pending.committed();
    Ok(())
}

/// Writes `index` to `path` without exposing a partial index.
///
/// Serialization happens in a same-directory temporary file. A failed format
/// conversion or write therefore leaves an existing destination untouched and
/// does not create an empty destination. On Unix, forced replacement is an
/// atomic rename. The Windows standard library lacks an atomic replacement
/// operation, so the implementation preserves and restores the previous file
/// if installation fails.
pub fn export(
    index: &DeflateIndex,
    path: &Path,
    format: IndexFormat,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if !force && path.try_exists()? {
        return Err(already_exists(path));
    }

    let (file, temporary_path) = temporary_file(path, "index")?;
    let mut pending = PendingFile::new(temporary_path);
    let mut writer = BufWriter::new(file);
    match format {
        IndexFormat::IndexedGzip => index.write_gzidx(&mut writer)?,
        IndexFormat::Gztool => index.write_gztool(&mut writer, WithLines::No)?,
        IndexFormat::GztoolWithLines => index.write_gztool(&mut writer, WithLines::Yes)?,
        IndexFormat::Native => index.write_native(&mut writer)?,
        IndexFormat::Gzi => index.write_gzi(&mut writer)?,
    }
    writer.flush()?;
    let file = writer.into_inner().map_err(|error| error.into_error())?;
    file.sync_all()?;
    drop(file);

    if force {
        commit_replace(&mut pending, path)
    } else {
        commit_new(&mut pending, path)
    }
}

fn looks_like_gzi(prefix: &[u8], file_length: u64) -> bool {
    let Ok(count_bytes) = prefix.get(..8).unwrap_or_default().try_into() else {
        return false;
    };
    let count = u64::from_le_bytes(count_bytes);
    count.checked_mul(16).and_then(|bytes| bytes.checked_add(8)) == Some(file_length)
}

/// Reads an index through bounded streaming I/O.
///
/// Self-identifying formats are selected from at most sixteen prefix bytes.
/// The headerless BGZF `.gzi` representation is accepted only when its count
/// exactly predicts the file length, preventing arbitrary files from being
/// interpreted as an empty GZI. `archive_size` supplies source-size metadata
/// to formats that do not store it. Trailing bytes are rejected for every
/// supported representation.
pub fn import(
    path: &Path,
    archive_size: Option<u64>,
) -> Result<DeflateIndex, Box<dyn std::error::Error>> {
    let file = File::open(path)?;
    let file_length = file.metadata()?.len();
    let mut reader = BufReader::new(file);
    let mut prefix = [0_u8; 16];
    let prefix_length = file_length.min(prefix.len() as u64) as usize;
    reader.read_exact(&mut prefix[..prefix_length])?;
    let prefix = &prefix[..prefix_length];
    reader.seek(SeekFrom::Start(0))?;

    let index = if prefix.starts_with(b"RGZIDX01") {
        DeflateIndex::read_native(&mut reader)?
    } else if prefix.starts_with(b"GZIDX") {
        DeflateIndex::read_gzidx(&mut reader, archive_size)?
    } else if prefix.len() >= 16 && matches!(&prefix[8..16], b"gzipindx" | b"gzipindX") {
        DeflateIndex::read_gztool(&mut reader, archive_size)?
    } else if looks_like_gzi(prefix, file_length) {
        DeflateIndex::read_gzi(&mut reader, archive_size)?
    } else {
        return Err(format!("{} has an unrecognized index format", path.display()).into());
    };

    let mut trailing = [0_u8; 1];
    if reader.read(&mut trailing)? != 0 {
        return Err(format!("{} contains trailing bytes after the index", path.display()).into());
    }
    Ok(index)
}
