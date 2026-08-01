//! Where compressed input comes from and where decoded output goes.
//!
//! Both decisions are made before any decoding starts, so a run that cannot
//! write its output fails before spending time on it.

use std::fs::{File, OpenOptions};
use std::io::{self, IsTerminal, Read, Seek, Write};
use std::path::{Path, PathBuf};

/// Compressed input, classified by whether it can be read positionally.
///
/// Only a positional source can be decoded in parallel, indexed, or seeked,
/// so the classification decides which actions are available at all.
pub enum Source {
    /// A seekable file, together with the path it was opened from.
    Positional(File, PathBuf),
    /// A forward-only stream: standard input, a FIFO, or a socket.
    Stream(Box<dyn Read + Send>),
}

impl Source {
    /// Returns the path when the source is a real file.
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Positional(_, path) => Some(path),
            Self::Stream(_) => None,
        }
    }

    /// Returns a name for diagnostics.
    pub fn display_name(&self) -> String {
        match self {
            Self::Positional(_, path) => path.display().to_string(),
            Self::Stream(_) => "standard input".to_owned(),
        }
    }
}

/// Opens `path`, mirroring the routing `Decoder::open` performs.
pub fn open_source(path: &Path) -> io::Result<Source> {
    if path.as_os_str() == "-" {
        return Ok(Source::Stream(Box::new(io::stdin())));
    }
    let mut file = File::open(path)?;
    let positional = match file.metadata() {
        Ok(metadata) if metadata.file_type().is_file() => true,
        Ok(_) => file.stream_position().is_ok(),
        Err(_) => false,
    };
    if positional {
        Ok(Source::Positional(file, path.to_path_buf()))
    } else {
        Ok(Source::Stream(Box::new(file)))
    }
}

/// Where decoded bytes are written.
pub enum Destination {
    /// Standard output.
    Stdout,
    /// A file that was created for this run.
    File(File),
    /// Nothing, for actions that only verify or measure.
    Sink,
}

impl Write for Destination {
    fn write(&mut self, buffer: &[u8]) -> io::Result<usize> {
        match self {
            Self::Stdout => io::stdout().write(buffer),
            Self::File(file) => file.write(buffer),
            Self::Sink => Ok(buffer.len()),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Stdout => io::stdout().flush(),
            Self::File(file) => file.flush(),
            Self::Sink => Ok(()),
        }
    }
}

/// Derives the output path rapidgzip would use for `input`.
///
/// A `.gz`, `.bgz`, `.zlib`, or `.z` suffix is stripped; anything else gains a
/// `.out` suffix rather than being overwritten by its own decompressed form.
pub fn derived_output_path(input: &Path) -> PathBuf {
    const SUFFIXES: [&str; 4] = ["gz", "bgz", "zlib", "z"];
    let matches_suffix = input
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| SUFFIXES.contains(&extension));
    if matches_suffix {
        return input.with_extension("");
    }
    let mut name = input.as_os_str().to_owned();
    name.push(".out");
    PathBuf::from(name)
}

/// Resolves where output goes, creating the file when one is named.
///
/// `explicit` is `-o`, `to_stdout` is `-c`, and `discard` covers the actions
/// that produce no payload. Refusing to write binary output to a terminal is
/// deliberate: it is never what the caller wanted and it is hard to undo.
pub fn open_destination(
    source: &Source,
    explicit: Option<&Path>,
    to_stdout: bool,
    discard: bool,
    force: bool,
) -> Result<Destination, Box<dyn std::error::Error>> {
    if discard {
        return Ok(Destination::Sink);
    }
    if let Some(path) = explicit {
        if path.as_os_str() == "-" {
            return Ok(Destination::Stdout);
        }
        return Ok(Destination::File(create_output(path, force)?));
    }
    if to_stdout {
        return Ok(Destination::Stdout);
    }
    // Standard input has no name to derive an output file from, so it behaves
    // as `-c`, which is also what rapidgzip and gzip do.
    let Some(input) = source.path() else {
        return Ok(Destination::Stdout);
    };
    if io::stdout().is_terminal() {
        let derived = derived_output_path(input);
        return Ok(Destination::File(create_output(&derived, force)?));
    }
    Ok(Destination::Stdout)
}

fn create_output(path: &Path, force: bool) -> Result<File, Box<dyn std::error::Error>> {
    let mut options = OpenOptions::new();
    options.write(true);
    if force {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    options.open(path).map_err(|error| {
        if error.kind() == io::ErrorKind::AlreadyExists {
            format!(
                "{} already exists; pass --force to overwrite it",
                path.display()
            )
            .into()
        } else {
            Box::new(error) as Box<dyn std::error::Error>
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compressed_suffixes_are_stripped() {
        for (input, expected) in [
            ("reads.fastq.gz", "reads.fastq"),
            ("reads.fastq.bgz", "reads.fastq"),
            ("payload.zlib", "payload"),
            ("payload.z", "payload"),
        ] {
            assert_eq!(
                derived_output_path(Path::new(input)),
                PathBuf::from(expected)
            );
        }
    }

    #[test]
    fn an_unrecognized_name_gains_a_suffix() {
        assert_eq!(
            derived_output_path(Path::new("archive")),
            PathBuf::from("archive.out")
        );
        assert_eq!(
            derived_output_path(Path::new("data.bin")),
            PathBuf::from("data.bin.out")
        );
    }

    #[test]
    fn a_path_with_directories_keeps_them() {
        assert_eq!(
            derived_output_path(Path::new("/tmp/reads.fastq.gz")),
            PathBuf::from("/tmp/reads.fastq")
        );
    }
}
