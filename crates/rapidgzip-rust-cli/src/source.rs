//! Compressed-input classification and decoded-output destinations.

use std::fs::{File, OpenOptions};
use std::io::{self, IsTerminal, Read, Write};
use std::path::{Path, PathBuf};

/// An input classified by whether positional reads are valid.
pub enum Source {
    /// A stable regular file, which supports parallel and indexed operations.
    Positional(File, PathBuf),
    /// Standard input or a non-regular filesystem stream.
    Stream(Box<dyn Read + Send>),
}

impl Source {
    /// Returns the stable source path when positional operations are possible.
    pub fn path(&self) -> Option<&Path> {
        match self {
            Self::Positional(_, path) => Some(path),
            Self::Stream(_) => None,
        }
    }

    /// Returns a human-readable source name for diagnostics.
    pub fn display_name(&self) -> String {
        match self {
            Self::Positional(_, path) => path.display().to_string(),
            Self::Stream(_) => "standard input".to_owned(),
        }
    }
}

/// Opens `path` using the same regular-file rule as the core decoder.
pub fn open_source(path: &Path) -> io::Result<Source> {
    if path.as_os_str() == "-" {
        return Ok(Source::Stream(Box::new(io::stdin())));
    }
    let file = File::open(path)?;
    if file
        .metadata()
        .is_ok_and(|metadata| metadata.file_type().is_file())
    {
        Ok(Source::Positional(file, path.to_path_buf()))
    } else {
        Ok(Source::Stream(Box::new(file)))
    }
}

/// A decoded-output destination.
pub enum Destination {
    /// Process standard output.
    Stdout,
    /// A filesystem output opened before decoding begins.
    File(File),
    /// A sink used by validation and counting actions.
    Sink,
}

impl Write for Destination {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        match self {
            Self::Stdout => io::stdout().write(bytes),
            Self::File(file) => file.write(bytes),
            Self::Sink => Ok(bytes.len()),
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

/// Derives an output path from a compressed input path.
///
/// Recognized compression suffixes are removed. An unrecognized filename
/// receives `.out`, avoiding accidental in-place replacement.
pub fn derived_output_path(input: &Path) -> PathBuf {
    const SUFFIXES: [&str; 4] = ["gz", "bgz", "zlib", "z"];
    if input
        .extension()
        .and_then(|extension| extension.to_str())
        .is_some_and(|extension| SUFFIXES.contains(&extension))
    {
        return input.with_extension("");
    }
    let mut name = input.as_os_str().to_owned();
    name.push(".out");
    PathBuf::from(name)
}

/// Resolves and opens the output destination before decoding begins.
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
        if source
            .path()
            .is_some_and(|input| paths_refer_to_same_file(input, path))
        {
            return Err("the output path refers to the compressed input".into());
        }
        return Ok(Destination::File(create_output(path, force)?));
    }
    if to_stdout || source.path().is_none() || !io::stdout().is_terminal() {
        return Ok(Destination::Stdout);
    }
    let path = derived_output_path(source.path().expect("checked above"));
    Ok(Destination::File(create_output(&path, force)?))
}

/// Returns whether two paths name the same existing file or exact pathname.
pub fn paths_refer_to_same_file(left: &Path, right: &Path) -> bool {
    if left == right {
        return true;
    }
    if std::fs::canonicalize(left)
        .ok()
        .zip(std::fs::canonicalize(right).ok())
        .is_some_and(|(left, right)| left == right)
    {
        return true;
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if let Some((left, right)) = std::fs::metadata(left)
            .ok()
            .zip(std::fs::metadata(right).ok())
        {
            return left.dev() == right.dev() && left.ino() == right.ino();
        }
    }
    false
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
    fn known_compression_suffixes_are_removed() {
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
    fn unknown_names_receive_an_output_suffix() {
        assert_eq!(
            derived_output_path(Path::new("archive")),
            PathBuf::from("archive.out")
        );
        assert_eq!(
            derived_output_path(Path::new("data.bin")),
            PathBuf::from("data.bin.out")
        );
    }
}
