//! Decode-only command-line interface for `rapidgzip-core`.

use clap::{ArgAction, Parser};
use rapidgzip_core::{DecodeError, DecodeReport, Decoder};
use std::fs::{File, OpenOptions};
use std::io::{self, Read, Seek, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(
    name = "rapidgzip-rust",
    version,
    about = "Parallel, verified gzip decompression"
)]
struct Arguments {
    /// Maximum decoder-worker budget.
    #[arg(short = 'P', long = "threads", value_name = "THREADS")]
    threads: Option<usize>,

    /// Write decompressed bytes to standard output (the default).
    #[arg(short = 'c', long = "stdout", action = ArgAction::SetTrue, conflicts_with_all = ["output", "test"])]
    stdout: bool,

    /// Write decompressed bytes to a newly created file.
    #[arg(short = 'o', long = "output", value_name = "PATH", conflicts_with_all = ["stdout", "test"])]
    output: Option<PathBuf>,

    /// Verify the complete input without retaining output.
    #[arg(short = 't', long = "test", action = ArgAction::SetTrue, conflicts_with_all = ["stdout", "output"])]
    test: bool,

    /// Gzip or BGZF input file, or `-` for standard input.
    ///
    /// A seekable file is decoded in parallel. A pipe, FIFO, or standard input
    /// is decoded sequentially with the same verification.
    #[arg(value_name = "INPUT")]
    input: PathBuf,
}

/// Compressed input, already classified by whether it can be read positionally.
enum Source {
    Positional(File),
    Stream(Box<dyn Read + Send>),
}

/// Mirrors the routing `Decoder::open` performs, for the push interface.
fn open_source(path: &Path) -> io::Result<Source> {
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
        Ok(Source::Positional(file))
    } else {
        Ok(Source::Stream(Box::new(file)))
    }
}

fn decode_into<W: Write>(
    decoder: &Decoder,
    source: Source,
    output: &mut W,
) -> Result<DecodeReport, DecodeError> {
    match source {
        Source::Positional(file) => decoder.decode(&file, output),
        Source::Stream(reader) => decoder.decode_stream(reader, output),
    }
}

fn run(arguments: Arguments) -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = Decoder::builder();
    if let Some(threads) = arguments.threads {
        builder = builder.decoder_threads(threads);
    }
    let decoder = builder.build()?;
    let input = open_source(&arguments.input)?;

    if arguments.test {
        let report = decode_into(&decoder, input, &mut io::sink())?;
        eprintln!(
            "{}: ok, {} member(s), {} decoded bytes",
            arguments.input.display(),
            report.member_count,
            report.decompressed_bytes
        );
        return Ok(());
    }

    if let Some(path) = arguments.output {
        let mut output = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(&path)?;
        decode_into(&decoder, input, &mut output)?;
        output.flush()?;
    } else {
        let stdout = io::stdout();
        let mut output = stdout.lock();
        decode_into(&decoder, input, &mut output)?;
        output.flush()?;
    }
    Ok(())
}

fn main() -> ExitCode {
    match run(Arguments::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error)
            if error
                .downcast_ref::<io::Error>()
                .is_some_and(|error| error.kind() == io::ErrorKind::BrokenPipe) =>
        {
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("rapidgzip-rust: {error}");
            ExitCode::FAILURE
        }
    }
}
