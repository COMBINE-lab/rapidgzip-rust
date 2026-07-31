//! Decode-only command-line interface for `rapidgzip-core`.

use clap::{ArgAction, Parser};
use rapidgzip_core::Decoder;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Debug, Parser)]
#[command(
    name = "rapidgzip-rs",
    version,
    about = "Parallel, verified gzip decompression"
)]
struct Arguments {
    /// Exact number of decoder workers.
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

    /// Seekable gzip or BGZF input file.
    #[arg(value_name = "INPUT")]
    input: PathBuf,
}

fn run(arguments: Arguments) -> Result<(), Box<dyn std::error::Error>> {
    let mut builder = Decoder::builder();
    if let Some(threads) = arguments.threads {
        builder = builder.decoder_threads(threads);
    }
    let decoder = builder.build()?;
    let input = File::open(&arguments.input)?;

    if arguments.test {
        let report = decoder.decode(&input, &mut io::sink())?;
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
        decoder.decode(&input, &mut output)?;
        output.flush()?;
    } else {
        let stdout = io::stdout();
        let mut output = stdout.lock();
        decoder.decode(&input, &mut output)?;
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
            eprintln!("rapidgzip-rs: {error}");
            ExitCode::FAILURE
        }
    }
}
