//! Decode-only command-line interface for `rapidgzip-core`.
//!
//! Option names and defaults follow rapidgzip 0.16.0, so a command line
//! written for that tool works here. The differences are deliberate and
//! documented in `README.md`: nothing is skipped when output goes to
//! `/dev/null`, `--no-verify` is refused because verification is structural
//! here, and three options are accepted no-ops because this crate has no
//! behaviour to attach to them.

mod analyze_report;
mod attributions;
mod cxx_format;
mod index;
mod ranges;
mod report;
mod source;

use clap::{ArgAction, Parser, ValueEnum};
use index::IndexFormat;
use rapidgzip_core::{
    DecodeError, DecodeReport, Decoder, DecoderBuilder, Format, GzipIndex, IndexedReader,
};
use report::Volume;
use source::{Source, open_destination, open_source};
use std::fs::File;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::{Duration, Instant};

/// Container framing, mirroring [`Format`] on the command line.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum CliFormat {
    /// Detect gzip against zlib from the first two bytes.
    Auto,
    /// gzip, including concatenated members and BGZF.
    Gzip,
    /// zlib (RFC 1950).
    Zlib,
    /// Raw DEFLATE (RFC 1951), which has no header to detect.
    #[value(name = "raw-deflate")]
    RawDeflate,
}

impl From<CliFormat> for Format {
    fn from(value: CliFormat) -> Self {
        match value {
            CliFormat::Auto => Self::Auto,
            CliFormat::Gzip => Self::Gzip,
            CliFormat::Zlib => Self::Zlib,
            CliFormat::RawDeflate => Self::RawDeflate,
        }
    }
}

/// Read strategy, accepted for command-line compatibility.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum IoReadMethod {
    Pread,
    Sequential,
    #[value(name = "locked-read")]
    LockedRead,
}

#[derive(Debug, Parser)]
#[command(
    name = "rapidgzip-rust",
    version,
    about = "Parallel, verified gzip decompression"
)]
struct Arguments {
    /// Maximum decoder-worker budget. 0 selects it automatically.
    #[arg(
        short = 'P',
        long = "decoder-parallelism",
        visible_alias = "threads",
        value_name = "THREADS"
    )]
    threads: Option<usize>,

    /// Write decompressed bytes to standard output.
    #[arg(short = 'c', long = "stdout", action = ArgAction::SetTrue, conflicts_with_all = ["output", "test"])]
    stdout: bool,

    /// Write decompressed bytes to this file.
    #[arg(short = 'o', long = "output", value_name = "PATH", conflicts_with_all = ["stdout", "test"])]
    output: Option<PathBuf>,

    /// Overwrite an existing output file.
    #[arg(short = 'f', long = "force", action = ArgAction::SetTrue)]
    force: bool,

    /// Accepted for compatibility. This tool never deletes its input.
    #[arg(short = 'k', long = "keep", action = ArgAction::SetTrue)]
    keep: bool,

    /// Accepted for compatibility. Decoding is all this tool does.
    #[arg(short = 'd', long = "decompress", action = ArgAction::SetTrue)]
    decompress: bool,

    /// Verify the complete input without retaining output.
    #[arg(short = 't', long = "test", action = ArgAction::SetTrue)]
    test: bool,

    /// Print the internal structure: streams, blocks, and their statistics.
    #[arg(long = "analyze", action = ArgAction::SetTrue)]
    analyze: bool,

    /// Print the decompressed size in bytes.
    #[arg(long = "count", action = ArgAction::SetTrue)]
    count: bool,

    /// Print the number of newline characters in the decompressed data.
    #[arg(short = 'l', long = "count-lines", action = ArgAction::SetTrue)]
    count_lines: bool,

    /// Decompress only these byte or line ranges, as SIZE@OFFSET.
    ///
    /// Example: 10@0,1KiB@15KiB,5L@20L,inf@40L decompresses the first ten
    /// bytes, one kibibyte at offset 15 KiB, the five lines after the first
    /// twenty, and everything after the first forty lines.
    #[arg(long = "ranges", value_name = "SPEC")]
    ranges: Option<String>,

    /// Write the index collected during this decode.
    #[arg(long = "export-index", value_name = "PATH")]
    export_index: Option<PathBuf>,

    /// Use an existing index instead of building one.
    #[arg(long = "import-index", value_name = "PATH")]
    import_index: Option<PathBuf>,

    /// Decompressed bytes between index checkpoints, in KiB.
    ///
    /// Denser indexes split a decode into more spans, which is what lets a
    /// parallel decode driven by --import-index keep every worker busy.
    #[arg(long = "index-spacing", value_name = "KIB")]
    index_spacing: Option<u64>,

    /// Format for --export-index.
    #[arg(long = "index-format", value_enum, default_value_t = IndexFormat::IndexedGzip)]
    index_format: IndexFormat,

    /// Container framing of the compressed input.
    #[arg(long = "format", value_enum, default_value_t = CliFormat::Auto)]
    format: CliFormat,

    /// Decompressed size to expect, the only check raw DEFLATE allows.
    #[arg(long = "expected-size", value_name = "BYTES")]
    expected_size: Option<u64>,

    /// Parallel chunk size in KiB.
    #[arg(long = "chunk-size", value_name = "KIB")]
    chunk_size: Option<usize>,

    /// Accepted for compatibility. Verification is unconditional here.
    #[arg(long = "verify", action = ArgAction::SetTrue)]
    verify: bool,

    /// Rejected: this decoder cannot skip verification.
    #[arg(long = "no-verify", action = ArgAction::SetTrue)]
    no_verify: bool,

    /// Accepted for compatibility. This tool always reads positionally.
    #[arg(long = "io-read-method", value_enum, value_name = "METHOD")]
    io_read_method: Option<IoReadMethod>,

    /// Accepted for compatibility. Index windows are always dense here.
    #[arg(long = "sparse-windows", action = ArgAction::SetTrue)]
    sparse_windows: bool,

    /// Accepted for compatibility, and what this tool already does.
    #[arg(long = "no-sparse-windows", action = ArgAction::SetTrue)]
    no_sparse_windows: bool,

    /// Suppress non-critical messages.
    #[arg(short = 'q', long = "quiet", action = ArgAction::SetTrue, conflicts_with = "verbose")]
    quiet: bool,

    /// Print the decode path, concurrency, and timings.
    #[arg(short = 'v', long = "verbose", action = ArgAction::SetTrue)]
    verbose: bool,

    /// Print open-source licenses and exit.
    #[arg(long = "oss-attributions", action = ArgAction::SetTrue)]
    oss_attributions: bool,

    /// Print open-source licenses as YAML and exit.
    #[arg(long = "oss-attributions-yaml", action = ArgAction::SetTrue)]
    oss_attributions_yaml: bool,

    /// Gzip, BGZF, zlib, or raw DEFLATE input, or `-` for standard input.
    ///
    /// A seekable file is decoded in parallel. A pipe, FIFO, or standard input
    /// is decoded sequentially with the same verification.
    #[arg(value_name = "INPUT", required_unless_present_any = ["oss_attributions", "oss_attributions_yaml"])]
    input: Option<PathBuf>,
}

impl Arguments {
    /// Returns whether the run produces no decompressed payload.
    const fn discards_output(&self) -> bool {
        self.test || self.count || self.count_lines
    }

    /// Returns whether an index has to be built during this decode.
    const fn needs_built_index(&self) -> bool {
        self.export_index.is_some()
    }
}

fn build_decoder(
    arguments: &Arguments,
    count_lines: bool,
    build_index: bool,
) -> Result<Decoder, Box<dyn std::error::Error>> {
    let mut builder: DecoderBuilder = Decoder::builder().format(arguments.format.into());
    // Zero means automatic, which is the default the builder already picked.
    if let Some(threads) = arguments.threads.filter(|&threads| threads > 0) {
        builder = builder.decoder_threads(threads);
    }
    if let Some(kibibytes) = arguments.chunk_size {
        builder = builder.compressed_chunk_size(kibibytes.saturating_mul(1024));
    }
    if let Some(size) = arguments.expected_size {
        builder = builder.expected_uncompressed_size(Some(size));
    }
    if count_lines {
        builder = builder.count_lines(true);
    }
    if build_index {
        builder = builder.build_index(true);
    }
    Ok(builder.build()?)
}

fn decode_into<W: Write>(
    decoder: &Decoder,
    source: Source,
    output: &mut W,
) -> Result<DecodeReport, DecodeError> {
    match source {
        Source::Positional(file, _) => decoder.decode(&file, output),
        Source::Stream(reader) => decoder.decode_stream(reader, output),
    }
}

/// Runs the actions that extract ranges, which need positional input.
fn run_ranges(
    arguments: &Arguments,
    source: Source,
    specification: &str,
    volume: Volume,
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed = ranges::parse(specification)?;
    let needs_lines = parsed.iter().any(ranges::Range::needs_lines);

    let Source::Positional(file, path) = source else {
        return Err(
            "--ranges needs a seekable input, since it seeks; standard input cannot be used".into(),
        );
    };
    let archive_size = file.metadata()?.len();

    let index = match &arguments.import_index {
        Some(path) => {
            let index = index::import(path, Some(archive_size))?;
            if needs_lines && index.total_line_count.is_none() {
                return Err(format!(
                    "{} records no line offsets, so a line-addressed range cannot use it; \
                     export one with --index-format gztool-with-lines or native while \
                     --count-lines is in effect",
                    path.display()
                )
                .into());
            }
            index
        }
        None => build_index_for(arguments, &path, needs_lines)?,
    };

    let mut reader = IndexedReader::new(File::open(&path)?, index)?;
    let mut destination = open_destination(
        &Source::Positional(File::open(&path)?, path.clone()),
        arguments.output.as_deref(),
        arguments.stdout,
        false,
        arguments.force,
    )?;
    let written = ranges::extract(&mut reader, &parsed, &mut destination)?;
    destination.flush()?;
    if volume == Volume::Verbose {
        writeln!(io::stderr(), "{} range bytes written", written)?;
    }
    Ok(())
}

/// Walks the input and prints its structure.
fn run_analyze(arguments: &Arguments, source: Source) -> Result<(), Box<dyn std::error::Error>> {
    let Source::Positional(file, _) = source else {
        return Err(
            "--analyze needs a seekable input, since it reads the whole stream into memory              and walks it; standard input cannot be used"
                .into(),
        );
    };
    let decoder = build_decoder(arguments, false, false)?;
    let started = Instant::now();
    let analysis = decoder.analyze(&file)?;
    let elapsed = started.elapsed();

    let stdout = io::stdout();
    let mut output = stdout.lock();
    analyze_report::write_report(
        &mut output,
        &analysis,
        analyze_report::Timings {
            // The walk does not separate header parsing from symbol decoding,
            // so the whole measured time is attributed to the decode.
            read_dynamic_header: Duration::ZERO,
            read_data: elapsed,
        },
    )?;
    output.flush()?;
    Ok(())
}

/// Decodes `path` once to collect an index for random access.
fn build_index_for(
    arguments: &Arguments,
    path: &std::path::Path,
    count_lines: bool,
) -> Result<GzipIndex, Box<dyn std::error::Error>> {
    let decoder = build_decoder(arguments, count_lines, true)?;
    let file = File::open(path)?;
    let report = decoder.decode(&file, &mut io::sink())?;
    report
        .index
        .ok_or_else(|| "the decoder produced no index".into())
}

fn run(arguments: Arguments) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.oss_attributions_yaml {
        print!("{}", attributions::YAML);
        return Ok(());
    }
    if arguments.oss_attributions {
        print!("{}", attributions::PLAIN);
        return Ok(());
    }
    if arguments.no_verify {
        return Err(
            "--no-verify is not supported: this decoder verifies every member's CRC32 and \
             size as part of accepting it, so there is nothing to switch off and no speed \
             to gain by pretending otherwise"
                .into(),
        );
    }

    // Accepted for command-line compatibility with rapidgzip. Each names
    // behaviour this crate does not have, so each is a no-op rather than a
    // silent lie about what happened.
    let _ = (
        arguments.keep,
        arguments.decompress,
        arguments.verify,
        arguments.io_read_method,
        arguments.sparse_windows,
        arguments.no_sparse_windows,
    );

    let volume = Volume::from_flags(arguments.quiet, arguments.verbose);
    if volume == Volume::Verbose && arguments.sparse_windows {
        writeln!(
            io::stderr(),
            "note: --sparse-windows is accepted but not implemented; the exported index \
             keeps every window byte, which is valid and merely larger"
        )?;
    }

    let input = arguments.input.clone().expect("required by clap");
    let source = open_source(&input)?;
    let name = source.display_name();

    if arguments.analyze {
        return run_analyze(&arguments, source);
    }

    if let Some(specification) = arguments.ranges.clone() {
        return run_ranges(&arguments, source, &specification, volume);
    }

    // An imported index now accelerates an ordinary decompression too: every
    // checkpoint gives a worker a resume point and its window, so each runs
    // plain zlib instead of decoding speculatively.
    let imported = match (&arguments.import_index, source.path()) {
        (Some(path), Some(input)) => {
            let size = std::fs::metadata(input)?.len();
            Some(index::import(path, Some(size))?)
        }
        (Some(_), None) => {
            return Err("--import-index needs a seekable input".into());
        }
        (None, _) => None,
    };

    let build_index = arguments.needs_built_index();
    if build_index && source.path().is_none() {
        return Err(
            "--export-index needs a seekable input; an index of standard input could not be \
             used to seek in it afterwards"
                .into(),
        );
    }
    if arguments.index_format.needs_line_counts() && !arguments.count_lines && build_index {
        return Err(
            "--index-format gztool-with-lines needs --count-lines, since the format stores a \
             line offset for every checkpoint"
                .into(),
        );
    }

    let mut decoder_builder_index = imported;
    let decoder = {
        let mut builder = Decoder::builder().format(arguments.format.into());
        if let Some(threads) = arguments.threads.filter(|&threads| threads > 0) {
            builder = builder.decoder_threads(threads);
        }
        if let Some(kibibytes) = arguments.chunk_size {
            builder = builder.compressed_chunk_size(kibibytes.saturating_mul(1024));
        }
        if let Some(size) = arguments.expected_size {
            builder = builder.expected_uncompressed_size(Some(size));
        }
        if arguments.count_lines {
            builder = builder.count_lines(true);
        }
        if build_index {
            builder = builder.build_index(true);
        }
        if let Some(kibibytes) = arguments.index_spacing {
            builder = builder.index_spacing(kibibytes.saturating_mul(1024));
        }
        builder.index(decoder_builder_index.take()).build()?
    };
    let mut destination = open_destination(
        &source,
        arguments.output.as_deref(),
        arguments.stdout,
        arguments.discards_output(),
        arguments.force,
    )?;

    let started = Instant::now();
    let report = decode_into(&decoder, source, &mut destination)?;
    let elapsed = started.elapsed();
    destination.flush()?;

    if let Some(path) = &arguments.export_index {
        let index = report
            .index
            .as_ref()
            .ok_or("the decoder produced no index")?;
        index::export(index, path, arguments.index_format)?;
    }

    if arguments.count {
        report::print_count(&report)?;
    }
    if arguments.count_lines {
        report::print_line_count(&report)?;
    }
    if arguments.test {
        report::print_test_result(&name, &report, volume)?;
    }
    if volume == Volume::Verbose {
        report::print_verbose_summary(&name, &report, elapsed)?;
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
            // A failure is printed even under --quiet: it is the one thing
            // that is never non-critical.
            let _ = writeln!(io::stderr(), "rapidgzip-rust: {error}");
            ExitCode::FAILURE
        }
    }
}
