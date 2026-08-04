//! Verified decoding CLI for `rapidgzip-core`.
//!
//! The option names follow rapidgzip where the Rust implementation can honor
//! their semantics. Options that would select an unimplemented I/O or sparse-
//! window strategy are rejected explicitly rather than accepted as no-ops.

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
    AnalyzeOptions, DecodeError, DecodeReport, Decoder, DecoderBuilder, DeflateIndex, Format,
    IndexOptions, IndexedDecodeReport, IndexedReader,
};
use report::Volume;
use source::{Destination, Source, open_destination, open_source, paths_refer_to_same_file};
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Instant;

/// Container framing selected on the command line.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum CliFormat {
    /// Detect gzip or zlib from the two-byte prefix.
    #[default]
    Auto,
    /// gzip, including concatenated members and BGZF.
    Gzip,
    /// RFC 1950 zlib framing.
    Zlib,
    /// Unwrapped RFC 1951 DEFLATE.
    #[value(name = "raw-deflate")]
    RawDeflate,
}

/// Input strategy names accepted by rapidgzip.
#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum IoReadMethod {
    /// Positional reads, which regular-file decoding already uses.
    Pread,
    /// A shared sequential cursor, not implemented by this decoder.
    Sequential,
    /// Serialized random seeks and reads, not implemented by this decoder.
    #[value(name = "locked-read")]
    LockedRead,
}

#[derive(Debug, Parser)]
#[command(
    name = "rapidgzip-rust",
    version,
    about = "Parallel, verified gzip, zlib, and raw-DEFLATE decoding"
)]
struct Arguments {
    /// Maximum decoder-worker budget; 0 selects the machine default.
    #[arg(
        short = 'P',
        long = "decoder-parallelism",
        visible_alias = "threads",
        value_name = "THREADS"
    )]
    threads: Option<usize>,

    /// Write decoded bytes to standard output.
    #[arg(short = 'c', long = "stdout", action = ArgAction::SetTrue, conflicts_with = "output")]
    stdout: bool,

    /// Write decoded bytes to this path.
    #[arg(
        short = 'o',
        long = "output",
        value_name = "PATH",
        conflicts_with = "stdout"
    )]
    output: Option<PathBuf>,

    /// Replace an existing output file.
    #[arg(short = 'f', long = "force", action = ArgAction::SetTrue)]
    force: bool,

    /// Compatibility alias; input files are never deleted.
    #[arg(short = 'k', long = "keep", action = ArgAction::SetTrue)]
    keep: bool,

    /// Compatibility alias; this program only decodes.
    #[arg(short = 'd', long = "decompress", action = ArgAction::SetTrue)]
    decompress: bool,

    /// Verify the complete input without retaining decoded bytes.
    #[arg(short = 't', long = "test", action = ArgAction::SetTrue, conflicts_with = "ranges")]
    test: bool,

    /// Print container and DEFLATE block structure instead of decoding output.
    #[arg(
        long = "analyze",
        action = ArgAction::SetTrue,
        conflicts_with_all = [
            "test",
            "count",
            "count_lines",
            "ranges",
            "export_index",
            "import_index",
            "stdout",
            "output",
            "quiet"
        ]
    )]
    analyze: bool,

    /// Maximum streams retained by --analyze (default: 100000).
    #[arg(
        long = "analysis-max-streams",
        value_name = "COUNT",
        requires = "analyze"
    )]
    analysis_max_streams: Option<usize>,

    /// Maximum DEFLATE blocks retained by --analyze (default: 100000).
    #[arg(
        long = "analysis-max-blocks",
        value_name = "COUNT",
        requires = "analyze"
    )]
    analysis_max_blocks: Option<usize>,

    /// Maximum optional gzip-header bytes retained across the input (default: 1 MiB).
    #[arg(
        long = "analysis-max-header-bytes",
        value_name = "BYTES",
        requires = "analyze"
    )]
    analysis_max_header_bytes: Option<usize>,

    /// Detailed predecessor-window references retained by --analyze --verbose.
    #[arg(
        long = "analysis-reference-limit",
        value_name = "COUNT",
        requires_all = ["analyze", "verbose"]
    )]
    analysis_reference_limit: Option<usize>,

    /// Print the complete decompressed size in bytes.
    #[arg(long = "count", action = ArgAction::SetTrue, conflicts_with = "ranges")]
    count: bool,

    /// Print the number of newline bytes in the complete decoded output.
    #[arg(short = 'l', long = "count-lines", action = ArgAction::SetTrue, conflicts_with = "ranges")]
    count_lines: bool,

    /// Extract comma-separated SIZE@OFFSET byte or line ranges.
    ///
    /// Example: 10@0,1KiB@15KiB,5L@20L,inf@40L.
    #[arg(long = "ranges", value_name = "SPEC")]
    ranges: Option<String>,

    /// Write the index collected or imported by this operation.
    #[arg(long = "export-index", value_name = "PATH")]
    export_index: Option<PathBuf>,

    /// Decode or seek through an existing index.
    #[arg(long = "import-index", value_name = "PATH")]
    import_index: Option<PathBuf>,

    /// Format written by --export-index.
    #[arg(long = "index-format", value_enum, default_value_t = IndexFormat::Native)]
    index_format: IndexFormat,

    /// Input container framing.
    #[arg(long = "format", value_enum, default_value_t = CliFormat::Auto)]
    format: CliFormat,

    /// Require this exact decompressed size.
    #[arg(long = "expected-size", value_name = "BYTES")]
    expected_size: Option<u64>,

    /// Target decoded handoff chunk size in KiB.
    #[arg(long = "chunk-size", value_name = "KIB")]
    chunk_size: Option<usize>,

    /// Require complete verification, including a full pass before imported range extraction.
    #[arg(long = "verify", action = ArgAction::SetTrue)]
    verify: bool,

    /// Unsupported because accepting a stream requires verification.
    #[arg(long = "no-verify", action = ArgAction::SetTrue)]
    no_verify: bool,

    /// Select the compressed-input read strategy.
    #[arg(long = "io-read-method", value_enum, value_name = "METHOD")]
    io_read_method: Option<IoReadMethod>,

    /// Unsupported sparse index-window transformation.
    #[arg(long = "sparse-windows", action = ArgAction::SetTrue, conflicts_with = "no_sparse_windows")]
    sparse_windows: bool,

    /// Retain complete predecessor windows, which is the current behavior.
    #[arg(long = "no-sparse-windows", action = ArgAction::SetTrue)]
    no_sparse_windows: bool,

    /// Suppress successful action diagnostics.
    #[arg(short = 'q', long = "quiet", action = ArgAction::SetTrue, conflicts_with = "verbose")]
    quiet: bool,

    /// Print verified sizes, timing, and throughput.
    #[arg(short = 'v', long = "verbose", action = ArgAction::SetTrue)]
    verbose: bool,

    /// Print linked open-source attributions and exit.
    #[arg(long = "oss-attributions", action = ArgAction::SetTrue)]
    oss_attributions: bool,

    /// Print linked open-source attributions as YAML and exit.
    #[arg(long = "oss-attributions-yaml", action = ArgAction::SetTrue)]
    oss_attributions_yaml: bool,

    /// gzip, BGZF, zlib, or raw-DEFLATE input; `-` means standard input.
    #[arg(value_name = "INPUT", required_unless_present_any = ["oss_attributions", "oss_attributions_yaml"])]
    input: Option<PathBuf>,
}

impl Arguments {
    const fn discards_output(&self) -> bool {
        (self.test || self.count || self.count_lines) && !self.stdout && self.output.is_none()
    }

    const fn builds_index(&self) -> bool {
        self.export_index.is_some() && self.import_index.is_none()
    }

    fn payload_uses_stdout(&self) -> bool {
        self.stdout
            || self
                .output
                .as_deref()
                .is_some_and(|path| path.as_os_str() == "-")
    }
}

fn build_decoder(
    arguments: &Arguments,
    count_lines: bool,
) -> Result<Decoder, Box<dyn std::error::Error>> {
    let mut builder: DecoderBuilder = Decoder::builder();
    builder = match arguments.format {
        CliFormat::Auto => builder.auto_detect_format(),
        CliFormat::Gzip => builder.format(Format::Gzip),
        CliFormat::Zlib => builder.format(Format::Zlib),
        CliFormat::RawDeflate => builder.format(Format::RawDeflate),
    };
    if let Some(threads) = arguments.threads.filter(|&threads| threads != 0) {
        builder = builder.decoder_threads(threads);
    }
    if let Some(kibibytes) = arguments.chunk_size {
        builder = builder.decoded_chunk_size(kibibytes.saturating_mul(1024));
    }
    if let Some(size) = arguments.expected_size {
        builder = builder.expected_uncompressed_size(Some(size));
    }
    builder = builder.count_lines(count_lines);
    Ok(builder.build()?)
}

fn decode_plain(
    decoder: &Decoder,
    source: &mut Source,
    output: &mut Destination,
) -> Result<DecodeReport, DecodeError> {
    match source {
        Source::Positional(file, _) => decoder.decode(file, output),
        Source::Stream(reader) => decoder.decode_stream(reader.as_mut(), output),
    }
}

fn decode_with_index(
    decoder: &Decoder,
    source: &mut Source,
    output: &mut Destination,
) -> Result<IndexedDecodeReport, rapidgzip_core::IndexingError> {
    match source {
        Source::Positional(file, _) => {
            decoder.decode_with_index(file, output, IndexOptions::default())
        }
        Source::Stream(reader) => {
            decoder.decode_stream_with_index(reader.as_mut(), output, IndexOptions::default())
        }
    }
}

fn validate_options(arguments: &Arguments) -> Result<(), Box<dyn std::error::Error>> {
    if arguments.no_verify {
        return Err(
            "--no-verify is unsupported: framing checks are part of accepting decoded output"
                .into(),
        );
    }
    if arguments.sparse_windows {
        return Err(
            "--sparse-windows is unsupported: sparse predecessor-window transformation is not implemented"
                .into(),
        );
    }
    if matches!(
        arguments.io_read_method,
        Some(IoReadMethod::Sequential | IoReadMethod::LockedRead)
    ) {
        return Err(
            "--io-read-method currently supports only pread; sequential and locked-read would require different I/O semantics"
                .into(),
        );
    }
    if arguments.analyze && arguments.threads.is_some() {
        return Err(
            "--decoder-parallelism does not affect the causally ordered --analyze walk".into(),
        );
    }
    if arguments.analyze && arguments.chunk_size.is_some() {
        return Err("--chunk-size controls decoded handoff and does not apply to --analyze".into());
    }
    if arguments.builds_index()
        && arguments.index_format.needs_line_counts()
        && !arguments.count_lines
        && arguments.ranges.is_none()
    {
        return Err(
            "--index-format gztool-with-lines requires --count-lines so every checkpoint can be annotated"
                .into(),
        );
    }
    if arguments.ranges.is_some() && arguments.import_index.is_some() && !arguments.verify {
        if arguments.expected_size.is_some() {
            return Err(
                "--expected-size with imported --ranges requires --verify so the complete output is checked"
                    .into(),
            );
        }
        if arguments.threads.is_some() {
            return Err(
                "--decoder-parallelism does not affect imported random access; add --verify to use it for the full verification pass"
                    .into(),
            );
        }
        if arguments.chunk_size.is_some() {
            return Err(
                "--chunk-size does not affect imported random access; add --verify to use it for the full verification pass"
                    .into(),
            );
        }
        if arguments.format != CliFormat::Auto {
            return Err(
                "--format does not select imported random-access framing; add --verify to check it against the index"
                    .into(),
            );
        }
    }

    // These aliases describe behavior that is already unconditional outside
    // the explicitly partial imported-range path handled above.
    let _ = (
        arguments.keep,
        arguments.decompress,
        arguments.no_sparse_windows,
    );
    Ok(())
}

fn run_analyze(
    arguments: &Arguments,
    source: &mut Source,
) -> Result<(), Box<dyn std::error::Error>> {
    let decoder = build_decoder(arguments, false)?;
    let mut options = AnalyzeOptions::default();
    if let Some(limit) = arguments.analysis_max_streams {
        options = options.maximum_streams(limit);
    }
    if let Some(limit) = arguments.analysis_max_blocks {
        options = options.maximum_blocks(limit);
    }
    if let Some(limit) = arguments.analysis_max_header_bytes {
        options = options.maximum_header_bytes(limit);
    }
    if arguments.verbose {
        options = options.maximum_retained_backreferences(
            arguments.analysis_reference_limit.unwrap_or(1_000_000),
        );
    }

    let started = Instant::now();
    let analysis = match source {
        Source::Positional(file, _) => decoder.analyze_with_options(file, options)?,
        Source::Stream(reader) => decoder.analyze_stream_with_options(reader.as_mut(), options)?,
    };
    let elapsed = started.elapsed();
    let stdout = io::stdout();
    let mut output = stdout.lock();
    analyze_report::write_report(
        &mut output,
        &analysis,
        analyze_report::Timings {
            read_dynamic_header: std::time::Duration::ZERO,
            read_data: elapsed,
        },
        arguments.verbose,
    )?;
    output.flush()?;
    Ok(())
}

fn run_ranges(
    arguments: &Arguments,
    source: Source,
    specification: &str,
    volume: Volume,
) -> Result<(), Box<dyn std::error::Error>> {
    let parsed = ranges::parse(specification)?;
    let needs_lines = parsed.iter().copied().any(ranges::Range::needs_lines)
        || (arguments.export_index.is_some() && arguments.index_format.needs_line_counts());
    let file = match &source {
        Source::Positional(file, _) => file,
        Source::Stream(_) => {
            return Err("--ranges requires a regular file with positional reads".into());
        }
    };
    let archive_size = file.metadata()?.len();

    let imported = arguments.import_index.is_some();
    let index = if let Some(index_path) = &arguments.import_index {
        let index = index::import(index_path, Some(archive_size))?;
        if needs_lines && index.checkpoint_at_or_before_line(0).is_none() {
            return Err(format!(
                "{} does not carry complete line metadata required by this operation",
                index_path.display()
            )
            .into());
        }
        index
    } else {
        let decoder = build_decoder(arguments, needs_lines)?;
        decoder
            .decode_with_index(file, &mut io::sink(), IndexOptions::default())?
            .index
    };

    if imported && arguments.verify {
        let decoder = build_decoder(arguments, needs_lines)?;
        let report = decoder.decode_from_index(file, &mut io::sink(), &index)?;
        if needs_lines
            && index
                .total_line_count()
                .is_some_and(|expected| report.line_count != Some(expected))
        {
            return Err("imported index total line count disagrees with verified output".into());
        }
    }

    if let Some(export_path) = &arguments.export_index {
        index::export(&index, export_path, arguments.index_format, arguments.force)?;
    }

    let mut destination = open_destination(
        &source,
        arguments.output.as_deref(),
        arguments.stdout,
        false,
        arguments.force,
    )?;
    let Source::Positional(file, path) = source else {
        unreachable!("range source was checked above")
    };
    let mut reader = IndexedReader::new(file, index)?;
    let written = ranges::extract(&mut reader, &parsed, &mut destination)?;
    destination.flush()?;
    if volume == Volume::Verbose {
        writeln!(
            io::stderr(),
            "{}: {written} range bytes written",
            path.display()
        )?;
    }
    Ok(())
}

fn imported_index(
    arguments: &Arguments,
    source: &Source,
) -> Result<Option<DeflateIndex>, Box<dyn std::error::Error>> {
    let Some(index_path) = &arguments.import_index else {
        return Ok(None);
    };
    let Source::Positional(file, _) = source else {
        return Err("--import-index requires a regular file with positional reads".into());
    };
    let index = index::import(index_path, Some(file.metadata()?.len()))?;
    if arguments.export_index.is_some()
        && arguments.index_format.needs_line_counts()
        && index.checkpoint_at_or_before_line(0).is_none()
    {
        return Err(format!(
            "{} has no complete line metadata to export as gztool-with-lines",
            index_path.display()
        )
        .into());
    }
    Ok(Some(index))
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
    validate_options(&arguments)?;

    let input = arguments.input.clone().expect("required by clap");
    let mut source = open_source(&input)?;
    if arguments.io_read_method == Some(IoReadMethod::Pread) && source.path().is_none() {
        return Err("--io-read-method pread requires a regular file".into());
    }
    let name = source.display_name();
    let volume = Volume::from_flags(arguments.quiet, arguments.verbose);

    if arguments.analyze {
        return run_analyze(&arguments, &mut source);
    }

    if let Some(export_path) = &arguments.export_index {
        if source.refers_to_path(export_path) {
            return Err("the index output path refers to the compressed input".into());
        }
        if arguments
            .output
            .as_ref()
            .is_some_and(|path| paths_refer_to_same_file(path, export_path))
        {
            return Err("the decoded output and index output paths must differ".into());
        }
        if !arguments.force && export_path.try_exists()? {
            return Err(format!(
                "{} already exists; pass --force to overwrite it",
                export_path.display()
            )
            .into());
        }
    }

    if let Some(specification) = arguments.ranges.as_deref() {
        return run_ranges(&arguments, source, specification, volume);
    }

    let imported = imported_index(&arguments, &source)?;
    let authenticate_imported_lines = imported.is_some()
        && arguments.export_index.is_some()
        && arguments.index_format.needs_line_counts();
    let decoder = build_decoder(
        &arguments,
        arguments.count_lines || authenticate_imported_lines,
    )?;
    let mut destination = open_destination(
        &source,
        arguments.output.as_deref(),
        arguments.stdout,
        arguments.discards_output(),
        arguments.force,
    )?;

    let started = Instant::now();
    let (report, exportable_index) = if let Some(index) = imported {
        let Source::Positional(file, _) = &source else {
            unreachable!("imported_index rejects streaming sources")
        };
        let report = decoder.decode_from_index(file, &mut destination, &index)?;
        (report, Some(index))
    } else if arguments.builds_index() {
        let indexed = decode_with_index(&decoder, &mut source, &mut destination)?;
        (indexed.decode, Some(indexed.index))
    } else {
        (decode_plain(&decoder, &mut source, &mut destination)?, None)
    };
    let elapsed = started.elapsed();
    destination.flush()?;

    if let Some(export_path) = &arguments.export_index {
        let index = exportable_index
            .as_ref()
            .ok_or("--export-index requested but no index was available")?;
        index::export(index, export_path, arguments.index_format, arguments.force)?;
    }
    let report_to_stderr = arguments.payload_uses_stdout();
    if arguments.count {
        report::print_count(&report, report_to_stderr)?;
    }
    if arguments.count_lines {
        report::print_line_count(&report, report_to_stderr)?;
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
        Err(error) if error_chain_has_broken_pipe(error.as_ref()) => ExitCode::SUCCESS,
        Err(error) => {
            let _ = writeln!(io::stderr(), "rapidgzip-rust: {error}");
            ExitCode::FAILURE
        }
    }
}

fn error_chain_has_broken_pipe(mut error: &(dyn std::error::Error + 'static)) -> bool {
    loop {
        if error
            .downcast_ref::<io::Error>()
            .is_some_and(|error| error.kind() == io::ErrorKind::BrokenPipe)
        {
            return true;
        }
        let Some(source) = error.source() else {
            return false;
        };
        error = source;
    }
}

#[cfg(test)]
mod tests {
    use super::error_chain_has_broken_pipe;
    use rapidgzip_core::DecodeError;
    use std::io;
    use std::sync::Arc;

    #[test]
    fn broken_pipe_is_found_through_decode_error_sources() {
        let error = DecodeError::Io {
            offset: None,
            source: Arc::new(io::Error::new(io::ErrorKind::BrokenPipe, "closed pipe")),
        };
        assert!(error_chain_has_broken_pipe(&error));
        assert!(!error_chain_has_broken_pipe(&io::Error::other("other")));
    }
}
