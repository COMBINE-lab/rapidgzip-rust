//! Decode-only command-line interface for `rapidgzip-core`.

use clap::{ArgAction, Parser, ValueEnum};
use rapidgzip_core::{
    read_gzip_index, DecodeReport, Decoder, Format, GzipIndex, IndexedReader, ReadAt,
};
use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, IsTerminal, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::Instant;

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum IndexFormat {
    /// indexed_gzip `GZIDX` (default).
    #[default]
    #[value(name = "indexed_gzip")]
    IndexedGzip,
    /// gztool without line counters (`gzipindx`).
    #[value(name = "gztool")]
    Gztool,
    /// gztool with per-point line counters (`gzipindX`).
    #[value(name = "gztool-with-lines")]
    GztoolWithLines,
    /// htslib/bgzip BGZF block index (`.gzi` / BGZI).
    #[value(name = "bgzi")]
    Bgzi,
}

/// Container format selection for decode (maps to [`Format`]).
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, ValueEnum)]
enum CliFormat {
    /// Detect gzip vs zlib from the stream prefix (default).
    #[default]
    Auto,
    /// Require gzip framing.
    Gzip,
    /// Require zlib framing.
    Zlib,
    /// Raw DEFLATE without gzip/zlib wrapper (never auto-detected).
    #[value(name = "raw")]
    RawDeflate,
}

impl CliFormat {
    fn to_core(self) -> Format {
        match self {
            Self::Auto => Format::Auto,
            Self::Gzip => Format::Gzip,
            Self::Zlib => Format::Zlib,
            Self::RawDeflate => Format::RawDeflate,
        }
    }
}

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

    /// Force decompression (accepted for gzip/rapidgzip compatibility; always on).
    #[arg(short = 'd', long = "decompress", action = ArgAction::SetTrue)]
    decompress: bool,

    /// Write decompressed bytes to standard output.
    ///
    /// Default when decompressing to a pipe, reading stdin, or when `-c` is
    /// given. Interactive terminals with a file input write to a stripped
    /// path instead (see default output naming).
    #[arg(short = 'c', long = "stdout", action = ArgAction::SetTrue, conflicts_with = "test")]
    stdout: bool,

    /// Write decompressed bytes to a newly created file.
    ///
    /// May be combined with `-c`/`--stdout` to tee (file + stdout).
    #[arg(short = 'o', long = "output", value_name = "PATH", conflicts_with = "test")]
    output: Option<PathBuf>,

    /// Verify the complete input without retaining output.
    #[arg(short = 't', long = "test", action = ArgAction::SetTrue, conflicts_with_all = ["stdout", "output", "analyze"])]
    test: bool,

    /// Print the internal gzip/BGZF structure (members, DEFLATE block types).
    ///
    /// Walks the archive sequentially; does not write decompressed payload.
    #[arg(
        long = "analyze",
        action = ArgAction::SetTrue,
        conflicts_with_all = [
            "stdout",
            "output",
            "test",
            "count",
            "count_lines",
            "ranges",
            "export_index",
            "import_index",
        ]
    )]
    analyze: bool,

    /// Print the decompressed size in bytes and exit without writing payload when possible.
    #[arg(long = "count", action = ArgAction::SetTrue, conflicts_with = "analyze")]
    count: bool,

    /// Print the number of Unix newlines (`\n`) in the decompressed output.
    ///
    /// Matches rapidgzip/gztool: the count is the number of `\n` bytes (not
    /// the number of text lines when the last line lacks a trailing newline).
    #[arg(short = 'l', long = "count-lines", action = ArgAction::SetTrue, conflicts_with = "analyze")]
    count_lines: bool,

    /// Write a gzip index after decoding (implies keep_index).
    ///
    /// Formats: `indexed_gzip` (GZIDX, default), `gztool`, `gztool-with-lines`,
    /// `bgzi` (htslib/bgzip `.gzi`). `gztool-with-lines` gathers Unix newline
    /// offsets during the decode.
    #[arg(long = "export-index", value_name = "PATH")]
    export_index: Option<PathBuf>,

    /// Index file format for `--export-index` (import auto-detects GZIDX / gztool / BGZI).
    #[arg(long = "index-format", value_enum, default_value_t = IndexFormat::IndexedGzip)]
    index_format: IndexFormat,

    /// Load a gzip index for the input archive (GZIDX, gztool, or BGZI; auto-detected).
    ///
    /// Full decompress via the imported index does not re-verify member CRC/ISIZE
    /// unless combined with `--test` (verified parallel path).
    #[arg(long = "import-index", value_name = "PATH")]
    import_index: Option<PathBuf>,

    /// Decompress only the specified uncompressed ranges (LENGTH@OFFSET,...).
    ///
    /// Byte units: `B`, `K`/`KiB`, `M`/`MiB`, `G`/`GiB` (default bytes).
    /// Line units: suffix `L` on length and/or offset (1-based line numbers,
    /// matching gztool `-L`). Example: `5L@20L` is five lines starting at
    /// line 20. Byte and line ranges cannot be mixed in one `--ranges` value.
    #[arg(long = "ranges", value_name = "SPEC", conflicts_with = "analyze")]
    ranges: Option<String>,

    /// Explicitly enable CRC/Adler verification (default: on).
    ///
    /// Inverse of `--no-verify`. Cannot be combined with `--no-verify`.
    #[arg(long = "verify", action = ArgAction::SetTrue, conflicts_with = "no_verify")]
    verify: bool,

    /// Disable gzip payload CRC32 / zlib Adler-32 verification.
    ///
    /// Cannot be combined with `--verify`.
    #[arg(long = "no-verify", action = ArgAction::SetTrue, conflicts_with = "verify")]
    no_verify: bool,

    /// Compressed container format: auto-detect (default), gzip, zlib, or raw DEFLATE.
    ///
    /// Auto inspects the stream prefix (gzip magic `1f 8b` vs a valid zlib
    /// CMF/FLG header). Raw unwrapped DEFLATE requires `--format raw` (no magic).
    #[arg(long = "format", value_enum, default_value_t = CliFormat::Auto)]
    format: CliFormat,

    /// Overwrite existing output files (`-o`, default stripped path, `--export-index`).
    ///
    /// Without this flag, an existing destination path is refused.
    #[arg(short = 'f', long = "force", action = ArgAction::SetTrue)]
    force: bool,

    /// Suppress noncritical stderr messages (for example the `--test` "ok" line
    /// and `--verbose` stats). Quiet wins over verbose.
    #[arg(short = 'q', long = "quiet", action = ArgAction::SetTrue)]
    quiet: bool,

    /// Print noncritical stats on stderr after a successful decode/test/count
    /// (members, sizes, wall time, throughput, thread budget). Suppressed by `-q`.
    #[arg(short = 'v', long = "verbose", action = ArgAction::SetTrue)]
    verbose: bool,

    /// Worker decoded chunk size in KiB (default: library default, 4096 = 4 MiB).
    #[arg(long = "chunk-size", value_name = "KIB")]
    chunk_size: Option<usize>,

    /// IndexedReader background prefetch window count (library default: 2; 0 disables).
    #[arg(long = "seek-prefetch", value_name = "N")]
    seek_prefetch: Option<usize>,

    /// Print open-source license attributions and exit.
    #[arg(long = "oss-attributions", action = ArgAction::SetTrue)]
    oss_attributions: bool,

    /// Print open-source license attributions in YAML form and exit.
    #[arg(long = "oss-attributions-yaml", action = ArgAction::SetTrue)]
    oss_attributions_yaml: bool,

    /// Seekable gzip or BGZF input file, or `-` for standard input.
    ///
    /// When omitted and standard input is not a terminal, reads from stdin
    /// (gzip-compatible). Ordinary stdin decompress uses [`Decoder::decode_read`]
    /// (sequential page stream, or multi-thread gzip spill-to-temp + parallel).
    /// Paths that need a known archive length / positional source
    /// (`--analyze`, `--import-index`, `--ranges`) spill stdin to a private
    /// temporary file (deleted on exit) rather than holding the full archive in RAM.
    #[arg(value_name = "INPUT")]
    input: Option<PathBuf>,
}

/// Where compressed bytes come from.
enum InputSource {
    /// Path on disk (not `-`).
    File(PathBuf),
    /// Standard input (explicit `-` or no path with non-tty stdin).
    Stdin,
}

/// Loaded compressed bytes ready for positional decode.
enum CompressedInput {
    File { path: PathBuf, file: File },
    /// Stdin spilled to a private temporary file (unlinked on drop).
    TempFile(tempfile::NamedTempFile),
}

impl CompressedInput {
    fn archive_len(&self) -> io::Result<u64> {
        match self {
            Self::File { file, .. } => Ok(file.metadata()?.len()),
            Self::TempFile(temp) => Ok(temp.as_file().metadata()?.len()),
        }
    }

    fn display_name(&self) -> String {
        match self {
            Self::File { path, .. } => path.display().to_string(),
            Self::TempFile(_) => "-".to_owned(),
        }
    }

    /// Borrow the underlying file for positional [`ReadAt`] access.
    fn as_read_at(&self) -> &File {
        match self {
            Self::File { file, .. } => file,
            Self::TempFile(temp) => temp.as_file(),
        }
    }

    /// Path for APIs that reopen by path (`open_with_index`).
    ///
    /// For stdin spills this is the private temporary path; the
    /// [`tempfile::NamedTempFile`] must outlive any reopened handle.
    fn path_for_open(&self) -> &Path {
        match self {
            Self::File { path, .. } => path.as_path(),
            Self::TempFile(temp) => temp.path(),
        }
    }
}

/// Where decompressed bytes should go when a full decode is performed.
#[derive(Debug, Clone, PartialEq, Eq)]
enum OutputDestination {
    Stdout,
    File(PathBuf),
    /// Write to file and stdout (tee).
    Tee(PathBuf),
    Sink,
}

struct TeeWriter<A, B> {
    primary: A,
    secondary: B,
}

impl<A: Write, B: Write> Write for TeeWriter<A, B> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.primary.write_all(buf)?;
        self.secondary.write_all(buf)?;
        Ok(buf.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        self.primary.flush()?;
        self.secondary.flush()?;
        Ok(())
    }
}

/// Open-source attributions text (human-readable). Printed by `--oss-attributions`.
const OSS_ATTRIBUTIONS: &str = concat!(
    "rapidgzip-rust open-source attributions\n",
    "=======================================\n",
    "\n",
    "This binary is rapidgzip-rust (package rapidgzip-rust-cli + rapidgzip-core),\n",
    "licensed under the combined terms of BSD-3-Clause and MIT.\n",
    "\n",
    "---- rapidgzip-rust: BSD-3-Clause ----\n",
    "\n",
    include_str!("../LICENSE-BSD-3-CLAUSE"),
    "\n",
    "---- rapidgzip-rust: MIT ----\n",
    "\n",
    include_str!("../LICENSE-MIT"),
    "\n",
    "---- Third-party dependencies (summary) ----\n",
    "\n",
    "zlib-rs / libz-rs-sys (inflate backend)\n",
    "  License: Zlib\n",
    "  https://crates.io/crates/zlib-rs\n",
    "\n",
    "crossbeam-deque / crossbeam-channel / crossbeam-epoch / crossbeam-utils\n",
    "  License: MIT OR Apache-2.0\n",
    "  https://crates.io/crates/crossbeam-deque\n",
    "\n",
    "clap / clap_builder / clap_derive / clap_lex (CLI parsing)\n",
    "  License: MIT OR Apache-2.0\n",
    "  https://crates.io/crates/clap\n",
    "\n",
    "Full dependency license texts are available from the corresponding crates\n",
    "and from the Cargo.lock resolution of this package.\n",
);

/// Short YAML form of attributions. Printed by `--oss-attributions-yaml`.
const OSS_ATTRIBUTIONS_YAML: &str = concat!(
    "# rapidgzip-rust open-source attributions\n",
    "packages:\n",
    "  - name: rapidgzip-rust\n",
    "    crates: [rapidgzip-rust-cli, rapidgzip-core]\n",
    "    licenses: [BSD-3-Clause, MIT]\n",
    "  - name: zlib-rs\n",
    "    crates: [zlib-rs, libz-rs-sys]\n",
    "    licenses: [Zlib]\n",
    "  - name: crossbeam\n",
    "    crates: [crossbeam-deque, crossbeam-channel, crossbeam-epoch, crossbeam-utils]\n",
    "    licenses: [MIT, Apache-2.0]\n",
    "  - name: clap\n",
    "    crates: [clap, clap_builder, clap_derive, clap_lex]\n",
    "    licenses: [MIT, Apache-2.0]\n",
);


fn wants_payload(arguments: &Arguments) -> bool {
    arguments.stdout || arguments.output.is_some()
}

/// True when `path` is the Unix discard device (`/dev/null`).
fn is_dev_null(path: &Path) -> bool {
    path == Path::new("/dev/null")
}

/// Compression suffixes stripped for default output file naming (ASCII
/// case-insensitive). Longer forms are listed first so `.gzip` / `.bgzf` win
/// over `.gz` / `.bgz`. `.tgz` / `.taz` rewrite the stem to `<stem>.tar`.
const COMPRESSION_SUFFIXES: &[&str] = &[".gzip", ".bgzf", ".tgz", ".taz", ".gz", ".bgz"];

/// Derive a default output path by stripping one trailing compression suffix.
///
/// Recognized suffixes (ASCII case-insensitive): `.gz`, `.gzip`, `.bgz`,
/// `.bgzf`, `.tgz`, `.taz`. For `.tgz` / `.taz` the result is `<stem>.tar`
/// (e.g. `foo.tgz` → `foo.tar`). For the others only the compression suffix is
/// removed (`reads.FASTQ.GZ` → `reads.FASTQ`). If stripping leaves an empty
/// file name, or no known suffix is present, the result is `<input>.out`.
fn default_output_path(input: &Path) -> PathBuf {
    let Some(name) = input.file_name().and_then(|s| s.to_str()) else {
        return path_with_out_suffix(input);
    };
    let name_lower = name.to_ascii_lowercase();
    for suffix in COMPRESSION_SUFFIXES {
        if !name_lower.ends_with(suffix) {
            continue;
        }
        let stripped = &name[..name.len() - suffix.len()];
        if stripped.is_empty() {
            return path_with_out_suffix(input);
        }
        if *suffix == ".tgz" || *suffix == ".taz" {
            let mut tar_name = String::with_capacity(stripped.len() + 4);
            tar_name.push_str(stripped);
            tar_name.push_str(".tar");
            return input.with_file_name(tar_name);
        }
        return input.with_file_name(stripped);
    }
    path_with_out_suffix(input)
}

fn path_with_out_suffix(input: &Path) -> PathBuf {
    let mut out = OsString::from(input.as_os_str());
    out.push(".out");
    PathBuf::from(out)
}

/// Where decompressed bytes should go.
///
/// Priority: `--test` → sink; `-o` → file; `-c` → stdout; count/export-only /
/// ranges rules; otherwise for a real file input with a TTY stdout, write to
/// the stripped default path (gzip-like interactive ergonomics). Pipes and
/// stdin keep streaming to stdout.
///
/// `input_file` is `Some` only for a path source (not stdin / `-`).
/// `stdout_is_tty` is injected so tests can exercise the TTY rule without a
/// real terminal.
fn output_destination(
    arguments: &Arguments,
    input_file: Option<&Path>,
    stdout_is_tty: bool,
) -> OutputDestination {
    if arguments.test {
        return OutputDestination::Sink;
    }
    if let Some(path) = arguments.output.clone() {
        let null_file = is_dev_null(&path);
        if arguments.stdout {
            // Tee: file + stdout. `/dev/null` file side is a no-op → stdout only.
            if null_file {
                return OutputDestination::Stdout;
            }
            return OutputDestination::Tee(path);
        }
        if null_file {
            return OutputDestination::Sink;
        }
        return OutputDestination::File(path);
    }
    if arguments.stdout {
        return OutputDestination::Stdout;
    }
    // --ranges implies payload extract; with --count/--count-lines and no
    // -c/-o, sink and only print totals.
    if arguments.ranges.is_some() {
        if arguments.count || arguments.count_lines {
            return OutputDestination::Sink;
        }
        return OutputDestination::Stdout;
    }
    // With --export-index / --count / --count-lines and no -c/-o, discard payload.
    // With only --import-index (no count/export), still decompress (stdout or
    // auto-named file).
    if arguments.count || arguments.count_lines || arguments.export_index.is_some() {
        return OutputDestination::Sink;
    }
    // Plain decompress: auto-name when input is a file and stdout is a TTY.
    if let Some(path) = input_file {
        if stdout_is_tty {
            return OutputDestination::File(default_output_path(path));
        }
    }
    OutputDestination::Stdout
}

/// Print `-v`/`--verbose` stats on stderr (no-op when quiet or not verbose).
fn print_verbose_stats(
    arguments: &Arguments,
    report: &DecodeReport,
    elapsed: std::time::Duration,
) {
    if arguments.quiet || !arguments.verbose {
        return;
    }
    let secs = elapsed.as_secs_f64();
    let mb = report.decompressed_bytes as f64 / (1024.0 * 1024.0);
    let throughput = if secs > 0.0 { mb / secs } else { f64::INFINITY };
    eprintln!(
        "rapidgzip-rust: {} member(s), {} compressed bytes, {} decompressed bytes, \
         {secs:.3}s, {throughput:.1} MB/s, threads={}",
        report.member_count,
        report.compressed_bytes,
        report.decompressed_bytes,
        report.decoder_threads
    );
}

fn resolve_input_source(arguments: &Arguments) -> Result<InputSource, Box<dyn std::error::Error>> {
    match arguments.input.as_ref() {
        Some(path) if path.as_os_str() == "-" => Ok(InputSource::Stdin),
        Some(path) => Ok(InputSource::File(path.clone())),
        None => {
            if !io::stdin().is_terminal() {
                Ok(InputSource::Stdin)
            } else {
                Err(
                    "missing input file; pass a path, or `-` / a pipe for standard input".into(),
                )
            }
        }
    }
}

/// Open a file path, or spill stdin to a private temporary file.
///
/// Stdin spill is used when a known archive length / positional source is
/// required (`--analyze`, `--import-index`, `--ranges`). Ordinary decompress
/// from stdin uses [`Decoder::decode_read`] instead (see `run_stdin_stream`).
/// The temporary file uses secure temp-dir defaults and is deleted on drop.
fn open_input(source: InputSource) -> Result<CompressedInput, Box<dyn std::error::Error>> {
    match source {
        InputSource::File(path) => {
            let file = File::open(&path)?;
            Ok(CompressedInput::File { path, file })
        }
        InputSource::Stdin => {
            let mut temp = tempfile::Builder::new()
                .prefix("rapidgzip-stdin-")
                .suffix(".gz")
                .tempfile()?;
            io::copy(&mut io::stdin().lock(), temp.as_file_mut())?;
            temp.as_file_mut().flush()?;
            Ok(CompressedInput::TempFile(temp))
        }
    }
}

fn load_index(
    path: &Path,
    archive_size: Option<u64>,
) -> Result<GzipIndex, Box<dyn std::error::Error>> {
    let mut file = File::open(path)?;
    let index = read_gzip_index(&mut file, archive_size)?;
    index.validate()?;
    Ok(index)
}

/// Open a destination file for writing.
///
/// Without `force`, refuses to overwrite an existing path (`create_new`).
/// With `force`, creates or truncates the path.
fn open_output_file(path: &Path, force: bool) -> Result<File, Box<dyn std::error::Error>> {
    let mut options = OpenOptions::new();
    options.write(true);
    if force {
        options.create(true).truncate(true);
    } else {
        options.create_new(true);
    }
    match options.open(path) {
        Ok(file) => Ok(file),
        Err(error) if !force && error.kind() == io::ErrorKind::AlreadyExists => Err(format!(
            "output file '{}' already exists; use --force to overwrite",
            path.display()
        )
        .into()),
        Err(error) => Err(error.into()),
    }
}

/// Convert `--chunk-size` (KiB) to bytes for [`DecoderBuilder::decoded_chunk_size`].
///
/// Rejects zero. Values that overflow `usize` when scaled are rejected here;
/// values larger than `u32::MAX` bytes fail later in [`DecoderBuilder::build`].
fn chunk_size_kib_to_bytes(kib: usize) -> Result<usize, Box<dyn std::error::Error>> {
    if kib == 0 {
        return Err("--chunk-size must be non-zero".into());
    }
    kib.checked_mul(1024)
        .ok_or_else(|| "--chunk-size is too large".into())
}

fn write_index(
    path: &Path,
    index: &GzipIndex,
    format: IndexFormat,
    force: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    if matches!(format, IndexFormat::GztoolWithLines) && !index.has_line_offsets {
        return Err(
            "cannot export --index-format gztool-with-lines: index has no line offsets \
             (rebuild with this export, or use --count-lines while building the index)"
                .into(),
        );
    }
    let mut file = open_output_file(path, force)?;
    match format {
        IndexFormat::IndexedGzip => index.export_indexed_gzip(&mut file)?,
        IndexFormat::Gztool => index.export_gztool(&mut file, false)?,
        IndexFormat::GztoolWithLines => index.export_gztool(&mut file, true)?,
        IndexFormat::Bgzi => index.export_bgzi(&mut file)?,
    }
    file.flush()?;
    Ok(())
}

fn print_count(size: u64) {
    println!("{size}");
}

/// Whether every requested action can be satisfied from an imported index
/// without decoding the archive (count and/or re-export only).
///
/// `--count-lines` can only be served from the index when it carries line
/// offsets (`has_line_offsets`); otherwise a decode is required.
fn can_serve_from_imported_index(arguments: &Arguments, index: &GzipIndex) -> bool {
    if arguments.import_index.is_none()
        || arguments.ranges.is_some()
        || arguments.test
        || wants_payload(arguments)
    {
        return false;
    }
    if arguments.count_lines && !index.has_line_offsets {
        return false;
    }
    // htslib BGZI leaves uncompressed size unknown (`u64::MAX`); force a decode.
    if arguments.count && index.uncompressed_size_in_bytes == u64::MAX {
        return false;
    }
    arguments.count || arguments.count_lines || arguments.export_index.is_some()
}

fn build_decoder(
    arguments: &Arguments,
    keep_index: bool,
    gather_line_offsets: bool,
) -> Result<Decoder, Box<dyn std::error::Error>> {
    let mut builder = Decoder::builder().format(arguments.format.to_core());
    if let Some(threads) = arguments.threads {
        builder = builder.decoder_threads(threads);
    }
    if let Some(kib) = arguments.chunk_size {
        let bytes = chunk_size_kib_to_bytes(kib)?;
        builder = builder.decoded_chunk_size(bytes);
    }
    if let Some(n) = arguments.seek_prefetch {
        builder = builder.seek_prefetch_windows(n);
    }
    if arguments.no_verify {
        builder = builder.crc32_enabled(false);
    }
    if keep_index {
        builder = builder.keep_index(true);
    }
    if gather_line_offsets {
        builder = builder.gather_line_offsets(true);
    }
    Ok(builder.build()?)
}

fn run_analyze(
    arguments: &Arguments,
    input: &CompressedInput,
) -> Result<(), Box<dyn std::error::Error>> {
    let decoder = build_decoder(arguments, false, false)?;
    let analysis = decoder.analyze(input.as_read_at())?;
    print!("{analysis}");
    Ok(())
}

fn run(arguments: Arguments) -> Result<(), Box<dyn std::error::Error>> {
    // Accepted for gzip/rapidgzip CLI compatibility; this tool is always
    // decompress-only, so `-d` / `--decompress` is intentionally a no-op.
    let _ = arguments.decompress;
    // Explicit `--verify` is the inverse of `--no-verify`; default is verify on.
    let _ = arguments.verify;

    if arguments.oss_attributions_yaml {
        print!("{OSS_ATTRIBUTIONS_YAML}");
        return Ok(());
    }
    if arguments.oss_attributions {
        print!("{OSS_ATTRIBUTIONS}");
        return Ok(());
    }

    let parsed_ranges = match arguments.ranges.as_deref() {
        Some(spec) => Some(parse_ranges(spec)?),
        None => None,
    };

    if parsed_ranges.is_some() && arguments.test {
        return Err("--ranges cannot be combined with --test (ranges extract payload)".into());
    }

    let source = resolve_input_source(&arguments)?;
    let input_file_for_output = match &source {
        InputSource::File(path) => Some(path.clone()),
        InputSource::Stdin => None,
    };
    let stdout_is_tty = io::stdout().is_terminal();

    // Stream stdin decompress/test/count via decode_read (no full RAM buffer;
    // multi-thread gzip may spill to a temp file inside the library).
    // Paths that need positional access or a known length spill stdin to a tempfile.
    if matches!(source, InputSource::Stdin)
        && !arguments.analyze
        && arguments.import_index.is_none()
        && parsed_ranges.is_none()
    {
        return run_stdin_stream(arguments, stdout_is_tty);
    }

    let input = open_input(source)?;
    let archive_len = input.archive_len()?;
    let input_name = input.display_name();

    if arguments.analyze {
        return run_analyze(&arguments, &input);
    }

    let imported_index = match arguments.import_index.as_ref() {
        Some(path) => Some(load_index(path, Some(archive_len))?),
        None => None,
    };

    // Range extraction always goes through IndexedReader (seek per range).
    if let Some(ref ranges) = parsed_ranges {
        return run_ranges(
            &arguments,
            ranges,
            input,
            imported_index,
            input_file_for_output.as_deref(),
            stdout_is_tty,
        );
    }

    if let Some(ref index) = imported_index
        && can_serve_from_imported_index(&arguments, index)
    {
        if let Some(ref export_path) = arguments.export_index {
            write_index(export_path, index, arguments.index_format, arguments.force)?;
        }
        if arguments.count {
            print_count(index.uncompressed_size_in_bytes);
        }
        if arguments.count_lines {
            let lines = index
                .total_line_count()
                .ok_or("imported index has no line offsets for --count-lines")?;
            print_count(lines);
        }
        return Ok(());
    }

    // Import-driven full decompress uses parallel `decode_with_index` (skip
    // marker speculation). --test without import uses the verified parallel
    // path. IndexedReader remains for --ranges only.
    //
    // When --count-lines is requested, prefer a verified decode that gathers
    // line counts rather than decode_with_index (which does not recount lines).
    let use_index_decode =
        imported_index.is_some() && !arguments.test && !arguments.count_lines;

    // Build a fresh index during decode only when exporting and no import was
    // supplied (imported indexes are re-exported after success instead).
    let need_keep_index = arguments.export_index.is_some() && imported_index.is_none();
    // Line offsets are required for --count-lines and for gztool-with-lines export.
    let need_gather_lines = arguments.count_lines
        || (need_keep_index && matches!(arguments.index_format, IndexFormat::GztoolWithLines));

    let decoder = build_decoder(&arguments, need_keep_index, need_gather_lines)?;
    let destination = output_destination(
        &arguments,
        input_file_for_output.as_deref(),
        stdout_is_tty,
    );

    let started = Instant::now();
    let (report, retained_import) = if use_index_decode {
        let index = imported_index.expect("use_index_decode requires import");
        let report = decode_via_index(
            &decoder,
            input.as_read_at(),
            &index,
            &destination,
            arguments.force,
        )?;
        // Retain import for export/count (decode_with_index does not clone it).
        (report, Some(index))
    } else {
        let report =
            decode_parallel(&decoder, input.as_read_at(), &destination, arguments.force)?;
        (report, imported_index)
    };
    let elapsed = started.elapsed();

    if arguments.test && !arguments.quiet {
        eprintln!(
            "{input_name}: ok, {} member(s), {} decoded bytes",
            report.member_count, report.decompressed_bytes
        );
    }

    if let Some(ref export_path) = arguments.export_index {
        if let Some(index) = report.index.as_ref() {
            write_index(export_path, index, arguments.index_format, arguments.force)?;
        } else if let Some(index) = retained_import.as_ref() {
            // --test (or other verified path) with --import-index: re-export
            // the validated import after a successful full decode.
            write_index(export_path, index, arguments.index_format, arguments.force)?;
        } else {
            return Err("index was not produced (keep_index did not yield an index)".into());
        }
    }

    if arguments.count {
        let size = report
            .index
            .as_ref()
            .map(|index| index.uncompressed_size_in_bytes)
            .or_else(|| {
                retained_import
                    .as_ref()
                    .map(|index| index.uncompressed_size_in_bytes)
            })
            .unwrap_or(report.decompressed_bytes);
        print_count(size);
    }

    if arguments.count_lines {
        let lines = report
            .line_count
            .or_else(|| {
                report
                    .index
                    .as_ref()
                    .and_then(GzipIndex::total_line_count)
            })
            .or_else(|| {
                retained_import
                    .as_ref()
                    .and_then(GzipIndex::total_line_count)
            })
            .ok_or("line count was not produced (gather_line_offsets did not run)")?;
        print_count(lines);
    }

    print_verbose_stats(&arguments, &report, elapsed);

    Ok(())
}

/// Streaming decode from stdin via [`Decoder::decode_read`].
///
/// Used for ordinary decompress / `--test` / `--count` / `--count-lines` /
/// `--export-index` without `--import-index` or `--ranges`. Single-thread /
/// zlib / raw stay page-streaming; multi-thread gzip spills to a temp file
/// inside the library then runs the parallel path.
fn run_stdin_stream(
    arguments: Arguments,
    stdout_is_tty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let need_keep_index = arguments.export_index.is_some();
    let need_gather_lines = arguments.count_lines
        || (need_keep_index && matches!(arguments.index_format, IndexFormat::GztoolWithLines));
    let decoder = build_decoder(&arguments, need_keep_index, need_gather_lines)?;
    // Stdin always streams decompressed payload to stdout (or sink for --test).
    let destination = output_destination(&arguments, None, stdout_is_tty);

    let started = Instant::now();
    let report = decode_via_read(&decoder, io::stdin().lock(), &destination, arguments.force)?;
    let elapsed = started.elapsed();

    if arguments.test && !arguments.quiet {
        eprintln!(
            "-: ok, {} member(s), {} decoded bytes",
            report.member_count, report.decompressed_bytes
        );
    }

    if let Some(ref export_path) = arguments.export_index {
        let index = report
            .index
            .as_ref()
            .ok_or("index was not produced (keep_index did not yield an index)")?;
        write_index(export_path, index, arguments.index_format, arguments.force)?;
    }

    if arguments.count {
        let size = report
            .index
            .as_ref()
            .map(|index| index.uncompressed_size_in_bytes)
            .unwrap_or(report.decompressed_bytes);
        print_count(size);
    }

    if arguments.count_lines {
        let lines = report
            .line_count
            .or_else(|| {
                report
                    .index
                    .as_ref()
                    .and_then(GzipIndex::total_line_count)
            })
            .ok_or("line count was not produced (gather_line_offsets did not run)")?;
        print_count(lines);
    }

    print_verbose_stats(&arguments, &report, elapsed);
    Ok(())
}

fn decode_via_read<R: Read>(
    decoder: &Decoder,
    reader: R,
    destination: &OutputDestination,
    force: bool,
) -> Result<DecodeReport, Box<dyn std::error::Error>> {
    let stdout = io::stdout();
    let mut output = open_destination_writer(destination, force, &stdout)?;
    let report = decoder.decode_read(reader, &mut output)?;
    output.flush()?;
    Ok(report)
}

enum DestinationWriter<'a> {
    Sink(io::Sink),
    Stdout(io::StdoutLock<'a>),
    File(File),
    Tee(TeeWriter<File, io::StdoutLock<'a>>),
}

impl Write for DestinationWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Self::Sink(w) => w.write(buf),
            Self::Stdout(w) => w.write(buf),
            Self::File(w) => w.write(buf),
            Self::Tee(w) => w.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Self::Sink(w) => w.flush(),
            Self::Stdout(w) => w.flush(),
            Self::File(w) => w.flush(),
            Self::Tee(w) => w.flush(),
        }
    }
}

/// Build a [`DestinationWriter`] for `destination` (opens the file when needed).
fn open_destination_writer<'a>(
    destination: &OutputDestination,
    force: bool,
    stdout: &'a io::Stdout,
) -> Result<DestinationWriter<'a>, Box<dyn std::error::Error>> {
    match destination {
        OutputDestination::Sink => Ok(DestinationWriter::Sink(io::sink())),
        OutputDestination::Stdout => Ok(DestinationWriter::Stdout(stdout.lock())),
        OutputDestination::File(path) => {
            Ok(DestinationWriter::File(open_output_file(path, force)?))
        }
        OutputDestination::Tee(path) => Ok(DestinationWriter::Tee(TeeWriter {
            primary: open_output_file(path, force)?,
            secondary: stdout.lock(),
        })),
    }
}

fn decode_parallel<R>(
    decoder: &Decoder,
    input: &R,
    destination: &OutputDestination,
    force: bool,
) -> Result<DecodeReport, Box<dyn std::error::Error>>
where
    R: ReadAt + ?Sized,
{
    let stdout = io::stdout();
    let mut output = open_destination_writer(destination, force, &stdout)?;
    let report = decoder.decode(input, &mut output)?;
    output.flush()?;
    Ok(report)
}

/// Full-stream decompress via parallel [`Decoder::decode_with_index`].
///
/// Skips marker/window speculation. Does not verify member CRC32/ISIZE on the
/// index path (same policy as the library); use `--test` without relying on
/// import for full footer verification, or decode without `--import-index`.
fn decode_via_index<R: ReadAt + ?Sized>(
    decoder: &Decoder,
    source: &R,
    index: &GzipIndex,
    destination: &OutputDestination,
    force: bool,
) -> Result<DecodeReport, Box<dyn std::error::Error>> {
    let stdout = io::stdout();
    let mut output = open_destination_writer(destination, force, &stdout)?;
    let report = decoder.decode_with_index(source, index, &mut output)?;
    output.flush()?;
    Ok(report)
}

/// Extract selected uncompressed ranges via `IndexedReader` seek/read.
///
/// When `--import-index` is missing, builds an index with a full decode
/// (`keep_index`) first so one-shot `--ranges ... file.gz` works. Line ranges
/// also enable `gather_line_offsets` (and require an index with line offsets
/// when importing).
fn run_ranges(
    arguments: &Arguments,
    ranges: &ParsedRanges,
    input: CompressedInput,
    imported_index: Option<GzipIndex>,
    input_file: Option<&Path>,
    stdout_is_tty: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    let need_line_offsets = matches!(ranges.unit, RangeUnit::Lines);
    let index = match imported_index {
        Some(index) => {
            if need_line_offsets && !index.has_line_offsets {
                return Err(
                    "imported index has no line offsets; rebuild with --count-lines or without --import-index for line ranges"
                        .into(),
                );
            }
            index
        }
        None => {
            let decoder = build_decoder(arguments, true, need_line_offsets)?;
            let report = decoder.decode(input.as_read_at(), &mut io::sink())?;
            report
                .index
                .ok_or("index was not produced (keep_index did not yield an index)")?
        }
    };

    let decoder = build_decoder(arguments, false, false)?;
    // Ranges never auto-name (always extract to stdout unless -o / count-only).
    let destination = output_destination(arguments, input_file, stdout_is_tty);

    // Reopen by path (real file or stdin spill tempfile). `input` must stay
    // alive so NamedTempFile is not deleted while the reader is open.
    let mut reader = decoder.open_with_index(input.path_for_open(), index)?;
    let (emitted_bytes, emitted_lines) =
        extract_ranges(&mut reader, ranges, &destination, arguments.force)?;
    if let Some(ref export_path) = arguments.export_index {
        write_index(
            export_path,
            &reader.into_index(),
            arguments.index_format,
            arguments.force,
        )?;
    }

    if arguments.count {
        print_count(emitted_bytes);
    }
    if arguments.count_lines {
        print_count(emitted_lines);
    }

    Ok(())
}

fn extract_ranges(
    reader: &mut IndexedReader<impl ReadAt>,
    ranges: &ParsedRanges,
    destination: &OutputDestination,
    force: bool,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    let stdout = io::stdout();
    let mut output = open_destination_writer(destination, force, &stdout)?;
    let mut total_bytes = 0u64;
    let mut total_lines = 0u64;
    for range in &ranges.ranges {
        let (b, l) = copy_range(reader, range, ranges.unit, &mut output)?;
        total_bytes += b;
        total_lines += l;
    }
    output.flush()?;
    Ok((total_bytes, total_lines))
}

/// Copies one range; returns `(bytes_written, newlines_in_written)`.
fn copy_range<W: Write>(
    reader: &mut IndexedReader<impl ReadAt>,
    range: &ExtractRange,
    unit: RangeUnit,
    output: &mut W,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    match unit {
        RangeUnit::Bytes => {
            reader.seek(SeekFrom::Start(range.offset))?;
            let mut counting = CountingWrite {
                inner: output,
                newlines: 0,
            };
            let written = match range.length {
                Some(length) => io::copy(&mut reader.take(length), &mut counting)?,
                None => io::copy(reader, &mut counting)?,
            };
            Ok((written, counting.newlines))
        }
        RangeUnit::Lines => {
            // `offset` is a 1-based line number (start of that line).
            reader.seek_to_line(range.offset)?;
            copy_line_span(reader, range.length, output)
        }
    }
}

/// Writer adapter that counts Unix newlines while forwarding bytes.
struct CountingWrite<'a, W> {
    inner: &'a mut W,
    newlines: u64,
}

impl<W: Write> Write for CountingWrite<'_, W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.newlines += buf[..n].iter().filter(|&&b| b == b'\n').count() as u64;
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.inner.flush()
    }
}

/// Copy `length` lines (or until EOF when `None`) from the current position.
///
/// Each "line" ends at a `\n` (included) or EOF. Returns `(bytes, newlines)`.
fn copy_line_span<R: Read, W: Write>(
    reader: &mut R,
    length: Option<u64>,
    output: &mut W,
) -> Result<(u64, u64), Box<dyn std::error::Error>> {
    match length {
        Some(0) => Ok((0, 0)),
        None => {
            let mut buf = Vec::new();
            reader.read_to_end(&mut buf)?;
            let newlines = buf.iter().filter(|&&b| b == b'\n').count() as u64;
            output.write_all(&buf)?;
            Ok((buf.len() as u64, newlines))
        }
        Some(n) => {
            let mut newlines = 0u64;
            let mut written = 0u64;
            let mut buf = [0u8; 8192];
            loop {
                let m = reader.read(&mut buf)?;
                if m == 0 {
                    break;
                }
                let mut take = m;
                for (i, &b) in buf[..m].iter().enumerate() {
                    if b == b'\n' {
                        newlines += 1;
                        if newlines == n {
                            take = i + 1;
                            break;
                        }
                    }
                }
                output.write_all(&buf[..take])?;
                written += take as u64;
                if newlines >= n {
                    break;
                }
            }
            Ok((written, newlines))
        }
    }
}

// --- Range specification parsing (CLI-local) ---------------------------------

/// Whether a ranges list is in byte or line units (cannot mix).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RangeUnit {
    Bytes,
    Lines,
}

/// One extract range: `LENGTH@OFFSET` (`length == None` means until EOF).
///
/// For [`RangeUnit::Bytes`], `offset`/`length` are uncompressed byte counts.
/// For [`RangeUnit::Lines`], `offset` is a **1-based** line number and `length`
/// is a line count (when present).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExtractRange {
    offset: u64,
    length: Option<u64>,
}

/// Parsed `--ranges` specification (homogeneous unit).
#[derive(Debug, Clone, PartialEq, Eq)]
struct ParsedRanges {
    unit: RangeUnit,
    ranges: Vec<ExtractRange>,
}

/// Parse a comma-separated `--ranges` spec into ordered ranges.
///
/// Forms: `LENGTH@OFFSET` with optional size units (`B`, `K`/`KiB`, `M`/`MiB`,
/// `G`/`GiB`) or line unit `L`, and `inf` for open-ended length. Mixing byte
/// and line units in one specification is an error.
fn parse_ranges(spec: &str) -> Result<ParsedRanges, Box<dyn std::error::Error>> {
    let spec = spec.trim();
    if spec.is_empty() {
        return Err("empty --ranges specification".into());
    }

    let mut ranges = Vec::new();
    let mut unit: Option<RangeUnit> = None;
    for part in spec.split(',') {
        let part = part.trim();
        if part.is_empty() {
            return Err("empty range in --ranges (stray comma?)".into());
        }
        let (range, part_unit) = parse_one_range(part)?;
        match unit {
            None => unit = Some(part_unit),
            Some(existing) if existing != part_unit => {
                return Err("cannot mix byte and line ranges".into());
            }
            Some(_) => {}
        }
        ranges.push(range);
    }
    Ok(ParsedRanges {
        unit: unit.expect("at least one range"),
        ranges,
    })
}

fn parse_one_range(part: &str) -> Result<(ExtractRange, RangeUnit), Box<dyn std::error::Error>> {
    let (length_str, offset_str) = part.split_once('@').ok_or_else(|| {
        format!("invalid range '{part}': expected LENGTH@OFFSET")
    })?;

    let length_is_line = size_uses_line_unit(length_str);
    let offset_is_line = size_uses_line_unit(offset_str);
    let length_is_inf = length_str.trim().eq_ignore_ascii_case("inf");

    // Homogeneous unit rules:
    // - neither side uses L → bytes (`5@0`, `inf@1KiB`)
    // - both use L → lines (`5L@20L`)
    // - `inf` length + L offset → lines (`inf@10L`)
    // - any other mix → error
    let unit = match (length_is_line, offset_is_line, length_is_inf) {
        (false, false, _) => RangeUnit::Bytes,
        (true, true, _) => RangeUnit::Lines,
        (false, true, true) => RangeUnit::Lines,
        _ => return Err("cannot mix byte and line ranges".into()),
    };

    let length = parse_length(length_str, unit)
        .map_err(|e| format!("invalid range length in '{part}': {e}"))?;
    let offset = parse_size_token(offset_str, unit)
        .map_err(|e| format!("invalid range offset in '{part}': {e}"))?;

    if unit == RangeUnit::Lines && offset == 0 {
        return Err("line offsets are 1-based; use 1L for the first line".into());
    }

    Ok((ExtractRange { offset, length }, unit))
}

fn parse_length(token: &str, unit: RangeUnit) -> Result<Option<u64>, String> {
    let token = token.trim();
    if token.is_empty() {
        return Err("empty length".into());
    }
    if token.eq_ignore_ascii_case("inf") {
        return Ok(None);
    }
    Ok(Some(parse_size_token(token, unit)?))
}

/// Returns true when the token uses the line unit `L` (not part of KiB/MiB/GiB).
fn size_uses_line_unit(token: &str) -> bool {
    let token = token.trim();
    if token.is_empty() || token.eq_ignore_ascii_case("inf") {
        return false;
    }
    match split_number_unit(token) {
        Ok((_, unit)) => unit.eq_ignore_ascii_case("l"),
        Err(_) => {
            // Fall back: trailing L after optional digits, excluding binary units.
            let lower = token.to_ascii_lowercase();
            lower.ends_with('l')
                && !lower.ends_with("kib")
                && !lower.ends_with("mib")
                && !lower.ends_with("gib")
        }
    }
}

fn parse_size_token(token: &str, expected: RangeUnit) -> Result<u64, String> {
    let token = token.trim();
    if token.is_empty() {
        return Err("empty size value".into());
    }
    if token.eq_ignore_ascii_case("inf") {
        return Err("'inf' is only valid as a range length, not an offset".into());
    }

    let (num_str, unit) = split_number_unit(token)?;
    let n: u64 = num_str
        .parse()
        .map_err(|_| format!("invalid number '{num_str}'"))?;

    let unit_lower = unit.to_ascii_lowercase();
    match expected {
        RangeUnit::Lines => match unit_lower.as_str() {
            "l" => Ok(n),
            "" => Err("line ranges require the L unit (for example 5L@1L)".into()),
            other => Err(format!(
                "unexpected unit '{other}' in line range (expected L)"
            )),
        },
        RangeUnit::Bytes => {
            let multiplier: u64 = match unit_lower.as_str() {
                "" | "b" => 1,
                "k" | "kib" => 1024,
                "m" | "mib" => 1024 * 1024,
                "g" | "gib" => 1024 * 1024 * 1024,
                "l" => return Err("cannot mix byte and line ranges".into()),
                other => return Err(format!("unknown size unit '{other}'")),
            };
            n.checked_mul(multiplier)
                .ok_or_else(|| format!("size overflow in '{token}'"))
        }
    }
}

/// Split a size token into a decimal number prefix and unit suffix.
fn split_number_unit(token: &str) -> Result<(&str, &str), String> {
    let bytes = token.as_bytes();
    let mut end = 0usize;
    while end < bytes.len() && bytes[end].is_ascii_digit() {
        end += 1;
    }
    if end == 0 {
        return Err(format!("expected a number in '{token}'"));
    }
    let (num, unit) = token.split_at(end);
    Ok((num, unit.trim()))
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    fn parse_args(args: &[&str]) -> Arguments {
        Arguments::try_parse_from(
            std::iter::once("rapidgzip-rust").chain(args.iter().copied()),
        )
        .expect("parse args")
    }

    #[test]
    fn default_output_path_strips_known_suffixes() {
        assert_eq!(
            default_output_path(Path::new("reads.fastq.gz")),
            PathBuf::from("reads.fastq")
        );
        assert_eq!(
            default_output_path(Path::new("archive.gzip")),
            PathBuf::from("archive")
        );
        // Longer suffix must win: `.gzip` is not treated as `…ip` + `.gz`.
        assert_eq!(
            default_output_path(Path::new("file.gzip")),
            PathBuf::from("file")
        );
        assert_eq!(
            default_output_path(Path::new("/tmp/data.bgz")),
            PathBuf::from("/tmp/data")
        );
        assert_eq!(
            default_output_path(Path::new("block.bgzf")),
            PathBuf::from("block")
        );
        // Only one trailing suffix; intermediate names are left alone.
        assert_eq!(
            default_output_path(Path::new("file.gz.bgz")),
            PathBuf::from("file.gz")
        );
    }

    #[test]
    fn default_output_path_case_insensitive_and_tgz_to_tar() {
        assert_eq!(
            default_output_path(Path::new("reads.FASTQ.GZ")),
            PathBuf::from("reads.FASTQ")
        );
        assert_eq!(
            default_output_path(Path::new("archive.GZIP")),
            PathBuf::from("archive")
        );
        assert_eq!(
            default_output_path(Path::new("data.BgZ")),
            PathBuf::from("data")
        );
        assert_eq!(
            default_output_path(Path::new("block.BGZF")),
            PathBuf::from("block")
        );
        assert_eq!(
            default_output_path(Path::new("foo.tgz")),
            PathBuf::from("foo.tar")
        );
        assert_eq!(
            default_output_path(Path::new("bar.TAZ")),
            PathBuf::from("bar.tar")
        );
        assert_eq!(
            default_output_path(Path::new("/tmp/archive.TGZ")),
            PathBuf::from("/tmp/archive.tar")
        );
    }

    #[test]
    fn default_output_path_falls_back_to_out_suffix() {
        // No known compression suffix.
        assert_eq!(
            default_output_path(Path::new("plain.bin")),
            PathBuf::from("plain.bin.out")
        );
        // Stripping would leave an empty file name.
        assert_eq!(
            default_output_path(Path::new(".gz")),
            PathBuf::from(".gz.out")
        );
        assert_eq!(
            default_output_path(Path::new("/tmp/.gzip")),
            PathBuf::from("/tmp/.gzip.out")
        );
        assert_eq!(
            default_output_path(Path::new(".tgz")),
            PathBuf::from(".tgz.out")
        );
        // Bare tarball-style name with no stem still falls back.
        assert_eq!(
            default_output_path(Path::new(".TGZ")),
            PathBuf::from(".TGZ.out")
        );
    }

    #[test]
    fn format_flag_is_accepted() {
        let args = parse_args(&["reads.fastq.gz"]);
        assert_eq!(args.format, CliFormat::Auto);
        assert_eq!(args.format.to_core(), Format::Auto);

        let args = parse_args(&["--format", "auto", "reads.fastq.gz"]);
        assert_eq!(args.format, CliFormat::Auto);

        let args = parse_args(&["--format", "gzip", "reads.fastq.gz"]);
        assert_eq!(args.format, CliFormat::Gzip);
        assert_eq!(args.format.to_core(), Format::Gzip);

        let args = parse_args(&["--format", "zlib", "x.zz"]);
        assert_eq!(args.format, CliFormat::Zlib);
        assert_eq!(args.format.to_core(), Format::Zlib);

        let args = parse_args(&["--format", "raw", "x.deflate"]);
        assert_eq!(args.format, CliFormat::RawDeflate);
        assert_eq!(args.format.to_core(), Format::RawDeflate);
    }

    #[test]
    fn output_destination_tty_auto_names_file_input() {
        let args = parse_args(&["reads.fastq.gz"]);
        let dest = output_destination(&args, Some(Path::new("reads.fastq.gz")), true);
        assert_eq!(dest, OutputDestination::File(PathBuf::from("reads.fastq")));

        let args = parse_args(&["reads.FASTQ.GZ"]);
        let dest = output_destination(&args, Some(Path::new("reads.FASTQ.GZ")), true);
        assert_eq!(dest, OutputDestination::File(PathBuf::from("reads.FASTQ")));

        let args = parse_args(&["archive.tgz"]);
        let dest = output_destination(&args, Some(Path::new("archive.tgz")), true);
        assert_eq!(dest, OutputDestination::File(PathBuf::from("archive.tar")));
    }

    #[test]
    fn output_destination_pipe_keeps_stdout_for_file_input() {
        let args = parse_args(&["reads.fastq.gz"]);
        let dest = output_destination(&args, Some(Path::new("reads.fastq.gz")), false);
        assert_eq!(dest, OutputDestination::Stdout);
    }

    #[test]
    fn output_destination_stdin_is_stdout() {
        let args = parse_args(&["-"]);
        let dest = output_destination(&args, None, true);
        assert_eq!(dest, OutputDestination::Stdout);
    }

    #[test]
    fn output_destination_respects_explicit_c_and_o() {
        let args = parse_args(&["-c", "reads.fastq.gz"]);
        assert_eq!(
            output_destination(&args, Some(Path::new("reads.fastq.gz")), true),
            OutputDestination::Stdout
        );

        let args = parse_args(&["-o", "out.bin", "reads.fastq.gz"]);
        assert_eq!(
            output_destination(&args, Some(Path::new("reads.fastq.gz")), true),
            OutputDestination::File(PathBuf::from("out.bin"))
        );
    }

    #[test]
    fn output_destination_test_count_export_and_ranges() {
        let args = parse_args(&["-t", "reads.fastq.gz"]);
        assert_eq!(
            output_destination(&args, Some(Path::new("reads.fastq.gz")), true),
            OutputDestination::Sink
        );

        let args = parse_args(&["--count", "reads.fastq.gz"]);
        assert_eq!(
            output_destination(&args, Some(Path::new("reads.fastq.gz")), true),
            OutputDestination::Sink
        );

        let args = parse_args(&["--export-index", "x.gzidx", "reads.fastq.gz"]);
        assert_eq!(
            output_destination(&args, Some(Path::new("reads.fastq.gz")), true),
            OutputDestination::Sink
        );

        let args = parse_args(&["--ranges", "10@0", "reads.fastq.gz"]);
        assert_eq!(
            output_destination(&args, Some(Path::new("reads.fastq.gz")), true),
            OutputDestination::Stdout
        );
    }

    #[test]
    fn decompress_flag_is_accepted() {
        let args = parse_args(&["-d", "reads.fastq.gz"]);
        assert!(args.decompress);
        let args = parse_args(&["--decompress", "-v", "reads.fastq.gz"]);
        assert!(args.decompress);
        assert!(args.verbose);
    }

    #[test]
    fn chunk_size_kib_to_bytes_scales_and_rejects_zero() {
        assert_eq!(chunk_size_kib_to_bytes(1).unwrap(), 1024);
        assert_eq!(chunk_size_kib_to_bytes(4096).unwrap(), 4 * 1024 * 1024);
        let err = chunk_size_kib_to_bytes(0).unwrap_err().to_string();
        assert!(err.contains("non-zero"), "{err}");
    }

    #[test]
    fn open_output_file_refuses_overwrite_without_force() {
        let dir = std::env::temp_dir().join(format!(
            "rapidgzip-rust-cli-force-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("out.bin");
        {
            let mut f = File::create(&path).unwrap();
            f.write_all(b"existing").unwrap();
        }

        let err = open_output_file(&path, false).unwrap_err().to_string();
        assert!(err.contains("already exists"), "{err}");
        assert!(err.contains("--force"), "{err}");
        assert_eq!(std::fs::read(&path).unwrap(), b"existing");

        {
            let mut f = open_output_file(&path, true).unwrap();
            f.write_all(b"new").unwrap();
            f.flush().unwrap();
        }
        assert_eq!(std::fs::read(&path).unwrap(), b"new");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_output_file_creates_when_missing() {
        let dir = std::env::temp_dir().join(format!(
            "rapidgzip-rust-cli-create-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("fresh.bin");

        {
            let mut f = open_output_file(&path, false).unwrap();
            f.write_all(b"ok").unwrap();
            f.flush().unwrap();
        }
        assert_eq!(std::fs::read(&path).unwrap(), b"ok");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn parse_simple_ranges() {
        let ranges = parse_ranges("10@0").unwrap();
        assert_eq!(ranges.unit, RangeUnit::Bytes);
        assert_eq!(
            ranges.ranges,
            vec![ExtractRange {
                offset: 0,
                length: Some(10)
            }]
        );
    }

    #[test]
    fn parse_units_and_inf() {
        let ranges = parse_ranges("100@1KiB,inf@1MiB,1K@2M").unwrap();
        assert_eq!(ranges.unit, RangeUnit::Bytes);
        assert_eq!(
            ranges.ranges,
            vec![
                ExtractRange {
                    offset: 1024,
                    length: Some(100)
                },
                ExtractRange {
                    offset: 1024 * 1024,
                    length: None
                },
                ExtractRange {
                    offset: 2 * 1024 * 1024,
                    length: Some(1024)
                },
            ]
        );
    }

    #[test]
    fn parse_multiple_comma_separated() {
        let ranges = parse_ranges("5@0, 5@10").unwrap();
        assert_eq!(ranges.ranges.len(), 2);
        assert_eq!(ranges.ranges[0], ExtractRange { offset: 0, length: Some(5) });
        assert_eq!(ranges.ranges[1], ExtractRange { offset: 10, length: Some(5) });
    }

    #[test]
    fn parse_line_ranges() {
        let ranges = parse_ranges("5L@1L").unwrap();
        assert_eq!(ranges.unit, RangeUnit::Lines);
        assert_eq!(
            ranges.ranges,
            vec![ExtractRange {
                offset: 1,
                length: Some(5)
            }]
        );

        let ranges = parse_ranges("5L@20L").unwrap();
        assert_eq!(
            ranges.ranges,
            vec![ExtractRange {
                offset: 20,
                length: Some(5)
            }]
        );

        let ranges = parse_ranges("inf@10L").unwrap();
        assert_eq!(ranges.unit, RangeUnit::Lines);
        assert_eq!(
            ranges.ranges,
            vec![ExtractRange {
                offset: 10,
                length: None
            }]
        );
    }

    #[test]
    fn reject_mixed_byte_and_line_ranges() {
        let err = parse_ranges("10@5L").unwrap_err().to_string();
        assert!(err.contains("cannot mix"), "{err}");

        let err = parse_ranges("5L@20").unwrap_err().to_string();
        assert!(err.contains("cannot mix"), "{err}");

        let err = parse_ranges("5@0,5L@1L").unwrap_err().to_string();
        assert!(err.contains("cannot mix"), "{err}");
    }

    #[test]
    fn reject_zero_based_line_offset() {
        let err = parse_ranges("5L@0L").unwrap_err().to_string();
        assert!(err.contains("1-based"), "{err}");
    }

    #[test]
    fn reject_empty_and_malformed() {
        assert!(parse_ranges("").is_err());
        assert!(parse_ranges("10").is_err());
        assert!(parse_ranges("10@0,").is_err());
        assert!(parse_ranges("@0").is_err());
        assert!(parse_ranges("inf@inf").is_err());
    }

    #[test]
    fn parse_gi_b_and_bare_b() {
        let ranges = parse_ranges("1B@0,2GiB@3G").unwrap();
        assert_eq!(
            ranges.ranges[0],
            ExtractRange {
                offset: 0,
                length: Some(1)
            }
        );
        assert_eq!(
            ranges.ranges[1],
            ExtractRange {
                offset: 3 * 1024 * 1024 * 1024,
                length: Some(2 * 1024 * 1024 * 1024)
            }
        );
    }

#[test]
    fn verify_flag_parses_and_conflicts_with_no_verify() {
        let args = parse_args(&["--verify", "reads.fastq.gz"]);
        assert!(args.verify);
        assert!(!args.no_verify);

        let args = parse_args(&["--no-verify", "reads.fastq.gz"]);
        assert!(!args.verify);
        assert!(args.no_verify);

        let err = Arguments::try_parse_from([
            "rapidgzip-rust",
            "--verify",
            "--no-verify",
            "reads.fastq.gz",
        ]);
        assert!(err.is_err(), "expected --verify/--no-verify conflict");
    }

#[test]
    fn oss_attributions_text_mentions_common_licenses() {
        let text = OSS_ATTRIBUTIONS;
        let has_marker = text.contains("BSD")
            || text.contains("MIT")
            || text.contains("zlib")
            || text.contains("Zlib");
        assert!(has_marker, "attributions should mention BSD, MIT, or zlib");
        assert!(
            text.contains("zlib-rs") || text.contains("Zlib"),
            "expected zlib-rs/Zlib mention"
        );
        assert!(OSS_ATTRIBUTIONS_YAML.contains("BSD-3-Clause") || OSS_ATTRIBUTIONS_YAML.contains("MIT"));
    }

#[test]
    fn oss_attributions_run_without_input() {
        // Prints to stdout; must succeed without an INPUT path.
        run(parse_args(&["--oss-attributions"])).unwrap();
        run(parse_args(&["--oss-attributions-yaml"])).unwrap();
        // Quiet must not block attributions (early exit before quiet is used).
        run(parse_args(&["-q", "--oss-attributions"])).unwrap();
    }
}
