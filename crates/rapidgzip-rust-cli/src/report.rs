//! What the tool prints when it is not writing decompressed bytes.

use rapidgzip_core::DecodeReport;
use std::io::{self, Write};
use std::time::Duration;

/// How much the run says about itself.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Volume {
    /// Only failures.
    Quiet,
    /// Failures and the result of an explicit action.
    Normal,
    /// Adds the path taken, the worker count, and timings.
    Verbose,
}

impl Volume {
    /// Resolves the volume from the two flags, which clap keeps exclusive.
    pub const fn from_flags(quiet: bool, verbose: bool) -> Self {
        if quiet {
            Self::Quiet
        } else if verbose {
            Self::Verbose
        } else {
            Self::Normal
        }
    }

    /// Returns whether ordinary result lines are printed.
    pub const fn prints_results(self) -> bool {
        !matches!(self, Self::Quiet)
    }
}

/// Prints the result of `--test`.
pub fn print_test_result(name: &str, report: &DecodeReport, volume: Volume) -> io::Result<()> {
    if !volume.prints_results() {
        return Ok(());
    }
    writeln!(
        io::stderr(),
        "{name}: ok, {} member(s), {} decoded bytes",
        report.member_count,
        report.decompressed_bytes
    )
}

/// Prints the decompressed size, which is the whole output of `--count`.
pub fn print_count(report: &DecodeReport) -> io::Result<()> {
    writeln!(io::stdout(), "{}", report.decompressed_bytes)
}

/// Prints the newline count, which is the whole output of `--count-lines`.
pub fn print_line_count(report: &DecodeReport) -> io::Result<()> {
    writeln!(
        io::stdout(),
        "{}",
        report.line_count.expect("line counting was requested")
    )
}

/// Prints the container, concurrency, and rate, under `--verbose` only.
pub fn print_verbose_summary(
    name: &str,
    report: &DecodeReport,
    elapsed: Duration,
) -> io::Result<()> {
    let seconds = elapsed.as_secs_f64();
    let rate = if seconds > 0.0 {
        report.decompressed_bytes as f64 / seconds / (1024.0 * 1024.0)
    } else {
        0.0
    };
    let mut stderr = io::stderr();
    writeln!(stderr, "{name}:")?;
    writeln!(stderr, "  format          : {}", report.format)?;
    writeln!(stderr, "  worker budget   : {}", report.decoder_threads)?;
    writeln!(stderr, "  members         : {}", report.member_count)?;
    writeln!(stderr, "  compressed      : {} B", report.compressed_bytes)?;
    writeln!(
        stderr,
        "  decompressed    : {} B",
        report.decompressed_bytes
    )?;
    if let Some(lines) = report.line_count {
        writeln!(stderr, "  lines           : {lines}")?;
    }
    writeln!(stderr, "  elapsed         : {seconds:.3} s")?;
    writeln!(stderr, "  rate            : {rate:.1} MiB/s")
}
