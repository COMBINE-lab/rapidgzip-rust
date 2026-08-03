//! Human-readable CLI reports that do not contain decoded payload bytes.

use rapidgzip_core::DecodeReport;
use std::io::{self, Write};
use std::time::Duration;

/// Requested diagnostic volume.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Volume {
    /// Print failures only.
    Quiet,
    /// Print explicit action results and failures.
    Normal,
    /// Also print decode statistics and elapsed throughput.
    Verbose,
}

impl Volume {
    /// Resolves mutually exclusive quiet and verbose flags.
    pub const fn from_flags(quiet: bool, verbose: bool) -> Self {
        if quiet {
            Self::Quiet
        } else if verbose {
            Self::Verbose
        } else {
            Self::Normal
        }
    }

    const fn prints_results(self) -> bool {
        !matches!(self, Self::Quiet)
    }
}

/// Prints the successful result of `--test`.
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

/// Prints the decompressed byte count to the selected diagnostic stream.
pub fn print_count(report: &DecodeReport, to_stderr: bool) -> io::Result<()> {
    if to_stderr {
        writeln!(io::stderr(), "{}", report.decompressed_bytes)
    } else {
        writeln!(io::stdout(), "{}", report.decompressed_bytes)
    }
}

/// Prints the newline count requested from the decoder to the selected stream.
pub fn print_line_count(report: &DecodeReport, to_stderr: bool) -> io::Result<()> {
    let count = report.line_count.ok_or_else(|| {
        io::Error::other("line counting was requested but the report has no count")
    })?;
    if to_stderr {
        writeln!(io::stderr(), "{count}")
    } else {
        writeln!(io::stdout(), "{count}")
    }
}

/// Prints verified stream statistics and elapsed throughput.
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
    writeln!(stderr, "  framing units   : {}", report.member_count)?;
    writeln!(stderr, "  compressed      : {} B", report.compressed_bytes)?;
    writeln!(
        stderr,
        "  decompressed    : {} B",
        report.decompressed_bytes
    )?;
    if let Some(lines) = report.line_count {
        writeln!(stderr, "  newlines        : {lines}")?;
    }
    writeln!(stderr, "  elapsed         : {seconds:.3} s")?;
    writeln!(stderr, "  throughput      : {rate:.1} MiB/s")
}
