//! Minimal programmatic-reader benchmark target.
//!
//! This deliberately measures the public `Decoder::open` -> `Read` ->
//! `DecoderReader::finish` path without telemetry sampling or output I/O.

use rapidgzip_core::Decoder;
use std::env;
use std::hint::black_box;
use std::io::Read;
use std::path::PathBuf;
use std::process::ExitCode;
use std::thread;
use std::time::Duration;

struct Arguments {
    input: PathBuf,
    threads: usize,
    read_buffer_bytes: usize,
    expected_decoded_bytes: u64,
    delay: Duration,
    stop_after_bytes: Option<u64>,
}

fn usage() -> &'static str {
    "usage: reader_decode INPUT.gz THREADS READ_BUFFER_BYTES EXPECTED_DECODED_BYTES \
     [DELAY_MICROS [STOP_AFTER_BYTES]]"
}

fn parse_usize(value: Option<String>, name: &str) -> Result<usize, String> {
    let value = value.ok_or_else(|| format!("missing {name}\n{}", usage()))?;
    let parsed = value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name} {value:?}: {error}"))?;
    if parsed == 0 {
        return Err(format!("{name} must be nonzero"));
    }
    Ok(parsed)
}

fn parse_u64(value: Option<String>, name: &str) -> Result<u64, String> {
    let value = value.ok_or_else(|| format!("missing {name}\n{}", usage()))?;
    value
        .parse::<u64>()
        .map_err(|error| format!("invalid {name} {value:?}: {error}"))
}

fn parse_arguments() -> Result<Arguments, String> {
    let mut arguments = env::args().skip(1);
    let input = arguments
        .next()
        .filter(|argument| !matches!(argument.as_str(), "--help" | "-h"))
        .ok_or_else(|| usage().to_owned())?;
    let threads = parse_usize(arguments.next(), "thread count")?;
    let read_buffer_bytes = parse_usize(arguments.next(), "read-buffer size")?;
    let expected_decoded_bytes = parse_u64(arguments.next(), "expected decoded size")?;
    let delay_micros = arguments
        .next()
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|error| format!("invalid delay {value:?}: {error}"))
        })
        .transpose()?
        .unwrap_or(0);
    let stop_after_bytes = arguments
        .next()
        .map(|value| {
            value
                .parse::<u64>()
                .map_err(|error| format!("invalid stop position {value:?}: {error}"))
        })
        .transpose()?;
    if arguments.next().is_some() {
        return Err(format!("too many arguments\n{}", usage()));
    }
    Ok(Arguments {
        input: PathBuf::from(input),
        threads,
        read_buffer_bytes,
        expected_decoded_bytes,
        delay: Duration::from_micros(delay_micros),
        stop_after_bytes,
    })
}

fn run(arguments: Arguments) -> Result<(), Box<dyn std::error::Error>> {
    let decoder = Decoder::builder()
        .decoder_threads(arguments.threads)
        .build()?;
    let mut reader = decoder.open(&arguments.input)?;
    let mut output = vec![0_u8; arguments.read_buffer_bytes];
    let mut consumed = 0_u64;
    while arguments
        .stop_after_bytes
        .is_none_or(|limit| consumed < limit)
    {
        let remaining = arguments.stop_after_bytes.map_or(output.len(), |limit| {
            usize::try_from(limit.saturating_sub(consumed))
                .unwrap_or(usize::MAX)
                .min(output.len())
        });
        if remaining == 0 {
            break;
        }
        let count = reader.read(&mut output[..remaining])?;
        if count == 0 {
            break;
        }
        // Make the writes observably consumed without adding a checksum pass.
        black_box(&output[..count]);
        consumed = consumed
            .checked_add(count as u64)
            .ok_or("consumed-byte count overflow")?;
        if !arguments.delay.is_zero() {
            thread::sleep(arguments.delay);
        }
    }
    let report = reader.finish()?;
    if report.decompressed_bytes != arguments.expected_decoded_bytes {
        return Err(format!(
            "decoded {} bytes; expected {}",
            report.decompressed_bytes, arguments.expected_decoded_bytes
        )
        .into());
    }
    println!("consumed_bytes\t{consumed}");
    println!("decoded_bytes\t{}", report.decompressed_bytes);
    println!("member_count\t{}", report.member_count);
    Ok(())
}

fn main() -> ExitCode {
    match parse_arguments().and_then(|arguments| run(arguments).map_err(|error| error.to_string()))
    {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("reader_decode: {error}");
            ExitCode::FAILURE
        }
    }
}
