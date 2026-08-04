//! Minimal programmatic-reader benchmark target.
//!
//! This deliberately measures the public `Decoder::open` -> `Read` ->
//! `DecoderReader::finish` path without telemetry sampling or output I/O.

use paraseq::{Record, fastq};
use rapidgzip_core::{Decoder, IndexOptions};
use std::env;
use std::fs::File;
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
    iterations: usize,
    workload: Workload,
}

#[derive(Clone, Copy)]
enum Workload {
    Ordinary,
    Indexed,
    Paraseq,
}

fn usage() -> &'static str {
    "usage: reader_decode INPUT.gz THREADS READ_BUFFER_BYTES EXPECTED_DECODED_BYTES \
     [DELAY_MICROS [STOP_AFTER_BYTES|all [ITERATIONS [ordinary|indexed|paraseq]]]]"
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
    let stop_after_bytes = match arguments.next() {
        None => None,
        Some(value) if value == "all" => None,
        Some(value) => Some(
            value
                .parse::<u64>()
                .map_err(|error| format!("invalid stop position {value:?}: {error}"))?,
        ),
    };
    let iterations = arguments
        .next()
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|error| format!("invalid iteration count {value:?}: {error}"))
                .and_then(|parsed| {
                    (parsed != 0)
                        .then_some(parsed)
                        .ok_or_else(|| "iteration count must be nonzero".to_owned())
                })
        })
        .transpose()?
        .unwrap_or(1);
    let workload = match arguments.next().as_deref() {
        None | Some("ordinary") => Workload::Ordinary,
        Some("indexed") => Workload::Indexed,
        Some("paraseq") => Workload::Paraseq,
        Some(mode) => return Err(format!("invalid reader mode {mode:?}")),
    };
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
        iterations,
        workload,
    })
}

fn consume<R: Read>(
    reader: &mut R,
    output: &mut [u8],
    arguments: &Arguments,
) -> Result<u64, Box<dyn std::error::Error>> {
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
    Ok(consumed)
}

fn validate_report(
    report: rapidgzip_core::DecodeReport,
    arguments: &Arguments,
    member_count: &mut Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    if report.decompressed_bytes != arguments.expected_decoded_bytes {
        return Err(format!(
            "decoded {} bytes; expected {}",
            report.decompressed_bytes, arguments.expected_decoded_bytes
        )
        .into());
    }
    if member_count
        .replace(report.member_count)
        .is_some_and(|previous| previous != report.member_count)
    {
        return Err("member count changed between iterations".into());
    }
    Ok(())
}

fn validate_counts(
    decompressed_bytes: u64,
    observed_members: u64,
    arguments: &Arguments,
    member_count: &mut Option<u64>,
) -> Result<(), Box<dyn std::error::Error>> {
    if decompressed_bytes != arguments.expected_decoded_bytes {
        return Err(format!(
            "decoded {decompressed_bytes} bytes; expected {}",
            arguments.expected_decoded_bytes
        )
        .into());
    }
    if member_count
        .replace(observed_members)
        .is_some_and(|previous| previous != observed_members)
    {
        return Err("member count changed between iterations".into());
    }
    Ok(())
}

fn run(arguments: Arguments) -> Result<(), Box<dyn std::error::Error>> {
    let decoder = Decoder::builder()
        .decoder_threads(arguments.threads)
        .build()?;
    let mut output = vec![0_u8; arguments.read_buffer_bytes];
    let mut total_consumed = 0_u64;
    let mut member_count = None;
    for _ in 0..arguments.iterations {
        let consumed = match arguments.workload {
            Workload::Ordinary => {
                let mut reader = decoder.open(&arguments.input)?;
                let consumed = consume(&mut reader, &mut output, &arguments)?;
                validate_report(reader.finish()?, &arguments, &mut member_count)?;
                consumed
            }
            Workload::Indexed => {
                let source = File::open(&arguments.input)?;
                let mut reader = decoder.reader_with_index(source, IndexOptions::default())?;
                let consumed = consume(&mut reader, &mut output, &arguments)?;
                validate_report(reader.finish()?.decode, &arguments, &mut member_count)?;
                consumed
            }
            Workload::Paraseq => {
                if !arguments.delay.is_zero() || arguments.stop_after_bytes.is_some() {
                    return Err("paraseq requires zero delay and stop-after=all".into());
                }
                let decoded = decoder.open(&arguments.input)?;
                let handle = decoded.handle();
                let mut reader = fastq::Reader::new(decoded);
                let mut records = reader.new_record_set();
                let mut record_count = 0_u64;
                while records.fill(&mut reader)? {
                    for record in records.iter() {
                        let record = record?;
                        black_box(record.id());
                        black_box(record.seq_raw());
                        black_box(record.qual());
                        record_count = record_count
                            .checked_add(1)
                            .ok_or("FASTQ record-count overflow")?;
                    }
                }
                black_box(record_count);
                let stats = handle.stats();
                validate_counts(
                    stats.decompressed_bytes,
                    stats.member_count,
                    &arguments,
                    &mut member_count,
                )?;
                stats.decompressed_bytes
            }
        };
        total_consumed = total_consumed
            .checked_add(consumed)
            .ok_or("total consumed-byte count overflow")?;
    }
    println!("consumed_bytes\t{total_consumed}");
    println!(
        "decoded_bytes\t{}",
        arguments.expected_decoded_bytes * arguments.iterations as u64
    );
    println!("member_count\t{}", member_count.unwrap_or(0));
    println!("iterations\t{}", arguments.iterations);
    println!(
        "reader_mode\t{}",
        match arguments.workload {
            Workload::Ordinary => "ordinary",
            Workload::Indexed => "indexed",
            Workload::Paraseq => "paraseq",
        }
    );
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
