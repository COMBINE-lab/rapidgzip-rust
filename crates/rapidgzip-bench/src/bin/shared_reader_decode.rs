//! Concurrent-reader driver for shared-pool versus private-pool measurements.

use paraseq::{Record, fastq};
use rapidgzip_core::{Decoder, DecoderPool, DecoderPoolStats};
use std::env;
use std::hint::black_box;
use std::io::Read;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Barrier};
use std::thread;
use std::time::{Duration, Instant};

#[derive(Clone, Copy)]
enum Allocation {
    Private,
    Shared,
    Growing,
}

#[derive(Clone, Copy)]
enum Workload {
    Read,
    Paraseq,
}

struct Arguments {
    input: PathBuf,
    decoders: usize,
    global_workers: usize,
    decoder_workers: usize,
    read_buffer_bytes: usize,
    expected_decoded_bytes: u64,
    expected_members: u64,
    iterations: usize,
    allocation: Allocation,
    workload: Workload,
}

#[derive(Default)]
struct PoolPeaks {
    samples: u64,
    busy_worker_samples: u64,
    active_worker_samples: u64,
    busy_workers: usize,
    active_workers: usize,
    spawned_workers: usize,
    auxiliary_threads: usize,
    queued_tasks: usize,
    attached_decoders: usize,
    waiting_decoders: usize,
}

impl PoolPeaks {
    fn observe(&mut self, stats: DecoderPoolStats) {
        self.samples += 1;
        self.busy_worker_samples += stats.busy_workers as u64;
        self.active_worker_samples += stats.active_workers as u64;
        self.busy_workers = self.busy_workers.max(stats.busy_workers);
        self.active_workers = self.active_workers.max(stats.active_workers);
        self.spawned_workers = self.spawned_workers.max(stats.spawned_workers);
        self.auxiliary_threads = self.auxiliary_threads.max(stats.auxiliary_threads);
        self.queued_tasks = self.queued_tasks.max(stats.queued_tasks);
        self.attached_decoders = self.attached_decoders.max(stats.attached_decoders);
        self.waiting_decoders = self.waiting_decoders.max(stats.waiting_decoders);
    }
}

fn usage() -> &'static str {
    "usage: shared_reader_decode INPUT.gz DECODERS GLOBAL_WORKERS \
     DECODER_WORKERS READ_BUFFER_BYTES EXPECTED_DECODED_BYTES EXPECTED_MEMBERS ITERATIONS \
     private|shared|growing read|paraseq"
}

fn parse_nonzero(value: Option<String>, name: &str) -> Result<usize, String> {
    let value = value.ok_or_else(|| format!("missing {name}\n{}", usage()))?;
    value
        .parse::<usize>()
        .map_err(|error| format!("invalid {name} {value:?}: {error}"))
        .and_then(|parsed| {
            (parsed != 0)
                .then_some(parsed)
                .ok_or_else(|| format!("{name} must be nonzero"))
        })
}

fn parse_arguments() -> Result<Arguments, String> {
    let mut values = env::args().skip(1);
    let input = values
        .next()
        .filter(|value| !matches!(value.as_str(), "-h" | "--help"))
        .ok_or_else(|| usage().to_owned())?;
    let decoders = parse_nonzero(values.next(), "decoder count")?;
    let global_workers = parse_nonzero(values.next(), "global worker count")?;
    let decoder_workers = parse_nonzero(values.next(), "per-decoder worker count")?;
    let read_buffer_bytes = parse_nonzero(values.next(), "read-buffer size")?;
    let expected_decoded_bytes = parse_nonzero(values.next(), "expected decoded bytes")? as u64;
    let expected_members = parse_nonzero(values.next(), "expected gzip members")? as u64;
    let iterations = parse_nonzero(values.next(), "iteration count")?;
    let allocation = match values.next().as_deref() {
        Some("private") => Allocation::Private,
        Some("shared") => Allocation::Shared,
        Some("growing") => Allocation::Growing,
        Some(value) => return Err(format!("invalid allocation {value:?}\n{}", usage())),
        None => return Err(format!("missing allocation\n{}", usage())),
    };
    let workload = match values.next().as_deref() {
        Some("read") => Workload::Read,
        Some("paraseq") => Workload::Paraseq,
        Some(value) => return Err(format!("invalid workload {value:?}\n{}", usage())),
        None => return Err(format!("missing workload\n{}", usage())),
    };
    if values.next().is_some() {
        return Err(format!("too many arguments\n{}", usage()));
    }
    Ok(Arguments {
        input: PathBuf::from(input),
        decoders,
        global_workers,
        decoder_workers,
        read_buffer_bytes,
        expected_decoded_bytes,
        expected_members,
        iterations,
        allocation,
        workload,
    })
}

fn consume_read(
    decoder: &Decoder,
    input: &PathBuf,
    buffer_bytes: usize,
    requested_workers: Option<usize>,
) -> Result<(u64, u64), String> {
    let mut reader = decoder.open(input).map_err(|error| error.to_string())?;
    if let Some(workers) = requested_workers {
        reader
            .request_workers(workers)
            .map_err(|error| error.to_string())?;
    }
    let mut buffer = vec![0_u8; buffer_bytes];
    let mut consumed = 0_u64;
    loop {
        let count = reader
            .read(&mut buffer)
            .map_err(|error| error.to_string())?;
        if count == 0 {
            break;
        }
        black_box(&buffer[..count]);
        consumed = consumed
            .checked_add(count as u64)
            .ok_or_else(|| "decoded byte count overflow".to_owned())?;
    }
    let report = reader.finish().map_err(|error| error.to_string())?;
    if report.decompressed_bytes != consumed {
        return Err("reader and report decoded-byte counts differ".to_owned());
    }
    Ok((consumed, report.member_count))
}

fn consume_paraseq(
    decoder: &Decoder,
    input: &PathBuf,
    requested_workers: Option<usize>,
) -> Result<(u64, u64), String> {
    let decoded = decoder.open(input).map_err(|error| error.to_string())?;
    if let Some(workers) = requested_workers {
        decoded
            .request_workers(workers)
            .map_err(|error| error.to_string())?;
    }
    let handle = decoded.handle();
    let mut reader = fastq::Reader::new(decoded);
    let mut records = reader.new_record_set();
    while records
        .fill(&mut reader)
        .map_err(|error| error.to_string())?
    {
        for record in records.iter() {
            let record = record.map_err(|error| error.to_string())?;
            black_box(record.id());
            black_box(record.seq_raw());
            black_box(record.qual());
        }
    }
    let stats = handle.stats();
    Ok((stats.decompressed_bytes, stats.member_count))
}

fn run(arguments: Arguments) -> Result<(), String> {
    let pool = match arguments.allocation {
        Allocation::Private => None,
        Allocation::Shared | Allocation::Growing => Some(
            DecoderPool::builder()
                .workers(arguments.global_workers)
                .initial_worker_limit(1)
                .build()
                .map_err(|error| error.to_string())?,
        ),
    };
    let mut builder = Decoder::builder().decoder_threads(arguments.decoder_workers);
    if let Some(pool) = &pool {
        builder = builder.decoder_pool(pool.clone());
    }
    let decoder = builder.build().map_err(|error| error.to_string())?;
    let mut total_bytes = 0_u64;
    let mut observed_members = None;
    let sampler_stop = Arc::new(AtomicBool::new(false));
    let sampler = pool.as_ref().map(|pool| {
        let pool = pool.clone();
        let sampler_stop = Arc::clone(&sampler_stop);
        thread::spawn(move || {
            let mut peaks = PoolPeaks::default();
            while !sampler_stop.load(Ordering::Relaxed) {
                peaks.observe(pool.stats());
                // One-kilohertz telemetry catches short saturation intervals.
                // Sampling is intentionally off the timed join path so the
                // polling interval cannot become decoder completion latency.
                thread::sleep(Duration::from_millis(1));
            }
            peaks.observe(pool.stats());
            peaks
        })
    });
    let total_started = Instant::now();

    for _ in 0..arguments.iterations {
        if let Some(pool) = &pool {
            pool.set_worker_limit(1)
                .map_err(|error| error.to_string())?;
        }
        let start = Arc::new(Barrier::new(arguments.decoders + 1));
        let mut workers = Vec::with_capacity(arguments.decoders);
        for _ in 0..arguments.decoders {
            let start = Arc::clone(&start);
            let decoder = decoder.clone();
            let input = arguments.input.clone();
            let workload = arguments.workload;
            let buffer_bytes = arguments.read_buffer_bytes;
            let requested_workers = matches!(arguments.allocation, Allocation::Growing)
                .then_some(arguments.decoder_workers);
            workers.push(thread::spawn(move || {
                start.wait();
                match workload {
                    Workload::Read => {
                        consume_read(&decoder, &input, buffer_bytes, requested_workers)
                    }
                    Workload::Paraseq => consume_paraseq(&decoder, &input, requested_workers),
                }
            }));
        }
        start.wait();
        if let Some(pool) = &pool {
            let attach_deadline = Instant::now() + Duration::from_secs(5);
            while pool.stats().attached_decoders < arguments.decoders {
                if Instant::now() >= attach_deadline {
                    return Err("timed out waiting for shared decoders to attach".to_owned());
                }
                thread::yield_now();
            }
            pool.set_worker_limit(arguments.global_workers)
                .map_err(|error| error.to_string())?;
        }
        for worker in workers {
            let (decoded_bytes, members) = worker
                .join()
                .map_err(|_| "reader thread panicked".to_owned())??;
            if decoded_bytes != arguments.expected_decoded_bytes {
                return Err(format!(
                    "decoded {decoded_bytes} bytes; expected {}",
                    arguments.expected_decoded_bytes
                ));
            }
            if members != arguments.expected_members {
                return Err(format!(
                    "decoded {members} gzip members; expected {}",
                    arguments.expected_members
                ));
            }
            if observed_members
                .replace(members)
                .is_some_and(|expected| expected != members)
            {
                return Err("gzip member count changed between readers".to_owned());
            }
            total_bytes = total_bytes
                .checked_add(decoded_bytes)
                .ok_or_else(|| "aggregate decoded byte count overflow".to_owned())?;
        }
    }

    let elapsed = total_started.elapsed().as_secs_f64();
    sampler_stop.store(true, Ordering::Relaxed);
    let peaks = sampler
        .map(|sampler| {
            sampler
                .join()
                .map_err(|_| "pool sampler thread panicked".to_owned())
        })
        .transpose()?
        .unwrap_or_default();
    println!("decoded_bytes\t{total_bytes}");
    println!(
        "member_count_per_decoder\t{}",
        observed_members.unwrap_or(0)
    );
    println!("elapsed_seconds\t{elapsed:.9}");
    println!(
        "decoded_mib_per_second\t{:.3}",
        total_bytes as f64 / (1024.0 * 1024.0) / elapsed
    );
    println!("peak_pool_busy_workers\t{}", peaks.busy_workers);
    println!("peak_pool_active_workers\t{}", peaks.active_workers);
    println!("peak_pool_spawned_workers\t{}", peaks.spawned_workers);
    println!("peak_pool_auxiliary_threads\t{}", peaks.auxiliary_threads);
    println!("peak_pool_queued_tasks\t{}", peaks.queued_tasks);
    println!("peak_pool_attached_decoders\t{}", peaks.attached_decoders);
    println!("peak_pool_waiting_decoders\t{}", peaks.waiting_decoders);
    println!("pool_samples\t{}", peaks.samples);
    if peaks.samples != 0 {
        println!(
            "mean_pool_busy_workers\t{:.3}",
            peaks.busy_worker_samples as f64 / peaks.samples as f64
        );
        println!(
            "mean_pool_active_workers\t{:.3}",
            peaks.active_worker_samples as f64 / peaks.samples as f64
        );
    }
    Ok(())
}

fn main() -> ExitCode {
    match parse_arguments().and_then(run) {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("{error}");
            ExitCode::FAILURE
        }
    }
}
