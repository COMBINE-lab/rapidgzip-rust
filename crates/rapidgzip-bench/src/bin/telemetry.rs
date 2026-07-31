//! Samples decoder telemetry while draining a real compressed file.

use rapidgzip_core::{Decoder, DecoderPressure, DecoderStats};
use std::env;
use std::io::Read;
use std::process::ExitCode;
use std::thread;
use std::time::{Duration, Instant};

#[derive(Default)]
struct Observed {
    samples: usize,
    maximum_active: usize,
    maximum_busy: usize,
    maximum_spawned: usize,
    maximum_auxiliary: usize,
    consumer_bound_samples: usize,
    decoder_bound_samples: usize,
    minimum_spawned_while_consumer_bound: usize,
    maximum_busy_while_consumer_bound: usize,
    maximum_active_while_consumer_bound: usize,
    consumer_bound_started: Option<Instant>,
    retired_after_backpressure: Option<Duration>,
}

impl Observed {
    fn observe(&mut self, stats: DecoderStats) {
        self.samples += 1;
        self.maximum_active = self.maximum_active.max(stats.active_workers);
        self.maximum_busy = self.maximum_busy.max(stats.busy_workers);
        self.maximum_spawned = self.maximum_spawned.max(stats.spawned_workers);
        self.maximum_auxiliary = self.maximum_auxiliary.max(stats.auxiliary_threads);
        match stats.pressure {
            DecoderPressure::ConsumerBound { .. } => {
                let now = Instant::now();
                let started = *self.consumer_bound_started.get_or_insert(now);
                self.consumer_bound_samples += 1;
                self.minimum_spawned_while_consumer_bound = self
                    .minimum_spawned_while_consumer_bound
                    .min(stats.spawned_workers);
                self.maximum_busy_while_consumer_bound = self
                    .maximum_busy_while_consumer_bound
                    .max(stats.busy_workers);
                self.maximum_active_while_consumer_bound = self
                    .maximum_active_while_consumer_bound
                    .max(stats.active_workers);
                if stats.spawned_workers <= 1 && self.retired_after_backpressure.is_none() {
                    self.retired_after_backpressure = Some(now.saturating_duration_since(started));
                }
            }
            DecoderPressure::DecoderBound { .. } => self.decoder_bound_samples += 1,
            _ => {}
        }
    }
}

fn parse_usize(value: Option<String>, name: &str) -> Result<Option<usize>, String> {
    value
        .map(|value| {
            value
                .parse::<usize>()
                .map_err(|error| format!("invalid {name} {value:?}: {error}"))
        })
        .transpose()
}

fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut arguments = env::args().skip(1);
    let Some(path) = arguments.next() else {
        return Err(
            "usage: telemetry INPUT.gz [CONFIGURED_WORKERS [RUNTIME_LIMIT [READ_DELAY_MICROS]]]"
                .into(),
        );
    };
    let configured_workers = parse_usize(arguments.next(), "configured worker count")?;
    let runtime_limit = parse_usize(arguments.next(), "runtime worker limit")?;
    let read_delay_micros =
        parse_usize(arguments.next(), "delay between 1 MiB consumer reads")?.unwrap_or(0);
    if arguments.next().is_some() {
        return Err("too many arguments".into());
    }

    let mut builder = Decoder::builder();
    if let Some(workers) = configured_workers {
        builder = builder.decoder_threads(workers);
    }
    let decoder = builder.build()?;
    let reader = decoder.open(&path)?;
    let handle = reader.handle();
    if let Some(workers) = runtime_limit {
        handle.set_worker_limit(workers)?;
    }

    let started = Instant::now();
    let consumer = thread::spawn(
        move || -> Result<_, Box<dyn std::error::Error + Send + Sync>> {
            let mut reader = reader;
            let mut buffer = vec![0_u8; 1024 * 1024];
            loop {
                let count = reader.read(&mut buffer)?;
                if count == 0 {
                    break;
                }
                if read_delay_micros != 0 {
                    thread::sleep(Duration::from_micros(read_delay_micros as u64));
                }
            }
            Ok(reader.finish()?)
        },
    );
    let mut observed = Observed {
        minimum_spawned_while_consumer_bound: usize::MAX,
        ..Observed::default()
    };
    while !consumer.is_finished() {
        observed.observe(handle.stats());
        thread::sleep(Duration::from_millis(2));
    }
    let report = consumer
        .join()
        .map_err(|_| "telemetry consumer thread panicked")??;
    let elapsed = started.elapsed();
    let final_stats = handle.stats();
    observed.observe(final_stats);
    let throughput = report.decompressed_bytes as f64 / elapsed.as_secs_f64();

    println!("path\t{:?}", final_stats.path);
    println!("elapsed_seconds\t{:.6}", elapsed.as_secs_f64());
    println!("decoded_bytes\t{}", report.decompressed_bytes);
    println!("decoded_mib_per_second\t{:.3}", throughput / 1_048_576.0);
    println!("configured_workers\t{}", final_stats.configured_workers);
    println!("runtime_worker_limit\t{}", final_stats.worker_limit);
    println!("maximum_active_workers\t{}", observed.maximum_active);
    println!("maximum_busy_workers\t{}", observed.maximum_busy);
    println!("maximum_spawned_workers\t{}", observed.maximum_spawned);
    println!("maximum_auxiliary_threads\t{}", observed.maximum_auxiliary);
    println!("best_workers\t{:?}", final_stats.best_workers);
    println!("samples\t{}", observed.samples);
    println!(
        "consumer_bound_samples\t{}",
        observed.consumer_bound_samples
    );
    if observed.consumer_bound_samples != 0 {
        println!(
            "minimum_spawned_while_consumer_bound\t{}",
            observed.minimum_spawned_while_consumer_bound
        );
        println!(
            "maximum_busy_while_consumer_bound\t{}",
            observed.maximum_busy_while_consumer_bound
        );
        println!(
            "maximum_active_while_consumer_bound\t{}",
            observed.maximum_active_while_consumer_bound
        );
        if let Some(elapsed) = observed.retired_after_backpressure {
            println!(
                "retired_after_backpressure_milliseconds\t{:.3}",
                elapsed.as_secs_f64() * 1_000.0
            );
        }
    }
    println!("decoder_bound_samples\t{}", observed.decoder_bound_samples);
    Ok(())
}

fn main() -> ExitCode {
    match run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("telemetry: {error}");
            ExitCode::FAILURE
        }
    }
}
