//! Indexed-versus-ordinary full-stream throughput on a caller-supplied gzip.

use rapidgzip_core::{Decoder, IndexOptions, ReadAt, WindowStorage};
use std::env;
use std::fs::File;
use std::io;
use std::num::NonZeroU64;
use std::path::PathBuf;
use std::time::{Duration, Instant};

fn median(mut samples: Vec<Duration>) -> Duration {
    samples.sort_unstable();
    samples[samples.len() / 2]
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut arguments = env::args_os().skip(1);
    let path = PathBuf::from(arguments.next().ok_or(
        "usage: indexed <archive.fastq.gz> [comma-separated-thread-counts] [repetitions] [raw|zlib] [checkpoint-MiB] [file|memory]",
    )?);
    let workers = arguments
        .next()
        .map(|value| {
            value
                .to_string_lossy()
                .split(',')
                .map(str::parse)
                .collect::<Result<Vec<usize>, _>>()
        })
        .transpose()?
        .unwrap_or_else(|| vec![1, 2, 4, 8, 16]);
    let repetitions = arguments
        .next()
        .map(|value| value.to_string_lossy().parse())
        .transpose()?
        .unwrap_or(5_usize)
        .max(1);
    let window_storage = match arguments
        .next()
        .as_deref()
        .map(|value| value.to_string_lossy())
        .as_deref()
    {
        None | Some("zlib") => WindowStorage::Zlib,
        Some("raw") => WindowStorage::Raw,
        Some(_) => return Err("window storage must be `raw` or `zlib`".into()),
    };
    let spacing_mib = arguments
        .next()
        .map(|value| value.to_string_lossy().parse::<u64>())
        .transpose()?
        .unwrap_or(1)
        .max(1);
    let source: Box<dyn ReadAt> = match arguments
        .next()
        .as_deref()
        .map(|value| value.to_string_lossy())
        .as_deref()
    {
        None | Some("file") => Box::new(File::open(&path)?),
        Some("memory") => Box::new(std::fs::read(&path)?),
        Some(_) => return Err("input mode must be `file` or `memory`".into()),
    };

    let builder = Decoder::builder().decoder_threads(1).build()?;
    let options = IndexOptions {
        checkpoint_spacing: NonZeroU64::new(spacing_mib * 1024 * 1024)
            .expect("positive MiB spacing is nonzero"),
        window_storage,
    };
    let started = Instant::now();
    let indexed = builder.decode_with_index(&source, &mut io::sink(), options)?;
    let build_elapsed = started.elapsed();
    eprintln!(
        "index\tcheckpoints={}\tdecoded_bytes={}\tbuild_seconds={:.6}\twindow_storage={window_storage:?}",
        indexed.index.checkpoint_count(),
        indexed.decode.decompressed_bytes,
        build_elapsed.as_secs_f64()
    );
    println!("path\tworkers\tmedian_seconds\tMiB_per_second");

    for workers in workers {
        let decoder = Decoder::builder().decoder_threads(workers).build()?;
        decoder.decode_from_index(&source, &mut io::sink(), &indexed.index)?;
        decoder.decode(&source, &mut io::sink())?;
        for (name, from_index) in [("indexed", true), ("ordinary", false)] {
            let mut samples = Vec::with_capacity(repetitions);
            let mut decoded_bytes = 0_u64;
            for _ in 0..repetitions {
                let started = Instant::now();
                let report = if from_index {
                    decoder.decode_from_index(&source, &mut io::sink(), &indexed.index)?
                } else {
                    decoder.decode(&source, &mut io::sink())?
                };
                samples.push(started.elapsed());
                decoded_bytes = report.decompressed_bytes;
            }
            let elapsed = median(samples);
            let throughput = decoded_bytes as f64 / elapsed.as_secs_f64() / (1024.0 * 1024.0);
            println!(
                "{name}\t{workers}\t{:.6}\t{throughput:.1}",
                elapsed.as_secs_f64()
            );
        }
    }
    Ok(())
}
