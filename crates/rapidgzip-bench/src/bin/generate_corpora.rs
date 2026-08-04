//! Generates deterministic, self-verified benchmark corpora.

use rapidgzip_bench::corpus::{
    BGZF_PAYLOAD_BYTES, bgzf, deflate_with_level, fastq_like_bytes, gzip_member, gzip_members,
    pseudo_random_bytes, stored_gzip_member,
};
use rapidgzip_core::{Decoder, DecoderPath, Format};
use std::env;
use std::error::Error;
use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

const DEFAULT_DECODED_MIB: usize = 256;
const DEFAULT_SEED: u64 = 1;
const DENSE_MEMBER_BYTES: usize = 256 * 1024;

struct Arguments {
    output: PathBuf,
    decoded_mib: usize,
    seed: u64,
}

struct ManifestRow<'a> {
    corpus: &'a str,
    path: &'a str,
    format: &'a str,
    compressed_bytes: usize,
    decoded_bytes: usize,
    member_count: u64,
    seed: u64,
    parameters: &'a str,
    cross_tool: bool,
}

struct ComparingWriter<'a> {
    expected: &'a [u8],
    offset: usize,
}

impl Write for ComparingWriter<'_> {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let end = self
            .offset
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("decoded length overflow"))?;
        if self.expected.get(self.offset..end) != Some(bytes) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "decoded bytes differ from generated payload",
            ));
        }
        self.offset = end;
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

fn usage() -> &'static str {
    "usage: generate_corpora [--output DIR] [--decoded-mib N] [--seed N]"
}

fn parse_arguments() -> Result<Arguments, String> {
    let mut output = PathBuf::from("target/bench-corpora");
    let mut decoded_mib = DEFAULT_DECODED_MIB;
    let mut seed = DEFAULT_SEED;
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        let value = match argument.as_str() {
            "--help" | "-h" => {
                println!("{}", usage());
                std::process::exit(0);
            }
            "--output" | "--decoded-mib" | "--seed" => arguments
                .next()
                .ok_or_else(|| format!("{argument} requires a value"))?,
            _ => return Err(format!("unknown argument {argument:?}\n{}", usage())),
        };
        match argument.as_str() {
            "--output" => output = PathBuf::from(value),
            "--decoded-mib" => {
                decoded_mib = value
                    .parse()
                    .map_err(|_| format!("invalid --decoded-mib value {value:?}"))?;
            }
            "--seed" => {
                seed = value
                    .parse()
                    .map_err(|_| format!("invalid --seed value {value:?}"))?;
            }
            _ => unreachable!(),
        }
    }
    if decoded_mib == 0 {
        return Err("--decoded-mib must be nonzero".to_owned());
    }
    Ok(Arguments {
        output,
        decoded_mib,
        seed,
    })
}

fn decoder(format: Format) -> Result<Decoder, Box<dyn Error>> {
    Ok(Decoder::builder()
        .format(format)
        .decoder_threads(4)
        .build()?)
}

fn verify(
    encoded: &[u8],
    expected: &[u8],
    format: Format,
    expected_members: u64,
) -> Result<(), Box<dyn Error>> {
    let mut output = ComparingWriter {
        expected,
        offset: 0,
    };
    let report = decoder(format)?.decode(encoded, &mut output)?;
    if output.offset != expected.len() {
        return Err(format!(
            "decoder emitted {} of {} expected bytes",
            output.offset,
            expected.len()
        )
        .into());
    }
    if report.decompressed_bytes != expected.len() as u64 {
        return Err("decoder report has the wrong decoded size".into());
    }
    if report.member_count != expected_members {
        return Err(format!(
            "decoder found {} framing units; expected {expected_members}",
            report.member_count
        )
        .into());
    }
    Ok(())
}

fn verify_bgzf_route(encoded: &[u8]) -> Result<(), Box<dyn Error>> {
    let source: Arc<[u8]> = Arc::from(encoded);
    let reader = decoder(Format::Gzip)?.reader(source)?;
    let handle = reader.handle();
    reader.finish()?;
    let path = handle.stats().path;
    if path != DecoderPath::Bgzf {
        return Err(format!("BGZF corpus selected {path:?}, not Bgzf").into());
    }
    Ok(())
}

fn write_row(
    directory: &Path,
    manifest: &mut String,
    encoded: Vec<u8>,
    decoded: &[u8],
    row: ManifestRow<'_>,
    selected_format: Format,
) -> Result<(), Box<dyn Error>> {
    verify(&encoded, decoded, selected_format, row.member_count)?;
    let path = directory.join(row.path);
    let mut output = File::create(&path)?;
    output.write_all(&encoded)?;
    output.sync_all()?;
    let row = ManifestRow {
        compressed_bytes: encoded.len(),
        ..row
    };
    manifest.push_str(&format!(
        "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
        row.corpus,
        row.path,
        row.format,
        row.compressed_bytes,
        row.decoded_bytes,
        row.member_count,
        row.seed,
        row.parameters,
        u8::from(row.cross_tool)
    ));
    Ok(())
}

fn member_count(length: usize, chunk: usize) -> u64 {
    length.div_ceil(chunk) as u64
}

#[allow(clippy::too_many_arguments)]
fn emit(
    directory: &Path,
    manifest: &mut String,
    encoded: Vec<u8>,
    decoded: &[u8],
    corpus: &str,
    path: &str,
    format_name: &str,
    format: Format,
    members: u64,
    seed: u64,
    parameters: &str,
    cross_tool: bool,
    require_bgzf_route: bool,
) -> Result<(), Box<dyn Error>> {
    if require_bgzf_route {
        verify_bgzf_route(&encoded)?;
    }
    write_row(
        directory,
        manifest,
        encoded,
        decoded,
        ManifestRow {
            corpus,
            path,
            format: format_name,
            compressed_bytes: 0,
            decoded_bytes: decoded.len(),
            member_count: members,
            seed,
            parameters,
            cross_tool,
        },
        format,
    )
}

fn run(arguments: Arguments) -> Result<(), Box<dyn Error>> {
    let decoded_bytes = arguments
        .decoded_mib
        .checked_mul(1024 * 1024)
        .ok_or("decoded corpus size overflows usize")?;
    if u32::try_from(decoded_bytes).is_err() {
        return Err("--decoded-mib must describe less than 4 GiB".into());
    }
    fs::create_dir_all(&arguments.output)?;

    let fastq = fastq_like_bytes(decoded_bytes);
    let mut manifest = String::from(
        "corpus\tpath\tformat\tcompressed_bytes\tdecoded_bytes\tmember_count\tseed\tparameters\tcross_tool\n",
    );

    let sparse_member_bytes = decoded_bytes.div_ceil(4);
    emit(
        &arguments.output,
        &mut manifest,
        gzip_member(&fastq, 6)?,
        &fastq,
        "fastq-single",
        "fastq-single.gz",
        "gzip",
        Format::Gzip,
        1,
        arguments.seed,
        "payload=fastq;level=6;members=1;mtime=0;os=255",
        true,
        false,
    )?;
    emit(
        &arguments.output,
        &mut manifest,
        gzip_members(&fastq, sparse_member_bytes, 6)?,
        &fastq,
        "fastq-sparse-members",
        "fastq-sparse-members.gz",
        "gzip",
        Format::Gzip,
        member_count(decoded_bytes, sparse_member_bytes),
        arguments.seed,
        "payload=fastq;level=6;members=4;mtime=0;os=255",
        true,
        false,
    )?;
    emit(
        &arguments.output,
        &mut manifest,
        gzip_members(&fastq, DENSE_MEMBER_BYTES, 6)?,
        &fastq,
        "fastq-dense-members",
        "fastq-dense-members.gz",
        "gzip",
        Format::Gzip,
        member_count(decoded_bytes, DENSE_MEMBER_BYTES),
        arguments.seed,
        "payload=fastq;level=6;member_bytes=262144;mtime=0;os=255",
        true,
        false,
    )?;
    emit(
        &arguments.output,
        &mut manifest,
        bgzf(&fastq, BGZF_PAYLOAD_BYTES, 6)?,
        &fastq,
        "fastq-bgzf",
        "fastq.bgzf",
        "gzip",
        Format::Gzip,
        member_count(decoded_bytes, BGZF_PAYLOAD_BYTES) + 1,
        arguments.seed,
        "payload=fastq;level=6;bgzf_bytes=61440;canonical_eof=1",
        true,
        true,
    )?;
    emit(
        &arguments.output,
        &mut manifest,
        stored_gzip_member(&fastq),
        &fastq,
        "stored",
        "stored.gz",
        "gzip",
        Format::Gzip,
        1,
        arguments.seed,
        "payload=fastq;deflate=stored;mtime=0;os=255",
        true,
        false,
    )?;
    emit(
        &arguments.output,
        &mut manifest,
        deflate_with_level(&fastq, 15, 6)?,
        &fastq,
        "fastq-zlib",
        "fastq.zlib",
        "zlib",
        Format::Zlib,
        1,
        arguments.seed,
        "payload=fastq;level=6;window_bits=15",
        false,
        false,
    )?;
    emit(
        &arguments.output,
        &mut manifest,
        deflate_with_level(&fastq, -15, 6)?,
        &fastq,
        "fastq-deflate",
        "fastq.deflate",
        "raw-deflate",
        Format::RawDeflate,
        1,
        arguments.seed,
        "payload=fastq;level=6;window_bits=-15",
        false,
        false,
    )?;
    drop(fastq);
    let random = pseudo_random_bytes(decoded_bytes, arguments.seed);
    emit(
        &arguments.output,
        &mut manifest,
        gzip_member(&random, 1)?,
        &random,
        "low-compression",
        "low-compression.gz",
        "gzip",
        Format::Gzip,
        1,
        arguments.seed,
        "payload=xorshift64;level=1;mtime=0;os=255",
        true,
        false,
    )?;
    let manifest_path = arguments.output.join("manifest.tsv");
    let mut output = File::create(&manifest_path)?;
    output.write_all(manifest.as_bytes())?;
    output.sync_all()?;
    println!("generated and verified {}", manifest_path.display());
    Ok(())
}

fn main() {
    let result = parse_arguments()
        .map_err(|error| error.into())
        .and_then(run);
    if let Err(error) = result {
        eprintln!("generate_corpora: {error}");
        std::process::exit(2);
    }
}
