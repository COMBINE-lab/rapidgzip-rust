//! Validates raw fair-benchmark rows and writes deterministic summaries.

use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::error::Error;
use std::fs;
use std::path::{Path, PathBuf};

const RAW_HEADER: &str = "timestamp_utc\tcorpus\tmode\ttool\tbackend\tthreads\trepetition\torder\twall_seconds\tuser_seconds\tsystem_seconds\tmax_rss_kib\tdecoded_bytes\tdecoded_mib_per_second\texit_status\tstatus\tstdout_log\tstderr_log";

#[derive(Default)]
struct Arguments {
    input: Option<PathBuf>,
    summary_tsv: Option<PathBuf>,
    summary_markdown: Option<PathBuf>,
    environment: Option<PathBuf>,
    corpora: Option<PathBuf>,
    commands: Option<PathBuf>,
    parity: Option<PathBuf>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Key {
    corpus: String,
    mode: String,
    tool: String,
    backend: String,
    threads: usize,
}

#[derive(Default)]
struct Group {
    attempts: usize,
    failures: usize,
    decoded_bytes: Option<u64>,
    wall: Vec<f64>,
    user: Vec<f64>,
    system: Vec<f64>,
    rss: Vec<f64>,
    throughput: Vec<f64>,
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct PairIdentity {
    corpus: String,
    mode: String,
    threads: usize,
    repetition: usize,
}

struct PairObservation {
    wall: f64,
    rss: f64,
    throughput: f64,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct ComparisonKey {
    corpus: String,
    mode: String,
    threads: usize,
}

#[derive(Default)]
struct Comparison {
    throughput_percent: Vec<f64>,
    wall_percent: Vec<f64>,
    rss_percent: Vec<f64>,
}

struct Outputs {
    tsv: String,
    markdown: String,
}

fn usage() -> &'static str {
    "usage: summarize_results --input raw.tsv --summary-tsv summary.tsv --summary-markdown \
     SUMMARY.md [--environment environment.tsv] [--corpora corpora.tsv] \
     [--commands commands.tsv] [--parity parity.tsv]"
}

fn parse_arguments() -> Result<Arguments, String> {
    let mut parsed = Arguments::default();
    let mut arguments = env::args().skip(1);
    while let Some(argument) = arguments.next() {
        if matches!(argument.as_str(), "--help" | "-h") {
            println!("{}", usage());
            std::process::exit(0);
        }
        let value = arguments
            .next()
            .ok_or_else(|| format!("{argument} requires a path"))?;
        let destination = match argument.as_str() {
            "--input" => &mut parsed.input,
            "--summary-tsv" => &mut parsed.summary_tsv,
            "--summary-markdown" => &mut parsed.summary_markdown,
            "--environment" => &mut parsed.environment,
            "--corpora" => &mut parsed.corpora,
            "--commands" => &mut parsed.commands,
            "--parity" => &mut parsed.parity,
            _ => return Err(format!("unknown argument {argument:?}\n{}", usage())),
        };
        *destination = Some(PathBuf::from(value));
    }
    if parsed.input.is_none() || parsed.summary_tsv.is_none() || parsed.summary_markdown.is_none() {
        return Err(usage().to_owned());
    }
    Ok(parsed)
}

fn field<'a>(fields: &'a [&str], index: usize, name: &str) -> Result<&'a str, String> {
    fields
        .get(index)
        .copied()
        .ok_or_else(|| format!("missing {name}"))
}

fn parse_usize(fields: &[&str], index: usize, name: &str) -> Result<usize, String> {
    field(fields, index, name)?
        .parse()
        .map_err(|_| format!("invalid {name}"))
}

fn parse_u64(fields: &[&str], index: usize, name: &str) -> Result<u64, String> {
    field(fields, index, name)?
        .parse()
        .map_err(|_| format!("invalid {name}"))
}

fn parse_metric(fields: &[&str], index: usize, name: &str) -> Result<f64, String> {
    let value: f64 = field(fields, index, name)?
        .parse()
        .map_err(|_| format!("invalid {name}"))?;
    if !value.is_finite() || value < 0.0 {
        return Err(format!("{name} must be finite and nonnegative"));
    }
    Ok(value)
}

fn median(values: &mut [f64]) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let middle = values.len() / 2;
    Some(if values.len().is_multiple_of(2) {
        (values[middle - 1] + values[middle]) / 2.0
    } else {
        values[middle]
    })
}

fn metric(value: Option<f64>) -> String {
    value.map_or_else(String::new, |value| format!("{value:.6}"))
}

fn percent_delta(candidate: f64, baseline: f64) -> Option<f64> {
    (baseline > 0.0).then_some((candidate / baseline - 1.0) * 100.0)
}

fn markdown_cell(value: &str) -> String {
    value.replace('|', "\\|").replace(['\r', '\n'], " ")
}

fn summarize(raw: &str, result_directory: &str) -> Result<Outputs, String> {
    let mut lines = raw.lines();
    let header = lines.next().ok_or("raw input is empty")?;
    if header != RAW_HEADER {
        return Err(format!("unexpected raw header: {header:?}"));
    }
    let mut groups: BTreeMap<Key, Group> = BTreeMap::new();
    let mut attempts = BTreeSet::new();
    let mut corpus_sizes = BTreeMap::new();
    let mut paired_observations: BTreeMap<PairIdentity, BTreeMap<String, PairObservation>> =
        BTreeMap::new();
    for (offset, line) in lines.enumerate() {
        let line_number = offset + 2;
        if line.is_empty() {
            return Err(format!("line {line_number}: empty row"));
        }
        let fields: Vec<_> = line.split('\t').collect();
        if fields.len() != 18 {
            return Err(format!(
                "line {line_number}: expected 18 fields, found {}",
                fields.len()
            ));
        }
        if fields.iter().any(|field| field.contains(['\r', '\n'])) {
            return Err(format!("line {line_number}: multiline field"));
        }
        let key = Key {
            corpus: field(&fields, 1, "corpus")?.to_owned(),
            mode: field(&fields, 2, "mode")?.to_owned(),
            tool: field(&fields, 3, "tool")?.to_owned(),
            backend: field(&fields, 4, "backend")?.to_owned(),
            threads: parse_usize(&fields, 5, "threads")?,
        };
        if field(&fields, 0, "timestamp_utc")?.is_empty()
            || key.corpus.is_empty()
            || key.mode.is_empty()
            || key.tool.is_empty()
            || key.backend.is_empty()
        {
            return Err(format!("line {line_number}: empty identity field"));
        }
        let repetition = parse_usize(&fields, 6, "repetition")?;
        let order = parse_usize(&fields, 7, "order")?;
        if key.threads == 0 || repetition == 0 || order == 0 {
            return Err(format!(
                "line {line_number}: threads, repetition, and order must be nonzero"
            ));
        }
        let identity = (
            key.corpus.clone(),
            key.mode.clone(),
            key.tool.clone(),
            key.threads,
            repetition,
        );
        if !attempts.insert(identity) {
            return Err(format!("line {line_number}: duplicate benchmark attempt"));
        }

        let exit_status = parse_usize(&fields, 14, "exit_status")?;
        let status = field(&fields, 15, "status")?;
        let success = status == "success";
        if success != (exit_status == 0) {
            return Err(format!(
                "line {line_number}: success status and exit status disagree"
            ));
        }
        let group = groups.entry(key.clone()).or_default();
        group.attempts += 1;
        if !success {
            group.failures += 1;
            continue;
        }
        let decoded_bytes = parse_u64(&fields, 12, "decoded_bytes")?;
        if let Some(previous) = corpus_sizes.insert(key.corpus.clone(), decoded_bytes) {
            if previous != decoded_bytes {
                return Err(format!(
                    "line {line_number}: decoded size disagrees for corpus {}",
                    key.corpus
                ));
            }
        }
        if group
            .decoded_bytes
            .replace(decoded_bytes)
            .is_some_and(|previous| previous != decoded_bytes)
        {
            return Err(format!("line {line_number}: group decoded sizes disagree"));
        }
        let wall = parse_metric(&fields, 8, "wall_seconds")?;
        let user = parse_metric(&fields, 9, "user_seconds")?;
        let system = parse_metric(&fields, 10, "system_seconds")?;
        let rss = parse_metric(&fields, 11, "max_rss_kib")?;
        let throughput = parse_metric(&fields, 13, "decoded_mib_per_second")?;
        group.wall.push(wall);
        group.user.push(user);
        group.system.push(system);
        group.rss.push(rss);
        group.throughput.push(throughput);
        paired_observations
            .entry(PairIdentity {
                corpus: key.corpus,
                mode: key.mode,
                threads: key.threads,
                repetition,
            })
            .or_default()
            .insert(
                key.tool,
                PairObservation {
                    wall,
                    rss,
                    throughput,
                },
            );
    }
    if groups.is_empty() {
        return Err("raw input has no observations".to_owned());
    }

    let summary_header = "corpus\tmode\ttool\tbackend\tthreads\tattempts\tsuccesses\tfailures\tdecoded_bytes\tmedian_wall_seconds\tmedian_user_seconds\tmedian_system_seconds\tmedian_max_rss_kib\tmedian_decoded_mib_per_second\n";
    let mut tsv = String::from(summary_header);
    let mut markdown = format!(
        "# Fair benchmark summary\n\nResult directory: `{}`\n\n",
        markdown_cell(result_directory)
    );
    markdown.push_str("| Corpus | Mode | Tool | Backend | Threads | Successes / attempts | Median wall (s) | Median MiB/s | Median RSS (KiB) |\n|---|---|---|---|---:|---:|---:|---:|---:|\n");
    for (key, mut group) in groups {
        let successes = group.attempts - group.failures;
        let wall = median(&mut group.wall);
        let user = median(&mut group.user);
        let system = median(&mut group.system);
        let rss = median(&mut group.rss);
        let throughput = median(&mut group.throughput);
        tsv.push_str(&format!(
            "{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\t{}\n",
            key.corpus,
            key.mode,
            key.tool,
            key.backend,
            key.threads,
            group.attempts,
            successes,
            group.failures,
            group
                .decoded_bytes
                .map_or_else(String::new, |value| value.to_string()),
            metric(wall),
            metric(user),
            metric(system),
            metric(rss),
            metric(throughput),
        ));
        markdown.push_str(&format!(
            "| {} | {} | {} | {} | {} | {} / {} | {} | {} | {} |\n",
            markdown_cell(&key.corpus),
            markdown_cell(&key.mode),
            markdown_cell(&key.tool),
            markdown_cell(&key.backend),
            key.threads,
            successes,
            group.attempts,
            metric(wall),
            metric(throughput),
            metric(rss),
        ));
    }

    let mut comparisons: BTreeMap<ComparisonKey, Comparison> = BTreeMap::new();
    for (identity, observations) in paired_observations {
        let (Some(baseline), Some(candidate)) =
            (observations.get("main"), observations.get("candidate"))
        else {
            continue;
        };
        let comparison = comparisons
            .entry(ComparisonKey {
                corpus: identity.corpus,
                mode: identity.mode,
                threads: identity.threads,
            })
            .or_default();
        if let Some(value) = percent_delta(candidate.throughput, baseline.throughput) {
            comparison.throughput_percent.push(value);
        }
        if let Some(value) = percent_delta(candidate.wall, baseline.wall) {
            comparison.wall_percent.push(value);
        }
        if let Some(value) = percent_delta(candidate.rss, baseline.rss) {
            comparison.rss_percent.push(value);
        }
    }
    if !comparisons.is_empty() {
        markdown.push_str("\n## Paired A/B comparison\n\nEach delta compares candidate and `main` within the same repetition before taking the median. Positive throughput is faster; negative wall time and RSS are lower.\n\n");
        markdown.push_str("| Corpus | Mode | Threads | Complete pairs | Median throughput delta | Median wall delta | Median RSS delta |\n|---|---|---:|---:|---:|---:|---:|\n");
        for (key, mut comparison) in comparisons {
            let pair_count = comparison.throughput_percent.len();
            markdown.push_str(&format!(
                "| {} | {} | {} | {} | {}% | {}% | {}% |\n",
                markdown_cell(&key.corpus),
                markdown_cell(&key.mode),
                key.threads,
                pair_count,
                metric(median(&mut comparison.throughput_percent)),
                metric(median(&mut comparison.wall_percent)),
                metric(median(&mut comparison.rss_percent)),
            ));
        }
    }
    Ok(Outputs { tsv, markdown })
}

fn append_provenance(
    markdown: &mut String,
    title: &str,
    path: &Path,
) -> Result<(), Box<dyn Error>> {
    let contents = fs::read_to_string(path)?;
    markdown.push_str(&format!("\n## {title}\n\n```text\n"));
    markdown.push_str(&contents);
    if !contents.ends_with('\n') {
        markdown.push('\n');
    }
    markdown.push_str("```\n");
    Ok(())
}

fn run(arguments: Arguments) -> Result<(), Box<dyn Error>> {
    let input = arguments.input.as_deref().expect("validated input");
    let raw = fs::read_to_string(input)?;
    let result_directory = input
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .display()
        .to_string();
    let mut outputs = summarize(&raw, &result_directory)?;
    if let Some(path) = arguments.environment.as_deref() {
        append_provenance(&mut outputs.markdown, "Environment", path)?;
    }
    if let Some(path) = arguments.corpora.as_deref() {
        append_provenance(&mut outputs.markdown, "Corpora", path)?;
    }
    if let Some(path) = arguments.commands.as_deref() {
        append_provenance(&mut outputs.markdown, "Commands", path)?;
    }
    if let Some(path) = arguments.parity.as_deref() {
        append_provenance(&mut outputs.markdown, "Correctness preflight", path)?;
    }
    fs::write(
        arguments.summary_tsv.as_deref().expect("validated output"),
        outputs.tsv,
    )?;
    fs::write(
        arguments
            .summary_markdown
            .as_deref()
            .expect("validated output"),
        outputs.markdown,
    )?;
    Ok(())
}

fn main() {
    let result = parse_arguments()
        .map_err(|error| error.into())
        .and_then(run);
    if let Err(error) = result {
        eprintln!("summarize_results: {error}");
        std::process::exit(2);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(repetition: usize, wall: &str, status: &str, exit: usize) -> String {
        format!(
            "2026-08-03T00:00:00Z\tfixture\tverify\trust\tgzip-rs\t4\t{repetition}\t1\t{wall}\t1.0\t0.1\t100\t1048576\t2.0\t{exit}\t{status}\t\t"
        )
    }

    #[test]
    fn median_and_failures_are_retained() {
        let raw = format!(
            "{RAW_HEADER}\n{}\n{}\n{}\n",
            row(1, "3.0", "success", 0),
            row(2, "1.0", "success", 0),
            row(3, "", "failed", 7),
        );
        let output = summarize(&raw, "results").unwrap();
        assert!(output.tsv.contains("\t3\t2\t1\t1048576\t2.000000\t"));
        assert!(output.markdown.contains("2 / 3"));
    }

    #[test]
    fn malformed_and_duplicate_rows_are_rejected() {
        assert!(summarize("wrong\n", "results").is_err());
        let duplicate = row(1, "1.0", "success", 0);
        let raw = format!("{RAW_HEADER}\n{duplicate}\n{duplicate}\n");
        assert!(summarize(&raw, "results").is_err());
    }

    #[test]
    fn candidate_and_main_are_compared_within_each_repetition() {
        let paired_row = |tool: &str, repetition: usize, wall: &str, throughput: &str| {
            format!(
                "2026-08-03T00:00:00Z\tfixture\treader\t{tool}\tgzip-rs\t1\t{repetition}\t1\t{wall}\t1.0\t0.1\t100\t1048576\t{throughput}\t0\tsuccess\t\t"
            )
        };
        let raw = format!(
            "{RAW_HEADER}\n{}\n{}\n{}\n{}\n",
            paired_row("main", 1, "2.0", "10.0"),
            paired_row("candidate", 1, "1.0", "20.0"),
            paired_row("main", 2, "4.0", "5.0"),
            paired_row("candidate", 2, "2.0", "10.0"),
        );
        let output = summarize(&raw, "results").unwrap();
        assert!(
            output
                .markdown
                .contains("| fixture | reader | 1 | 2 | 100.000000% | -50.000000% | 0.000000% |")
        );
    }
}
