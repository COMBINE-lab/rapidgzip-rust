#!/usr/bin/env python3
"""Gate optional lifetime CPU accounting against an uninstrumented CLI build."""

import argparse
import hashlib
import json
import random
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path


def sha256(path):
    digest = hashlib.sha256()
    with path.open("rb") as source:
        while True:
            chunk = source.read(1024 * 1024)
            if not chunk:
                return digest.hexdigest()
            digest.update(chunk)


def percentile(values, fraction):
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, int(len(ordered) * fraction))]


def bootstrap_median_upper(ratios, seed, iterations=10000):
    rng = random.Random(seed)
    medians = []
    for _ in range(iterations):
        medians.append(statistics.median(rng.choice(ratios) for _ in ratios))
    return percentile(medians, 0.95)


def execute(binary, source, threads, timing_path):
    command = [
        "/usr/bin/time",
        "-f",
        '{"user_seconds":%U,"system_seconds":%S,"wall_seconds":%e,"peak_rss_kib":%M}',
        "-o",
        str(timing_path),
        str(binary),
        "--decoder-parallelism",
        str(threads),
        "--test",
        "--quiet",
        str(source),
    ]
    started = time.monotonic()
    result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    record = {
        "command": command,
        "exit_code": result.returncode,
        "runner_wall_seconds": time.monotonic() - started,
        "stdout": result.stdout.decode("utf-8", "replace"),
        "stderr": result.stderr.decode("utf-8", "replace"),
    }
    if result.returncode == 0:
        record["process"] = json.loads(timing_path.read_text())
    return record


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--baseline", type=Path, required=True)
    parser.add_argument("--accounting", type=Path, required=True)
    parser.add_argument("--input", type=Path, required=True)
    parser.add_argument("--threads", type=int, required=True)
    parser.add_argument("--repetitions", type=int, default=30)
    parser.add_argument("--margin", type=float, default=0.01)
    parser.add_argument("--seed", type=int, default=20260807)
    parser.add_argument("--results", type=Path, required=True)
    args = parser.parse_args()
    if args.repetitions < 30:
        raise ValueError("the formal gate requires at least 30 repetitions")

    args.results.mkdir(parents=True, exist_ok=True)
    variants = {"baseline": args.baseline, "accounting": args.accounting}
    provenance = {
        "input": str(args.input.resolve()),
        "input_sha256": sha256(args.input),
        "threads": args.threads,
        "binary_sha256": {name: sha256(path) for name, path in variants.items()},
    }
    rng = random.Random(args.seed)
    orders = [("baseline", "accounting"), ("accounting", "baseline")]
    block_orders = [orders[index % 2] for index in range(args.repetitions)]
    rng.shuffle(block_orders)

    records = []
    raw_path = args.results / "runs.jsonl"
    with raw_path.open("w") as raw, tempfile.TemporaryDirectory() as timing_dir:
        for repetition, order in enumerate(block_orders, 1):
            for position, variant in enumerate(order):
                print(
                    "[%d/%d] %s rep %d"
                    % (
                        (repetition - 1) * 2 + position + 1,
                        args.repetitions * 2,
                        variant,
                        repetition,
                    ),
                    flush=True,
                )
                record = {
                    **provenance,
                    "variant": variant,
                    "repetition": repetition,
                    "block_order": order,
                    "block_position": position,
                }
                timing_path = Path(timing_dir) / ("%d-%s.json" % (repetition, variant))
                record.update(execute(variants[variant], args.input, args.threads, timing_path))
                records.append(record)
                raw.write(json.dumps(record, sort_keys=True) + "\n")
                raw.flush()

    by_variant = {
        variant: {record["repetition"]: record for record in records if record["variant"] == variant}
        for variant in variants
    }
    complete = all(
        len(by_variant[variant]) == args.repetitions
        and all(record["exit_code"] == 0 for record in by_variant[variant].values())
        for variant in variants
    )
    metrics = {
        "wall": lambda record: record["process"]["wall_seconds"],
        "cpu": lambda record: record["process"]["user_seconds"]
        + record["process"]["system_seconds"],
    }
    summary = {"complete": complete, "margin": args.margin, "metrics": {}, "gate_passed": complete}
    if complete:
        for metric_index, (name, metric) in enumerate(metrics.items()):
            ratios = [
                metric(by_variant["accounting"][repetition])
                / metric(by_variant["baseline"][repetition])
                for repetition in sorted(by_variant["baseline"])
            ]
            upper = bootstrap_median_upper(ratios, args.seed + metric_index + 1)
            passed = upper <= 1.0 + args.margin
            summary["metrics"][name] = {
                "median_ratio": statistics.median(ratios),
                "ratio_upper_95": upper,
                "equivalent": passed,
            }
            summary["gate_passed"] &= passed
    (args.results / "summary.json").write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n"
    )
    return 0 if summary["gate_passed"] else 1


if __name__ == "__main__":
    sys.exit(main())
