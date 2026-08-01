#!/usr/bin/env bash
# Thin wrapper: fair rapidgzip-rust vs C++ rapidgzip benchmarks.
# Forwards all args to benchmarks/run-fair.sh from the repo root.
set -euo pipefail
root=$(cd "$(dirname "$0")/.." && pwd)
exec "$root/benchmarks/run-fair.sh" "$@"
