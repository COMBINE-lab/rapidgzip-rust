#!/usr/bin/env bash
# Prepare, validate, tag, and publish a rapidgzip-rust workspace release.
#
# Publishable crates (dependency order):
#   1. rapidgzip-core
#   2. rapidgzip-rust-cli  (depends on rapidgzip-core by version on crates.io)
#
# rapidgzip-bench has publish = false and is never uploaded.
#
# A dry run restores Cargo.toml and Cargo.lock before exiting. A real release
# pushes its commit and tag before uploading, so the source referenced by
# crates.io is already available upstream.
#
# Dry-run packaging uses a single multi-package `cargo publish -p … -p …`
# invocation so CLI verification can use the just-packaged core via Cargo's
# temporary registry. Standalone `cargo package -p rapidgzip-rust-cli` fails
# until that core version exists on crates.io (and would wrongly resolve the
# incomplete 0.1.0 stub if the workspace were still at 0.1.0).
#
# Real publish uploads core first, waits until crates.io serves that version,
# then publishes the CLI.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/bump_and_publish.sh [--dry-run] [--yes] VERSION

Arguments:
  VERSION    SemVer release number without a leading "v" (for example, 0.2.0).
             For an initial release, this may equal the workspace version.

Options:
  --dry-run  Validate and package the release, then restore version files
  --yes      Skip the final interactive confirmation
  -h, --help Show this help

Publish order (always):
  rapidgzip-core, then rapidgzip-rust-cli

Notes:
  crates.io has an incomplete rapidgzip-core 0.1.0 stub. Ship 0.2.0+ so
  dependents do not resolve that API. Do not treat 0.1.0 as the library contract.
EOF
}

dry_run=false
assume_yes=false
version=""

while (($# > 0)); do
    case "$1" in
        --dry-run)
            dry_run=true
            ;;
        --yes)
            assume_yes=true
            ;;
        -h | --help)
            usage
            exit 0
            ;;
        -* )
            echo "error: unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            if [[ -n "$version" ]]; then
                echo "error: expected exactly one VERSION argument" >&2
                usage >&2
                exit 2
            fi
            version="$1"
            ;;
    esac
    shift
done

if [[ -z "$version" ]]; then
    echo "error: VERSION is required" >&2
    usage >&2
    exit 2
fi

# This deliberately excludes SemVer build metadata. crates.io ignores build
# metadata when deciding whether package versions are unique.
semver_re='^(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)(-([0-9A-Za-z-]+\.)*[0-9A-Za-z-]+)?$'
if [[ ! "$version" =~ $semver_re ]]; then
    echo "error: VERSION must be valid SemVer without a leading v or build metadata" >&2
    exit 2
fi

repo_root=$(git rev-parse --show-toplevel 2>/dev/null) || {
    echo "error: this script must run inside a Git repository" >&2
    exit 1
}
cd "$repo_root"

if [[ ! -f Cargo.toml || ! -f Cargo.lock ]]; then
    echo "error: Cargo.toml and Cargo.lock must exist at the repository root" >&2
    exit 1
fi

if [[ -n "$(git status --porcelain)" ]]; then
    echo "error: the working tree must be clean before preparing a release" >&2
    exit 1
fi

branch=$(git branch --show-current)
if [[ "$branch" != "main" ]]; then
    echo "error: releases must be prepared from main (current branch: $branch)" >&2
    exit 1
fi

if ! git remote get-url origin >/dev/null 2>&1; then
    echo "error: the origin remote is not configured" >&2
    exit 1
fi

current_version=$(
    awk '
        $0 == "[workspace.package]" { in_package = 1; next }
        /^\[/ { in_package = 0 }
        in_package && $1 == "version" {
            gsub(/"/, "", $3)
            print $3
            exit
        }
    ' Cargo.toml
)

if [[ -z "$current_version" ]]; then
    echo "error: could not read workspace.package.version from Cargo.toml" >&2
    exit 1
fi
if git rev-parse --verify --quiet "refs/tags/v$version" >/dev/null; then
    echo "error: tag v$version already exists" >&2
    exit 1
fi

version_changed=true
if [[ "$version" == "$current_version" ]]; then
    version_changed=false
fi

release_committed=false
restore_version_files() {
    if [[ "$release_committed" == false && "$version_changed" == true ]]; then
        git restore -- Cargo.toml Cargo.lock
    fi
}
trap restore_version_files ERR
if [[ "$dry_run" == true ]]; then
    trap restore_version_files EXIT
fi

if [[ "$version_changed" == true ]]; then
    CURRENT_VERSION="$current_version" TARGET_VERSION="$version" python3 - <<'PY'
import os
import re
from pathlib import Path

path = Path("Cargo.toml")
old = os.environ["CURRENT_VERSION"]
new = os.environ["TARGET_VERSION"]
lines = path.read_text(encoding="utf-8").splitlines(keepends=True)
section = ""
package_updates = 0
dependency_updates = 0

for index, line in enumerate(lines):
    section_match = re.match(r"^\[([^]]+)]\s*$", line)
    if section_match:
        section = section_match.group(1)
        continue

    if section == "workspace.package" and re.match(r"^version\s*=", line):
        expected = f'version = "{old}"'
        if line.rstrip("\n") != expected:
            raise SystemExit(f"error: expected {expected!r}, found {line.rstrip()!r}")
        lines[index] = line.replace(f'"{old}"', f'"{new}"', 1)
        package_updates += 1
    elif section == "workspace.dependencies" and line.startswith("rapidgzip-core ="):
        replacement, count = re.subn(
            rf'version\s*=\s*"{re.escape(old)}"',
            f'version = "{new}"',
            line,
            count=1,
        )
        if count != 1:
            raise SystemExit("error: could not update the rapidgzip-core dependency version")
        lines[index] = replacement
        dependency_updates += 1

if package_updates != 1 or dependency_updates != 1:
    raise SystemExit(
        "error: expected exactly one workspace version and one rapidgzip-core dependency update"
    )

path.write_text("".join(lines), encoding="utf-8")
PY
    # Path packages record their version in Cargo.lock; keep it aligned.
    cargo update -p rapidgzip-core -p rapidgzip-rust-cli -p rapidgzip-bench
fi

if [[ "$version_changed" == true ]]; then
    echo "Preparing rapidgzip-rust $current_version -> $version"
else
    echo "Preparing initial rapidgzip-rust v$version release"
fi

cargo check --workspace --locked
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked

# Multi-package dry-run: packages both crates, verifies core, then verifies the
# CLI against the just-packaged core via Cargo's temporary registry. Prefer
# explicit -p over --workspace so the publish set is obvious (bench is
# publish=false and would be skipped either way).
#
# Standalone alternatives that are *not* a full CLI crates.io verify:
#   cargo package -p rapidgzip-core --locked --allow-dirty
#   cargo check -p rapidgzip-rust-cli --locked   # path dep only
# Standalone `cargo package -p rapidgzip-rust-cli` fails until core@$VERSION
# exists on crates.io.
echo "Packaging/verifying publishable crates (core, then CLI)…"
cargo publish -p rapidgzip-core -p rapidgzip-rust-cli --dry-run --allow-dirty --locked

if [[ "$dry_run" == true ]]; then
    echo "Dry run for v$version passed; version files will be restored."
    exit 0
fi

if [[ "$assume_yes" == false ]]; then
    echo
    if [[ "$version_changed" == true ]]; then
        echo "The checks passed. This will commit and tag v$version, push both to origin,"
    else
        echo "The checks passed. This will tag the current commit as v$version, push it to origin,"
    fi
    echo "then publish rapidgzip-core@$version, wait for crates.io, and publish"
    echo "rapidgzip-rust-cli@$version."
    read -r -p "Continue? [y/N] " reply
    if [[ ! "$reply" =~ ^[Yy]$ ]]; then
        echo "Release cancelled; restoring version files."
        restore_version_files
        exit 1
    fi
fi

if [[ "$version_changed" == true ]]; then
    git add Cargo.toml Cargo.lock
    git commit -m "Release v$version"
fi
git tag -a "v$version" -m "Release v$version"
release_committed=true
trap - ERR

git push origin main "v$version"

# Wait until crates.io serves a crate version (HTTP 200 on the version API).
# Required so the subsequent CLI publish can resolve rapidgzip-core = "^$version"
# from the registry after path deps are rewritten.
wait_for_crate_version() {
    local crate_name="$1"
    local crate_version="$2"
    local max_attempts="${3:-90}"
    local sleep_secs="${4:-10}"
    local url="https://crates.io/api/v1/crates/${crate_name}/${crate_version}"
    local user_agent="rapidgzip-rust-bump-and-publish (https://github.com/COMBINE-lab/rapidgzip-rust)"
    local attempt http_code

    echo "Waiting for ${crate_name}@${crate_version} on crates.io…"
    for ((attempt = 1; attempt <= max_attempts; attempt++)); do
        http_code=$(
            curl -sS -A "$user_agent" -o /tmp/rapidgzip-crate-wait.json \
                -w '%{http_code}' "$url" || echo "000"
        )
        if [[ "$http_code" == "200" ]]; then
            echo "${crate_name}@${crate_version} is available on crates.io (attempt ${attempt})."
            return 0
        fi
        echo "  attempt ${attempt}/${max_attempts}: HTTP ${http_code}; retrying in ${sleep_secs}s…"
        sleep "$sleep_secs"
    done
    echo "error: timed out waiting for ${crate_name}@${crate_version} on crates.io" >&2
    return 1
}

echo "Publishing rapidgzip-core@$version…"
cargo publish -p rapidgzip-core --locked
wait_for_crate_version rapidgzip-core "$version"

# Cargo's crates.io *index* can lag the HTTP API slightly. Retry CLI publish
# so packaging can resolve rapidgzip-core = "^$version" after path rewrite.
echo "Publishing rapidgzip-rust-cli@$version…"
cli_publish_attempts=30
cli_publish_sleep=15
cli_published=false
for ((attempt = 1; attempt <= cli_publish_attempts; attempt++)); do
    set +e
    cargo publish -p rapidgzip-rust-cli --locked
    cli_status=$?
    set -e
    if [[ "$cli_status" -eq 0 ]]; then
        cli_published=true
        break
    fi
    echo "CLI publish failed with exit ${cli_status} (attempt ${attempt}/${cli_publish_attempts})."
    echo "Retrying in ${cli_publish_sleep}s in case the crates.io index is still catching up…"
    sleep "$cli_publish_sleep"
done
if [[ "$cli_published" != true ]]; then
    echo "error: failed to publish rapidgzip-rust-cli@$version after ${cli_publish_attempts} attempts" >&2
    echo "note: rapidgzip-core@$version was already uploaded; publish the CLI manually once the index shows core." >&2
    exit 1
fi

echo "Published rapidgzip-core and rapidgzip-rust-cli v$version."
