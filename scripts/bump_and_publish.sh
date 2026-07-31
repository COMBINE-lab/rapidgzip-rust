#!/usr/bin/env bash
# Prepare, validate, tag, and publish a rapidgzip-rust workspace release.
#
# A dry run restores Cargo.toml and Cargo.lock before exiting. A real release
# pushes its commit and tag before uploading the publishable workspace crates,
# so the source referenced by crates.io is already available upstream.

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: scripts/bump_and_publish.sh [--dry-run] [--yes] VERSION

Arguments:
  VERSION    SemVer release number without a leading "v" (for example, 0.2.0)

Options:
  --dry-run  Validate and package the release, then restore version files
  --yes      Skip the final interactive confirmation
  -h, --help Show this help
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
if [[ "$version" == "$current_version" ]]; then
    echo "error: workspace version is already $version" >&2
    exit 1
fi
if git rev-parse --verify --quiet "refs/tags/v$version" >/dev/null; then
    echo "error: tag v$version already exists" >&2
    exit 1
fi

release_committed=false
restore_version_files() {
    if [[ "$release_committed" == false ]]; then
        git restore -- Cargo.toml Cargo.lock
    fi
}
trap restore_version_files ERR
if [[ "$dry_run" == true ]]; then
    trap restore_version_files EXIT
fi

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

echo "Preparing rapidgzip-rust $current_version -> $version"
cargo check --workspace
cargo fmt --all --check
cargo clippy --workspace --all-targets --locked -- -D warnings
cargo test --workspace --all-targets --locked
RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps --locked
cargo publish --workspace --dry-run --allow-dirty --locked

if [[ "$dry_run" == true ]]; then
    echo "Dry run for v$version passed; version files will be restored."
    exit 0
fi

if [[ "$assume_yes" == false ]]; then
    echo
    echo "The checks passed. This will commit and tag v$version, push both to origin,"
    echo "and publish the workspace crates to crates.io."
    read -r -p "Continue? [y/N] " reply
    if [[ ! "$reply" =~ ^[Yy]$ ]]; then
        echo "Release cancelled; restoring version files."
        restore_version_files
        exit 1
    fi
fi

git add Cargo.toml Cargo.lock
git commit -m "Release v$version"
git tag -a "v$version" -m "Release v$version"
release_committed=true
trap - ERR

git push origin main "v$version"
cargo publish --workspace --locked

echo "Published rapidgzip-rust v$version."
