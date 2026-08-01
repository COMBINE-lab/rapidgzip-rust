# Release process

How to cut a `rapidgzip-core` / `rapidgzip-rust-cli` release for crates.io.

## crates.io caveats (read first)

| Fact | Implication |
|------|-------------|
| **`rapidgzip-core` 0.1.0 is an incomplete stub** | Smaller API than the current tree. Dependents must use **0.2.0+** (or path/git), not registry `0.1.0`. |
| **Publish order** | Always **`rapidgzip-core` first**, then **`rapidgzip-rust-cli`**. The CLI rewrites its path dep to a **version** dep on crates.io. |
| **Standalone CLI package** | `cargo package -p rapidgzip-rust-cli` (or `cargo publish -p rapidgzip-rust-cli` alone) **fails** until `rapidgzip-core` at that version is on crates.io. At 0.1.0 it would wrongly verify against the **stub**. |
| **`rapidgzip-bench`** | `publish = false`; never uploaded. Prefer explicit `-p` packages over a bare mental model of “whole workspace publish,” though `--workspace` would skip it. |

Ship this series as **0.2.0+** so resolution never lands on the 0.1.0 stub API.

## Script

[`scripts/bump_and_publish.sh`](../scripts/bump_and_publish.sh) prepares,
validates, tags, and publishes in dependency order.

```console
# Validate without retaining version bumps or publishing
scripts/bump_and_publish.sh --dry-run 0.2.0

# Real release (interactive confirm; requires clean main + origin)
scripts/bump_and_publish.sh 0.2.0

# Unattended real release after checks pass
scripts/bump_and_publish.sh --yes 0.2.0
```

`VERSION` is SemVer **without** a leading `v` (e.g. `0.2.0`, `0.2.0-rc.1`).
crates.io ignores build metadata; the script rejects it.

### What the script does

1. Requires a **clean** Git working tree on **`main`**, with **`origin`** set.
2. Rejects if tag `v$VERSION` already exists.
3. If `$VERSION` differs from `workspace.package.version`, rewrites
   `Cargo.toml` (workspace version + `rapidgzip-core` path dependency version)
   and refreshes those package versions in `Cargo.lock`.
4. Runs, all `--locked`:
   - `cargo check --workspace`
   - `cargo fmt --all --check`
   - `cargo clippy --workspace --all-targets -- -D warnings`
   - `cargo test --workspace --all-targets`
   - `RUSTDOCFLAGS="-D warnings" cargo doc --workspace --no-deps`
   - **Package dry-run (both crates together):**
     `cargo publish -p rapidgzip-core -p rapidgzip-rust-cli --dry-run --allow-dirty`
5. **`--dry-run`**: exits after checks; restores version files if they were
   bumped.
6. **Real run**: after confirmation (unless `--yes`), commits version files if
   changed, creates annotated tag `v$VERSION`, pushes `main` + tag to `origin`,
   then:
   1. `cargo publish -p rapidgzip-core --locked`
   2. **Wait/retry** until crates.io serves `rapidgzip-core@$VERSION`
   3. `cargo publish -p rapidgzip-rust-cli --locked`

Dry-run restores version files on exit/error so a failed or cancelled dry run
does not leave a dirty tree. A real release pushes the commit/tag **before**
crates.io upload so the published source matches the tag.

### Why multi-package dry-run, sequential real publish

| Mode | Command shape | Why |
|------|---------------|-----|
| **Dry-run verify** | One invocation: `-p rapidgzip-core -p rapidgzip-rust-cli --dry-run` | Cargo packages **both**, verifies core, then verifies the CLI using the **just-packaged** core via a temporary registry. Full CLI tarball verify **without** core already on crates.io. |
| **Real publish** | Core upload → wait for index → CLI upload | After upload, the CLI’s registry dep must resolve on crates.io. Waiting avoids a race where CLI packaging cannot find `rapidgzip-core@$VERSION`. |

Equivalent local checks when you are **not** using the full script:

```console
# Always works: core tarball in isolation
cargo package -p rapidgzip-core --locked --allow-dirty

# Path-dep compile of the CLI (does not prove crates.io resolution)
cargo check -p rapidgzip-rust-cli --locked

# Full dual-crate packaging verify (same as script dry-run packaging step)
cargo publish -p rapidgzip-core -p rapidgzip-rust-cli --dry-run --allow-dirty --locked
```

**Do not** expect this to succeed before core is published at the same version:

```console
cargo package -p rapidgzip-rust-cli --locked --allow-dirty
# error: failed to select rapidgzip-core = "^$VERSION" from crates.io
```

## Packaging (CI and local)

CI (quality job) continuously checks publish readiness for **core** and a
path-dep CLI check:

```console
cargo package -p rapidgzip-core --locked
cargo check -p rapidgzip-rust-cli --locked
```

- `cargo package -p rapidgzip-core` builds and verifies the crates.io tarball in
  isolation (source layout, licenses, and that the packaged crate compiles).
- There is no `package.include` filter on `rapidgzip-core`; all `src/` modules
  (including `index/`, `parallel/`, `seek`, analyze, etc.) ship by default.
- `cargo check -p rapidgzip-rust-cli` uses the **path** dependency and always
  reflects in-tree `rapidgzip-core`.
- Full CLI package verification against the **registry** only works after core
  is published, or via the multi-package dry-run above at release time.

## Pre-release checklist

Do these **before** a real `bump_and_publish` (the script re-runs tests but
does not author docs):

| Step | Action |
|------|--------|
| **CHANGELOG** | Move `[0.2.0] - Unreleased` (or RC) to a dated section; summarize user-facing changes. |
| **Version** | Confirm `workspace.package.version` and in-tree `rapidgzip-core` dependency match intent (script can bump). Use **≥ 0.2.0**, not a re-publish of the 0.1.0 stub. |
| **Docs** | README, ARCHITECTURE, PERFORMANCE_AUDIT residuals match shipped behavior. |
| **Tests** | Locally: `cargo fmt`, `clippy -D warnings`, `cargo test --workspace`, doc build. |
| **Package** | `cargo package -p rapidgzip-core --locked` (CI also runs this); CLI path check via `cargo check -p rapidgzip-rust-cli`. |
| **Publish dry-run** | `scripts/bump_and_publish.sh --dry-run $VERSION` on clean `main`. |
| **Benchmark (optional)** | Fair matrix vs C++ rapidgzip 0.16 if claiming performance; see [BENCHMARKING.md](../BENCHMARKING.md). |
| **crates.io** | Ensure publish rights for **both** packages; no secrets in the tree. |

## After publish

- Confirm crates.io pages for `rapidgzip-core` and `rapidgzip-rust-cli` at
  `$VERSION` (not the 0.1.0 stub page alone).
- Smoke-install: `cargo install rapidgzip-rust-cli --version $VERSION` (or
  `cargo add rapidgzip-core@$VERSION` in a throwaway crate).
- Open a short GitHub release notes entry from `CHANGELOG.md` if desired.

## Non-goals of the script

- Does **not** edit `CHANGELOG.md` or other docs.
- Does **not** run the benchmark harness.
- Does **not** publish from a dirty tree or non-`main` branch.
- Does **not** publish `rapidgzip-bench`.

See also the short pointer under **Releasing** in [README.md](../README.md).
