# Adaptive marker/window admission

Status: implemented

Origin: the performance finding in PR #13, reimplemented from current `main`

## Problem and scope

The generic sequential and parallel paths use different DEFLATE algorithms.
Sequential decoding starts at an authoritative framing boundary and uses
zlib-rs. The rapidgzip path starts near estimated compressed-grid positions,
decodes with unknown-history markers, and resolves each marker buffer after its
predecessor window is known. Their crossover varies with the input, machine,
architecture, requested budget, runtime worker ceiling, and useful task count.
A fixed minimum worker count cannot represent those variables.

Admission applies only to the generic estimated marker/window route used by
ordinary and sparse-member gzip, zlib, and raw DEFLATE. Independently known
BGZF blocks, fully stored streams, and densely spaced authenticated gzip
members keep their specialized paths and do not pay for this probe.

## Useful admission width

Admission begins with the same machine- and budget-derived bootstrap used by
steady-state adaptive concurrency. It is bounded by:

```text
configured decoder budget
current application worker ceiling
affinity-visible processors
adaptive machine bootstrap
two complete waves of estimated grid tasks
```

One useful worker selects sequential decoding immediately. The compressed
source must also expose at least sixteen configured grid cells; shorter inputs
cannot reliably amortize classification and a second worker-pool startup.

The application ceiling is sampled again after the probe. Lowering it during
admission invalidates a wider speculative sample and selects sequential
decoding. Later changes remain authoritative through the existing runtime
worker controller.

## Bounded empirical screen

Eligible inputs are classified before output or index checkpoints become
visible:

1. Decode the first 128 KiB compressed interval exactly three times with
   zlib-rs `Z_BLOCK`. Retain the first result only for validation; use the best
   service time so a scheduling interruption cannot make sequential decoding
   look artificially slow.
2. Decode one adjacent 128 KiB interval per useful worker concurrently with
   unknown history. Validate exact boundary adjacency, resolve the marker
   output against its predecessor, and account for ordered window propagation.
3. Model full marker replacement as worker-pool work and compare the aggregate
   speculative service rate with the exact service rate. The admission gate is
   45%, 35%, and 25% at widths two, three, and four, decreasing by ten
   percentage points per additional useful worker to a 20% floor. The relaxed
   gate compensates for structural-search and marker setup that dominate this
   one-eighth-grid screen but are amortized by the normal 1 MiB grid.
4. Invalid, discontinuous, too-small, cancelled, or ambiguous samples select
   sequential decoding. A passing sample starts the unmodified steady marker
   grid; the small screen is discarded.

The screen allocates work only for workers that are concurrently useful. It
does not eagerly create the builder maximum, and its speculative memory is
bounded by the existing per-task output allowance. Re-running the normal
sequential entry point after a rejection benchmarked faster than resuming
zlib-rs from the screen's exact block boundary, so rejected probe output is
deliberately not emitted or indexed.

This controller is distinct from `AdaptiveConcurrency`: admission chooses the
algorithm, while adaptive concurrency tunes worker count after marker/window
has been selected.

## Telemetry and correctness

`DecoderPath::MarkerAdmission` identifies the transient screen. Its terminal
path is `Sequential` or `MarkerWindow`. A sequential choice sets the effective
target to one; terminal `spawned_workers` remains zero because the short probe
workers have joined. `configured_workers` and the application ceiling retain
their published meanings.

Probe output is never trusted as container validation and never reaches the
caller. Both terminal paths still pass through their normal checksum, footer,
trailing-data, output-limit, and expected-size machinery. Index construction
does not observe discarded work: the initial checkpoint is offered only after
marker admission, while a sequential restart records its own checkpoints.
Multi-member gzip remains general because later verified member boundaries use
the first estimated task strictly after each new header, independent of the
admission screen. If the first member ends inside the exact screen, admission
conservatively selects the sequential path; dense short-member archives have
already been offered to the specialized member path, while a later iteration
may sample across authenticated member boundaries for unusual short-prefix,
long-tail archives.

## Implementation plan

1. **Separate admission policy from decoding.** Add a pure internal policy
   module for effective-width bounds, minimum-work checks, representative
   samples, and worker-scaled service-rate gates. Unit-test each bound and
   threshold independently of wall-clock behavior. **Complete.**
2. **Preserve first-refusal routing.** Leave BGZF, stored-block, and dense-member
   recognition ahead of generic admission. Route ineligible generic inputs to
   the existing authoritative sequential decoder. **Complete.**
3. **Add the bounded empirical screen.** Build a temporary 128 KiB task grid,
   take conservative exact samples, run only the useful speculative width,
   validate adjacency, and account for both parallel marker replacement and
   ordered successor-window construction. Discard every screen result before
   entering either terminal decoder. **Complete.**
4. **Integrate live control and telemetry.** Share the adaptive controller's
   bootstrap, honor the configured and application worker limits, recheck a
   lowered application ceiling after the wave, and expose the transient
   `MarkerAdmission` path without changing terminal worker statistics.
   **Complete.**
5. **Protect framing and index semantics.** Delay index publication until a
   marker decision, restart the ordinary sequential path after rejection, and
   select later-member tasks by ordered grid position rather than arithmetic
   assumptions. Exercise gzip, zlib, raw DEFLATE, multi-member gzip, BGZF,
   corruption, output limits, and seeking. **Complete.**
6. **Tune against routing matrices, not one scalar benchmark.** Compare clean
   `main` and the replacement on repeated FASTQ runs at one through four
   workers, then check short text, sparse members, dense members, and BGZF with
   terminal-path telemetry. Retain the diagnostic matrix in `BENCHMARKING.md`.
   **Complete.**
7. **Release-quality validation.** Run formatting, Clippy with warnings denied,
   the full debug and release suites, rustdoc with warnings denied, the declared
   Rust 1.87 MSRV check, and package verification. **Complete for the core
   crate; CLI tarball verification requires the next version bump because its
   packaged manifest correctly resolves the already-published 0.1.0 core.**

## Validation

- Pure tests cover task and machine bounds, runtime/requested budgets, sample
  size, and each worker-scaled gate.
- Existing integration tests cover bytes, framing, corruption, output limits,
  indexing, `Read + Send`, gzip/zlib/raw, multi-member gzip, and BGZF.
- The low-thread matrix includes one-member FASTQ, short text, sparse and dense
  multi-member gzip, and BGZF. It records the terminal path as well as
  throughput so a faster result cannot hide a wrong algorithm choice.

The implementation starts at current `main`; it does not merge PR #13's
stacked history. The replacement PR references #13 for the original finding
and preserves attribution while replacing its fixed threshold with this
input- and machine-aware controller.
