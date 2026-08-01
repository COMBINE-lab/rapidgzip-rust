//! Lock-free decoder telemetry and runtime worker-budget control.

use crate::index::{Checkpoint, GzipIndex, IndexBuilder, IndexError};
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Condvar, Mutex};
use std::time::Instant;

const NO_BEST_WORKER_COUNT: usize = usize::MAX;

/// Decoder implementation selected for the current input.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum DecoderPath {
    /// Input classification has not completed yet.
    #[default]
    Starting,
    /// Serial gzip-member inflation on the coordinator thread.
    Sequential,
    /// Direct copying of independently indexed stored DEFLATE blocks.
    Stored,
    /// Independent inflation of densely spaced ordinary gzip members.
    DenseMembers,
    /// The rapidgzip marker/window pipeline for ordinary DEFLATE streams.
    MarkerWindow,
    /// Independent inflation of indexed BGZF blocks.
    Bgzf,
    /// Independent zlib inflation of spans delimited by an index.
    Indexed,
}

impl DecoderPath {
    const fn encoded(self) -> u8 {
        match self {
            Self::Starting => 0,
            Self::Sequential => 1,
            Self::Stored => 2,
            Self::DenseMembers => 3,
            Self::MarkerWindow => 4,
            Self::Bgzf => 5,
            Self::Indexed => 6,
        }
    }

    const fn from_encoded(value: u8) -> Self {
        match value {
            1 => Self::Sequential,
            2 => Self::Stored,
            3 => Self::DenseMembers,
            4 => Self::MarkerWindow,
            5 => Self::Bgzf,
            6 => Self::Indexed,
            _ => Self::Starting,
        }
    }
}

/// Current high-level constraint on decoder progress.
///
/// This is an approximate observation assembled from relaxed atomic loads. It
/// describes rapidgzip's own task state, not operating-system CPU accounting.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub enum DecoderPressure {
    /// Input classification or worker startup is still in progress.
    Starting,
    /// The final decoded-output handoff is blocked by its consumer.
    ConsumerBound {
        /// Fraction of live workers not currently executing a decoder task.
        idle_worker_fraction: f32,
    },
    /// All admitted workers are busy while runnable work remains queued.
    DecoderBound {
        /// Approximate number of immediately runnable decoder tasks.
        queued_tasks: usize,
    },
    /// Empirical calibration selected a stable worker count.
    Converged {
        /// Worker count selected by empirical calibration.
        at_workers: usize,
    },
    /// No decoder task was running or immediately runnable when sampled.
    Idle,
    /// The complete compressed stream has reached a terminal state.
    Finished,
}

/// Approximate, lock-free snapshot of a running decoder.
///
/// Fields are loaded independently with relaxed atomic ordering. A snapshot is
/// therefore suitable for telemetry and scheduling feedback, but is not a
/// transactionally consistent record of a single instant.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub struct DecoderStats {
    /// Decoder implementation selected for the input.
    pub path: DecoderPath,
    /// Immutable maximum worker budget supplied to the builder.
    pub configured_workers: usize,
    /// Current application-controlled ceiling on decoder workers.
    pub worker_limit: usize,
    /// Effective worker target after application and adaptive limits.
    pub active_workers: usize,
    /// Workers currently executing decoder tasks.
    pub busy_workers: usize,
    /// Live decoder-worker operating-system threads.
    pub spawned_workers: usize,
    /// Live coordinator and scanner operating-system threads.
    pub auxiliary_threads: usize,
    /// Empirically selected worker count, once calibration has completed.
    pub best_workers: Option<usize>,
    /// Decompressed bytes emitted into the final output handoff.
    pub decompressed_bytes: u64,
    /// Decompressed bytes returned through [`std::io::Read`].
    pub consumed_bytes: u64,
    /// Gzip members accepted through verified output so far.
    pub member_count: u64,
    /// Average decoded-output production rate since decoder startup.
    pub decode_throughput_bps: f64,
    /// Average [`std::io::Read`] consumption rate since decoder startup.
    pub consumer_throughput_bps: f64,
    /// Current high-level decoder pressure classification.
    pub pressure: DecoderPressure,
}

/// Invalid runtime decoder-worker limit.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerLimitError {
    requested: usize,
    configured: usize,
}

impl WorkerLimitError {
    /// Rejected worker count.
    pub const fn requested(self) -> usize {
        self.requested
    }

    /// Maximum worker count configured for the decoder.
    pub const fn configured(self) -> usize {
        self.configured
    }
}

impl Display for WorkerLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "worker limit {} is outside 1..={}",
            self.requested, self.configured
        )
    }
}

impl Error for WorkerLimitError {}

/// Cloneable telemetry and control handle for a running [`crate::DecoderReader`].
///
/// The handle remains usable after the reader has moved into another component
/// such as a FASTQ parser. Cloning a handle does not create decoder workers.
#[derive(Clone)]
pub struct DecoderHandle {
    pub(crate) state: Arc<RuntimeState>,
}

impl fmt::Debug for DecoderHandle {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecoderHandle")
            .field("stats", &self.stats())
            .finish()
    }
}

impl DecoderHandle {
    pub(crate) fn new(state: Arc<RuntimeState>) -> Self {
        Self { state }
    }

    /// Returns an approximate lock-free snapshot of decoder activity.
    pub fn stats(&self) -> DecoderStats {
        self.state.stats()
    }

    /// Changes the maximum number of decoder workers that may accept work.
    ///
    /// The method is nonblocking. Workers already executing a task finish that
    /// task before a lower limit takes effect; persistently excess workers then
    /// retire. Raising the limit allows the coordinator to create replacement
    /// workers lazily when useful work is available.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLimitError`] when `workers` is zero or exceeds the
    /// immutable worker budget supplied to [`crate::DecoderBuilder`].
    pub fn set_worker_limit(&self, workers: usize) -> Result<(), WorkerLimitError> {
        self.state.set_worker_limit(workers)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum AuxiliaryKind {
    Coordinator,
    Scanner,
}

pub(crate) struct ThreadRegistration {
    state: Arc<RuntimeState>,
    auxiliary: bool,
}

impl Drop for ThreadRegistration {
    fn drop(&mut self) {
        let counter = if self.auxiliary {
            &self.state.auxiliary_threads
        } else {
            &self.state.spawned_workers
        };
        counter.fetch_sub(1, Ordering::Relaxed);
    }
}

pub(crate) struct BusyRegistration<'a>(&'a RuntimeState);

impl Drop for BusyRegistration<'_> {
    fn drop(&mut self) {
        self.0.busy_workers.fetch_sub(1, Ordering::Relaxed);
    }
}

/// State shared by the reader, coordinator, scanner, and decoder workers.
pub(crate) struct RuntimeState {
    configured_workers: usize,
    worker_limit: AtomicUsize,
    adaptive_target: AtomicUsize,
    limit_epoch: AtomicUsize,
    path: AtomicU8,
    busy_workers: AtomicUsize,
    spawned_workers: AtomicUsize,
    auxiliary_threads: AtomicUsize,
    queued_tasks: AtomicUsize,
    best_workers: AtomicUsize,
    decompressed_bytes: AtomicU64,
    consumed_bytes: AtomicU64,
    member_count: AtomicU64,
    consumer_blocked: AtomicBool,
    terminal: AtomicBool,
    terminal_elapsed_nanos: AtomicU64,
    started: Instant,
    limit_mutex: Mutex<()>,
    limit_signal: Condvar,
    index_enabled: AtomicBool,
    index: Mutex<Option<IndexBuilder>>,
    line_counting: AtomicBool,
    line_count: AtomicU64,
    emitted_bytes: AtomicU64,
}

impl RuntimeState {
    pub(crate) fn new(configured_workers: usize) -> Arc<Self> {
        Arc::new(Self {
            configured_workers,
            worker_limit: AtomicUsize::new(configured_workers),
            adaptive_target: AtomicUsize::new(1),
            limit_epoch: AtomicUsize::new(0),
            path: AtomicU8::new(DecoderPath::Starting.encoded()),
            busy_workers: AtomicUsize::new(0),
            spawned_workers: AtomicUsize::new(0),
            auxiliary_threads: AtomicUsize::new(0),
            queued_tasks: AtomicUsize::new(0),
            best_workers: AtomicUsize::new(NO_BEST_WORKER_COUNT),
            decompressed_bytes: AtomicU64::new(0),
            consumed_bytes: AtomicU64::new(0),
            member_count: AtomicU64::new(0),
            consumer_blocked: AtomicBool::new(false),
            terminal: AtomicBool::new(false),
            terminal_elapsed_nanos: AtomicU64::new(0),
            started: Instant::now(),
            limit_mutex: Mutex::new(()),
            limit_signal: Condvar::new(),
            index_enabled: AtomicBool::new(false),
            index: Mutex::new(None),
            line_counting: AtomicBool::new(false),
            line_count: AtomicU64::new(0),
            emitted_bytes: AtomicU64::new(0),
        })
    }

    /// Starts collecting a random-access index for this decode.
    ///
    /// Decode paths offer checkpoints through [`Self::offer_checkpoint`], which
    /// is a cheap atomic load away from a no-op when indexing is off.
    pub(crate) fn enable_index(&self, spacing: u64, compress_windows: bool) {
        let mut builder = IndexBuilder::new(spacing, compress_windows);
        if self.line_counting() {
            builder.enable_line_annotation();
        }
        *self.index.lock().expect("index mutex") = Some(builder);
        self.index_enabled.store(true, Ordering::Release);
    }

    /// Returns whether this decode collects an index.
    pub(crate) fn index_enabled(&self) -> bool {
        self.index_enabled.load(Ordering::Acquire)
    }

    /// Offers a checkpoint with its resolved predecessor window.
    ///
    /// An empty window marks a point needing no history. Offers may arrive in
    /// any order; the builder orders them when the index is finalized.
    pub(crate) fn offer_checkpoint(&self, checkpoint: Checkpoint, window: &[u8]) {
        if !self.index_enabled() {
            return;
        }
        if let Some(builder) = self.index.lock().expect("index mutex").as_mut() {
            builder.offer(checkpoint, window);
        }
    }

    /// Starts counting newlines in the decompressed output of this decode.
    pub(crate) fn enable_line_counting(&self) {
        self.line_counting.store(true, Ordering::Release);
    }

    /// Returns whether this decode counts newlines.
    pub(crate) fn line_counting(&self) -> bool {
        self.line_counting.load(Ordering::Acquire)
    }

    /// Records one ordered run of decompressed output.
    ///
    /// Every path funnels its output through [`crate::backend::Output`], and
    /// the implementations call this before handing bytes on. The bytes are
    /// therefore final: markers are already resolved, which is why counting
    /// happens here rather than in the workers.
    ///
    /// When an index is also being collected, checkpoints whose decompressed
    /// offset falls within this run receive their line offset here. The offer
    /// always precedes the emit of the bytes at that offset, so a checkpoint
    /// is never passed before it is known.
    pub(crate) fn note_emitted(&self, bytes: &[u8]) {
        if !self.line_counting() {
            return;
        }
        let start = self
            .emitted_bytes
            .fetch_add(bytes.len() as u64, Ordering::AcqRel);
        let lines_before = self.line_count.load(Ordering::Acquire);
        if self.index_enabled() {
            if let Some(builder) = self.index.lock().expect("index mutex").as_mut() {
                builder.note_output(start, lines_before, bytes);
            }
        }
        let newlines = bytes.iter().filter(|&&byte| byte == b'\n').count() as u64;
        self.line_count
            .store(lines_before + newlines, Ordering::Release);
    }

    /// Returns the newline count so far, or `None` when counting is off.
    pub(crate) fn line_count(&self) -> Option<u64> {
        self.line_counting()
            .then(|| self.line_count.load(Ordering::Acquire))
    }

    /// Attaches the collected index and line count to a finished report.
    pub(crate) fn attach_index(
        &self,
        mut report: crate::DecodeReport,
    ) -> Result<crate::DecodeReport, crate::DecodeError> {
        report.line_count = self.line_count();
        if let Some(index) = self.take_index(report.compressed_bytes, report.decompressed_bytes) {
            let index = index
                .map_err(|error| crate::DecodeError::input_io(0, std::io::Error::other(error)))?;
            report.index = Some(index);
        }
        Ok(report)
    }

    /// Finalizes the collected index against the verified decode sizes.
    ///
    /// Returns `None` when indexing was not requested.
    pub(crate) fn take_index(
        &self,
        compressed_bytes: u64,
        uncompressed_bytes: u64,
    ) -> Option<Result<GzipIndex, IndexError>> {
        let mut builder = self.index.lock().expect("index mutex").take()?;
        builder.finish(compressed_bytes, uncompressed_bytes, self.line_count());
        Some(builder.into_index())
    }

    fn set_worker_limit(&self, workers: usize) -> Result<(), WorkerLimitError> {
        if workers == 0 || workers > self.configured_workers {
            return Err(WorkerLimitError {
                requested: workers,
                configured: self.configured_workers,
            });
        }
        let previous = self.worker_limit.swap(workers, Ordering::Relaxed);
        if previous != workers {
            self.limit_epoch.fetch_add(1, Ordering::Relaxed);
            self.limit_signal.notify_all();
        }
        Ok(())
    }

    pub(crate) fn limit_epoch(&self) -> usize {
        self.limit_epoch.load(Ordering::Relaxed)
    }

    pub(crate) fn set_adaptive_target(&self, workers: usize) {
        let workers = workers.clamp(1, self.configured_workers);
        let previous = self.adaptive_target.swap(workers, Ordering::Relaxed);
        if previous != workers {
            self.limit_signal.notify_all();
        }
    }

    pub(crate) fn effective_worker_limit(&self) -> usize {
        let adaptive = self.adaptive_target.load(Ordering::Relaxed);
        let requested = self.worker_limit.load(Ordering::Relaxed);
        if self.consumer_blocked.load(Ordering::Relaxed) {
            1
        } else {
            adaptive.min(requested).max(1)
        }
    }

    pub(crate) fn wait_for_limit_change(&self, timeout: std::time::Duration) {
        let guard = self
            .limit_mutex
            .lock()
            .expect("runtime limit mutex poisoned");
        let _guard = self
            .limit_signal
            .wait_timeout(guard, timeout)
            .expect("runtime limit mutex poisoned");
    }

    pub(crate) fn notify_limit_waiters(&self) {
        self.limit_signal.notify_all();
    }

    pub(crate) fn set_path(&self, path: DecoderPath) {
        self.path.store(path.encoded(), Ordering::Relaxed);
    }

    pub(crate) fn register_worker(self: &Arc<Self>) -> ThreadRegistration {
        self.spawned_workers.fetch_add(1, Ordering::Relaxed);
        ThreadRegistration {
            state: Arc::clone(self),
            auxiliary: false,
        }
    }

    pub(crate) fn register_auxiliary(self: &Arc<Self>, _kind: AuxiliaryKind) -> ThreadRegistration {
        self.auxiliary_threads.fetch_add(1, Ordering::Relaxed);
        ThreadRegistration {
            state: Arc::clone(self),
            auxiliary: true,
        }
    }

    pub(crate) fn begin_task(&self) -> BusyRegistration<'_> {
        self.busy_workers.fetch_add(1, Ordering::Relaxed);
        BusyRegistration(self)
    }

    pub(crate) fn set_queued_tasks(&self, count: usize) {
        self.queued_tasks.store(count, Ordering::Relaxed);
    }

    pub(crate) fn set_best_workers(&self, workers: Option<usize>) {
        self.best_workers
            .store(workers.unwrap_or(NO_BEST_WORKER_COUNT), Ordering::Relaxed);
    }

    pub(crate) fn add_decompressed_bytes(&self, count: usize) {
        self.decompressed_bytes
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(crate) fn add_consumed_bytes(&self, count: usize) {
        self.consumed_bytes
            .fetch_add(count as u64, Ordering::Relaxed);
    }

    pub(crate) fn set_member_count(&self, count: u64) {
        self.member_count.store(count, Ordering::Relaxed);
    }

    pub(crate) fn set_consumer_blocked(&self, blocked: bool) {
        let previous = self.consumer_blocked.swap(blocked, Ordering::Relaxed);
        if previous != blocked {
            self.limit_signal.notify_all();
        }
    }

    pub(crate) fn mark_terminal(&self) {
        let elapsed_nanos = u64::try_from(self.started.elapsed().as_nanos())
            .unwrap_or(u64::MAX)
            .max(1);
        let _ = self.terminal_elapsed_nanos.compare_exchange(
            0,
            elapsed_nanos,
            Ordering::Relaxed,
            Ordering::Relaxed,
        );
        self.terminal.store(true, Ordering::Relaxed);
        self.consumer_blocked.store(false, Ordering::Relaxed);
        self.queued_tasks.store(0, Ordering::Relaxed);
        self.notify_limit_waiters();
    }

    fn stats(&self) -> DecoderStats {
        let path = DecoderPath::from_encoded(self.path.load(Ordering::Relaxed));
        let configured_workers = self.configured_workers;
        let worker_limit = self.worker_limit.load(Ordering::Relaxed);
        let adaptive_target = self.adaptive_target.load(Ordering::Relaxed);
        let consumer_blocked = self.consumer_blocked.load(Ordering::Relaxed);
        let terminal = self.terminal.load(Ordering::Relaxed);
        let active_workers = if terminal {
            0
        } else if consumer_blocked {
            1
        } else {
            adaptive_target.min(worker_limit).max(1)
        };
        let busy_workers = self.busy_workers.load(Ordering::Relaxed);
        let spawned_workers = self.spawned_workers.load(Ordering::Relaxed);
        let auxiliary_threads = self.auxiliary_threads.load(Ordering::Relaxed);
        let queued_tasks = self.queued_tasks.load(Ordering::Relaxed);
        let best_workers = match self.best_workers.load(Ordering::Relaxed) {
            NO_BEST_WORKER_COUNT => None,
            workers => Some(workers),
        };
        let decompressed_bytes = self.decompressed_bytes.load(Ordering::Relaxed);
        let consumed_bytes = self.consumed_bytes.load(Ordering::Relaxed);
        let member_count = self.member_count.load(Ordering::Relaxed);
        let terminal_elapsed_nanos = self.terminal_elapsed_nanos.load(Ordering::Relaxed);
        let elapsed = if terminal_elapsed_nanos == 0 {
            self.started.elapsed().as_secs_f64()
        } else {
            terminal_elapsed_nanos as f64 / 1_000_000_000.0
        }
        .max(f64::MIN_POSITIVE);
        let pressure = if terminal {
            DecoderPressure::Finished
        } else if consumer_blocked {
            let idle = spawned_workers.saturating_sub(busy_workers);
            let idle_worker_fraction = if spawned_workers == 0 {
                1.0
            } else {
                idle as f32 / spawned_workers as f32
            };
            DecoderPressure::ConsumerBound {
                idle_worker_fraction,
            }
        } else if queued_tasks != 0 && busy_workers >= active_workers {
            DecoderPressure::DecoderBound { queued_tasks }
        } else if let Some(at_workers) = best_workers {
            DecoderPressure::Converged { at_workers }
        } else if busy_workers == 0 && queued_tasks == 0 {
            DecoderPressure::Idle
        } else {
            DecoderPressure::Starting
        };

        DecoderStats {
            path,
            configured_workers,
            worker_limit,
            active_workers,
            busy_workers,
            spawned_workers,
            auxiliary_threads,
            best_workers,
            decompressed_bytes,
            consumed_bytes,
            member_count,
            decode_throughput_bps: decompressed_bytes as f64 / elapsed,
            consumer_throughput_bps: consumed_bytes as f64 / elapsed,
            pressure,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DecoderHandle, DecoderPressure, RuntimeState};

    #[test]
    fn runtime_limits_are_validated_and_visible() {
        let state = RuntimeState::new(8);
        let handle = DecoderHandle::new(state);
        handle.set_worker_limit(3).unwrap();
        let stats = handle.stats();
        assert_eq!(stats.configured_workers, 8);
        assert_eq!(stats.worker_limit, 3);
        assert_eq!(stats.active_workers, 1);
        assert_eq!(handle.set_worker_limit(0).unwrap_err().requested(), 0);
        assert_eq!(handle.set_worker_limit(9).unwrap_err().configured(), 8);
    }

    #[test]
    fn consumer_backpressure_caps_admission() {
        let state = RuntimeState::new(8);
        state.set_adaptive_target(6);
        let worker = state.register_worker();
        let _busy = state.begin_task();
        state.set_consumer_blocked(true);
        let stats = DecoderHandle::new(Arc::clone(&state)).stats();
        assert_eq!(stats.active_workers, 1);
        assert!(matches!(
            stats.pressure,
            DecoderPressure::ConsumerBound { .. }
        ));
        drop(worker);
    }

    use std::sync::Arc;
}
