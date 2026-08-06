//! Lock-free decoder telemetry and runtime worker-budget control.

use crate::pool::{DecoderPool, PoolMember, PoolPermit};

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
    /// Serial container inflation on the caller or coordinator thread.
    Sequential,
    /// Direct copying of independently indexed stored DEFLATE blocks.
    Stored,
    /// Independent inflation of densely spaced ordinary gzip members.
    DenseMembers,
    /// Comparing exact and speculative service rates before path selection.
    MarkerAdmission,
    /// The rapidgzip marker/window pipeline for gzip, zlib, or raw DEFLATE.
    MarkerWindow,
    /// Independent inflation of indexed BGZF blocks.
    Bgzf,
    /// Plain zlib-rs inflation resumed from caller-supplied index checkpoints.
    IndexedParallel,
}

impl DecoderPath {
    const fn encoded(self) -> u8 {
        match self {
            Self::Starting => 0,
            Self::Sequential => 1,
            Self::Stored => 2,
            Self::DenseMembers => 3,
            Self::MarkerAdmission => 4,
            Self::MarkerWindow => 5,
            Self::Bgzf => 6,
            Self::IndexedParallel => 7,
        }
    }

    const fn from_encoded(value: u8) -> Self {
        match value {
            1 => Self::Sequential,
            2 => Self::Stored,
            3 => Self::DenseMembers,
            4 => Self::MarkerAdmission,
            5 => Self::MarkerWindow,
            6 => Self::Bgzf,
            7 => Self::IndexedParallel,
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
/// transactionally consistent record of a single instant. Every counter is a
/// current value rather than a lifetime high-water mark.
///
/// Worker fields form the following hierarchy:
///
/// 1. [`Self::configured_workers`] is immutable per-operation headroom;
/// 2. [`Self::worker_limit`] is the application's hard ceiling;
/// 3. [`Self::requested_workers`] is an optional floor under adaptive demand;
/// 4. [`Self::desired_workers`] is the resulting target before pool contention;
/// 5. [`Self::active_workers`] is the target after an optional pool grant;
/// 6. [`Self::busy_workers`] counts tasks executing now; and
/// 7. [`Self::spawned_workers`] counts live decoder-worker OS threads.
///
/// Available tasks, path selection, worker startup/retirement, and handoff
/// backpressure mean these values need not be equal. For example, a final
/// `spawned_workers` sample cannot prove the maximum width used during an
/// earlier adaptive probe.
#[derive(Clone, Copy, Debug, PartialEq)]
#[non_exhaustive]
pub struct DecoderStats {
    /// Decoder implementation selected for the input.
    pub path: DecoderPath,
    /// Immutable maximum worker budget supplied to the builder.
    pub configured_workers: usize,
    /// Current application-controlled ceiling on decoder workers.
    pub worker_limit: usize,
    /// Persistent application growth request before hard ceilings.
    ///
    /// `None` leaves the target entirely under adaptive control. `Some(n)` is
    /// set by [`DecoderHandle::request_workers`] and can remain above the
    /// current [`Self::worker_limit`] so a later ceiling increase takes effect
    /// without repeating the request.
    pub requested_workers: Option<usize>,
    /// Worker count requested after adaptive and application policy but before
    /// shared-pool contention.
    pub desired_workers: usize,
    /// Effective decode-concurrency target after adaptive, application, and
    /// shared-pool limits.
    ///
    /// This is an admission target, not the number of tasks currently executing
    /// or the number of live operating-system threads.
    pub active_workers: usize,
    /// Approximate number of decoder workers, or the synchronous sequential
    /// caller, currently decoding.
    pub busy_workers: usize,
    /// Live decoder-worker operating-system threads.
    ///
    /// This can temporarily exceed [`Self::active_workers`] while a lower limit
    /// takes effect. In particular, a worker that owns a completed result may
    /// remain parked on a bounded handoff until output advances or the decode is
    /// cancelled.
    pub spawned_workers: usize,
    /// Live coordinator and scanner operating-system threads.
    pub auxiliary_threads: usize,
    /// Empirically selected worker count, once calibration has completed.
    ///
    /// This remains the controller's unconstrained choice even when an
    /// explicit request or shared-pool grant changes actual admission.
    pub best_workers: Option<usize>,
    /// Decompressed bytes emitted into the final output handoff.
    pub decompressed_bytes: u64,
    /// Decompressed bytes returned through [`std::io::Read`].
    pub consumed_bytes: u64,
    /// Completed framing units: gzip members, or one zlib/raw stream.
    pub member_count: u64,
    /// Average decoded-output production rate since decoder startup.
    pub decode_throughput_bps: f64,
    /// Average [`std::io::Read`] consumption rate since decoder startup.
    pub consumer_throughput_bps: f64,
    /// Current high-level decoder pressure classification.
    pub pressure: DecoderPressure,
    /// Whether a shared pool grants less than the desired width or at least
    /// one task is waiting for a shared execution slot.
    pub pool_limited: bool,
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

/// Invalid persistent decoder growth request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct WorkerRequestError {
    requested: usize,
    configured: usize,
}

impl WorkerRequestError {
    /// Rejected worker count.
    pub const fn requested(self) -> usize {
        self.requested
    }

    /// Maximum worker count configured for the decoder.
    pub const fn configured(self) -> usize {
        self.configured
    }
}

impl Display for WorkerRequestError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "worker request {} is outside 1..={}",
            self.requested, self.configured
        )
    }
}

impl Error for WorkerRequestError {}

/// Cloneable telemetry and control handle for a running [`crate::DecoderReader`].
///
/// The handle remains usable after the reader has moved into another component
/// such as a FASTQ parser. Cloning a handle does not create decoder workers and
/// does not keep the decode alive after its reader and coordinator finish.
/// Retained handles continue to expose the terminal snapshot.
///
/// # Examples
///
/// ```no_run
/// use rapidgzip_core::{Decoder, DecoderPressure};
/// use std::io::Read;
///
/// # fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let reader = Decoder::builder()
///     .decoder_threads(32)
///     .build()?
///     .open("reads.fastq.gz")?;
/// let control = reader.handle();
/// let mut parser_input: Box<dyn Read + Send> = Box::new(reader);
///
/// // Existing callers can impose only a hard ceiling.
/// control.set_worker_limit(8)?;
/// // A scheduler can also ask adaptive control to grow up to that ceiling.
/// control.request_workers(8)?;
/// if matches!(control.stats().pressure, DecoderPressure::ConsumerBound { .. }) {
///     control.set_worker_limit(2)?;
/// }
/// std::io::copy(&mut parser_input, &mut std::io::sink())?;
/// # Ok(())
/// # }
/// ```
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
    ///
    /// Sampling does not coordinate with worker threads and is suitable for a
    /// supervisory polling loop. See [`DecoderStats`] for field relationships
    /// and snapshot limitations.
    pub fn stats(&self) -> DecoderStats {
        self.state.stats()
    }

    /// Changes the maximum number of decoder workers that may accept work.
    ///
    /// The method is nonblocking. Workers already executing a task finish it and
    /// publish any completed result they own before retiring. A worker whose
    /// bounded result handoff is blocked therefore remains live until output
    /// advances or the decode is cancelled. Raising the limit allows the
    /// coordinator to create replacement workers lazily when useful work is
    /// available. This method changes permission, not adaptive demand: raising
    /// the ceiling alone does not force the decoder to use more workers. Use
    /// [`Self::request_workers`] when the application has made that allocation
    /// decision explicitly.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerLimitError`] when `workers` is zero or exceeds the
    /// immutable worker budget supplied to [`crate::DecoderBuilder`].
    pub fn set_worker_limit(&self, workers: usize) -> Result<(), WorkerLimitError> {
        self.state.set_worker_limit(workers)
    }

    /// Requests that adaptive control make at least `workers` useful when work
    /// and shared-pool capacity are available.
    ///
    /// The request is persistent and is distinct from the hard ceiling set by
    /// [`Self::set_worker_limit`]. A request above the current hard ceiling is
    /// retained, so raising that ceiling later can grow without another
    /// request. Consumer backpressure, insufficient tasks, a sequential path,
    /// and contention in an attached [`DecoderPool`] can all keep actual
    /// concurrency below the request. Worker threads are still created lazily;
    /// this method does not synchronously spawn or reserve `workers` threads.
    ///
    /// # Errors
    ///
    /// Returns [`WorkerRequestError`] for zero or a value above the immutable
    /// worker budget supplied to [`crate::DecoderBuilder`].
    pub fn request_workers(&self, workers: usize) -> Result<(), WorkerRequestError> {
        self.state.request_workers(workers)
    }

    /// Clears a persistent growth request and returns targeting entirely to
    /// the decoder's adaptive controller.
    ///
    /// The hard ceiling last set through [`Self::set_worker_limit`] is
    /// unchanged.
    pub fn clear_worker_request(&self) {
        self.state.clear_worker_request();
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
        if let Some(member) = &self.state.pool_member {
            if self.auxiliary {
                member.unregister_auxiliary();
            } else {
                member.unregister_worker();
            }
        }
    }
}

pub(crate) struct BusyRegistration<'a> {
    state: &'a RuntimeState,
    _pool_permit: Option<PoolPermit<'a>>,
}

impl Drop for BusyRegistration<'_> {
    fn drop(&mut self) {
        self.state.busy_workers.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A worker-local execution slot that can be retained across nonblocking
/// handoffs and immediately available work.
///
/// The caller must call [`Self::release`] before waiting or performing an
/// operation that may block. This amortizes shared-pool permit traffic for
/// very small tasks without allowing an idle worker to reserve capacity.
pub(crate) struct ReusableTaskSlot<'a> {
    state: &'a RuntimeState,
    pool_permit: Option<PoolPermit<'a>>,
    executing: bool,
}

impl ReusableTaskSlot<'_> {
    pub(crate) fn begin(&mut self) {
        debug_assert!(!self.executing);
        if self.pool_permit.is_none()
            && let Some(member) = &self.state.pool_member
        {
            self.pool_permit = Some(member.acquire());
        }
        self.state.busy_workers.fetch_add(1, Ordering::Relaxed);
        self.executing = true;
    }

    pub(crate) fn end(&mut self) {
        debug_assert!(self.executing);
        self.state.busy_workers.fetch_sub(1, Ordering::Relaxed);
        self.executing = false;
    }

    pub(crate) fn release(&mut self) {
        if self.executing {
            self.end();
        }
        self.pool_permit.take();
    }
}

impl Drop for ReusableTaskSlot<'_> {
    fn drop(&mut self) {
        self.release();
    }
}

/// State shared by the reader, coordinator, scanner, and decoder workers.
pub(crate) struct RuntimeState {
    configured_workers: usize,
    worker_limit: AtomicUsize,
    requested_workers: AtomicUsize,
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
    pool_member: Option<PoolMember>,
}

impl RuntimeState {
    pub(crate) fn new(configured_workers: usize, decoder_pool: Option<&DecoderPool>) -> Arc<Self> {
        Arc::new(Self {
            configured_workers,
            worker_limit: AtomicUsize::new(configured_workers),
            requested_workers: AtomicUsize::new(0),
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
            pool_member: decoder_pool.map(|pool| pool.state.register_decoder()),
        })
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
            self.refresh_pool_demand();
            self.limit_signal.notify_all();
        }
        Ok(())
    }

    pub(crate) fn limit_epoch(&self) -> usize {
        self.limit_epoch.load(Ordering::Relaxed)
    }

    pub(crate) fn pool_limit_epoch(&self) -> usize {
        self.pool_member.as_ref().map_or(0, PoolMember::limit_epoch)
    }

    fn request_workers(&self, workers: usize) -> Result<(), WorkerRequestError> {
        if workers == 0 || workers > self.configured_workers {
            return Err(WorkerRequestError {
                requested: workers,
                configured: self.configured_workers,
            });
        }
        let previous = self.requested_workers.swap(workers, Ordering::Relaxed);
        if previous != workers {
            self.limit_epoch.fetch_add(1, Ordering::Relaxed);
            self.refresh_pool_demand();
            self.limit_signal.notify_all();
        }
        Ok(())
    }

    fn clear_worker_request(&self) {
        let previous = self.requested_workers.swap(0, Ordering::Relaxed);
        if previous != 0 {
            self.limit_epoch.fetch_add(1, Ordering::Relaxed);
            self.refresh_pool_demand();
            self.limit_signal.notify_all();
        }
    }

    /// Current application-controlled ceiling before adaptive throttling.
    pub(crate) fn application_worker_limit(&self) -> usize {
        self.worker_limit.load(Ordering::Relaxed)
    }

    pub(crate) fn request_saturates_configured_workers(&self) -> bool {
        self.requested_workers.load(Ordering::Relaxed) >= self.configured_workers
    }

    pub(crate) fn set_adaptive_target(&self, workers: usize) {
        let workers = workers.clamp(1, self.configured_workers);
        let previous = self.adaptive_target.swap(workers, Ordering::Relaxed);
        if previous != workers || self.pool_member.is_some() {
            self.refresh_pool_demand();
        }
        if previous != workers {
            self.limit_signal.notify_all();
        }
    }

    fn desired_worker_limit(&self) -> usize {
        let adaptive = self.adaptive_target.load(Ordering::Relaxed);
        let requested_floor = self.requested_workers.load(Ordering::Relaxed);
        let requested = self.worker_limit.load(Ordering::Relaxed);
        if self.consumer_blocked.load(Ordering::Relaxed) {
            1
        } else {
            adaptive.max(requested_floor).min(requested).max(1)
        }
    }

    /// Long-lived demand published to the shared allocator.
    ///
    /// Short consumer-channel stalls still cap this decoder locally through
    /// `desired_worker_limit`, but they must not repeatedly redistribute spawn
    /// allowances between peer decoders. Completion, queue idleness, adaptive
    /// policy, and explicit application controls remain allocation signals.
    fn pool_demand_worker_limit(&self) -> usize {
        let adaptive = self.adaptive_target.load(Ordering::Relaxed);
        let requested_floor = self.requested_workers.load(Ordering::Relaxed);
        let requested = self.worker_limit.load(Ordering::Relaxed);
        adaptive.max(requested_floor).min(requested).max(1)
    }

    fn refresh_pool_demand(&self) {
        if let Some(member) = &self.pool_member {
            let desired = if self.terminal.load(Ordering::Relaxed) {
                0
            } else {
                self.pool_demand_worker_limit()
            };
            member.set_desired_workers(desired);
        }
    }

    pub(crate) fn effective_worker_limit(&self) -> usize {
        let desired = self.desired_worker_limit();
        self.pool_member.as_ref().map_or(desired, |member| {
            // When the pool has fewer slots than runnable decoders, retain one
            // waiting worker so a decoder can enter the pool's fair FIFO rather
            // than deadlocking before it has a task representative.
            desired.min(member.granted_workers().max(1))
        })
    }

    /// Dynamic decoded-result backlog for a shared positional reader.
    ///
    /// The configured channel remains the physical maximum. When the decoder
    /// owns less than the complete live pool, a smaller logical window prevents
    /// broad per-file worker headroom from multiplying decoded buffers. The
    /// returned booleans are the high- and low-watermark conditions.
    pub(crate) fn shared_output_backlog_pressure(&self, next_bytes: usize) -> (bool, bool) {
        let Some(member) = &self.pool_member else {
            return (false, true);
        };
        if next_bytes == 0 {
            return (false, true);
        }
        let granted_workers = member.granted_workers().max(1);
        if member.attached_decoders() <= 1 || granted_workers >= member.worker_limit() {
            return (false, true);
        }
        let chunk_limit = granted_workers.saturating_add(2) as u64;
        let next_bytes = next_bytes as u64;
        let produced = self.decompressed_bytes.load(Ordering::Relaxed);
        let consumed = self.consumed_bytes.load(Ordering::Relaxed);
        let backlog = produced.saturating_sub(consumed);
        (
            backlog.saturating_add(next_bytes) > next_bytes.saturating_mul(chunk_limit),
            backlog <= next_bytes.saturating_mul(granted_workers as u64),
        )
    }

    /// Immutable worker width used to decide whether future parallel growth is
    /// worthwhile. A shared pool contributes its configured maximum, not its
    /// transient runtime throttle.
    pub(crate) fn admission_worker_budget(&self) -> usize {
        self.pool_member
            .as_ref()
            .map_or(self.configured_workers, |member| {
                self.configured_workers.min(member.configured_workers())
            })
    }

    pub(crate) const fn uses_shared_pool(&self) -> bool {
        self.pool_member.is_some()
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
        if let Some(member) = &self.pool_member {
            member.register_worker();
        }
        ThreadRegistration {
            state: Arc::clone(self),
            auxiliary: false,
        }
    }

    pub(crate) fn register_auxiliary(self: &Arc<Self>, _kind: AuxiliaryKind) -> ThreadRegistration {
        self.auxiliary_threads.fetch_add(1, Ordering::Relaxed);
        if let Some(member) = &self.pool_member {
            member.register_auxiliary();
        }
        ThreadRegistration {
            state: Arc::clone(self),
            auxiliary: true,
        }
    }

    pub(crate) fn begin_task(&self) -> BusyRegistration<'_> {
        let pool_permit = self.pool_member.as_ref().map(PoolMember::acquire);
        self.busy_workers.fetch_add(1, Ordering::Relaxed);
        BusyRegistration {
            state: self,
            _pool_permit: pool_permit,
        }
    }

    /// Accounts CPU work that exists only to make shared-pool scheduling
    /// complete. Private decoders retain their pre-pool hot path with no extra
    /// atomics or guard construction.
    pub(crate) fn begin_pool_task(&self) -> Option<BusyRegistration<'_>> {
        self.pool_member.as_ref()?;
        Some(self.begin_task())
    }

    /// Retains a private path's coarse task accounting while a shared decoder
    /// accounts only the non-blocking CPU regions inside that path.
    pub(crate) fn begin_private_task(&self) -> Option<BusyRegistration<'_>> {
        self.pool_member.is_none().then(|| self.begin_task())
    }

    pub(crate) fn reusable_task_slot(&self) -> ReusableTaskSlot<'_> {
        ReusableTaskSlot {
            state: self,
            pool_permit: None,
            executing: false,
        }
    }

    pub(crate) fn set_queued_tasks(&self, count: usize) {
        if let Some(member) = &self.pool_member {
            member.set_queued_tasks(count);
        } else {
            self.queued_tasks.store(count, Ordering::Relaxed);
        }
    }

    fn queued_tasks(&self) -> usize {
        self.pool_member.as_ref().map_or_else(
            || self.queued_tasks.load(Ordering::Relaxed),
            PoolMember::queued_tasks,
        )
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
            self.refresh_pool_demand();
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
        if let Some(member) = &self.pool_member {
            member.set_queued_tasks(0);
            member.set_desired_workers(0);
        } else {
            self.queued_tasks.store(0, Ordering::Relaxed);
        }
        self.notify_limit_waiters();
    }

    /// Removes a completed operation from aggregate pool membership while
    /// allowing retained telemetry handles to keep this runtime snapshot live.
    pub(crate) fn detach_pool(&self) {
        if let Some(member) = &self.pool_member {
            member.detach();
        }
    }

    fn stats(&self) -> DecoderStats {
        let path = DecoderPath::from_encoded(self.path.load(Ordering::Relaxed));
        let configured_workers = self.configured_workers;
        let worker_limit = self.worker_limit.load(Ordering::Relaxed);
        let requested_workers = match self.requested_workers.load(Ordering::Relaxed) {
            0 => None,
            workers => Some(workers),
        };
        let consumer_blocked = self.consumer_blocked.load(Ordering::Relaxed);
        let terminal = self.terminal.load(Ordering::Relaxed);
        let desired_workers = if terminal {
            0
        } else {
            self.desired_worker_limit()
        };
        let active_workers = if terminal {
            0
        } else {
            self.pool_member.as_ref().map_or(desired_workers, |member| {
                desired_workers.min(member.granted_workers().max(1))
            })
        };
        let busy_workers = self.busy_workers.load(Ordering::Relaxed);
        let spawned_workers = self.spawned_workers.load(Ordering::Relaxed);
        let auxiliary_threads = self.auxiliary_threads.load(Ordering::Relaxed);
        let queued_tasks = self.queued_tasks();
        let pool_limited = self.pool_member.as_ref().is_some_and(|member| {
            member.waiting_workers() != 0 || member.granted_workers() < desired_workers
        });
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
            requested_workers,
            desired_workers,
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
            pool_limited,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{DecoderHandle, DecoderPath, DecoderPressure, RuntimeState};
    use crate::DecoderPool;

    #[test]
    fn runtime_limits_are_validated_and_visible() {
        let state = RuntimeState::new(8, None);
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
        let state = RuntimeState::new(8, None);
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

    #[test]
    fn marker_admission_path_is_visible_in_telemetry() {
        let state = RuntimeState::new(4, None);
        state.set_path(DecoderPath::MarkerAdmission);
        assert_eq!(
            DecoderHandle::new(state).stats().path,
            DecoderPath::MarkerAdmission
        );
    }

    #[test]
    fn growth_request_is_a_floor_below_the_hard_ceiling() {
        let state = RuntimeState::new(8, None);
        state.set_adaptive_target(2);
        let handle = DecoderHandle::new(Arc::clone(&state));
        handle.request_workers(6).unwrap();
        assert_eq!(handle.stats().requested_workers, Some(6));
        assert_eq!(handle.stats().desired_workers, 6);

        handle.set_worker_limit(4).unwrap();
        assert_eq!(handle.stats().desired_workers, 4);
        handle.clear_worker_request();
        assert_eq!(handle.stats().requested_workers, None);
        assert_eq!(handle.stats().desired_workers, 2);
    }

    #[test]
    fn shared_pool_contention_is_visible_per_decoder() {
        let pool = DecoderPool::builder().workers(2).build().unwrap();
        let state = RuntimeState::new(8, Some(&pool));
        state.set_adaptive_target(8);
        state.set_queued_tasks(8);
        let stats = DecoderHandle::new(state).stats();
        assert_eq!(stats.desired_workers, 8);
        assert_eq!(stats.active_workers, 2);
        assert!(stats.pool_limited);
    }

    #[test]
    fn shared_output_window_activates_only_for_a_divided_pool() {
        let pool = DecoderPool::builder().workers(4).build().unwrap();
        let first = RuntimeState::new(4, Some(&pool));
        first.add_decompressed_bytes(500);
        assert_eq!(first.shared_output_backlog_pressure(100), (false, true));

        let _second = RuntimeState::new(4, Some(&pool));
        assert_eq!(first.shared_output_backlog_pressure(100), (true, false));
        first.add_consumed_bytes(300);
        assert_eq!(first.shared_output_backlog_pressure(100), (false, true));
    }

    use std::sync::Arc;
}
