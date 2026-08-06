//! Process-wide decode execution-slot allocation.

use std::collections::VecDeque;
use std::error::Error;
use std::fmt::{self, Display, Formatter};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, Weak};

/// Invalid [`DecoderPool`] configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecoderPoolConfigError(&'static str);

impl Display for DecoderPoolConfigError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.0)
    }
}

impl Error for DecoderPoolConfigError {}

/// Invalid runtime worker limit for a [`DecoderPool`].
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct DecoderPoolLimitError {
    requested: usize,
    configured: usize,
}

impl DecoderPoolLimitError {
    /// Rejected worker count.
    pub const fn requested(self) -> usize {
        self.requested
    }

    /// Immutable maximum configured for the pool.
    pub const fn configured(self) -> usize {
        self.configured
    }
}

impl Display for DecoderPoolLimitError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "pool worker limit {} is outside 1..={}",
            self.requested, self.configured
        )
    }
}

impl Error for DecoderPoolLimitError {}

/// Approximate process-wide snapshot of a [`DecoderPool`].
///
/// Fields are sampled independently. The member list is held briefly while
/// aggregate queue depth is summed, but the snapshot is not a transactionally
/// consistent view of a single instant.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub struct DecoderPoolStats {
    /// Immutable maximum number of simultaneous decode execution slots.
    pub configured_workers: usize,
    /// Current application-controlled pool ceiling.
    pub worker_limit: usize,
    /// Decode tasks currently holding a pool execution slot.
    pub busy_workers: usize,
    /// Worker allowances currently distributed across attached decoders.
    pub active_workers: usize,
    /// Live decoder-worker operating-system threads attached to the pool.
    pub spawned_workers: usize,
    /// Live coordinator and scanner threads attached to the pool.
    pub auxiliary_threads: usize,
    /// Approximate runnable tasks across attached decoders.
    pub queued_tasks: usize,
    /// Live decoder operations attached to the pool.
    pub attached_decoders: usize,
    /// Attached decoders with queued, executing, or pool-waiting work.
    pub runnable_decoders: usize,
    /// Attached decoders with at least one task waiting for a pool slot.
    pub waiting_decoders: usize,
}

/// Cloneable process-wide budget shared by multiple decoders.
///
/// The pool controls decode *execution slots*. It deliberately does not expose
/// or promise a permanent one-to-one mapping between slots and operating-system
/// threads. Each decoder retains its format-specific worker loop and scratch
/// storage, while CPU-intensive tasks acquire a fair pool slot. A task releases
/// its slot before any result or reader-channel operation that would block, so
/// a slow consumer cannot occupy the shared decode budget.
#[derive(Clone)]
pub struct DecoderPool {
    pub(crate) state: Arc<PoolState>,
}

impl fmt::Debug for DecoderPool {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("DecoderPool")
            .field("stats", &self.stats())
            .finish()
    }
}

#[bon::bon]
impl DecoderPool {
    /// Creates a process-wide decoder pool.
    ///
    /// Use [`DecoderPool::builder`] for named configuration.
    ///
    /// # Errors
    ///
    /// Returns [`DecoderPoolConfigError`] when `workers` is zero or
    /// `initial_worker_limit` is zero or greater than `workers`.
    #[builder]
    pub fn new(
        /// Immutable maximum number of simultaneous decode execution slots.
        workers: usize,
        /// Initial runtime ceiling; omitted means `workers`.
        initial_worker_limit: Option<usize>,
    ) -> Result<Self, DecoderPoolConfigError> {
        if workers == 0 {
            return Err(DecoderPoolConfigError("pool workers must be non-zero"));
        }
        let initial_worker_limit = initial_worker_limit.unwrap_or(workers);
        if initial_worker_limit == 0 || initial_worker_limit > workers {
            return Err(DecoderPoolConfigError(
                "initial_worker_limit must be within 1..=workers",
            ));
        }
        Ok(Self {
            state: Arc::new(PoolState::new(workers, initial_worker_limit)),
        })
    }

    /// Returns an approximate snapshot of aggregate pool activity.
    ///
    /// The small member registry is locked briefly to sum per-decoder queue
    /// depths and allowances. No decoder task or pool permit is held while
    /// waiting for this snapshot.
    pub fn stats(&self) -> DecoderPoolStats {
        self.state.stats()
    }

    /// Changes the maximum number of decode tasks that may execute at once.
    ///
    /// The operation is nonblocking. Existing tasks retain their slots until
    /// they finish CPU-intensive work; no new task is admitted above the lower
    /// limit. Raising the limit wakes queued decoders immediately.
    ///
    /// # Errors
    ///
    /// Returns [`DecoderPoolLimitError`] for zero or a value above the pool's
    /// immutable configured maximum.
    pub fn set_worker_limit(&self, workers: usize) -> Result<(), DecoderPoolLimitError> {
        self.state.set_worker_limit(workers)
    }
}

pub(crate) struct PoolState {
    configured_workers: usize,
    worker_limit: AtomicUsize,
    limit_epoch: AtomicUsize,
    busy_workers: AtomicUsize,
    spawned_workers: AtomicUsize,
    auxiliary_threads: AtomicUsize,
    attached_decoders: AtomicUsize,
    runnable_decoders: AtomicUsize,
    waiting_decoders: AtomicUsize,
    next_ticket: AtomicU64,
    scheduler: Mutex<PoolScheduler>,
    signal: Condvar,
}

impl PoolState {
    fn new(configured_workers: usize, worker_limit: usize) -> Self {
        Self {
            configured_workers,
            worker_limit: AtomicUsize::new(worker_limit),
            limit_epoch: AtomicUsize::new(0),
            busy_workers: AtomicUsize::new(0),
            spawned_workers: AtomicUsize::new(0),
            auxiliary_threads: AtomicUsize::new(0),
            attached_decoders: AtomicUsize::new(0),
            runnable_decoders: AtomicUsize::new(0),
            waiting_decoders: AtomicUsize::new(0),
            next_ticket: AtomicU64::new(0),
            scheduler: Mutex::new(PoolScheduler::default()),
            signal: Condvar::new(),
        }
    }

    pub(crate) const fn configured_workers(&self) -> usize {
        self.configured_workers
    }

    fn set_worker_limit(&self, workers: usize) -> Result<(), DecoderPoolLimitError> {
        if workers == 0 || workers > self.configured_workers {
            return Err(DecoderPoolLimitError {
                requested: workers,
                configured: self.configured_workers,
            });
        }
        let previous = self.worker_limit.swap(workers, Ordering::Release);
        if previous != workers {
            self.limit_epoch.fetch_add(1, Ordering::Relaxed);
            let mut scheduler = self.scheduler.lock().expect("decoder pool mutex poisoned");
            self.rebalance(&mut scheduler, true);
            self.signal.notify_all();
        }
        Ok(())
    }

    fn try_acquire_fast(&self) -> bool {
        if self.waiting_decoders.load(Ordering::Acquire) != 0 {
            return false;
        }
        let mut busy = self.busy_workers.load(Ordering::Relaxed);
        loop {
            let limit = self.worker_limit.load(Ordering::Acquire);
            if busy >= limit {
                return false;
            }
            match self.busy_workers.compare_exchange_weak(
                busy,
                busy + 1,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => return true,
                Err(observed) => busy = observed,
            }
        }
    }

    fn acquire<'a>(&'a self, member: &'a PoolMemberState) -> PoolPermit<'a> {
        if self.try_acquire_fast() {
            let previous_busy = member.busy_workers.fetch_add(1, Ordering::Relaxed);
            if previous_busy == 0 && !member.runnable.load(Ordering::Acquire) {
                self.refresh_runnable(member);
            }
            return PoolPermit { pool: self, member };
        }

        let ticket = self.next_ticket.fetch_add(1, Ordering::Relaxed);
        let first_for_decoder = member.waiting_workers.fetch_add(1, Ordering::AcqRel) == 0;
        if first_for_decoder {
            self.waiting_decoders.fetch_add(1, Ordering::Relaxed);
            if !member.runnable.load(Ordering::Acquire) {
                self.refresh_runnable(member);
            }
        }
        let mut scheduler = self.scheduler.lock().expect("decoder pool mutex poisoned");
        scheduler.waiters.push_back(ticket);
        loop {
            let is_front = scheduler
                .waiters
                .front()
                .is_some_and(|front| *front == ticket);
            let busy = self.busy_workers.load(Ordering::Relaxed);
            let limit = self.worker_limit.load(Ordering::Acquire);
            if is_front && busy < limit {
                let popped = scheduler.waiters.pop_front();
                debug_assert_eq!(popped, Some(ticket));
                self.busy_workers.fetch_add(1, Ordering::AcqRel);
                // Publish the busy task before removing its waiting state so
                // the member remains continuously runnable to the allocator.
                member.busy_workers.fetch_add(1, Ordering::Relaxed);
                if member.waiting_workers.fetch_sub(1, Ordering::AcqRel) == 1 {
                    self.waiting_decoders.fetch_sub(1, Ordering::Relaxed);
                }
                self.signal.notify_all();
                return PoolPermit { pool: self, member };
            }
            scheduler = self
                .signal
                .wait(scheduler)
                .expect("decoder pool mutex poisoned");
        }
    }

    fn release(&self, member: &PoolMemberState) {
        let previous_busy = member.busy_workers.fetch_sub(1, Ordering::Relaxed);
        self.busy_workers.fetch_sub(1, Ordering::AcqRel);
        if previous_busy == 1
            && member.queued_tasks.load(Ordering::Relaxed) == 0
            && member.waiting_workers.load(Ordering::Relaxed) == 0
        {
            self.refresh_runnable(member);
        }
        self.signal.notify_all();
    }

    pub(crate) fn register_decoder(self: &Arc<Self>) -> PoolMember {
        self.attached_decoders.fetch_add(1, Ordering::Relaxed);
        let state = Arc::new(PoolMemberState::default());
        state.attached.store(true, Ordering::Relaxed);
        // Until path selection publishes a measured demand, reserve broad
        // headroom for this attached operation. This prevents the first of a
        // concurrently opening group from consuming every spawn allowance
        // merely because its format scan finished first.
        state
            .desired_workers
            .store(self.configured_workers, Ordering::Relaxed);
        let mut scheduler = self.scheduler.lock().expect("decoder pool mutex poisoned");
        scheduler.members.push(Arc::downgrade(&state));
        self.rebalance(&mut scheduler, true);
        drop(scheduler);
        PoolMember {
            pool: Arc::clone(self),
            state,
        }
    }

    fn rebalance(&self, scheduler: &mut PoolScheduler, immediate_growth: bool) {
        let members: Vec<_> = scheduler.members.iter().filter_map(Weak::upgrade).collect();
        scheduler
            .members
            .retain(|member| member.strong_count() != 0);
        for member in &members {
            member.target_granted_workers.store(0, Ordering::Relaxed);
        }

        let candidates: Vec<_> = members
            .iter()
            .filter(|member| {
                member.attached.load(Ordering::Acquire)
                    && member.desired_workers.load(Ordering::Relaxed) != 0
            })
            .collect();
        if candidates.is_empty() {
            for member in members {
                member.set_granted_workers(0);
            }
            return;
        }

        let candidate_count = candidates.len();
        let mut remaining = self.worker_limit.load(Ordering::Acquire);
        let mut granted = vec![0_usize; candidates.len()];
        let start = scheduler.rotation % candidates.len();
        while remaining != 0 {
            let mut made_progress = false;
            for offset in 0..candidates.len() {
                let index = (start + offset) % candidates.len();
                let desired = candidates[index].desired_workers.load(Ordering::Relaxed);
                if granted[index] >= desired {
                    continue;
                }
                granted[index] += 1;
                remaining -= 1;
                made_progress = true;
                if remaining == 0 {
                    break;
                }
            }
            if !made_progress {
                break;
            }
        }
        for (member, grant) in candidates.into_iter().zip(granted) {
            member
                .target_granted_workers
                .store(grant, Ordering::Relaxed);
        }
        for member in members {
            let target = member.target_granted_workers.load(Ordering::Relaxed);
            let current = member.granted_workers.load(Ordering::Relaxed);
            if target <= current || immediate_growth {
                member.set_granted_workers(target);
                member.grant_growth_events.store(0, Ordering::Relaxed);
            }
        }
        scheduler.rotation = (start + 1) % candidate_count;
    }

    /// Refreshes whether `member` has work that can consume an execution slot.
    ///
    /// Rebalancing occurs only when that boolean changes. Queue depths change
    /// for every task, so avoiding a scheduler lock for nonzero-to-nonzero
    /// updates keeps the private worker loops' hot publication path cheap.
    fn refresh_runnable(&self, member: &PoolMemberState) {
        let runnable = member.desired_workers.load(Ordering::Relaxed) != 0
            && (member.queued_tasks.load(Ordering::Relaxed) != 0
                || member.busy_workers.load(Ordering::Relaxed) != 0
                || member.waiting_workers.load(Ordering::Relaxed) != 0);
        let previous = member.runnable.swap(runnable, Ordering::AcqRel);
        if previous == runnable {
            return;
        }
        if runnable {
            self.runnable_decoders.fetch_add(1, Ordering::Relaxed);
        } else {
            self.runnable_decoders.fetch_sub(1, Ordering::Relaxed);
        }
    }

    fn stats(&self) -> DecoderPoolStats {
        let (queued_tasks, active_workers) = self
            .scheduler
            .lock()
            .expect("decoder pool mutex poisoned")
            .members
            .iter()
            .filter_map(Weak::upgrade)
            .fold((0_usize, 0_usize), |(queued, active), member| {
                (
                    queued.saturating_add(member.queued_tasks.load(Ordering::Relaxed)),
                    active.saturating_add(member.granted_workers.load(Ordering::Relaxed)),
                )
            });
        DecoderPoolStats {
            configured_workers: self.configured_workers,
            worker_limit: self.worker_limit.load(Ordering::Relaxed),
            busy_workers: self.busy_workers.load(Ordering::Relaxed),
            active_workers,
            spawned_workers: self.spawned_workers.load(Ordering::Relaxed),
            auxiliary_threads: self.auxiliary_threads.load(Ordering::Relaxed),
            queued_tasks,
            attached_decoders: self.attached_decoders.load(Ordering::Relaxed),
            runnable_decoders: self.runnable_decoders.load(Ordering::Relaxed),
            waiting_decoders: self.waiting_decoders.load(Ordering::Relaxed),
        }
    }
}

#[derive(Default)]
struct PoolScheduler {
    waiters: VecDeque<u64>,
    members: Vec<Weak<PoolMemberState>>,
    rotation: usize,
}

#[derive(Default)]
pub(crate) struct PoolMemberState {
    attached: AtomicBool,
    queued_tasks: AtomicUsize,
    busy_workers: AtomicUsize,
    waiting_workers: AtomicUsize,
    desired_workers: AtomicUsize,
    target_granted_workers: AtomicUsize,
    granted_workers: AtomicUsize,
    grant_growth_events: AtomicUsize,
    grant_epoch: AtomicUsize,
    runnable: AtomicBool,
}

impl PoolMemberState {
    fn set_granted_workers(&self, workers: usize) {
        let previous = self.granted_workers.swap(workers, Ordering::Release);
        if previous != workers {
            self.grant_epoch.fetch_add(1, Ordering::Relaxed);
        }
    }
}

pub(crate) struct PoolMember {
    pool: Arc<PoolState>,
    state: Arc<PoolMemberState>,
}

impl PoolMember {
    const GRANT_GROWTH_QUEUE_EVENTS: usize = 128;

    pub(crate) fn acquire(&self) -> PoolPermit<'_> {
        self.pool.acquire(&self.state)
    }

    #[cfg(test)]
    pub(crate) fn busy_workers(&self) -> usize {
        self.state.busy_workers.load(Ordering::Relaxed)
    }

    pub(crate) fn waiting_workers(&self) -> usize {
        self.state.waiting_workers.load(Ordering::Relaxed)
    }

    pub(crate) fn queued_tasks(&self) -> usize {
        self.state.queued_tasks.load(Ordering::Relaxed)
    }

    pub(crate) fn granted_workers(&self) -> usize {
        self.state.granted_workers.load(Ordering::Acquire)
    }

    pub(crate) fn set_desired_workers(&self, count: usize) {
        let previous = self.state.desired_workers.swap(count, Ordering::Relaxed);
        if previous == count {
            return;
        }
        self.pool.refresh_runnable(&self.state);
        let mut scheduler = self
            .pool
            .scheduler
            .lock()
            .expect("decoder pool mutex poisoned");
        self.pool.rebalance(&mut scheduler, count != 0);
        self.pool.signal.notify_all();
    }

    pub(crate) fn set_queued_tasks(&self, count: usize) {
        let previous = self.state.queued_tasks.swap(count, Ordering::Relaxed);
        if (previous == 0) != (count == 0) {
            self.pool.refresh_runnable(&self.state);
        }
        self.maybe_promote_grant();
    }

    /// Delays allocation growth caused only by a peer becoming terminal until
    /// enough local queue activity remains to amortize additional OS threads.
    /// Explicit demand changes and pool-limit increases bypass this filter.
    fn maybe_promote_grant(&self) {
        let target = self.state.target_granted_workers.load(Ordering::Acquire);
        let current = self.state.granted_workers.load(Ordering::Relaxed);
        if target <= current {
            return;
        }
        let events = self
            .state
            .grant_growth_events
            .fetch_add(1, Ordering::Relaxed)
            + 1;
        if events < Self::GRANT_GROWTH_QUEUE_EVENTS {
            return;
        }

        let target = self.state.target_granted_workers.load(Ordering::Acquire);
        let mut current = self.state.granted_workers.load(Ordering::Relaxed);
        while current < target {
            match self.state.granted_workers.compare_exchange_weak(
                current,
                target,
                Ordering::AcqRel,
                Ordering::Relaxed,
            ) {
                Ok(_) => {
                    self.state.grant_epoch.fetch_add(1, Ordering::Relaxed);
                    break;
                }
                Err(observed) => current = observed,
            }
        }
        self.state.grant_growth_events.store(0, Ordering::Relaxed);
    }

    pub(crate) fn register_worker(&self) {
        self.pool.spawned_workers.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn unregister_worker(&self) {
        self.pool.spawned_workers.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn register_auxiliary(&self) {
        self.pool.auxiliary_threads.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn unregister_auxiliary(&self) {
        self.pool.auxiliary_threads.fetch_sub(1, Ordering::Relaxed);
    }

    pub(crate) fn configured_workers(&self) -> usize {
        self.pool.configured_workers()
    }

    pub(crate) fn worker_limit(&self) -> usize {
        self.pool.worker_limit.load(Ordering::Acquire)
    }

    pub(crate) fn attached_decoders(&self) -> usize {
        self.pool.attached_decoders.load(Ordering::Acquire)
    }

    pub(crate) fn limit_epoch(&self) -> usize {
        self.pool
            .limit_epoch
            .load(Ordering::Relaxed)
            .wrapping_add(self.state.grant_epoch.load(Ordering::Relaxed))
    }

    pub(crate) fn detach(&self) {
        if !self.state.attached.swap(false, Ordering::AcqRel) {
            return;
        }
        self.state.queued_tasks.store(0, Ordering::Relaxed);
        self.state.desired_workers.store(0, Ordering::Relaxed);
        if self.state.runnable.swap(false, Ordering::AcqRel) {
            self.pool.runnable_decoders.fetch_sub(1, Ordering::Relaxed);
        }
        self.pool.attached_decoders.fetch_sub(1, Ordering::Relaxed);
        let mut scheduler = self
            .pool
            .scheduler
            .lock()
            .expect("decoder pool mutex poisoned");
        let detached = Arc::downgrade(&self.state);
        scheduler
            .members
            .retain(|member| !Weak::ptr_eq(member, &detached));
        self.pool.rebalance(&mut scheduler, false);
        drop(scheduler);
        self.pool.signal.notify_all();
    }
}

impl Drop for PoolMember {
    fn drop(&mut self) {
        debug_assert_eq!(self.state.busy_workers.load(Ordering::Relaxed), 0);
        debug_assert_eq!(self.state.waiting_workers.load(Ordering::Relaxed), 0);
        self.detach();
    }
}

pub(crate) struct PoolPermit<'a> {
    pool: &'a PoolState,
    member: &'a PoolMemberState,
}

impl Drop for PoolPermit<'_> {
    fn drop(&mut self) {
        self.pool.release(self.member);
    }
}

#[cfg(test)]
mod tests {
    use super::{DecoderPool, PoolMember};
    use std::sync::{Arc, Barrier};
    use std::thread;
    use std::time::Duration;

    #[test]
    fn builder_validates_limits() {
        assert!(DecoderPool::builder().workers(0).build().is_err());
        assert!(
            DecoderPool::builder()
                .workers(4)
                .initial_worker_limit(5)
                .build()
                .is_err()
        );
        let pool = DecoderPool::builder()
            .workers(4)
            .initial_worker_limit(2)
            .build()
            .unwrap();
        assert_eq!(pool.stats().configured_workers, 4);
        assert_eq!(pool.stats().worker_limit, 2);
    }

    #[test]
    fn shared_limit_bounds_concurrent_execution() {
        let pool = DecoderPool::builder().workers(2).build().unwrap();
        let member = Arc::new(pool.state.register_decoder());
        let start = Arc::new(Barrier::new(5));
        let maximum = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let mut workers = Vec::new();
        for _ in 0..4 {
            let member = Arc::clone(&member);
            let start = Arc::clone(&start);
            let maximum = Arc::clone(&maximum);
            workers.push(thread::spawn(move || {
                start.wait();
                let _permit = member.acquire();
                let busy = member.busy_workers();
                maximum.fetch_max(busy, std::sync::atomic::Ordering::Relaxed);
                thread::sleep(Duration::from_millis(5));
            }));
        }
        start.wait();
        for worker in workers {
            worker.join().unwrap();
        }
        assert_eq!(maximum.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(pool.stats().busy_workers, 0);
    }

    #[test]
    fn attached_demand_stabilizes_spawn_allowances_until_detach() {
        let pool = DecoderPool::builder().workers(4).build().unwrap();
        let first = pool.state.register_decoder();
        let second = pool.state.register_decoder();
        first.set_desired_workers(4);
        second.set_desired_workers(4);

        first.set_queued_tasks(8);
        assert_eq!(first.granted_workers(), 2);
        assert_eq!(second.granted_workers(), 2);
        assert_eq!(pool.stats().runnable_decoders, 1);

        second.set_queued_tasks(8);
        assert_eq!(first.granted_workers(), 2);
        assert_eq!(second.granted_workers(), 2);
        assert_eq!(pool.stats().active_workers, 4);

        let final_second_task = second.acquire();
        second.set_queued_tasks(0);
        drop(final_second_task);
        assert_eq!(first.granted_workers(), 2);
        assert_eq!(second.granted_workers(), 2);
        assert_eq!(pool.stats().runnable_decoders, 1);

        drop(second);
        for queued in 8..(8 + PoolMember::GRANT_GROWTH_QUEUE_EVENTS) {
            first.set_queued_tasks(queued);
        }
        assert_eq!(first.granted_workers(), 4);
    }

    #[test]
    fn resizing_redistributes_live_allowances() {
        let pool = DecoderPool::builder()
            .workers(4)
            .initial_worker_limit(2)
            .build()
            .unwrap();
        let first = pool.state.register_decoder();
        let second = pool.state.register_decoder();
        first.set_desired_workers(4);
        second.set_desired_workers(4);
        first.set_queued_tasks(4);
        second.set_queued_tasks(4);
        assert_eq!(pool.stats().active_workers, 2);

        pool.set_worker_limit(4).unwrap();
        assert_eq!(pool.stats().active_workers, 4);
        assert_eq!(first.granted_workers(), 2);
        assert_eq!(second.granted_workers(), 2);

        pool.set_worker_limit(1).unwrap();
        assert_eq!(pool.stats().active_workers, 1);
        assert_eq!(first.granted_workers() + second.granted_workers(), 1);
        assert_eq!(pool.set_worker_limit(0).unwrap_err().requested(), 0);
    }
}
