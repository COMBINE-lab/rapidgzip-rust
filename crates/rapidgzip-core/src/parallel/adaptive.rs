//! Empirical concurrency control for parallel decode paths.
//!
//! Parallel gzip paths can become limited by memory bandwidth, task size, or
//! speculative buffers well before every visible processor is useful. The
//! controller therefore measures native worker completions while probing a
//! budget-derived range. Measurements happen before ordered output handoff, so
//! a slow `Read` consumer does not masquerade as a slow decoder.

use std::time::Instant;

const RATE_TOLERANCE: f64 = 0.03;
const SAMPLES_PER_CANDIDATE: usize = 5;
const MINIMUM_CALIBRATION_WAVES: usize = 8;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Baseline,
    Down,
    Up,
    Stable,
}

/// Learns a useful decode/resolve concurrency without assuming a fixed cap.
///
/// A large request begins near twice the square root of the smaller of its
/// worker budget and visible processor count. That grows with both inputs
/// without immediately multiplying speculative memory by every processor.
/// Calibration first grows from that conservative bootstrap while each
/// increase improves throughput, then probes downward around the best setting.
/// A lower setting within the noise tolerance is preferred because it reduces
/// memory pressure without a material throughput loss.
#[derive(Debug)]
pub(crate) struct AdaptiveConcurrency {
    maximum: usize,
    current: usize,
    down_step: usize,
    up_step: usize,
    phase: Phase,
    sample_bytes: u64,
    generation: usize,
    interval_bytes: u64,
    interval_started: Option<Instant>,
    sample_rates: [f64; SAMPLES_PER_CANDIDATE],
    sample_count: usize,
    best_limit: usize,
    peak_rate: f64,
    measured: bool,
    retried_upward_candidate: bool,
}

impl AdaptiveConcurrency {
    pub(crate) fn new(
        maximum: usize,
        machine_parallelism: usize,
        sample_bytes: usize,
        work_items: usize,
    ) -> Self {
        debug_assert!(maximum != 0);
        let limits = controller_limits(maximum, machine_parallelism, work_items);
        Self {
            maximum: limits.maximum,
            current: limits.initial,
            down_step: limits.down_step,
            up_step: limits.up_step,
            phase: if limits.calibrate {
                Phase::Baseline
            } else {
                Phase::Stable
            },
            sample_bytes: sample_bytes.max(1) as u64,
            generation: 0,
            interval_bytes: 0,
            interval_started: None,
            sample_rates: [0.0; SAMPLES_PER_CANDIDATE],
            sample_count: 0,
            best_limit: limits.initial,
            peak_rate: 0.0,
            measured: false,
            retried_upward_candidate: false,
        }
    }

    pub(crate) const fn current_limit(&self) -> usize {
        self.current
    }

    /// Largest worker rank that calibration can enable for this machine.
    pub(crate) const fn worker_pool_limit(&self) -> usize {
        self.maximum
    }

    pub(crate) const fn generation(&self) -> usize {
        self.generation
    }

    pub(crate) const fn is_stable(&self) -> bool {
        matches!(self.phase, Phase::Stable)
    }

    /// Empirically selected worker count, if calibration actually ran.
    pub(crate) const fn best_limit(&self) -> Option<usize> {
        if self.is_stable() && self.measured {
            Some(self.best_limit)
        } else {
            None
        }
    }

    /// Discards a partial sample after an external limit change.
    pub(crate) fn pause_observation(&mut self) {
        self.reset_candidate();
    }

    /// Marks the beginning of native work performed under `generation`.
    ///
    /// Work begun before a concurrency change is deliberately excluded from
    /// the new candidate.  The first new task starts the candidate's clock;
    /// old tasks that are still leaving the pipeline can only contend with it,
    /// not inflate its completed-byte count.
    pub(crate) fn start_work(&mut self, generation: usize, now: Instant) {
        if !self.is_stable() && generation == self.generation && self.interval_started.is_none() {
            self.interval_started = Some(now);
        }
    }

    /// Records native bytes completed under `generation`.
    ///
    /// Returns true when either the active limit or calibration state changed.
    pub(crate) fn observe_work(
        &mut self,
        generation: usize,
        decoded_bytes: usize,
        now: Instant,
    ) -> bool {
        if self.is_stable() || generation != self.generation || decoded_bytes == 0 {
            return false;
        }
        let Some(started) = self.interval_started else {
            return false;
        };
        self.interval_bytes = self.interval_bytes.saturating_add(decoded_bytes as u64);
        if self.interval_bytes < self.sample_bytes {
            return false;
        }
        let Some(elapsed) = now.checked_duration_since(started) else {
            return false;
        };
        if elapsed.is_zero() {
            return false;
        }

        self.sample_rates[self.sample_count] =
            self.interval_bytes as f64 / elapsed.as_nanos() as f64;
        self.sample_count += 1;
        self.interval_bytes = 0;
        self.interval_started = Some(now);
        if self.sample_count < SAMPLES_PER_CANDIDATE {
            return false;
        }

        let mut samples = self.sample_rates;
        samples.sort_by(f64::total_cmp);
        self.evaluate(samples[SAMPLES_PER_CANDIDATE / 2])
    }

    fn evaluate(&mut self, rate: f64) -> bool {
        self.measured = true;
        match self.phase {
            Phase::Baseline => {
                self.peak_rate = rate;
                self.best_limit = self.current;
                self.begin_upward_search()
            }
            Phase::Down => {
                if rate >= self.peak_rate * (1.0 - RATE_TOLERANCE) {
                    self.peak_rate = self.peak_rate.max(rate);
                    self.best_limit = self.current;
                    let lower = self.current.saturating_sub(self.down_step).max(1);
                    if lower < self.current {
                        self.switch_to(lower, Phase::Down)
                    } else {
                        self.finish_at_best()
                    }
                } else {
                    self.finish_at_best()
                }
            }
            Phase::Up => {
                if self.current == self.maximum && self.maximum <= self.up_step.saturating_mul(2) {
                    self.peak_rate = self.peak_rate.max(rate);
                    self.best_limit = self.current;
                    return self.finish_at_best();
                }
                if rate > self.peak_rate * (1.0 + RATE_TOLERANCE) {
                    self.peak_rate = rate;
                    self.best_limit = self.current;
                    self.retried_upward_candidate = false;
                    let higher = self.current.saturating_add(self.up_step).min(self.maximum);
                    if higher > self.current {
                        self.switch_to(higher, Phase::Up)
                    } else {
                        self.begin_downward_search()
                    }
                } else if self.retried_upward_candidate {
                    self.retried_upward_candidate = false;
                    self.begin_downward_search()
                } else {
                    self.retried_upward_candidate = true;
                    self.retry_candidate()
                }
            }
            Phase::Stable => false,
        }
    }

    fn begin_upward_search(&mut self) -> bool {
        let higher = self
            .best_limit
            .saturating_add(self.up_step)
            .min(self.maximum);
        if higher <= self.best_limit {
            return self.begin_downward_search();
        }
        self.switch_to(higher, Phase::Up)
    }

    fn begin_downward_search(&mut self) -> bool {
        let lower = self.best_limit.saturating_sub(self.down_step).max(1);
        if lower >= self.best_limit {
            return self.finish_at_best();
        }
        self.switch_to(lower, Phase::Down)
    }

    fn switch_to(&mut self, limit: usize, phase: Phase) -> bool {
        debug_assert_ne!(limit, self.current);
        self.current = limit;
        self.phase = phase;
        self.generation = self.generation.wrapping_add(1);
        self.reset_candidate();
        true
    }

    fn retry_candidate(&mut self) -> bool {
        self.generation = self.generation.wrapping_add(1);
        self.reset_candidate();
        true
    }

    fn finish_at_best(&mut self) -> bool {
        let changed = self.current != self.best_limit || !self.is_stable();
        self.current = self.best_limit;
        self.phase = Phase::Stable;
        self.generation = self.generation.wrapping_add(1);
        self.reset_candidate();
        changed
    }

    fn reset_candidate(&mut self) {
        self.interval_bytes = 0;
        self.interval_started = None;
        self.sample_rates = [0.0; SAMPLES_PER_CANDIDATE];
        self.sample_count = 0;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ControllerLimits {
    initial: usize,
    maximum: usize,
    down_step: usize,
    up_step: usize,
    calibrate: bool,
}

/// Derives both the bootstrap and worker-pool extent from the smaller of
/// affinity-visible processors and the caller's maximum worker budget.
fn controller_limits(
    maximum: usize,
    machine_parallelism: usize,
    work_items: usize,
) -> ControllerLimits {
    let visible = machine_parallelism.max(1);
    let maximum = maximum.min(visible);
    if maximum <= 4 {
        return ControllerLimits {
            initial: maximum,
            maximum,
            down_step: 1,
            up_step: 1,
            calibrate: false,
        };
    }

    let scaled_budget = maximum.saturating_mul(4);
    let square_root = scaled_budget.isqrt();
    let initial = square_root
        .saturating_add(usize::from(
            square_root.saturating_mul(square_root) < scaled_budget,
        ))
        .max(1)
        .min(maximum);
    let down_step = (initial / 8).max(1);
    let up_step = initial.max(1);
    let search_maximum = maximum;
    // Each candidate needs several waves of independent tasks to get beyond
    // startup and transition costs. On shorter streams, probing consumes a
    // material fraction of the entire decode, so the machine-derived bootstrap
    // is a better end-to-end choice than an optimum found just before EOF.
    let calibrate =
        initial < search_maximum && work_items >= initial.saturating_mul(MINIMUM_CALIBRATION_WAVES);
    ControllerLimits {
        initial,
        maximum: if calibrate { search_maximum } else { initial },
        down_step,
        up_step,
        calibrate,
    }
}

#[cfg(test)]
mod tests {
    use super::{AdaptiveConcurrency, ControllerLimits, SAMPLES_PER_CANDIDATE, controller_limits};
    use std::time::{Duration, Instant};

    const SAMPLE_BYTES: usize = 100;

    fn feed_rate(controller: &mut AdaptiveConcurrency, cursor: &mut Instant, rate: u64) -> bool {
        let generation = controller.generation();
        controller.start_work(generation, *cursor);
        let mut changed = false;
        for _ in 0..SAMPLES_PER_CANDIDATE {
            *cursor += Duration::from_nanos((SAMPLE_BYTES as u64 * 1_000 / rate).max(1));
            changed |= controller.observe_work(generation, SAMPLE_BYTES, *cursor);
        }
        changed
    }

    #[test]
    fn machine_wide_pool_is_sublinear_and_bidirectional() {
        assert_eq!(
            controller_limits(64, 88, usize::MAX),
            ControllerLimits {
                initial: 16,
                maximum: 64,
                down_step: 2,
                up_step: 16,
                calibrate: true,
            }
        );
        assert_eq!(controller_limits(64, 64, usize::MAX).initial, 16);
        assert_eq!(controller_limits(44, 44, usize::MAX).initial, 14);
        assert_eq!(controller_limits(64, 8, usize::MAX).initial, 6);
    }

    #[test]
    fn smaller_explicit_budgets_control_the_bootstrap() {
        let controller = AdaptiveConcurrency::new(16, 88, SAMPLE_BYTES, usize::MAX);
        assert_eq!(controller.current_limit(), 8);
        assert_eq!(controller.worker_pool_limit(), 16);
        assert!(!controller.is_stable());
    }

    #[test]
    fn modest_budget_grows_from_bootstrap_to_the_requested_ceiling() {
        let mut cursor = Instant::now();
        let mut controller = AdaptiveConcurrency::new(16, 88, SAMPLE_BYTES, usize::MAX);
        assert_eq!(controller.current_limit(), 8);
        assert!(feed_rate(&mut controller, &mut cursor, 1_000));
        assert_eq!(controller.current_limit(), 16);
        assert!(feed_rate(&mut controller, &mut cursor, 500));
        assert_eq!(controller.current_limit(), 16);
        assert!(controller.is_stable());
    }

    #[test]
    fn bootstrap_is_monotonic_across_requested_budgets() {
        let below = controller_limits(16, 88, usize::MAX);
        let middle = controller_limits(44, 88, usize::MAX);
        let above = controller_limits(45, 88, usize::MAX);
        assert_eq!(below.initial, 8);
        assert_eq!(middle.initial, 14);
        assert_eq!(above.initial, 14);
        assert!(middle.maximum <= above.maximum);
    }

    #[test]
    fn searches_down_and_prefers_a_near_tied_lower_limit() {
        let mut cursor = Instant::now();
        let mut controller = AdaptiveConcurrency::new(64, 81, SAMPLE_BYTES, usize::MAX);
        assert_eq!(controller.current_limit(), 16);
        assert!(feed_rate(&mut controller, &mut cursor, 1_000));
        assert_eq!(controller.current_limit(), 32);
        assert!(feed_rate(&mut controller, &mut cursor, 990));
        assert_eq!(controller.current_limit(), 32);
        assert!(feed_rate(&mut controller, &mut cursor, 990));
        assert_eq!(controller.current_limit(), 14);
        assert!(feed_rate(&mut controller, &mut cursor, 990));
        assert_eq!(controller.current_limit(), 12);
        assert!(feed_rate(&mut controller, &mut cursor, 900));
        assert_eq!(controller.current_limit(), 14);
        assert!(controller.is_stable());
    }

    #[test]
    fn searches_up_until_a_candidate_stops_improving() {
        let mut cursor = Instant::now();
        let mut controller = AdaptiveConcurrency::new(64, 81, SAMPLE_BYTES, usize::MAX);
        assert!(feed_rate(&mut controller, &mut cursor, 1_000));
        assert_eq!(controller.current_limit(), 32);
        assert!(feed_rate(&mut controller, &mut cursor, 1_100));
        assert_eq!(controller.current_limit(), 48);
        assert!(feed_rate(&mut controller, &mut cursor, 1_180));
        assert_eq!(controller.current_limit(), 64);
        assert!(feed_rate(&mut controller, &mut cursor, 1_170));
        assert_eq!(controller.current_limit(), 64);
        assert!(feed_rate(&mut controller, &mut cursor, 1_170));
        assert_eq!(controller.current_limit(), 46);
        assert!(feed_rate(&mut controller, &mut cursor, 1_000));
        assert_eq!(controller.current_limit(), 48);
        assert!(controller.is_stable());
    }

    #[test]
    fn ignores_work_from_an_old_generation() {
        let mut cursor = Instant::now();
        let mut controller = AdaptiveConcurrency::new(64, 81, SAMPLE_BYTES, usize::MAX);
        let old_generation = controller.generation();
        assert!(feed_rate(&mut controller, &mut cursor, 1_000));
        assert_ne!(controller.generation(), old_generation);
        controller.start_work(old_generation, cursor);
        cursor += Duration::from_millis(1);
        assert!(!controller.observe_work(old_generation, SAMPLE_BYTES * 10, cursor));
        assert_eq!(controller.current_limit(), 32);
    }

    #[test]
    fn short_inputs_use_the_machine_bootstrap_without_probing() {
        let controller = AdaptiveConcurrency::new(64, 88, SAMPLE_BYTES, 100);
        assert_eq!(controller.current_limit(), 16);
        assert_eq!(controller.worker_pool_limit(), 16);
        assert!(controller.is_stable());
    }
}
