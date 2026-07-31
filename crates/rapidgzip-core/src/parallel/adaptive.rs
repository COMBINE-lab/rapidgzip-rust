//! Empirical concurrency control for the speculative DEFLATE pipeline.
//!
//! The generic rapidgzip path is limited by memory bandwidth on some hosts,
//! so the fastest worker count can be lower than the number of available
//! processors. This module measures ordered decoded-output throughput while it
//! explores nearby concurrency levels and retains the best observation.

use std::time::Instant;

const RATE_TOLERANCE: f64 = 0.03;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Phase {
    Baseline,
    Up,
    Stable,
}

/// Learns a useful decode/resolve concurrency without assuming a fixed cap.
///
/// Every setting gets a warmup interval so already-running work can leave the
/// pipeline. The following interval measures bytes committed in order, which
/// includes native decode, marker resolution, reordering, CRC calculation,
/// and output handoff. This is deliberately an end-to-end metric rather than
/// a sum of worker-local rates.
#[derive(Debug)]
pub(crate) struct AdaptiveConcurrency {
    maximum: usize,
    current: usize,
    phase: Phase,
    sample_bytes: u64,
    interval_bytes: u64,
    interval_started: Option<Instant>,
    warming: bool,
    best_limit: usize,
    peak_rate: f64,
}

impl AdaptiveConcurrency {
    pub(crate) fn new(maximum: usize, machine_parallelism: usize, sample_bytes: usize) -> Self {
        debug_assert!(maximum != 0);
        let initial = bootstrap_limit(maximum, machine_parallelism);
        Self {
            maximum,
            current: initial,
            phase: if initial < maximum {
                Phase::Baseline
            } else {
                Phase::Stable
            },
            sample_bytes: sample_bytes.max(1) as u64,
            interval_bytes: 0,
            interval_started: None,
            warming: true,
            best_limit: initial,
            peak_rate: 0.0,
        }
    }

    pub(crate) const fn current_limit(&self) -> usize {
        self.current
    }

    #[cfg(test)]
    const fn is_stable(&self) -> bool {
        matches!(self.phase, Phase::Stable)
    }

    /// Records newly committed bytes and returns true when the limit changed.
    pub(crate) fn observe_progress(&mut self, decoded_bytes: usize, now: Instant) -> bool {
        if self.phase == Phase::Stable || decoded_bytes == 0 {
            return false;
        }
        self.interval_started.get_or_insert(now);
        self.interval_bytes = self.interval_bytes.saturating_add(decoded_bytes as u64);
        if self.interval_bytes < self.sample_bytes {
            return false;
        }
        if self.warming {
            self.warming = false;
            self.interval_bytes = 0;
            self.interval_started = Some(now);
            return false;
        }

        let Some(elapsed) = now.checked_duration_since(
            self.interval_started
                .expect("a measured interval has a start"),
        ) else {
            return false;
        };
        if elapsed.is_zero() {
            return false;
        }
        let rate = self.interval_bytes as f64 / elapsed.as_nanos() as f64;
        self.evaluate(rate)
    }

    fn evaluate(&mut self, rate: f64) -> bool {
        match self.phase {
            Phase::Baseline => {
                self.peak_rate = rate;
                self.best_limit = self.current;
                self.switch_to(next_higher(self.current, self.maximum), Phase::Up)
            }
            Phase::Up => {
                if rate > self.peak_rate * (1.0 + RATE_TOLERANCE) {
                    self.peak_rate = rate;
                    self.best_limit = self.current;
                    self.phase = Phase::Stable;
                    false
                } else {
                    self.finish_at_best()
                }
            }
            Phase::Stable => false,
        }
    }

    fn switch_to(&mut self, limit: usize, phase: Phase) -> bool {
        debug_assert_ne!(limit, self.current);
        self.current = limit;
        self.phase = phase;
        self.warming = true;
        self.reset_interval();
        true
    }

    fn finish_at_best(&mut self) -> bool {
        self.phase = Phase::Stable;
        if self.current == self.best_limit {
            return false;
        }
        self.current = self.best_limit;
        self.reset_interval();
        true
    }

    fn reset_interval(&mut self) {
        self.interval_bytes = 0;
        self.interval_started = None;
    }
}

/// Seeds calibration from the processors visible through the process affinity
/// mask. Small pools and explicitly smaller budgets begin fully enabled; only
/// large machine-wide pools use a conservative fractional starting point.
fn bootstrap_limit(maximum: usize, machine_parallelism: usize) -> usize {
    let visible = machine_parallelism.max(1);
    if visible <= 8 || maximum.saturating_mul(2) <= visible {
        return maximum;
    }
    visible.div_ceil(3).max(4).min(maximum)
}

fn next_higher(current: usize, maximum: usize) -> usize {
    current.saturating_add(1).min(maximum)
}

#[cfg(test)]
mod tests {
    use super::{AdaptiveConcurrency, bootstrap_limit};
    use std::time::{Duration, Instant};

    const SAMPLE_BYTES: usize = 100;

    fn feed_rate(controller: &mut AdaptiveConcurrency, cursor: &mut Instant, rate: u64) -> bool {
        assert!(!controller.observe_progress(SAMPLE_BYTES, *cursor));
        let elapsed = Duration::from_nanos((SAMPLE_BYTES as u64 * 1_000 / rate).max(1));
        *cursor += elapsed;
        controller.observe_progress(SAMPLE_BYTES, *cursor)
    }

    #[test]
    fn bootstrap_uses_visible_machine_parallelism() {
        assert_eq!(bootstrap_limit(44, 44), 15);
        assert_eq!(bootstrap_limit(16, 44), 16);
        assert_eq!(bootstrap_limit(8, 8), 8);
        assert_eq!(bootstrap_limit(64, 24), 8);
    }

    #[test]
    fn smaller_explicit_budgets_do_not_need_calibration() {
        let controller = AdaptiveConcurrency::new(16, 44, SAMPLE_BYTES);
        assert_eq!(controller.current_limit(), 16);
        assert!(controller.is_stable());
    }

    #[test]
    fn explores_gradually_and_keeps_the_best_limit() {
        let mut cursor = Instant::now();
        let mut controller = AdaptiveConcurrency::new(44, 44, SAMPLE_BYTES);
        assert_eq!(controller.current_limit(), 15);
        assert!(feed_rate(&mut controller, &mut cursor, 1_000));
        assert_eq!(controller.current_limit(), 16);
        assert!(feed_rate(&mut controller, &mut cursor, 950));
        assert_eq!(controller.current_limit(), 15);
        assert!(controller.is_stable());
    }

    #[test]
    fn accepts_a_measured_neighbor_improvement() {
        let mut cursor = Instant::now();
        let mut controller = AdaptiveConcurrency::new(24, 24, SAMPLE_BYTES);
        assert_eq!(controller.current_limit(), 8);
        assert!(feed_rate(&mut controller, &mut cursor, 1_000));
        assert_eq!(controller.current_limit(), 9);
        assert!(!feed_rate(&mut controller, &mut cursor, 1_100));
        assert_eq!(controller.current_limit(), 9);
        assert!(controller.is_stable());
    }

    #[test]
    fn ignores_empty_progress() {
        let mut controller = AdaptiveConcurrency::new(44, 44, SAMPLE_BYTES);
        assert!(!controller.observe_progress(0, Instant::now()));
        assert_eq!(controller.current_limit(), 15);
    }
}
