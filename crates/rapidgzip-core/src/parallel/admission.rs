//! Input-aware admission for the speculative marker/window path.

use super::adaptive::initial_parallelism;
use std::time::Duration;

/// The short screen pays disproportionately for structural search and marker
/// setup. Wider waves amortize more of that fixed cost at the configured grid.
const SCREEN_GATE_BASE_PERCENT: usize = 65;
const SCREEN_GATE_DISCOUNT_PER_WORKER: usize = 10;
const MINIMUM_SCREEN_GATE_PERCENT: usize = 20;
const SCREEN_GATE_DENOMINATOR: u128 = 100;

/// Tiny samples are dominated by clock and allocation noise.
const MINIMUM_SAMPLE_BYTES: usize = 256 * 1024;

/// One wave can contain no work after worker startup and cannot demonstrate
/// sustained parallelism. Two waves also bound the probe to inputs large
/// enough to amortize it.
const MINIMUM_TASK_WAVES: usize = 2;

/// Inputs below this many configured grid cells cannot amortize classification
/// plus a new worker pool even when a single service-rate sample looks good.
const MINIMUM_GRID_TASKS: usize = 16;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct WorkSample {
    bytes: usize,
    nanos: u128,
}

impl WorkSample {
    pub(crate) fn new(bytes: usize, elapsed: Duration) -> Self {
        Self {
            bytes,
            nanos: elapsed.as_nanos().max(1),
        }
    }

    #[cfg(test)]
    const fn from_nanos(bytes: usize, nanos: u128) -> Self {
        Self {
            bytes,
            nanos: if nanos == 0 { 1 } else { nanos },
        }
    }

    const fn is_representative(self) -> bool {
        self.bytes >= MINIMUM_SAMPLE_BYTES
    }
}

/// Useful worker count before the marker path's own adaptive controller runs.
pub(crate) fn effective_parallelism(
    configured: usize,
    application_limit: usize,
    machine_parallelism: usize,
    task_count: usize,
) -> usize {
    let two_wave_task_limit = task_count / MINIMUM_TASK_WAVES;
    let budget = configured.min(application_limit).min(machine_parallelism);
    initial_parallelism(budget.max(1), machine_parallelism, task_count)
        .min(budget)
        .min(two_wave_task_limit)
}

/// Whether the input exposes enough useful work for an empirical probe.
pub(crate) fn should_probe(effective_workers: usize, task_count: usize) -> bool {
    effective_workers >= 2
        && task_count >= MINIMUM_GRID_TASKS
        && task_count >= effective_workers.saturating_mul(MINIMUM_TASK_WAVES)
}

/// Whether a bounded, setup-heavy screen admits the steady marker pipeline.
pub(crate) fn screen_admits_marker(
    effective_workers: usize,
    exact: WorkSample,
    speculative: WorkSample,
) -> bool {
    if effective_workers < 2 || !exact.is_representative() || !speculative.is_representative() {
        return false;
    }
    let gate_percent = SCREEN_GATE_BASE_PERCENT
        .saturating_sub(effective_workers.saturating_mul(SCREEN_GATE_DISCOUNT_PER_WORKER))
        .max(MINIMUM_SCREEN_GATE_PERCENT);
    let marker_score = (speculative.bytes as u128)
        .saturating_mul(exact.nanos)
        .saturating_mul(SCREEN_GATE_DENOMINATOR);
    let exact_score = (exact.bytes as u128)
        .saturating_mul(speculative.nanos)
        .saturating_mul(gate_percent as u128);
    marker_score >= exact_score
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: usize = 1024 * 1024;

    #[test]
    fn effective_parallelism_honors_every_bound() {
        assert_eq!(effective_parallelism(8, 6, 4, 8), 4);
        assert_eq!(effective_parallelism(8, 2, 4, 18), 2);
        assert_eq!(effective_parallelism(1, 8, 8, 8), 1);
        assert_eq!(effective_parallelism(8, 8, 8, 3), 1);
        assert_eq!(effective_parallelism(8, 8, 8, 0), 0);
    }

    #[test]
    fn probe_requires_two_complete_task_waves() {
        assert!(!should_probe(1, 64));
        assert!(!should_probe(2, 3));
        assert!(!should_probe(2, 15));
        assert!(should_probe(2, 16));
        assert!(!should_probe(4, 7));
        assert!(should_probe(4, 16));
    }

    #[test]
    fn screen_gate_is_conservative_but_allows_fixed_cost_amortization() {
        let exact = WorkSample::from_nanos(MIB, 1_000);
        let forty_four_percent = WorkSample::from_nanos(MIB, 2_273);
        let forty_five_percent = WorkSample::from_nanos(MIB, 2_222);
        let twenty_four_percent = WorkSample::from_nanos(MIB, 4_167);
        let twenty_five_percent = WorkSample::from_nanos(MIB, 4_000);

        assert!(!screen_admits_marker(2, exact, forty_four_percent));
        assert!(screen_admits_marker(2, exact, forty_five_percent));
        assert!(!screen_admits_marker(4, exact, twenty_four_percent));
        assert!(screen_admits_marker(4, exact, twenty_five_percent));
    }

    #[test]
    fn large_budgets_use_the_adaptive_bootstrap() {
        assert_eq!(effective_parallelism(64, 64, 64, 128), 16);
        assert_eq!(effective_parallelism(64, 8, 64, 128), 6);
        assert_eq!(effective_parallelism(64, 3, 64, 128), 3);
    }

    #[test]
    fn ambiguous_samples_choose_sequential() {
        let representative = WorkSample::from_nanos(MIB, 1_000);
        let tiny = WorkSample::from_nanos(MINIMUM_SAMPLE_BYTES - 1, 1);

        assert!(!screen_admits_marker(1, representative, representative));
        assert!(!screen_admits_marker(2, tiny, representative));
        assert!(!screen_admits_marker(2, representative, tiny));
    }
}
