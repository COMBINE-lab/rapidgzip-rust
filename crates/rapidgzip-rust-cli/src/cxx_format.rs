//! Reproductions of the C++ formatting rapidgzip's report relies on.
//!
//! The report is meant to be byte-identical to rapidgzip 0.16.0, which means
//! reproducing how C++ streams print numbers rather than how Rust does. These
//! are pure functions with their own tests, because a rounding rule that is
//! wrong only at an edge case would otherwise surface as an opaque diff.

/// Formats like a default C++ `ostream`, which is `%.6g`.
///
/// Six significant digits, trailing zeros removed, switching to exponent form
/// when the exponent is below -4 or at least 6.
#[must_use]
pub fn general(value: f64) -> String {
    format_general(value, 6)
}

/// Formats like C++ `std::scientific` at default precision, which is `%.6e`.
///
/// Six digits after the point and an exponent of at least two digits, so
/// 193515.8 prints as `1.935158e+05`.
#[must_use]
pub fn scientific(value: f64) -> String {
    format_scientific(value, 6)
}

fn format_scientific(value: f64, precision: usize) -> String {
    if value == 0.0 {
        return format!("{:.*}e+00", precision, 0.0);
    }
    if !value.is_finite() {
        return if value.is_nan() {
            "nan".to_owned()
        } else if value > 0.0 {
            "inf".to_owned()
        } else {
            "-inf".to_owned()
        };
    }
    let negative = value < 0.0;
    let magnitude = value.abs();
    let mut exponent = magnitude.log10().floor() as i32;
    let mut mantissa = magnitude / 10_f64.powi(exponent);
    // Rounding the mantissa can carry it to ten, which belongs one exponent up.
    if format!("{mantissa:.precision$}").starts_with("10") {
        mantissa /= 10.0;
        exponent += 1;
    }
    let sign = if negative { "-" } else { "" };
    let exponent_sign = if exponent < 0 { '-' } else { '+' };
    format!(
        "{sign}{mantissa:.precision$}e{exponent_sign}{:02}",
        exponent.abs()
    )
}

fn format_general(value: f64, precision: usize) -> String {
    if value == 0.0 {
        return "0".to_owned();
    }
    if !value.is_finite() {
        return format_scientific(value, precision);
    }
    let exponent = value.abs().log10().floor() as i32;
    // C++ picks the exponent form outside [-4, precision), matching printf.
    if exponent < -4 || exponent >= precision as i32 {
        let text = format_scientific(value, precision.saturating_sub(1));
        let (mantissa, exponent_part) = text.split_once('e').expect("scientific has an e");
        return format!("{}e{exponent_part}", trim_fraction(mantissa));
    }
    let decimals = (precision as i32 - 1 - exponent).max(0) as usize;
    trim_fraction(&format!("{value:.decimals$}"))
}

/// Removes the trailing zeros, and then the point, that `%g` suppresses.
fn trim_fraction(text: &str) -> String {
    if !text.contains('.') {
        return text.to_owned();
    }
    let trimmed = text.trim_end_matches('0');
    trimmed.trim_end_matches('.').to_owned()
}

/// Formats a bit count as rapidgzip does, `"{bytes} B {bits} b"`.
#[must_use]
pub fn bits(value: u64) -> String {
    format!("{} B {} b", value / 8, value % 8)
}

/// Formats a byte count as a sum of binary units, largest first.
///
/// Each unit contributes its own remainder, so 31 KiB and 180 B prints as
/// `31 KiB 180 B` rather than being rounded into one unit.
#[must_use]
pub fn bytes(value: u64) -> String {
    const UNITS: [(&str, u64); 7] = [
        ("EiB", 1 << 60),
        ("PiB", 1 << 50),
        ("TiB", 1 << 40),
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
        ("B", 1),
    ];
    let mut parts: Vec<String> = Vec::new();
    for (unit, multiplier) in UNITS {
        let remainder = (value / multiplier) % 1024;
        if remainder != 0 {
            parts.push(format!("{remainder} {unit}"));
        }
    }
    if parts.is_empty() {
        return "0 B".to_owned();
    }
    parts.join(" ")
}

/// A bar-chart histogram matching rapidgzip's.
pub struct Histogram {
    minimum: f64,
    maximum: f64,
    bins: Vec<u64>,
    unit: String,
    integral: bool,
}

const BAR_WIDTH: usize = 20;

impl Histogram {
    /// Builds a histogram of integral values with at most `bin_count` bins.
    ///
    /// The bin count shrinks to the number of distinct integers when the range
    /// is narrower, which is what keeps a histogram of small counts readable.
    #[must_use]
    pub fn integers(values: &[u64], bin_count: usize, unit: &str) -> Self {
        let doubles: Vec<f64> = values.iter().map(|&value| value as f64).collect();
        let mut histogram = Self::new(&doubles, bin_count, unit, true);
        histogram.fill(&doubles);
        histogram
    }

    /// Builds a histogram of real values with exactly `bin_count` bins.
    #[must_use]
    pub fn reals(values: &[f64], bin_count: usize, unit: &str) -> Self {
        let mut histogram = Self::new(values, bin_count, unit, false);
        histogram.fill(values);
        histogram
    }

    fn new(values: &[f64], bin_count: usize, unit: &str, integral: bool) -> Self {
        let minimum = values.iter().copied().fold(f64::INFINITY, f64::min);
        let maximum = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let mut bins = vec![0_u64; bin_count];
        if values.is_empty() {
            bins.clear();
        } else if integral {
            let useful = (maximum - minimum + 1.0) as usize;
            if useful < bin_count {
                bins.truncate(useful.max(1));
            }
        }
        Self {
            minimum,
            maximum,
            bins,
            unit: unit.to_owned(),
            integral,
        }
    }

    fn fill(&mut self, values: &[f64]) {
        for &value in values {
            self.merge(value);
        }
    }

    fn merge(&mut self, value: f64) {
        if self.bins.is_empty() || value < self.minimum || value > self.maximum {
            return;
        }
        let index = if value == self.maximum {
            self.bins.len() - 1
        } else {
            let unit_value = (value - self.minimum) / (self.maximum - self.minimum);
            (unit_value * self.bins.len() as f64).floor() as usize
        };
        if let Some(bin) = self.bins.get_mut(index) {
            *bin += 1;
        }
    }

    /// Returns how many values landed in the histogram.
    #[must_use]
    pub fn count(&self) -> u64 {
        self.bins.iter().sum()
    }

    fn bin_center(&self, index: usize) -> f64 {
        self.minimum + (self.maximum - self.minimum) / self.bins.len() as f64 * (index as f64 + 0.5)
    }

    fn label(&self, value: f64) -> String {
        let mut text = if value.round() == value {
            general(value)
        } else {
            scientific(value)
        };
        if !self.unit.is_empty() {
            text.push(' ');
            text.push_str(&self.unit);
        }
        text
    }

    /// Renders the bar chart, one line per bin.
    ///
    /// Only the first, last, and largest bins carry a label, right aligned to
    /// the widest of them, which is what makes the bars line up.
    #[must_use]
    pub fn plot(&self) -> String {
        if self.bins.is_empty() {
            return String::new();
        }
        let largest = self.bins.iter().copied().max().unwrap_or(0);
        let largest_index = self
            .bins
            .iter()
            .position(|&bin| bin == largest)
            .unwrap_or(0);

        let mut labels = vec![String::new(); self.bins.len()];
        labels[0] = self.label(self.minimum);
        let last = self.bins.len() - 1;
        labels[last] = self.label(self.maximum);
        if largest_index != 0 && largest_index != last {
            labels[largest_index] = self.label(self.bin_center(largest_index));
        }
        let label_width = labels.iter().map(String::len).max().unwrap_or(0);

        let mut result = String::new();
        for (index, &bin) in self.bins.iter().enumerate() {
            let filled = if largest == 0 {
                0
            } else {
                (bin as f64 / largest as f64 * BAR_WIDTH as f64) as usize
            };
            let bar = "=".repeat(filled.min(BAR_WIDTH));
            let count = if bin > 0 {
                format!("({bin})")
            } else {
                String::new()
            };
            result.push_str(&format!(
                "{:>label_width$} |{:<BAR_WIDTH$} {count}\n",
                labels[index], bar
            ));
        }
        let _ = self.integral;
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn general_matches_cxx_defaults() {
        // Values taken from real rapidgzip output.
        assert_eq!(general(3.2910795), "3.29108");
        assert_eq!(general(3.1497495), "3.14975");
        assert_eq!(general(34.767703), "34.7677");
        assert_eq!(general(92.347516), "92.3475");
        assert_eq!(general(8.1817626), "8.18176");
        assert_eq!(general(19.354838), "19.3548");
        assert_eq!(general(0.0), "0");
        assert_eq!(general(100.0), "100");
        assert_eq!(general(0.5), "0.5");
    }

    #[test]
    fn general_switches_to_exponent_form_like_cxx() {
        assert_eq!(general(1_000_000.0), "1e+06");
        assert_eq!(general(1_234_567.0), "1.23457e+06");
        assert_eq!(general(0.0001), "0.0001");
        assert_eq!(general(0.00001), "1e-05");
        assert_eq!(general(-1_234_567.0), "-1.23457e+06");
    }

    #[test]
    fn scientific_matches_cxx_defaults() {
        assert_eq!(scientific(193_515.8), "1.935158e+05");
        assert_eq!(scientific(2.875201), "2.875201e+00");
        assert_eq!(scientific(81_811.94), "8.181194e+04");
        assert_eq!(scientific(0.0), "0.000000e+00");
        assert_eq!(scientific(-1.5), "-1.500000e+00");
    }

    #[test]
    fn bits_split_into_bytes_and_remainder() {
        assert_eq!(bits(0), "0 B 0 b");
        assert_eq!(bits(8), "1 B 0 b");
        assert_eq!(bits(189_051), "23631 B 3 b");
    }

    #[test]
    fn bytes_sum_every_non_zero_unit() {
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(1), "1 B");
        assert_eq!(bytes(1024), "1 KiB");
        assert_eq!(bytes(31 * 1024 + 180), "31 KiB 180 B");
        assert_eq!(bytes(30 * 1024 + 415), "30 KiB 415 B");
        assert_eq!(bytes(1 << 20), "1 MiB");
        assert_eq!(bytes((1 << 20) + 1024 + 1), "1 MiB 1 KiB 1 B");
    }

    #[test]
    fn a_narrow_integer_range_shrinks_the_bin_count() {
        let histogram = Histogram::integers(&[3, 4, 5], 8, "");
        assert_eq!(histogram.bins.len(), 3);
        assert_eq!(histogram.count(), 3);
    }

    #[test]
    fn every_equal_value_lands_in_one_bin() {
        let histogram = Histogram::integers(&[7, 7, 7, 7], 8, "");
        assert_eq!(histogram.count(), 4);
        assert_eq!(histogram.bins.iter().filter(|&&bin| bin > 0).count(), 1);
    }

    #[test]
    fn the_maximum_lands_in_the_last_bin() {
        let histogram = Histogram::integers(&[0, 100], 8, "");
        assert_eq!(*histogram.bins.last().expect("bins"), 1);
        assert_eq!(histogram.bins[0], 1);
    }

    #[test]
    fn an_empty_histogram_plots_nothing() {
        let histogram = Histogram::integers(&[], 8, "");
        assert_eq!(histogram.plot(), "");
    }

    #[test]
    fn a_plot_labels_only_the_ends_and_the_peak() {
        let histogram = Histogram::integers(&[0, 50, 50, 50, 100], 8, "Bytes");
        let plot = histogram.plot();
        let lines: Vec<&str> = plot.lines().collect();
        assert_eq!(lines.len(), 8);
        // Labels are right aligned to the widest of them, so the leading
        // width depends on the peak label rather than being fixed.
        assert!(
            lines[0].trim_start().starts_with("0 Bytes |"),
            "{}",
            lines[0]
        );
        assert!(lines[7].contains("100 Bytes |"), "{}", lines[7]);
        // The peak bin carries the widest bar and the count in parentheses.
        assert!(plot.contains("(3)"), "{plot}");
        // Bins with no values print a bar of spaces and no count.
        assert!(lines[1].ends_with(' '), "{:?}", lines[1]);
    }
}
