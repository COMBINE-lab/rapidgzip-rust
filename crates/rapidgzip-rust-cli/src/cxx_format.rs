//! Formatting primitives used by rapidgzip 0.16.0's analysis report.
//!
//! C++ iostream defaults and the reference histogram have edge behavior that
//! differs from Rust's ordinary formatting. Keeping the compatibility layer in
//! the CLI prevents those presentation quirks from entering the core model.

/// Formats like a default C++ stream (`%.6g`).
pub fn general(value: f64) -> String {
    format_general(value, 6)
}

/// Formats like C++ `std::scientific` at its default precision (`%.6e`).
pub fn scientific(value: f64) -> String {
    format_scientific(value, 6)
}

fn format_scientific(value: f64, precision: usize) -> String {
    if value == 0.0 {
        let sign = if value.is_sign_negative() { "-" } else { "" };
        return format!("{sign}{:.*}e+00", precision, 0.0);
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
        return if value.is_sign_negative() {
            "-0".to_owned()
        } else {
            "0".to_owned()
        };
    }
    if !value.is_finite() {
        return format_scientific(value, precision);
    }
    let exponent = value.abs().log10().floor() as i32;
    if exponent < -4 || exponent >= precision as i32 {
        let text = format_scientific(value, precision.saturating_sub(1));
        let (mantissa, exponent_part) = text.split_once('e').expect("scientific has an e");
        return format!("{}e{exponent_part}", trim_fraction(mantissa));
    }
    let decimals = (precision as i32 - 1 - exponent).max(0) as usize;
    trim_fraction(&format!("{value:.decimals$}"))
}

fn trim_fraction(text: &str) -> String {
    if !text.contains('.') {
        return text.to_owned();
    }
    text.trim_end_matches('0').trim_end_matches('.').to_owned()
}

/// Formats a bit count as `"{bytes} B {bits} b"`.
pub fn bits(value: u64) -> String {
    format!("{} B {} b", value / 8, value % 8)
}

/// Formats bytes as the reference's sum of binary-unit remainders.
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
    let mut parts = Vec::new();
    for (unit, multiplier) in UNITS {
        let remainder = (value / multiplier) % 1024;
        if remainder != 0 {
            parts.push(format!("{remainder} {unit}"));
        }
    }
    if parts.is_empty() {
        "0 B".to_owned()
    } else {
        parts.join(" ")
    }
}

/// Bar-chart histogram compatible with rapidgzip's report.
pub struct Histogram {
    minimum: f64,
    maximum: f64,
    bins: Vec<u64>,
    unit: String,
}

const BAR_WIDTH: usize = 20;

impl Histogram {
    /// Builds a histogram for integer observations.
    pub fn integers(values: &[u64], bin_count: usize, unit: &str) -> Self {
        let doubles: Vec<f64> = values.iter().map(|&value| value as f64).collect();
        let mut histogram = Self::new(&doubles, bin_count, unit, true);
        histogram.fill(&doubles);
        histogram
    }

    /// Builds a histogram for real observations.
    pub fn reals(values: &[f64], bin_count: usize, unit: &str) -> Self {
        let mut histogram = Self::new(values, bin_count, unit, false);
        histogram.fill(values);
        histogram
    }

    fn new(values: &[f64], bin_count: usize, unit: &str, integral: bool) -> Self {
        let minimum = values.iter().copied().fold(f64::INFINITY, f64::min);
        let maximum = values.iter().copied().fold(f64::NEG_INFINITY, f64::max);
        let mut bins = vec![0; bin_count];
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
        }
    }

    fn fill(&mut self, values: &[f64]) {
        for &value in values {
            if self.bins.is_empty() || value < self.minimum || value > self.maximum {
                continue;
            }
            let index = if value == self.maximum {
                self.bins.len() - 1
            } else {
                let unit_value = (value - self.minimum) / (self.maximum - self.minimum);
                (unit_value * self.bins.len() as f64).floor() as usize
            };
            self.bins[index] += 1;
        }
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

    /// Renders one reference-compatible line per bin.
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
            let count = if bin == 0 {
                String::new()
            } else {
                format!("({bin})")
            };
            result.push_str(&format!(
                "{:>label_width$} |{:<BAR_WIDTH$} {count}\n",
                labels[index], bar
            ));
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn general_matches_cxx_defaults() {
        assert_eq!(general(3.2910795), "3.29108");
        assert_eq!(general(34.767703), "34.7677");
        assert_eq!(general(0.0), "0");
        assert_eq!(general(-0.0), "-0");
        assert_eq!(general(1_000_000.0), "1e+06");
        assert_eq!(general(0.00001), "1e-05");
    }

    #[test]
    fn scientific_matches_cxx_defaults() {
        assert_eq!(scientific(193_515.8), "1.935158e+05");
        assert_eq!(scientific(2.875201), "2.875201e+00");
        assert_eq!(scientific(0.0), "0.000000e+00");
        assert_eq!(scientific(-0.0), "-0.000000e+00");
    }

    #[test]
    fn bit_and_byte_units_match_reference_examples() {
        assert_eq!(bits(189_051), "23631 B 3 b");
        assert_eq!(bytes(0), "0 B");
        assert_eq!(bytes(31 * 1024 + 180), "31 KiB 180 B");
        assert_eq!(bytes((1 << 20) + 1024 + 1), "1 MiB 1 KiB 1 B");
    }

    #[test]
    fn histogram_handles_narrow_equal_and_max_boundaries() {
        let narrow = Histogram::integers(&[3, 4, 5], 8, "");
        assert_eq!(narrow.bins.len(), 3);
        let equal = Histogram::integers(&[7, 7, 7, 7], 8, "");
        assert_eq!(equal.bins.iter().filter(|&&bin| bin > 0).count(), 1);
        let maximum = Histogram::integers(&[0, 100], 8, "");
        assert_eq!(maximum.bins[0], 1);
        assert_eq!(*maximum.bins.last().unwrap(), 1);
        assert_eq!(Histogram::integers(&[], 8, "").plot(), "");
    }

    #[test]
    fn histogram_plot_labels_ends_and_peak() {
        let plot = Histogram::integers(&[0, 50, 50, 50, 100], 8, "Bytes").plot();
        let lines: Vec<_> = plot.lines().collect();
        assert_eq!(lines.len(), 8);
        assert!(lines[0].trim_start().starts_with("0 Bytes |"));
        assert!(lines[7].contains("100 Bytes |"));
        assert!(plot.contains("(3)"));
    }
}
