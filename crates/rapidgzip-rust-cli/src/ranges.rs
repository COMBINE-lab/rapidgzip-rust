//! Parsing and extraction for rapidgzip-compatible `--ranges` requests.

use rapidgzip_core::{IndexedReader, ReadAt};
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read, Seek, SeekFrom, Write};

/// A byte- or line-addressed quantity.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Quantity {
    /// A number of bytes.
    Bytes(u64),
    /// A number of newline-delimited lines.
    Lines(u64),
    /// All remaining output, valid only as a range size.
    Rest,
}

/// One `SIZE@OFFSET` extraction request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Range {
    /// Amount of output requested.
    pub size: Quantity,
    /// Starting byte or line offset.
    pub offset: Quantity,
}

impl Range {
    /// Returns whether either part requires line-aware index metadata.
    pub const fn needs_lines(self) -> bool {
        matches!(self.size, Quantity::Lines(_)) || matches!(self.offset, Quantity::Lines(_))
    }
}

/// A malformed range specification.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeError(String);

impl Display for RangeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RangeError {}

/// Parses comma-separated `SIZE@OFFSET` requests.
///
/// Sizes and offsets accept bare bytes, `B`, or binary `KiB` through `EiB`
/// suffixes. `L` selects lines and `inf` is accepted only as a size.
pub fn parse(specification: &str) -> Result<Vec<Range>, RangeError> {
    if specification.trim().is_empty() {
        return Err(RangeError("the range specification is empty".to_owned()));
    }
    specification.split(',').map(parse_range).collect()
}

fn parse_range(element: &str) -> Result<Range, RangeError> {
    let element = element.trim();
    let Some((size, offset)) = element.split_once('@') else {
        return Err(RangeError(format!(
            "range \"{element}\" is not SIZE@OFFSET"
        )));
    };
    if offset.contains('@') {
        return Err(RangeError(format!(
            "range \"{element}\" contains more than one @"
        )));
    }
    let size = parse_quantity(size.trim(), true)
        .map_err(|reason| RangeError(format!("size of range \"{element}\": {reason}")))?;
    let offset = parse_quantity(offset.trim(), false)
        .map_err(|reason| RangeError(format!("offset of range \"{element}\": {reason}")))?;
    Ok(Range { size, offset })
}

fn parse_quantity(text: &str, allow_rest: bool) -> Result<Quantity, String> {
    if text.is_empty() {
        return Err("is empty".to_owned());
    }
    if text == "inf" {
        return allow_rest
            .then_some(Quantity::Rest)
            .ok_or_else(|| "\"inf\" is only meaningful as a size".to_owned());
    }
    if let Some(digits) = text.strip_suffix('L') {
        return digits
            .parse::<u64>()
            .map(Quantity::Lines)
            .map_err(|_| format!("\"{digits}\" is not a line count"));
    }

    const UNITS: [(&str, u64); 7] = [
        ("EiB", 1 << 60),
        ("PiB", 1 << 50),
        ("TiB", 1 << 40),
        ("GiB", 1 << 30),
        ("MiB", 1 << 20),
        ("KiB", 1 << 10),
        ("B", 1),
    ];
    for (unit, multiplier) in UNITS {
        let Some(digits) = text.strip_suffix(unit) else {
            continue;
        };
        let count = digits
            .parse::<u64>()
            .map_err(|_| format!("\"{digits}\" is not a number"))?;
        return count
            .checked_mul(multiplier)
            .map(Quantity::Bytes)
            .ok_or_else(|| format!("\"{text}\" overflows a 64-bit byte count"));
    }

    text.parse::<u64>()
        .map(Quantity::Bytes)
        .map_err(|_| format!("\"{text}\" is not a number, unit, or line count"))
}

/// Writes every range in the supplied order and returns bytes written.
///
/// Overlapping ranges remain separate; the requested sequence is preserved.
pub fn extract<R: ReadAt, W: Write>(
    reader: &mut IndexedReader<R>,
    ranges: &[Range],
    output: &mut W,
) -> io::Result<u64> {
    let mut written = 0_u64;
    for range in ranges {
        let start = match range.offset {
            Quantity::Bytes(offset) => reader.seek(SeekFrom::Start(offset))?,
            Quantity::Lines(line) => reader.seek_to_line(line)?,
            Quantity::Rest => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "inf is only meaningful as a size",
                ));
            }
        };
        let count = match range.size {
            Quantity::Bytes(count) => io::copy(&mut reader.take(count), output)?,
            Quantity::Rest => io::copy(reader, output)?,
            Quantity::Lines(count) => {
                let end = end_of_lines(reader, start, count)?;
                reader.seek(SeekFrom::Start(start))?;
                io::copy(&mut reader.take(end - start), output)?
            }
        };
        written = written.checked_add(count).ok_or_else(|| {
            io::Error::new(io::ErrorKind::InvalidData, "range byte count overflow")
        })?;
    }
    Ok(written)
}

fn end_of_lines<R: ReadAt>(
    reader: &mut IndexedReader<R>,
    start: u64,
    count: u64,
) -> io::Result<u64> {
    if count == 0 {
        return Ok(start);
    }
    let mut remaining = count;
    let mut position = start;
    let mut scratch = [0_u8; 64 * 1024];
    loop {
        let read = reader.read(&mut scratch)?;
        if read == 0 {
            return Ok(position);
        }
        for (index, &byte) in scratch[..read].iter().enumerate() {
            if byte == b'\n' {
                remaining -= 1;
                if remaining == 0 {
                    return Ok(position + index as u64 + 1);
                }
            }
        }
        position += read as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_example_parses() {
        assert_eq!(
            parse("10@0,1KiB@15KiB,5L@20L,inf@40L").expect("parse"),
            vec![
                Range {
                    size: Quantity::Bytes(10),
                    offset: Quantity::Bytes(0),
                },
                Range {
                    size: Quantity::Bytes(1024),
                    offset: Quantity::Bytes(15 * 1024),
                },
                Range {
                    size: Quantity::Lines(5),
                    offset: Quantity::Lines(20),
                },
                Range {
                    size: Quantity::Rest,
                    offset: Quantity::Lines(40),
                },
            ]
        );
    }

    #[test]
    fn binary_units_are_checked() {
        for (text, expected) in [
            ("1B", 1_u64),
            ("2KiB", 2 << 10),
            ("3MiB", 3 << 20),
            ("4GiB", 4 << 30),
            ("5TiB", 5 << 40),
            ("6PiB", 6 << 50),
            ("7EiB", 7 << 60),
        ] {
            assert_eq!(
                parse(&format!("{text}@0")).expect("parse")[0].size,
                Quantity::Bytes(expected)
            );
        }
        assert!(parse("18446744073709551615EiB@0").is_err());
    }

    #[test]
    fn malformed_elements_are_rejected_with_context() {
        for specification in [
            "", "   ", "10", "@0", "10@", "abc@0", "10@abc", "10kb@0", "inf@inf", "10@inf",
            "1LL@0", "1@2@3",
        ] {
            assert!(parse(specification).is_err(), "accepted {specification:?}");
        }
        assert!(
            parse("10@0,bogus@5")
                .expect_err("invalid second element")
                .to_string()
                .contains("bogus@5")
        );
    }

    #[test]
    fn line_addressing_is_detected() {
        assert!(!parse("10@0").expect("parse")[0].needs_lines());
        assert!(parse("10@5L").expect("parse")[0].needs_lines());
        assert!(parse("5L@10").expect("parse")[0].needs_lines());
    }
}
