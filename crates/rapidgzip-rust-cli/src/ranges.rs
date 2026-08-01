//! Parsing and extracting `--ranges` specifications.
//!
//! The syntax follows rapidgzip: a comma-separated list of `SIZE@OFFSET`,
//! where each side is independently a byte count or a line count, and the size
//! may be `inf` to mean the rest of the input.

use rapidgzip_core::{IndexedReader, ReadAt};
use std::fmt::{self, Display, Formatter};
use std::io::{self, Read, Seek, SeekFrom, Write};

/// A quantity measured in bytes or in lines.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Quantity {
    /// A byte count.
    Bytes(u64),
    /// A line count.
    Lines(u64),
    /// Everything remaining, valid only as a size.
    Rest,
}

/// One `SIZE@OFFSET` request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Range {
    /// How much to emit.
    pub size: Quantity,
    /// Where to start.
    pub offset: Quantity,
}

impl Range {
    /// Returns whether this range is addressed by line at either end.
    pub const fn needs_lines(&self) -> bool {
        matches!(self.size, Quantity::Lines(_)) || matches!(self.offset, Quantity::Lines(_))
    }
}

/// Why a range specification could not be parsed.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RangeError(String);

impl Display for RangeError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for RangeError {}

/// Parses a complete `--ranges` specification.
///
/// # Errors
///
/// Returns [`RangeError`] naming the offending element, since a mistyped unit
/// in one element of a long list is otherwise hard to find.
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
        return if allow_rest {
            Ok(Quantity::Rest)
        } else {
            Err("\"inf\" is only meaningful as a size".to_owned())
        };
    }
    if let Some(digits) = text.strip_suffix('L') {
        let count = digits
            .parse::<u64>()
            .map_err(|_| format!("\"{digits}\" is not a line count"))?;
        return Ok(Quantity::Lines(count));
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
        let bytes = count
            .checked_mul(multiplier)
            .ok_or_else(|| format!("\"{text}\" overflows a 64-bit byte count"))?;
        return Ok(Quantity::Bytes(bytes));
    }

    text.parse::<u64>()
        .map(Quantity::Bytes)
        .map_err(|_| format!("\"{text}\" is not a number, a unit, or a line count"))
}

/// Writes every requested range to `output`, in the order given.
///
/// Ranges may overlap and are not merged: the caller asked for a specific
/// sequence of extracts and gets exactly that.
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
                    "\"inf\" is only meaningful as a size",
                ));
            }
        };
        written += match range.size {
            Quantity::Bytes(count) => io::copy(&mut reader.take(count), output)?,
            Quantity::Rest => io::copy(reader, output)?,
            Quantity::Lines(count) => {
                let end = end_of_lines(reader, start, count)?;
                reader.seek(SeekFrom::Start(start))?;
                io::copy(&mut reader.take(end - start), output)?
            }
        };
    }
    Ok(written)
}

/// Returns the offset just past `count` lines beginning at `start`.
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
            if byte != b'\n' {
                continue;
            }
            remaining -= 1;
            if remaining == 0 {
                return Ok(position + index as u64 + 1);
            }
        }
        position += read as u64;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_reference_example_parses() {
        assert_eq!(
            parse("10@0,1KiB@15KiB,5L@20L,inf@40L").expect("parse"),
            vec![
                Range {
                    size: Quantity::Bytes(10),
                    offset: Quantity::Bytes(0)
                },
                Range {
                    size: Quantity::Bytes(1024),
                    offset: Quantity::Bytes(15 * 1024)
                },
                Range {
                    size: Quantity::Lines(5),
                    offset: Quantity::Lines(20)
                },
                Range {
                    size: Quantity::Rest,
                    offset: Quantity::Lines(40)
                },
            ]
        );
    }

    #[test]
    fn every_binary_unit_is_accepted() {
        for (text, expected) in [
            ("1B", 1_u64),
            ("2KiB", 2048),
            ("3MiB", 3 << 20),
            ("4GiB", 4 << 30),
            ("5TiB", 5 << 40),
            ("6PiB", 6 << 50),
            ("7EiB", 7 << 60),
        ] {
            assert_eq!(
                parse(&format!("{text}@0")).expect("parse")[0].size,
                Quantity::Bytes(expected),
                "for {text}"
            );
        }
    }

    #[test]
    fn surrounding_space_is_ignored() {
        assert_eq!(
            parse(" 10@0 , 20@30 ").expect("parse").len(),
            2,
            "spaces around elements should not matter"
        );
    }

    #[test]
    fn malformed_specifications_are_rejected() {
        for specification in [
            "",
            "   ",
            "10",
            "@0",
            "10@",
            "abc@0",
            "10@abc",
            "10kb@0",
            "inf@inf",
            "10@inf",
            "1LL@0",
            "18446744073709551615EiB@0",
        ] {
            assert!(
                parse(specification).is_err(),
                "\"{specification}\" should not parse"
            );
        }
    }

    #[test]
    fn an_error_names_the_offending_element() {
        let error = parse("10@0,bogus@5").expect_err("second element is invalid");
        assert!(
            error.to_string().contains("bogus@5"),
            "error should name the element: {error}"
        );
    }

    #[test]
    fn line_addressing_is_detected() {
        assert!(!parse("10@0").expect("parse")[0].needs_lines());
        assert!(parse("10@5L").expect("parse")[0].needs_lines());
        assert!(parse("5L@10").expect("parse")[0].needs_lines());
    }
}
