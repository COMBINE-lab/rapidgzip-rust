use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// DEFLATE history-window size.
pub const WINDOW_SIZE: usize = 32 * 1024;

/// A speculative decoded symbol.
///
/// Values `0..=255` are literals. Values `32768..=65535` refer to a byte in
/// the predecessor window, ordered from oldest to newest.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(transparent)]
pub struct Symbol(pub(crate) u16);

impl Symbol {
    /// Creates a literal byte symbol.
    pub const fn literal(byte: u8) -> Self {
        Self(byte as u16)
    }

    /// Creates a reference into the predecessor window.
    pub fn marker(index: usize) -> Result<Self, MarkerError> {
        if index >= WINDOW_SIZE {
            return Err(MarkerError::IndexOutOfRange(index));
        }
        Ok(Self((WINDOW_SIZE + index) as u16))
    }

    /// Returns the encoded symbol.
    pub const fn encoded(self) -> u16 {
        self.0
    }

    /// Returns the literal value, when this is not a marker.
    pub const fn as_literal(self) -> Option<u8> {
        if self.0 <= u8::MAX as u16 {
            Some(self.0 as u8)
        } else {
            None
        }
    }

    fn marker_index(self) -> Option<usize> {
        if self.0 >= WINDOW_SIZE as u16 {
            Some(self.0 as usize - WINDOW_SIZE)
        } else {
            None
        }
    }

    pub(crate) const fn from_encoded(encoded: u16) -> Self {
        Self(encoded)
    }
}

/// A predecessor window in oldest-to-newest order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Window(Vec<u8>);

impl Window {
    /// Creates a validated window of at most 32 KiB.
    pub fn new(bytes: Vec<u8>) -> Result<Self, MarkerError> {
        if bytes.len() > WINDOW_SIZE {
            return Err(MarkerError::WindowTooLarge(bytes.len()));
        }
        Ok(Self(bytes))
    }

    /// Creates the empty history used at a gzip member boundary.
    pub const fn empty() -> Self {
        Self(Vec::new())
    }

    /// Returns the window bytes.
    pub fn as_slice(&self) -> &[u8] {
        &self.0
    }

    pub(crate) fn advanced_by(&self, bytes: &[u8]) -> Self {
        if bytes.len() >= WINDOW_SIZE {
            return Self(bytes[bytes.len() - WINDOW_SIZE..].to_vec());
        }
        let retained = WINDOW_SIZE.saturating_sub(bytes.len()).min(self.0.len());
        let mut result = Vec::with_capacity(retained + bytes.len());
        result.extend_from_slice(&self.0[self.0.len() - retained..]);
        result.extend_from_slice(bytes);
        Self(result)
    }
}

/// Speculative output retaining unknown predecessor-window references.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct MarkerBuffer {
    symbols: Vec<Symbol>,
}

impl MarkerBuffer {
    /// Creates a buffer from speculative symbols.
    pub const fn new(symbols: Vec<Symbol>) -> Self {
        Self { symbols }
    }

    /// Returns the stored symbols.
    pub fn symbols(&self) -> &[Symbol] {
        &self.symbols
    }

    pub(crate) fn len(&self) -> usize {
        self.symbols.len()
    }

    pub(crate) fn append_resolved_range(
        &self,
        range: std::ops::Range<usize>,
        output: &mut Vec<u8>,
        window: &Window,
    ) -> Result<(), MarkerError> {
        let symbols = self
            .symbols
            .get(range)
            .ok_or(MarkerError::IndexOutOfRange(self.symbols.len()))?;
        output.reserve(symbols.len());
        for &symbol in symbols {
            output.push(resolve_symbol(symbol, window)?);
        }
        Ok(())
    }

    /// Resolves marker references without re-decoding the chunk.
    pub fn resolve(self, window: &Window) -> Result<Vec<u8>, MarkerError> {
        self.resolve_ref(window)
    }

    /// Resolves marker references while retaining the encoded buffer.
    pub(crate) fn resolve_ref(&self, window: &Window) -> Result<Vec<u8>, MarkerError> {
        if self.symbols.len() >= 128 * 1024 && window.0.len() == WINDOW_SIZE {
            let output = resolve_lut(&self.symbols, window);
            return Ok(output);
        }
        let mut output = vec![0_u8; self.len()];
        #[cfg(target_arch = "x86_64")]
        if std::arch::is_x86_feature_detected!("sse4.1") {
            // SAFETY: runtime feature detection proves SSE4.1 availability.
            // The function receives slices whose bounds it checks before every
            // 128-bit load and 64-bit store.
            unsafe { resolve_sse41(&self.symbols, &mut output, window)? };
            return Ok(output);
        }
        #[cfg(target_arch = "aarch64")]
        {
            // SAFETY: Advanced SIMD is part of the baseline AArch64 ISA. The
            // function bounds-checks each vector load/store through chunking.
            unsafe { resolve_neon(&self.symbols, &mut output, window)? };
            Ok(output)
        }
        // Advanced SIMD is unconditional on AArch64, so the scalar fallback is
        // only reachable elsewhere: on x86-64 without SSE4.1, or on any other
        // architecture.
        #[cfg(not(target_arch = "aarch64"))]
        {
            resolve_scalar(&self.symbols, &mut output, window)?;
            Ok(output)
        }
    }
}

/// Resolves a large marker buffer through a branch-free 16-bit lookup table.
///
/// The low 256 entries preserve literal bytes and the high 32 Ki entries map
/// marker encodings directly into the full predecessor window. Speculative
/// chunks are normally several MiB, so amortizing the 64 KiB table setup avoids
/// a marker/literal branch for every decoded byte.
fn resolve_lut(symbols: &[Symbol], window: &Window) -> Vec<u8> {
    debug_assert_eq!(window.0.len(), WINDOW_SIZE);

    let mut lookup = [0_u8; u16::MAX as usize + 1];
    for (value, byte) in lookup[..=u8::MAX as usize].iter_mut().enumerate() {
        *byte = value as u8;
    }
    lookup[WINDOW_SIZE..].copy_from_slice(&window.0);
    let mut output = Vec::with_capacity(symbols.len());
    for (target, symbol) in output.spare_capacity_mut().iter_mut().zip(symbols) {
        target.write(lookup[usize::from(symbol.encoded())]);
    }
    // SAFETY: the loop above writes exactly one initialized byte for every
    // symbol into distinct slots of the vector's allocated spare capacity.
    unsafe { output.set_len(symbols.len()) };
    output
}

fn resolve_symbol(symbol: Symbol, window: &Window) -> Result<u8, MarkerError> {
    if let Some(literal) = symbol.as_literal() {
        return Ok(literal);
    }
    let index = symbol
        .marker_index()
        .expect("all non-literal symbol encodings are markers");
    let missing = WINDOW_SIZE.saturating_sub(window.0.len());
    if index < missing {
        return Err(MarkerError::WindowTooSmall {
            required: WINDOW_SIZE - index,
            actual: window.0.len(),
        });
    }
    Ok(window.0[index - missing])
}

fn resolve_scalar(
    symbols: &[Symbol],
    output: &mut [u8],
    window: &Window,
) -> Result<(), MarkerError> {
    for (target, &symbol) in output.iter_mut().zip(symbols) {
        *target = resolve_symbol(symbol, window)?;
    }
    Ok(())
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse4.1")]
unsafe fn resolve_sse41(
    symbols: &[Symbol],
    output: &mut [u8],
    window: &Window,
) -> Result<(), MarkerError> {
    use core::arch::x86_64::{
        __m128i, _mm_and_si128, _mm_loadu_si128, _mm_packus_epi16, _mm_set1_epi16,
        _mm_storel_epi64, _mm_testz_si128,
    };

    let vectorized = symbols.len() / 8 * 8;
    let high_byte_mask = _mm_set1_epi16(0xFF00_u16 as i16);
    for offset in (0..vectorized).step_by(8) {
        // SAFETY: `offset + 8 <= symbols.len()` and `Symbol` is transparent
        // over `u16`, so this unaligned 16-byte load is within the slice.
        let values = unsafe { _mm_loadu_si128(symbols.as_ptr().add(offset).cast::<__m128i>()) };
        if _mm_testz_si128(_mm_and_si128(values, high_byte_mask), high_byte_mask) != 0 {
            let packed = _mm_packus_epi16(values, values);
            // SAFETY: the loop invariant proves that eight output bytes remain.
            unsafe {
                _mm_storel_epi64(output.as_mut_ptr().add(offset).cast::<__m128i>(), packed);
            }
        } else {
            resolve_scalar(
                &symbols[offset..offset + 8],
                &mut output[offset..offset + 8],
                window,
            )?;
        }
    }
    resolve_scalar(&symbols[vectorized..], &mut output[vectorized..], window)
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn resolve_neon(
    symbols: &[Symbol],
    output: &mut [u8],
    window: &Window,
) -> Result<(), MarkerError> {
    use core::arch::aarch64::{
        vandq_u16, vld1q_u16, vmaxvq_u16, vmovn_u16, vsetq_lane_u16, vst1_u8,
    };

    let vectorized = symbols.len() / 8 * 8;
    // SAFETY: the pointer refers to a live eight-element `u16` array, which is
    // exactly the width this load reads.
    let mut mask = unsafe { vld1q_u16([0xFF00_u16; 8].as_ptr()) };
    // Keep an explicit lane operation so compilers consistently materialize
    // this as a vector constant across supported Rust/LLVM versions.
    mask = vsetq_lane_u16(0xFF00, mask, 0);
    for offset in (0..vectorized).step_by(8) {
        // SAFETY: the chunk calculation proves eight `u16` inputs remain.
        let values = unsafe { vld1q_u16(symbols.as_ptr().add(offset).cast::<u16>()) };
        if vmaxvq_u16(vandq_u16(values, mask)) == 0 {
            let packed = vmovn_u16(values);
            // SAFETY: the chunk calculation proves eight output bytes remain.
            unsafe { vst1_u8(output.as_mut_ptr().add(offset), packed) };
        } else {
            resolve_scalar(
                &symbols[offset..offset + 8],
                &mut output[offset..offset + 8],
                window,
            )?;
        }
    }
    resolve_scalar(&symbols[vectorized..], &mut output[vectorized..], window)
}

/// Marker construction or resolution failure.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum MarkerError {
    /// Marker index was not within a 32 KiB window.
    IndexOutOfRange(usize),
    /// A supplied history window exceeded DEFLATE's maximum.
    WindowTooLarge(usize),
    /// The supplied partial history did not contain a referenced byte.
    WindowTooSmall {
        /// Minimum number of newest history bytes required.
        required: usize,
        /// Number supplied.
        actual: usize,
    },
}

impl Display for MarkerError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::IndexOutOfRange(index) => {
                write!(formatter, "marker index {index} is out of range")
            }
            Self::WindowTooLarge(size) => write!(formatter, "window size {size} exceeds 32768"),
            Self::WindowTooSmall { required, actual } => write!(
                formatter,
                "marker requires {required} predecessor bytes, but only {actual} were supplied"
            ),
        }
    }
}

impl Error for MarkerError {}

#[cfg(test)]
mod tests {
    use super::{MarkerBuffer, Symbol, Window, resolve_scalar};
    use proptest::prelude::*;

    #[test]
    fn resolves_full_window_markers() {
        let window = Window::new((0..=255).cycle().take(32 * 1024).collect()).unwrap();
        let buffer = MarkerBuffer::new(vec![
            Symbol::literal(b'x'),
            Symbol::marker(0).unwrap(),
            Symbol::marker(32 * 1024 - 1).unwrap(),
        ]);
        assert_eq!(buffer.resolve(&window).unwrap(), [b'x', 0, 255]);
    }

    #[test]
    fn partial_window_uses_newest_alignment() {
        let window = Window::new(vec![10, 11, 12]).unwrap();
        let buffer = MarkerBuffer::new(vec![Symbol::marker(32 * 1024 - 3).unwrap()]);
        assert_eq!(buffer.resolve(&window).unwrap(), [10]);
    }

    #[test]
    fn dispatched_resolution_matches_scalar_for_mixed_symbols() {
        let window = Window::new(
            (0..super::WINDOW_SIZE)
                .map(|index| (index.wrapping_mul(37)) as u8)
                .collect(),
        )
        .unwrap();
        let symbols: Vec<_> = (0..65_537)
            .map(|index| {
                if index % 11 == 0 {
                    Symbol::marker(index % super::WINDOW_SIZE).unwrap()
                } else {
                    Symbol::literal(index as u8)
                }
            })
            .collect();
        let mut scalar = vec![0; symbols.len()];
        resolve_scalar(&symbols, &mut scalar, &window).unwrap();
        let dispatched = MarkerBuffer::new(symbols).resolve(&window).unwrap();
        assert_eq!(dispatched, scalar);
    }

    proptest! {
        #[test]
        fn dispatched_resolution_matches_scalar_for_arbitrary_valid_symbols(
            encoded in prop::collection::vec(any::<u16>(), 0..4096)
        ) {
            let window = Window::new(
                (0..super::WINDOW_SIZE)
                    .map(|index| (index.wrapping_mul(131)) as u8)
                    .collect(),
            )
            .unwrap();
            let symbols: Vec<_> = encoded
                .into_iter()
                .map(|value| {
                    if value & 1 == 0 {
                        Symbol::literal((value >> 1) as u8)
                    } else {
                        Symbol::marker(usize::from(value) % super::WINDOW_SIZE).unwrap()
                    }
                })
                .collect();
            let mut scalar = vec![0; symbols.len()];
            resolve_scalar(&symbols, &mut scalar, &window).unwrap();
            let dispatched = MarkerBuffer::new(symbols).resolve(&window).unwrap();
            prop_assert_eq!(dispatched, scalar);
        }
    }
}
