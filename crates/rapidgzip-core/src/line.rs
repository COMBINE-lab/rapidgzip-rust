//! Vectorized newline counting shared by reports and index annotation.

#[inline]
pub(crate) fn count_newlines(bytes: &[u8]) -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        if std::arch::is_x86_feature_detected!("avx2") {
            // SAFETY: runtime detection proves AVX2 is available, and the
            // helper bounds every unaligned load to the supplied slice.
            unsafe { count_newlines_avx2(bytes) }
        } else {
            // SAFETY: SSE2 is part of the x86-64 architecture baseline, and
            // the helper bounds every unaligned load to the supplied slice.
            unsafe { count_newlines_sse2(bytes) }
        }
    }
    #[cfg(target_arch = "aarch64")]
    {
        // SAFETY: Advanced SIMD is part of the AArch64 baseline, and the
        // helper bounds every unaligned load to the supplied slice.
        unsafe { count_newlines_neon(bytes) }
    }
    #[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
    count_newlines_scalar(bytes)
}

#[inline]
fn count_newlines_scalar(bytes: &[u8]) -> u64 {
    bytes.iter().filter(|&&byte| byte == b'\n').count() as u64
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "avx2")]
unsafe fn count_newlines_avx2(bytes: &[u8]) -> u64 {
    use std::arch::x86_64::{
        __m256i, _mm256_cmpeq_epi8, _mm256_loadu_si256, _mm256_movemask_epi8, _mm256_set1_epi8,
    };

    let needle = _mm256_set1_epi8(b'\n' as i8);
    let mut offset = 0_usize;
    let mut count = 0_u64;
    while offset + 32 <= bytes.len() {
        // SAFETY: the loop condition proves bytes[offset..] contains 32
        // readable bytes. `loadu` has no alignment requirement.
        let matches = unsafe {
            let input = _mm256_loadu_si256(bytes.as_ptr().add(offset).cast::<__m256i>());
            _mm256_cmpeq_epi8(input, needle)
        };
        count += u64::from((_mm256_movemask_epi8(matches) as u32).count_ones());
        offset += 32;
    }
    count + count_newlines_scalar(&bytes[offset..])
}

#[cfg(target_arch = "x86_64")]
#[target_feature(enable = "sse2")]
unsafe fn count_newlines_sse2(bytes: &[u8]) -> u64 {
    use std::arch::x86_64::{
        __m128i, _mm_cmpeq_epi8, _mm_loadu_si128, _mm_movemask_epi8, _mm_set1_epi8,
    };

    let needle = _mm_set1_epi8(b'\n' as i8);
    let mut offset = 0_usize;
    let mut count = 0_u64;
    while offset + 16 <= bytes.len() {
        // SAFETY: the loop condition proves bytes[offset..] contains 16
        // readable bytes. `loadu` has no alignment requirement.
        let matches = unsafe {
            let input = _mm_loadu_si128(bytes.as_ptr().add(offset).cast::<__m128i>());
            _mm_cmpeq_epi8(input, needle)
        };
        count += u64::from((_mm_movemask_epi8(matches) as u32).count_ones());
        offset += 16;
    }
    count + count_newlines_scalar(&bytes[offset..])
}

#[cfg(target_arch = "aarch64")]
#[target_feature(enable = "neon")]
unsafe fn count_newlines_neon(bytes: &[u8]) -> u64 {
    use std::arch::aarch64::{vaddvq_u8, vceqq_u8, vdupq_n_u8, vld1q_u8, vshrq_n_u8};

    let needle = vdupq_n_u8(b'\n');
    let mut offset = 0_usize;
    let mut count = 0_u64;
    while offset + 16 <= bytes.len() {
        // SAFETY: the loop condition proves bytes[offset..] contains 16
        // readable bytes. AArch64 NEON loads permit unaligned addresses.
        let input = unsafe { vld1q_u8(bytes.as_ptr().add(offset)) };
        let matches = vshrq_n_u8::<7>(vceqq_u8(input, needle));
        count += u64::from(vaddvq_u8(matches));
        offset += 16;
    }
    count + count_newlines_scalar(&bytes[offset..])
}

#[cfg(test)]
mod tests {
    use super::{count_newlines, count_newlines_scalar};

    #[test]
    fn dispatched_count_matches_scalar_across_vector_edges() {
        let mut bytes = vec![b'x'; 4097];
        for offset in [0, 1, 15, 16, 17, 31, 32, 33, 255, 1024, 4096] {
            bytes[offset] = b'\n';
        }
        for length in 0..=bytes.len() {
            assert_eq!(
                count_newlines(&bytes[..length]),
                count_newlines_scalar(&bytes[..length]),
                "length {length}",
            );
        }
    }
}
