use crate::DecodeError;

/// Incremental IEEE CRC32 used by gzip (reflected / gzip trailer convention).
pub(crate) struct Crc32(u32);

#[cfg(feature = "isal")]
// Linked via the `isal` feature (same `libisal` as inflate). Prefer ISA-L's
// PCLMUL path for verify-on decode; same reflected IEEE poly as zlib/gzip
// (check value for "123456789" is still 0xCBF4_3926).
unsafe extern "C" {
    fn crc32_gzip_refl(init_crc: u32, buf: *const u8, len: u64) -> u32;
}

impl Crc32 {
    pub(crate) const fn new() -> Self {
        Self(0)
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        #[cfg(feature = "isal")]
        {
            // SAFETY: `bytes` is a live immutable slice for the call; ISA-L
            // only reads `len` bytes from `buf`. Same reflected IEEE poly as
            // gzip trailers / zlib `crc32`.
            self.0 = unsafe { crc32_gzip_refl(self.0, bytes.as_ptr(), bytes.len() as u64) };
        }
        #[cfg(not(feature = "isal"))]
        {
            // SAFETY: `bytes` is a live immutable slice for the call.
            // zlib-rs runtime dispatch (PCLMUL/ACLE or portable braid).
            self.0 =
                unsafe { libz_rs_sys::crc32_z(self.0.into(), bytes.as_ptr(), bytes.len()) as u32 };
        }
    }

    pub(crate) const fn finish(&self) -> u32 {
        self.0
    }
}

/// Verifies an optional external whole-stream CRC32 for raw DEFLATE.
///
/// When `list` is empty, succeeds immediately. Otherwise compares the
/// gzip-style IEEE CRC32 of the full uncompressed output against the single
/// expected value. Call sites only run after [`crate::DecoderBuilder::build`],
/// which rejects lists longer than one element.
pub(crate) fn verify_raw_crc32_list(list: &[u32], crc: &Crc32) -> Result<(), DecodeError> {
    debug_assert!(
        list.len() <= 1,
        "raw_crc32_list longer than one must be rejected at DecoderBuilder::build"
    );
    let Some(&expected) = list.first() else {
        return Ok(());
    };
    let actual = crc.finish();
    if expected != actual {
        return Err(DecodeError::ChecksumMismatch {
            member: 0,
            expected,
            actual,
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::Crc32;

    #[test]
    fn standard_check_value() {
        let mut crc = Crc32::new();
        crc.update(b"123456789");
        assert_eq!(crc.finish(), 0xCBF4_3926);
    }
}
