/// Incremental IEEE CRC32 used by gzip.
pub(crate) struct Crc32(u32);

impl Crc32 {
    pub(crate) const fn new() -> Self {
        Self(0)
    }

    pub(crate) fn update(&mut self, bytes: &[u8]) {
        // SAFETY: `bytes.as_ptr()` and `bytes.len()` describe one live,
        // immutable allocation for the duration of the call. `crc32_z`
        // performs runtime dispatch to zlib-rs's PCLMUL/ACLE implementations
        // when supported and otherwise uses its portable braid routine.
        self.0 = unsafe { libz_rs_sys::crc32_z(self.0.into(), bytes.as_ptr(), bytes.len()) as u32 };
    }

    pub(crate) const fn finish(&self) -> u32 {
        self.0
    }
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
