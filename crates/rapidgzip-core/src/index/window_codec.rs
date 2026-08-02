//! zlib-wrapper codec for stored predecessor windows.
//!
//! The gztool on-disk format stores windows zlib-compressed, and the native
//! format and the in-memory index reuse the same encoding to bound resident
//! memory. Every non-empty payload expands to exactly [`WINDOW_SIZE`].

use super::{IndexError, WINDOW_SIZE};
use libz_rs_sys as z;
use std::ffi::{c_int, c_ulong};

/// Compression level used for stored windows, matching gztool.
const LEVEL: c_int = 9;

/// Compresses `bytes` under a zlib wrapper.
///
/// An empty input produces an empty payload, which callers use to mean that no
/// history is stored.
pub(crate) fn zlib_compress_window(bytes: &[u8]) -> Result<Vec<u8>, IndexError> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let mut stream = z::z_stream::default();
    // SAFETY: `stream` is a live, uniquely borrowed `z_stream`, `zlibVersion`
    // returns a static NUL-terminated version string, and the reported
    // structure size matches the exact Rust ABI type passed.
    let status = unsafe {
        z::deflateInit_(
            &mut stream,
            LEVEL,
            z::zlibVersion(),
            size_of::<z::z_stream>() as i32,
        )
    };
    if status != z::Z_OK {
        return Err(IndexError::WindowCodec("deflate initialization failed"));
    }
    let guard = DeflateGuard(&mut stream);

    // `deflateBound` is an upper bound for compressing the whole input in one
    // pass, so a single `Z_FINISH` call always has room. Its argument is a
    // `c_ulong`, which is 32 bits on Windows and 64 elsewhere, and a window is
    // far below either limit.
    let source_length = c_ulong::try_from(bytes.len()).unwrap_or(c_ulong::MAX);
    // SAFETY: the stream was initialized above and is uniquely borrowed here.
    let bound = unsafe { z::deflateBound(guard.0, source_length) } as usize;
    let mut output = vec![0u8; bound.max(64)];

    guard.0.next_in = bytes.as_ptr();
    guard.0.avail_in = bytes.len() as u32;
    guard.0.next_out = output.as_mut_ptr();
    guard.0.avail_out = output.len() as u32;

    // SAFETY: the pointers and counts above describe the live `bytes` and
    // `output` slices for the duration of the call.
    let status = unsafe { z::deflate(guard.0, z::Z_FINISH) };
    if status != z::Z_STREAM_END {
        return Err(IndexError::WindowCodec(
            "deflate did not finish in one pass",
        ));
    }
    let produced = output.len() - guard.0.avail_out as usize;
    output.truncate(produced);
    Ok(output)
}

/// Expands a zlib-wrapped window payload, refusing anything that would exceed
/// [`WINDOW_SIZE`] bytes.
pub(crate) fn zlib_decompress_window(payload: &[u8]) -> Result<Vec<u8>, IndexError> {
    if payload.is_empty() {
        return Err(IndexError::WindowCodec("empty compressed window payload"));
    }

    let mut stream = z::z_stream::default();
    // SAFETY: as in `zlib_compress_window`, with `15` selecting the zlib
    // wrapper over a 32 KiB window.
    let status = unsafe {
        z::inflateInit2_(
            &mut stream,
            15,
            z::zlibVersion(),
            size_of::<z::z_stream>() as i32,
        )
    };
    if status != z::Z_OK {
        return Err(IndexError::WindowCodec("inflate initialization failed"));
    }
    let guard = InflateGuard(&mut stream);

    // One extra byte of room detects payloads that expand past a full window.
    let mut output = vec![0u8; WINDOW_SIZE + 1];
    guard.0.next_in = payload.as_ptr();
    guard.0.avail_in = payload.len() as u32;
    guard.0.next_out = output.as_mut_ptr();
    guard.0.avail_out = output.len() as u32;

    // SAFETY: the pointers and counts above describe the live `payload` and
    // `output` slices for the duration of the call.
    let status = unsafe { z::inflate(guard.0, z::Z_FINISH) };
    let produced = output.len() - guard.0.avail_out as usize;
    if produced > WINDOW_SIZE {
        return Err(IndexError::WindowCodec(
            "window payload expands beyond 32768 bytes",
        ));
    }
    if status != z::Z_STREAM_END {
        return Err(IndexError::WindowCodec("invalid window payload"));
    }
    if guard.0.avail_in != 0 {
        return Err(IndexError::WindowCodec(
            "trailing bytes after compressed window",
        ));
    }
    if produced != WINDOW_SIZE {
        return Err(IndexError::InvalidWindowSize(produced as u64));
    }
    output.truncate(produced);
    Ok(output)
}

struct DeflateGuard<'a>(&'a mut z::z_stream);

impl Drop for DeflateGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: the guard holds exactly one successfully initialized stream
        // and ends it exactly once.
        let _ = unsafe { z::deflateEnd(self.0) };
    }
}

struct InflateGuard<'a>(&'a mut z::z_stream);

impl Drop for InflateGuard<'_> {
    fn drop(&mut self) {
        // SAFETY: the guard holds exactly one successfully initialized stream
        // and ends it exactly once.
        let _ = unsafe { z::inflateEnd(self.0) };
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::index::StoredWindow;

    fn pseudo_random(length: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; length];
        let mut state = 0x1234_5678u32;
        for byte in &mut bytes {
            state = state.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            *byte = (state >> 24) as u8;
        }
        bytes
    }

    #[test]
    fn round_trips_a_compressible_window() {
        let window = vec![0x5au8; WINDOW_SIZE];
        let compressed = zlib_compress_window(&window).expect("compress");
        assert!(compressed.len() < window.len());
        assert_eq!(
            zlib_decompress_window(&compressed).expect("decompress"),
            window
        );
    }

    #[test]
    fn round_trips_an_incompressible_window() {
        let window = pseudo_random(WINDOW_SIZE);
        let compressed = zlib_compress_window(&window).expect("compress");
        assert_eq!(
            zlib_decompress_window(&compressed).expect("decompress"),
            window
        );
    }

    #[test]
    fn rejects_an_empty_payload() {
        assert!(zlib_compress_window(&[]).expect("compress").is_empty());
        assert!(matches!(
            zlib_decompress_window(&[]),
            Err(IndexError::WindowCodec(_))
        ));
    }

    #[test]
    fn rejects_a_payload_that_expands_beyond_the_window() {
        let oversized = pseudo_random(WINDOW_SIZE + 1);
        let compressed = zlib_compress_window(&oversized).expect("compress");
        assert_eq!(
            zlib_decompress_window(&compressed),
            Err(IndexError::WindowCodec(
                "window payload expands beyond 32768 bytes"
            ))
        );
    }

    #[test]
    fn rejects_corrupt_payloads() {
        assert!(matches!(
            zlib_decompress_window(&[0xff, 0xff, 0xff, 0xff]),
            Err(IndexError::WindowCodec(_))
        ));
    }

    #[test]
    fn rejects_trailing_bytes_after_a_complete_window() {
        let mut compressed = zlib_compress_window(&vec![3; WINDOW_SIZE]).expect("compress");
        compressed.extend_from_slice(&[1, 2, 3]);
        assert_eq!(
            zlib_decompress_window(&compressed),
            Err(IndexError::WindowCodec(
                "trailing bytes after compressed window"
            ))
        );
    }

    #[test]
    fn stored_window_hides_whether_it_is_compressed() {
        let window = vec![0x11u8; WINDOW_SIZE];
        let stored = StoredWindow::from_raw_maybe_compress(window.clone(), true).expect("store");
        assert!(stored.is_compressed());
        assert!(stored.stored_len() < WINDOW_SIZE);
        assert_eq!(stored.decompressed().expect("expand").as_ref(), &window[..]);

        let raw = StoredWindow::from_raw_maybe_compress(window.clone(), false).expect("store");
        assert!(!raw.is_compressed());
        assert_eq!(raw.decompressed().expect("expand").as_ref(), &window[..]);
    }

    #[test]
    fn stored_window_keeps_incompressible_history_raw() {
        let window = pseudo_random(WINDOW_SIZE);
        let stored = StoredWindow::from_raw_maybe_compress(window.clone(), true).expect("store");
        assert!(!stored.is_compressed());
        assert_eq!(stored.decompressed().expect("expand").as_ref(), &window[..]);
    }
}
