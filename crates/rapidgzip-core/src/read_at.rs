use std::fs::File;
use std::io;
use std::sync::Arc;

/// Thread-safe positional compressed input.
///
/// Implementations must return `Ok(0)` only at or beyond [`ReadAt::len`], and
/// callers must keep both the snapshotted length and contents stable during a
/// decode.
pub trait ReadAt: Send + Sync {
    /// Returns the current source length in bytes.
    fn len(&self) -> io::Result<u64>;

    /// Reads bytes beginning at `offset` without changing shared cursor state.
    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize>;

    /// Returns whether this source is empty.
    fn is_empty(&self) -> io::Result<bool> {
        self.len().map(|length| length == 0)
    }
}

#[cfg(unix)]
impl ReadAt for File {
    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }

    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        std::os::unix::fs::FileExt::read_at(self, buffer, offset)
    }
}

#[cfg(windows)]
impl ReadAt for File {
    fn len(&self) -> io::Result<u64> {
        self.metadata().map(|metadata| metadata.len())
    }

    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        std::os::windows::fs::FileExt::seek_read(self, buffer, offset)
    }
}

impl ReadAt for [u8] {
    fn len(&self) -> io::Result<u64> {
        Ok(self.len() as u64)
    }

    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        let start = usize::try_from(offset).unwrap_or(usize::MAX);
        if start >= self.len() {
            return Ok(0);
        }
        let count = buffer.len().min(self.len() - start);
        buffer[..count].copy_from_slice(&self[start..start + count]);
        Ok(count)
    }
}

impl ReadAt for Vec<u8> {
    fn len(&self) -> io::Result<u64> {
        Ok(self.as_slice().len() as u64)
    }

    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        self.as_slice().read_at(offset, buffer)
    }
}

impl<T: ReadAt + ?Sized> ReadAt for Arc<T> {
    fn len(&self) -> io::Result<u64> {
        (**self).len()
    }

    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        (**self).read_at(offset, buffer)
    }
}

impl<T: ReadAt + ?Sized> ReadAt for Box<T> {
    fn len(&self) -> io::Result<u64> {
        (**self).len()
    }

    fn read_at(&self, offset: u64, buffer: &mut [u8]) -> io::Result<usize> {
        (**self).read_at(offset, buffer)
    }
}
