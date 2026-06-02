use crate::Result;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;
use zstd::Encoder;

/*
Centralized point where writing to disk happens.

There are two types:
1. CountingWriter, a simple wrapper around a type which implements Write. We use this to track how many compressed bytes are written over the lifetime of a file.
2. ActiveFile, takes care of the actual writing of bytes to disk.
*/

struct CountingWriter<W: Write> {
    inner: W,
    compressed_size_b: u64,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.compressed_size_b += n as u64;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.inner.flush()
    }
}

pub struct ActiveFile {
    path: PathBuf,
    writer: Encoder<'static, CountingWriter<BufWriter<File>>>,
}

impl ActiveFile {
    const BUFFER_CAPACITY: usize = 1024 * 64;

    pub fn new(directory: &Path, compression_level: i32) -> Result<Self> {
        let mut path = directory.join(Uuid::new_v4().to_string());
        path.set_extension("jsonl.zst");

        let file = File::options().create(true).append(true).open(&path)?;

        let counting_writer = CountingWriter {
            inner: BufWriter::with_capacity(Self::BUFFER_CAPACITY, file),
            compressed_size_b: 0,
        };

        let writer = Encoder::new(counting_writer, compression_level)?;

        Ok(ActiveFile { path, writer })
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.writer.write_all(bytes)?;
        Ok(())
    }

    pub fn close(mut self) -> Result<(PathBuf, u64)> {
        self.writer.flush()?;
        self.writer.do_finish()?;
        self.writer.get_mut().flush()?;
        Ok((self.path, self.writer.get_ref().compressed_size_b))
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use std::fs;
    use tempfile::TempDir;

    #[test]
    fn test_write_and_close() {
        let dir = TempDir::new().unwrap();

        let mut file = ActiveFile::new(dir.path(), 3).unwrap();

        let input = b"first line\nsecond line\nthird line\nfourth line\n";

        file.write_all(input).unwrap();

        let (path, compressed_size_b) = file.close().unwrap();

        assert!(compressed_size_b > 0);

        let compressed = fs::read(path).unwrap();

        let decompressed = zstd::decode_all(compressed.as_slice()).unwrap();

        assert_eq!(decompressed, input);
    }

    #[test]
    fn test_file_extension() {
        let dir = TempDir::new().unwrap();

        let file = ActiveFile::new(dir.path(), 3).unwrap();

        let path = file.path;

        assert!(path.exists());

        assert!(path.to_str().unwrap().ends_with("jsonl.zst"));
    }

    #[test]
    fn test_counting_writer() {
        let mut writer = CountingWriter {
            inner: Vec::new(),
            compressed_size_b: 0,
        };

        writer.write_all(b"12345").unwrap();

        assert_eq!(writer.compressed_size_b, 5);
    }

    #[test]
    fn test_multiple_writes_concatenate() {
        let dir = TempDir::new().unwrap();

        let mut file = ActiveFile::new(dir.path(), 3).unwrap();

        file.write_all(b"first\n").unwrap();
        file.write_all(b"second\n").unwrap();
        file.write_all(b"third\n").unwrap();

        let (path, _) = file.close().unwrap();

        let compressed = fs::read(path).unwrap();

        let decompressed = zstd::decode_all(compressed.as_slice()).unwrap();

        assert_eq!(decompressed, b"first\nsecond\nthird\n");
    }

    #[test]
    fn test_close_without_writes() {
        let dir = TempDir::new().unwrap();

        let file = ActiveFile::new(dir.path(), 3).unwrap();

        let (path, compressed_size_b) = file.close().unwrap();

        // valid zstd frame with no content
        let compressed = fs::read(path).unwrap();

        let decompressed = zstd::decode_all(compressed.as_slice()).unwrap();

        assert!(decompressed.is_empty());

        assert!(compressed_size_b > 0); // zstd frame header/footer
    }
}
