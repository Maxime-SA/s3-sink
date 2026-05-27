use crate::Result;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
};
use uuid::Uuid;
use zstd::Encoder;

/*
Todo:
- Review unit tests
*/

/*
Wrapper to track how many compressed bytes are written
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

    pub fn finalize(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.do_finish()?;
        self.writer.get_mut().flush()?;
        Ok(())
    }

    pub fn compressed_size_b(&self) -> u64 {
        self.writer.get_ref().compressed_size_b
    }

    pub fn path(self) -> PathBuf {
        self.path
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
