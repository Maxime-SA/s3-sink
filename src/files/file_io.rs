use crate::Result;
use std::{
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::Instant,
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
    compressed_size_b: usize,
}

impl<W: Write> Write for CountingWriter<W> {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        let n = self.inner.write(buf)?;
        self.compressed_size_b += n;
        Ok(n)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(self.inner.flush()?)
    }
}

pub struct ActiveFile {
    path: PathBuf,
    writer: Encoder<'static, CountingWriter<BufWriter<File>>>,
    raw_size_b: usize,
    created_at: Instant,
}

impl ActiveFile {
    pub fn new(directory: &Path, compression_level: i32) -> Result<Self> {
        let path = directory.join(Uuid::new_v4().to_string());

        let file = File::options().create(true).append(true).open(&path)?;

        let counting_writer = CountingWriter {
            inner: BufWriter::new(file),
            compressed_size_b: 0,
        };

        let writer = Encoder::new(counting_writer, compression_level)?;

        Ok(ActiveFile {
            path,
            writer,
            raw_size_b: 0,
            created_at: Instant::now(),
        })
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.raw_size_b += bytes.len();
        self.writer.write_all(bytes)?;
        Ok(())
    }

    pub fn finalize(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.do_finish()?;
        Ok(())
    }

    pub fn compressed_size_b(&self) -> usize {
        self.writer.get_ref().compressed_size_b
    }

    pub fn raw_size_b(&self) -> usize {
        self.raw_size_b
    }

    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    pub fn path(self) -> PathBuf {
        self.path
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
