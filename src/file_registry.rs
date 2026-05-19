use crate::{
    Result,
    error::SinkError,
    offset_registry::{OffsetRegistry, SealedOffsets, TopicOffsets},
    record_router::FileId,
};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::Instant,
};
use uuid::Uuid;
use zstd::Encoder;

pub struct FileRegistry {
    files: HashMap<FileId, (ActiveFile, OffsetRegistry)>,
    directory: PathBuf,
    compression_level: i32,
}
impl FileRegistry {
    pub fn new(directory: &Path, compression_level: i32) -> Self {
        FileRegistry {
            files: HashMap::new(),
            directory: directory.into(),
            compression_level,
        }
    }

    pub fn get_active_file_or_create(&mut self, id: &FileId) -> Result<&mut ActiveFile> {
        if !self.files.contains_key(id) {
            let file = ActiveFile::new(self.directory.as_path(), self.compression_level)?;
            self.files.insert(id.clone(), (file, OffsetRegistry::new()));
        }

        Ok(&mut self.files.get_mut(id).unwrap().0)
    }

    pub fn seal(&mut self, id: &FileId) -> Result<SealedFile> {
        if let Some((mut file, offsets)) = self.files.remove(id) {
            file.finalize()?;
            Ok(SealedFile::new(file.path, file.record_count, offsets))
        } else {
            Err(SinkError::FileRegistry(format!(
                "could not find file '{id}' in file registry (seal)"
            )))
        }
    }

    pub fn add_offset(
        &mut self,
        id: &FileId,
        topic_name: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let (_, offsets) = self
            .files
            .get_mut(id)
            .ok_or(SinkError::FileRegistry(format!(
                "could not find file '{id}' in file registry (add_offset)"
            )))?;

        offsets.add(topic_name, partition, offset);

        Ok(())
    }
}

pub struct SealedFile {
    path: PathBuf,
    record_count: usize,
    offsets: SealedOffsets,
}
impl SealedFile {
    pub fn new(path: PathBuf, record_count: usize, offsets: OffsetRegistry) -> Self {
        SealedFile {
            path,
            record_count,
            offsets: SealedOffsets::new(offsets),
        }
    }

    pub fn offsets(self) -> TopicOffsets {
        self.offsets.offsets()
    }

    pub fn into_parts(self) -> (PathBuf, TopicOffsets) {
        (self.path, self.offsets.offsets())
    }
}

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
    record_count: usize,
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
            record_count: 0,
            created_at: Instant::now(),
        })
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.raw_size_b += bytes.len();
        self.writer.write_all(bytes)?;
        Ok(())
    }

    pub fn inc_record_count(&mut self) {
        self.record_count += 1;
    }

    pub fn finalize(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.do_finish()?;
        Ok(())
    }

    pub fn raw_size_b(&self) -> usize {
        self.raw_size_b
    }

    pub fn compressed_size_b(&self) -> usize {
        self.writer.get_ref().compressed_size_b
    }
}
