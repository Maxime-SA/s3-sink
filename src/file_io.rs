use crate::partitioner::FileId;
use crate::{Result, error::SinkError};
use std::{
    collections::HashMap,
    fs::File,
    io::{BufWriter, Write},
    path::{Path, PathBuf},
    time::Instant,
};
use uuid::Uuid;
use zstd::Encoder;

pub struct FilesState {
    directory: PathBuf,
    compression_level: i32,
    active: HashMap<FileId, FileHandle>, // should never have more than one active file with PartitionId
    files_to_gc: Vec<PathBuf>,           // list of files that can be garbage collected
}
impl FilesState {
    pub fn new(directory: &Path, compression_level: i32) -> Self {
        FilesState {
            directory: PathBuf::from(directory),
            compression_level,
            active: HashMap::new(),
            files_to_gc: vec![],
        }
    }

    pub fn active_file(&mut self, start_offset: i64, file_id: &FileId) -> Result<&mut FileHandle> {
        if !self.active.contains_key(file_id) {
            let file = FileHandle::new(
                start_offset,
                self.directory.as_path(),
                self.compression_level,
            )?;

            self.active.insert(file_id.clone(), file);
        }

        Ok(self.active.get_mut(file_id).unwrap())
    }

    pub fn seal_file(&mut self, file_id: &FileId) -> Result<SealedFile> {
        if let Some(mut file_handle) = self.active.remove(file_id) {
            file_handle.finalize()?;

            // not sure if this is actually required
            self.files_to_gc.push(file_handle.path.clone());

            Ok(SealedFile::new(
                file_handle.path,
                file_handle.size_bytes,
                file_handle.start_offset,
                file_handle.end_offset,
                file_handle.created_at,
            ))
        } else {
            Err(SinkError::IOError(
                "could not find file to seal in active files".into(),
            ))
        }
    }

    pub fn garbage_collect_files(&mut self) {
        while let Some(pathbuf) = self.files_to_gc.pop() {
            let _ = std::fs::remove_file(pathbuf);
        }
    }
}

pub struct SealedFile {
    path: PathBuf,
    size_bytes: usize,
    start_offset: i64,
    end_offset: i64,
    created_at: Instant,
}
impl SealedFile {
    fn new(
        path: PathBuf,
        size_bytes: usize,
        start_offset: i64,
        end_offset: i64,
        created_at: Instant,
    ) -> Self {
        SealedFile {
            path,
            size_bytes,
            start_offset,
            end_offset,
            created_at,
        }
    }
}

pub struct FileHandle {
    path: PathBuf,
    writer: Encoder<'static, BufWriter<File>>,
    start_offset: i64,
    end_offset: i64,
    size_bytes: usize,
    record_count: usize,
    created_at: Instant,
}
impl FileHandle {
    pub fn new(start_offset: i64, directory: &Path, compression_level: i32) -> Result<Self> {
        let path = directory.join(Uuid::new_v4().to_string());

        let file = File::options().create(true).append(true).open(&path)?;

        let writer = Encoder::new(BufWriter::new(file), compression_level)?;

        Ok(FileHandle {
            path,
            writer,
            start_offset,
            end_offset: start_offset,
            size_bytes: 0,
            record_count: 0,
            created_at: Instant::now(),
        })
    }

    pub fn write_all(&mut self, bytes: &[u8]) -> Result<()> {
        self.size_bytes += bytes.len();
        self.writer.write_all(bytes)?;
        Ok(())
    }

    pub fn update_end_offset(&mut self, offset: i64) {
        self.record_count += 1;
        self.end_offset = offset;
    }

    pub fn finalize(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.writer.do_finish()?;
        Ok(())
    }

    pub fn size(&self) -> usize {
        self.size_bytes
    }
}
