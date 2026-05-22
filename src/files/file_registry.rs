use crate::{
    Result, envelopes::SealedFile, error::SinkError, files::file_io::ActiveFile, record::StreamId,
};
use std::{
    collections::{HashMap, hash_map::Entry},
    path::{Path, PathBuf},
    time::Instant,
};

/*
Todo:
- Review unit tests
- Avoid StreamId allocation on:
    - files
    - older_than
*/

/*
Manage all active files.
*/
pub struct FileRegistry {
    directory: PathBuf,
    compression_level: i32,
    files: HashMap<StreamId, (ActiveFile, u64)>, // StreamId -> (ActiveFile, RecordCount)
}
impl FileRegistry {
    pub fn new(directory: &Path, compression_level: i32) -> Self {
        FileRegistry {
            directory: directory.to_path_buf(),
            compression_level,
            files: HashMap::new(),
        }
    }

    pub fn seal(&mut self, id: &StreamId) -> Result<SealedFile> {
        let (mut file, record_count) = self
            .files
            .remove(id)
            .ok_or_else(|| self.file_not_found("seal", id))?;
        file.finalize()?;
        Ok(SealedFile::new(file, record_count))
    }

    pub fn files_older_than(&mut self, cut_off: Instant) -> Vec<StreamId> {
        let mut result = Vec::new();
        for (id, file) in &self.files {
            if file.0.created_at() < cut_off {
                result.push(id.clone());
            }
        }
        result
    }

    pub fn write_all(&mut self, id: &StreamId, bytes: &[u8]) -> Result<()> {
        let (file, record_count) = self.get_mut_active_file_or_create(id)?;
        *record_count += 1;
        file.write_all(bytes)
    }

    pub fn raw_file_size_b(&self, id: &StreamId) -> Result<u64> {
        Ok(self
            .files
            .get(id)
            .ok_or_else(|| self.file_not_found("file_size", id))?
            .0
            .raw_size_b())
    }

    pub fn compressed_file_size_b(&self, id: &StreamId) -> Result<u64> {
        Ok(self
            .files
            .get(id)
            .ok_or_else(|| self.file_not_found("file_size", id))?
            .0
            .compressed_size_b())
    }

    pub fn active_file_count(&self) -> u64 {
        self.files.len() as u64
    }

    fn get_mut_active_file_or_create(&mut self, id: &StreamId) -> Result<&mut (ActiveFile, u64)> {
        Ok(match self.files.entry(id.clone()) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => {
                let file = ActiveFile::new(self.directory.as_path(), self.compression_level)?;
                vacant.insert((file, 0))
            }
        })
    }

    fn file_not_found(&self, method: &str, id: &StreamId) -> SinkError {
        SinkError::FileRegistry(format!("{method}: active file '{id}' not found"))
    }
}

#[cfg(test)]
mod test {
    use super::*;
}
