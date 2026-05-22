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
    files: HashMap<StreamId, ActiveFile>,
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
        let mut file = self
            .files
            .remove(id)
            .ok_or_else(|| self.file_not_found("seal", id))?;
        file.finalize()?;
        Ok(SealedFile::new(file))
    }

    pub fn files_older_than(&mut self, cut_off: Instant) -> Vec<StreamId> {
        let mut result = Vec::new();
        for (id, file) in &self.files {
            if file.created_at() < cut_off {
                result.push(id.clone());
            }
        }
        result
    }

    pub fn write_all(&mut self, id: &StreamId, bytes: &[u8]) -> Result<()> {
        self.get_mut_active_file_or_create(id)?.write_all(bytes)
    }

    pub fn file_size(&self, id: &StreamId) -> Result<usize> {
        Ok(self
            .files
            .get(id)
            .ok_or_else(|| self.file_not_found("file_size", id))?
            .raw_size_b())
    }

    fn get_mut_active_file_or_create(&mut self, id: &StreamId) -> Result<&mut ActiveFile> {
        Ok(match self.files.entry(id.clone()) {
            Entry::Occupied(occupied) => occupied.into_mut(),
            Entry::Vacant(vacant) => {
                let file = ActiveFile::new(self.directory.as_path(), self.compression_level)?;
                vacant.insert(file)
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
