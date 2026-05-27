use crate::{Result, data_model::StreamId, error::SinkError, files::file_io::ActiveFile};
use std::{
    collections::{HashMap, hash_map::Entry},
    path::{Path, PathBuf},
};

/*
Todo:
- Review unit tests
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

    pub fn seal(&mut self, id: &StreamId) -> Result<ActiveFile> {
        let mut file = self
            .files
            .remove(id)
            .ok_or_else(|| self.file_not_found("seal", id))?;
        file.finalize()?;
        Ok(file)
    }

    pub fn write_all(&mut self, id: StreamId, bytes: &[u8]) -> Result<()> {
        let file = self.get_mut_active_file_or_create(id)?;
        file.write_all(bytes)
    }

    pub fn active_file_count(&self) -> u64 {
        self.files.len() as u64
    }

    fn get_mut_active_file_or_create(&mut self, id: StreamId) -> Result<&mut ActiveFile> {
        Ok(match self.files.entry(id) {
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
