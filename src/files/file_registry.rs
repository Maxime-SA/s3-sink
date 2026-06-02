use crate::{
    Result,
    data_model::StreamId,
    envelopes::ClosedFile,
    error::SinkError,
    files::{FileRegistry, file_io::ActiveFile},
};
use std::{
    collections::{HashMap, hash_map::Entry},
    path::{Path, PathBuf},
};

/*
Registry of all active files (i.e. ActiveFile) we have in our sink at any given point in time.

There is one type:
1. DiskFileRegistry, contains all active files and a couple of wrapper methods around ActiveFile.
*/

pub struct DiskFileRegistry {
    directory: PathBuf,
    compression_level: i32,
    files: HashMap<StreamId, ActiveFile>,
}
impl DiskFileRegistry {
    pub fn new(directory: &Path, compression_level: i32) -> Self {
        DiskFileRegistry {
            directory: directory.to_path_buf(),
            compression_level,
            files: HashMap::new(),
        }
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
}

impl FileRegistry for DiskFileRegistry {
    fn write_all(&mut self, id: StreamId, bytes: &[u8]) -> Result<()> {
        let file = self.get_mut_active_file_or_create(id)?;
        file.write_all(bytes)
    }

    fn close(&mut self, id: &StreamId) -> Result<ClosedFile> {
        let file = self
            .files
            .remove(id)
            .ok_or_else(|| SinkError::FileRegistry(format!("active file '{id}' not found")))?;

        Ok(file.close()?)
    }

    fn active_file_count(&self) -> u64 {
        self.files.len() as u64
    }
}

#[cfg(test)]
mod test {
    use std::{fs, rc::Rc};

    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_initial_state() {
        let dir = TempDir::new().unwrap();

        let registry = DiskFileRegistry::new(dir.path(), 3);

        assert_eq!(registry.active_file_count(), 0);
    }

    #[test]
    fn test_write_to_non_existing_stream() {
        let dir = TempDir::new().unwrap();

        let mut registry = DiskFileRegistry::new(dir.path(), 3);

        let stream_id = StreamId(Rc::from("test-stream"));

        let input = b"first line\nsecond line\nthird line\n";

        registry.write_all(stream_id, input).unwrap();

        assert_eq!(registry.active_file_count(), 1);

        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
    }

    #[test]
    fn test_write_to_existing_stream() {
        let dir = TempDir::new().unwrap();

        let mut registry = DiskFileRegistry::new(dir.path(), 3);

        let stream_id = StreamId(Rc::from("test-stream"));

        let input = b"first line\nsecond line\nthird line\n";

        registry.write_all(stream_id.clone(), input).unwrap();

        assert_eq!(registry.active_file_count(), 1);

        registry.write_all(stream_id, input).unwrap();

        assert_eq!(registry.active_file_count(), 1);

        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);
    }

    #[test]
    fn test_write_to_multiple_streams() {
        let dir = TempDir::new().unwrap();

        let mut registry = DiskFileRegistry::new(dir.path(), 3);

        let input = b"first line\nsecond line\nthird line\n";

        let first_stream_id = StreamId(Rc::from("first-stream"));

        registry.write_all(first_stream_id, input).unwrap();

        assert_eq!(registry.active_file_count(), 1);

        assert_eq!(fs::read_dir(&dir).unwrap().count(), 1);

        let second_stream_id = StreamId(Rc::from("second-stream"));

        registry.write_all(second_stream_id, input).unwrap();

        assert_eq!(registry.active_file_count(), 2);

        assert_eq!(fs::read_dir(&dir).unwrap().count(), 2);
    }

    #[test]
    fn test_close_file() {
        let dir = TempDir::new().unwrap();

        let mut registry = DiskFileRegistry::new(dir.path(), 3);

        let stream_id = StreamId(Rc::from("test-stream"));

        let input = b"first line\nsecond line\nthird line\n";

        registry.write_all(stream_id.clone(), input).unwrap();

        let (path, compressed_size_b) = registry.close(&stream_id).unwrap().into_parts();

        assert_eq!(registry.active_file_count(), 0);

        assert_eq!(path.parent().unwrap(), dir.path());

        assert!(compressed_size_b > 0)
    }
}
