use crate::{
    Result,
    error::SinkError,
    files::{SealedFile, file_io::ActiveFile},
    offset::{ConsumedOffset, OffsetEnvelope},
    record::FileId,
};
use std::{collections::HashMap, path::Path};

/*
- Backpressure:
    - Sum bytes in active files
    - Active files count
- 
*/
pub struct FileRegistry<'a> {
    directory: &'a Path,
    compression_level: i32,
    files: HashMap<FileId, (ActiveFile, OffsetEnvelope<ConsumedOffset>)>,
}
impl<'a> FileRegistry<'a> {
    pub fn new(directory: &'a Path, compression_level: i32) -> Self {
        FileRegistry {
            directory: directory,
            compression_level,
            files: HashMap::new(),
        }
    }

    pub fn get_mut_active_file_or_create(&mut self, id: &FileId) -> Result<&mut ActiveFile> {
        if !self.files.contains_key(id) {
            let file = ActiveFile::new(self.directory, self.compression_level)?;
            self.files
                .insert(id.clone(), (file, OffsetEnvelope::new(None)));
        }

        Ok(&mut self.files.get_mut(id).unwrap().0)
    }

    pub fn seal(&mut self, id: &FileId) -> Result<SealedFile> {
        let (mut file, offsets) = self.files.remove(id).ok_or_else(|| {
            SinkError::FileRegistry(format!(
                "could not find active file '{id}' in file registry (seal)"
            ))
        })?;

        // review this, finalize()? could return everything I need instead of then calling into_parts

        file.finalize()?;

        let (path, record_count, raw_size_b, compressed_size_b, _) = file.into_parts();

        Ok(SealedFile::new(
            path,
            record_count,
            raw_size_b,
            compressed_size_b,
            offsets,
        ))
    }

    pub fn add_offset(
        &mut self,
        id: &FileId,
        topic_name: &str,
        partition: i32,
        offset: i64,
    ) -> Result<()> {
        let (_, offsets) = self.files.get_mut(id).ok_or_else(|| {
            SinkError::FileRegistry(format!(
                "could not find active file '{id}' in file registry (add_offset)"
            ))
        })?;

        offsets
            .get_mut_offsets()
            .entry((topic_name.into(), partition))
            .or_default()
            .push(offset);

        Ok(())
    }
}
