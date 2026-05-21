use crate::{files::ActiveFile, offset_registry::OffsetsVec};
use std::{path::PathBuf, time::Instant};

pub enum UploadResult {
    Success(PathBuf, OffsetsVec),
    Failure(ToUpload),
}
impl UploadResult {
    pub fn success(file_to_gc: PathBuf, offsets: OffsetsVec) -> Self {
        UploadResult::Success(file_to_gc, offsets)
    }

    pub fn failure(to_upload: ToUpload) -> Self {
        UploadResult::Failure(to_upload)
    }
}

#[derive(Debug, PartialEq)]
pub struct ToUpload {
    file: SealedFile,
    offsets: SealedOffsets,
}
impl ToUpload {
    pub fn new(file: SealedFile, offsets: SealedOffsets) -> Self {
        ToUpload { file, offsets }
    }

    pub fn into_parts(self) -> (SealedFile, SealedOffsets) {
        (self.file, self.offsets)
    }
}

#[derive(Debug, PartialEq)]
pub struct SealedOffsets(OffsetsVec);
impl SealedOffsets {
    pub fn new(offsets: OffsetsVec) -> Self {
        SealedOffsets(offsets)
    }

    pub fn into_parts(self) -> OffsetsVec {
        self.0
    }
}

#[derive(Debug, PartialEq)]
pub struct SealedFile {
    path: PathBuf,
    raw_size_b: usize,
    compressed_size_b: usize,
    created_at: Instant,
}
impl SealedFile {
    pub fn new(file: ActiveFile) -> Self {
        SealedFile {
            raw_size_b: file.raw_size_b(),
            compressed_size_b: file.compressed_size_b(),
            created_at: file.created_at(),
            path: file.path(),
        }
    }

    pub fn into_parts(self) -> (PathBuf, usize, usize, Instant) {
        (
            self.path,
            self.raw_size_b,
            self.compressed_size_b,
            self.created_at,
        )
    }
}
