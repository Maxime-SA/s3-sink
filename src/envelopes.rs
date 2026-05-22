use crate::{error::SinkError, files::ActiveFile, offset_registry::OffsetsVec};
use std::{
    path::{Path, PathBuf},
    time::Instant,
};

pub enum UploadResult {
    Success(PathBuf, OffsetsVec),
    Failure(ToUpload, SinkError),
}
impl UploadResult {
    pub fn success(file_to_gc: PathBuf, offsets: OffsetsVec) -> Self {
        UploadResult::Success(file_to_gc, offsets)
    }

    pub fn failure(to_upload: ToUpload, sink_error: SinkError) -> Self {
        UploadResult::Failure(to_upload, sink_error)
    }
}

#[derive(Debug, PartialEq)]
pub struct ToUpload {
    object_key: String,
    file: SealedFile,
    offsets: SealedOffsets,
}
impl ToUpload {
    pub fn new(object_key: String, file: SealedFile, offsets: SealedOffsets) -> Self {
        ToUpload {
            object_key,
            file,
            offsets,
        }
    }

    pub fn path_ref(&self) -> &Path {
        self.file.path()
    }

    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub fn into_parts(self) -> (PathBuf, OffsetsVec) {
        (self.file.path, self.offsets.0)
    }

    pub fn record_count(&self) -> u64 {
        self.file.record_count()
    }

    pub fn raw_size_b(&self) -> u64 {
        self.file.raw_size_b()
    }
}

#[derive(Debug, PartialEq)]
pub struct SealedOffsets(OffsetsVec);
impl SealedOffsets {
    pub fn new(offsets: OffsetsVec) -> Self {
        SealedOffsets(offsets)
    }
}

#[derive(Debug, PartialEq)]
pub struct SealedFile {
    path: PathBuf,
    raw_size_b: u64,
    compressed_size_b: u64,
    record_count: u64,
    created_at: Instant,
}
impl SealedFile {
    pub fn new(file: ActiveFile, record_count: u64) -> Self {
        SealedFile {
            raw_size_b: file.raw_size_b(),
            compressed_size_b: file.compressed_size_b(),
            created_at: file.created_at(),
            path: file.path(),
            record_count,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn raw_size_b(&self) -> u64 {
        self.raw_size_b
    }

    pub fn compressed_size_b(&self) -> u64 {
        self.compressed_size_b
    }

    pub fn record_count(&self) -> u64 {
        self.record_count
    }

    pub fn created_at(&self) -> Instant {
        self.created_at
    }
}
