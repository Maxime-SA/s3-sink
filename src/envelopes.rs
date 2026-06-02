use crate::{data_model::TopicId, error::SinkError};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    time::Instant,
};

pub enum UploadResult {
    Success(PathBuf, HashMap<TopicId, Vec<i64>>),
    Failure(ToUpload, SinkError),
}
impl UploadResult {
    pub fn success(file_to_gc: PathBuf, offsets: HashMap<TopicId, Vec<i64>>) -> Self {
        UploadResult::Success(file_to_gc, offsets)
    }

    pub fn failure(to_upload: ToUpload, sink_error: SinkError) -> Self {
        UploadResult::Failure(to_upload, sink_error)
    }
}

#[derive(PartialEq, Debug)]
pub struct ToUpload {
    object_key: String,
    file: SealedFile,
    retries: u64,
}
impl ToUpload {
    pub fn new(object_key: String, file: SealedFile, retries: u64) -> Self {
        ToUpload {
            object_key,
            file,
            retries,
        }
    }

    pub fn path_ref(&self) -> &Path {
        self.file.path()
    }

    pub fn object_key(&self) -> &str {
        &self.object_key
    }

    pub fn record_count(&self) -> u64 {
        self.file.record_count()
    }

    pub fn raw_size_b(&self) -> u64 {
        self.file.raw_size_b()
    }

    pub fn compressed_size_b(&self) -> u64 {
        self.file.compressed_size_b()
    }

    pub fn retries(&self) -> u64 {
        self.retries
    }

    pub fn into_parts(self) -> (PathBuf, HashMap<TopicId, Vec<i64>>) {
        (self.file.path, self.file.offsets_consumed)
    }

    pub fn decrement(self) -> Self {
        ToUpload {
            object_key: self.object_key,
            file: self.file,
            retries: self.retries - 1,
        }
    }
}

#[derive(PartialEq, Debug)]
pub struct SealedFile {
    path: PathBuf,
    raw_size_b: u64,
    compressed_size_b: u64,
    record_count: u64,
    offsets_consumed: HashMap<TopicId, Vec<i64>>,
    created_at: Instant,
}
impl SealedFile {
    pub fn new(
        path: PathBuf,
        raw_size_b: u64,
        compressed_size_b: u64,
        record_count: u64,
        offsets_consumed: HashMap<TopicId, Vec<i64>>,
        created_at: Instant,
    ) -> Self {
        SealedFile {
            path,
            compressed_size_b,
            raw_size_b,
            record_count,
            offsets_consumed,
            created_at,
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

#[derive(PartialEq, Debug)]
pub struct ClosedFile {
    path: PathBuf,
    compressed_size_b: u64,
}
impl ClosedFile {
    pub fn new(path: PathBuf, compressed_size_b: u64) -> Self {
        Self {
            path,
            compressed_size_b,
        }
    }

    pub fn into_parts(self) -> (PathBuf, u64) {
        (self.path, self.compressed_size_b)
    }
}
