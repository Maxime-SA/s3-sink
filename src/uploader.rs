use crate::{Result, file_registry::SealedFile, offset_registry::TopicOffsets};
use std::{path::PathBuf, pin::Pin};
use tracing::info;

/*
Pin:
- Do not move this data on the heap.
- If the Future is moved around in-between await points we might end-up with invalid references.

Box:
- Indirection pointer. A way to allocate data on the heap.

dyn:
- Trait objects (i.e. type erasure).
*/
pub type BoxFuture = Pin<Box<dyn Future<Output = Result<UploadResult>>>>;

pub struct UploadResult {
    file_to_gc: PathBuf,
    offsets_to_commit: TopicOffsets,
}
impl UploadResult {
    pub fn new(file_to_gc: PathBuf, offsets_to_commit: TopicOffsets) -> Self {
        UploadResult {
            file_to_gc,
            offsets_to_commit,
        }
    }

    pub fn into_parts(self) -> (PathBuf, TopicOffsets) {
        (self.file_to_gc, self.offsets_to_commit)
    }
}

pub trait Uploader {
    fn upload(&self, sealed_file: SealedFile) -> BoxFuture;
}

pub struct S3Upload {
    client: aws_sdk_s3::Client,
}
impl S3Upload {
    fn new() -> S3Upload {
        todo!()
    }
}

impl Uploader for S3Upload {
    fn upload(&self, sealed_file: SealedFile) -> BoxFuture {
        Box::pin(async {
            info!("uploading to s3");

            let (file_to_gc, record_count, raw_size_b, compressed_size_b, offsets_to_commit) =
                sealed_file.into_parts();

            Ok(UploadResult::new(file_to_gc, offsets_to_commit))
        })
    }
}

pub struct MockUploader;
impl Uploader for MockUploader {
    fn upload(&self, sealed_file: SealedFile) -> BoxFuture {
        Box::pin(async {
            info!("sleeping for X seconds");

            let (file_to_gc, record_count, raw_size_b, compressed_size_b, offsets_to_commit) =
                sealed_file.into_parts();

            Ok(UploadResult::new(file_to_gc, offsets_to_commit))
        })
    }
}
