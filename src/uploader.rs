use std::pin::Pin;

use tracing::info;

use crate::{Result, file_registry::SealedFile, offset_registry::TopicOffsets};

/*
Pin:
- Do not move this data on the heap.
- If the Future is moved around in-between await points we might end-up with invalid references.

Box:
- Indirection pointer. A way to allocate data on the heap.

dyn:
- Trait objects (i.e. type erasure).
*/
pub type BoxFuture = Pin<Box<dyn Future<Output = Result<TopicOffsets>>>>;

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
            Ok(sealed_file.offsets())
        })
    }
}

pub struct MockUploader;
impl Uploader for MockUploader {
    fn upload(&self, sealed_file: SealedFile) -> BoxFuture {
        Box::pin(async {
            info!("sleeping for X seconds");
            Ok(sealed_file.offsets())
        })
    }
}
