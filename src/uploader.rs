mod mock_uploader; // temporary for testing
pub use mock_uploader::MockUploader;

use crate::{
    Result,
    files::SealedFile,
    offset::{OffsetEnvelope, UploadedOffset},
};
use std::{path::PathBuf, pin::Pin};

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

pub trait Uploader {
    fn upload(&self, sealed_file: SealedFile) -> BoxFuture;
}

pub struct UploadResult {
    file_to_gc: PathBuf,
    offsets: OffsetEnvelope<UploadedOffset>,
}
impl UploadResult {
    pub fn new(file_to_gc: PathBuf, offsets: OffsetEnvelope<UploadedOffset>) -> Self {
        UploadResult {
            file_to_gc,
            offsets: offsets,
        }
    }

    pub fn into_parts(self) -> (PathBuf, OffsetEnvelope<UploadedOffset>) {
        (self.file_to_gc, self.offsets)
    }
}
