mod mock_uploader;
mod s3_uploader;

use crate::envelopes::{ToUpload, UploadResult};
pub use mock_uploader::MockUploader;
pub use s3_uploader::S3Upload;
use std::pin::Pin;

/*
Pin:
- Do not move this data on the heap.
- If the Future is moved around in-between await points we might end-up with invalid references.

Box:
- Indirection pointer. A way to allocate data on the heap.

dyn:
- Trait objects (i.e. type erasure).
*/
pub type BoxFuture = Pin<Box<dyn Future<Output = UploadResult>>>;

pub trait Uploader {
    fn upload(&self, to_upload: ToUpload) -> BoxFuture;
}
