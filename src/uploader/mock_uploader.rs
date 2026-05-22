use super::{BoxFuture, UploadResult, Uploader};
use crate::envelopes::ToUpload;
use tracing::info;

pub struct MockUploader;
impl Uploader for MockUploader {
    fn upload(&self, sealed_upload: ToUpload) -> BoxFuture {
        Box::pin(async {
            let (file, sealed_offsets) = sealed_upload.into_parts();

            let (path, raw_size_b, compressed_size_b, record_count, created_at) = file.into_parts();

            let offsets = sealed_offsets.into_parts();

            UploadResult::success(path, offsets)
        })
    }
}
