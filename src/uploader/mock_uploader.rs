use super::{BoxFuture, UploadResult, Uploader};
use crate::envelopes::ToUpload;

pub struct MockUploader;
impl Uploader for MockUploader {
    fn upload(&self, to_upload: ToUpload) -> BoxFuture {
        Box::pin(async {
            let (path, offsets) = to_upload.into_parts();

            UploadResult::success(path, offsets)
        })
    }
}
