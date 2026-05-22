use super::{BoxFuture, UploadResult, Uploader};
use crate::envelopes::ToUpload;
use tracing::info;

pub struct S3Upload {
    client: aws_sdk_s3::Client,
}
impl S3Upload {
    fn new() -> S3Upload {
        todo!()
    }
}

impl Uploader for S3Upload {
    fn upload(&self, sealed_upload: ToUpload) -> BoxFuture {
        Box::pin(async {
            info!("uploading to s3");

            let (file, sealed_offsets) = sealed_upload.into_parts();

            let (path, raw_size_b, compressed_size_b, record_count, created_at) = file.into_parts();

            let offsets = sealed_offsets.into_parts();

            UploadResult::success(path, offsets)
        })
    }
}
