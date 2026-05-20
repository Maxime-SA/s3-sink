use super::{BoxFuture, SealedFile, UploadResult, Uploader};
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
    fn upload(&self, sealed_file: SealedFile) -> BoxFuture {
        Box::pin(async {
            info!("uploading to s3");

            let (file_to_gc, record_count, raw_size_b, compressed_size_b, offsets_to_commit) =
                sealed_file.into_parts();

            Ok(UploadResult::new(file_to_gc, offsets_to_commit.into()))
        })
    }
}
