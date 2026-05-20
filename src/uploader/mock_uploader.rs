use super::{BoxFuture, SealedFile, UploadResult, Uploader};
use tracing::info;

pub struct MockUploader;
impl Uploader for MockUploader {
    fn upload(&self, sealed_file: SealedFile) -> BoxFuture {
        Box::pin(async {
            info!("sleeping for X seconds");

            let (file_to_gc, record_count, raw_size_b, compressed_size_b, offsets_to_commit) =
                sealed_file.into_parts();

            Ok(UploadResult::new(file_to_gc, offsets_to_commit.into()))
        })
    }
}
