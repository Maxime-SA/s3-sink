mod mock_uploader;
mod s3_uploader;

use crate::{
    RouterStrategy,
    data_model::StreamId,
    envelopes::{ToUpload, UploadResult},
};
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

pub fn partition_spec(id: &StreamId) -> String {
    let mut parts: Vec<&str> = id.0.split(RouterStrategy::DELIMITER).collect();

    let now = chrono::Utc::now();

    let suffix = format!(
        "ingest_year_month_day={}/{}-{}.jsonl.zst",
        now.format("%Y-%m-%d"),
        now.format("%Y-%m-%dT%H:%M:%SZ"),
        &uuid::Uuid::new_v4().to_string()[..8]
    );

    parts.push(suffix.as_str());

    parts.join("/")
}
