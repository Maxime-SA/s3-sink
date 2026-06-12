use super::{BoxFuture, Uploader};
use crate::{
    Result,
    envelopes::{ToUpload, UploadResult},
    error::SinkError,
};
use aws_config::Region;
use aws_sdk_s3_transfer_manager::{
    Client as TransferClient, Config as TransferConfig,
    io::InputStream,
    types::{ConcurrencyMode, PartSize},
};
use rand::RngExt;
use std::time::Duration;

pub struct S3Upload {
    client: TransferClient,
    bucket: String,
    max_uploads_retry: u64,
    is_miniio: bool,
}
impl S3Upload {
    pub async fn new(
        region: Region,
        bucket: String,
        max_uploads_retry: u64,
        endpoint_opt: Option<&str>,
        part_size_target_opt: Option<u64>,
        multipart_threshold_opt: Option<u64>,
        concurrency_control_opt: Option<usize>,
    ) -> S3Upload {
        let mut config_loader =
            aws_config::defaults(aws_config::BehaviorVersion::latest()).region(region);

        // only needed for MiniIO
        if let Some(endpoint) = endpoint_opt {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        let sdk_config = config_loader.load().await;

        let mut s3_config_builder = aws_sdk_s3::Config::from(&sdk_config).to_builder();

        // only needed for MiniIO
        if endpoint_opt.is_some() {
            s3_config_builder = s3_config_builder.force_path_style(true);
        }

        let s3_client = aws_sdk_s3::Client::from_conf(s3_config_builder.build());

        let part_size_target = part_size_target_opt
            .map(PartSize::Target)
            .unwrap_or_default();

        let multipart_threshold = multipart_threshold_opt
            .map(PartSize::Target)
            .unwrap_or_default();

        let concurrency_control = concurrency_control_opt
            .map(ConcurrencyMode::Explicit)
            .unwrap_or_default();

        let tm_config = TransferConfig::builder()
            .client(s3_client)
            .part_size(part_size_target)
            .multipart_threshold(multipart_threshold)
            .concurrency(concurrency_control)
            .build();

        let client = TransferClient::new(tm_config);

        S3Upload {
            client,
            bucket,
            max_uploads_retry,
            is_miniio: endpoint_opt.is_some(),
        }
    }
}

impl Uploader for S3Upload {
    fn upload(&self, to_upload: ToUpload) -> BoxFuture {
        let transfer_manager = self.client.clone();
        let bucket = self.bucket.clone();
        let is_miniio = self.is_miniio;
        let max_uploads_retry = self.max_uploads_retry;

        Box::pin(async move {
            // exponential backoff - do we want to make these configurable?
            let attempt = max_uploads_retry - to_upload.retries();
            if attempt > 0 {
                let max_backoff_ms = 5000u64;
                let backoff_ms = (100 * 2u64.pow(attempt.min(6) as u32)).min(max_backoff_ms);
                let jittered = rand::rng().random_range(0..=backoff_ms);
                tokio::time::sleep(Duration::from_millis(jittered)).await;
            }

            // upload to S3
            let result: Result<_> = async {
                let input_stream = InputStream::from_path(to_upload.path_ref())?;

                // metadata on the object
                let record_count = to_upload.record_count();
                let raw_size_b = to_upload.raw_size_b();
                let compressed_size_b = to_upload.compressed_size_b();
                let compression_ratio = format!(
                    "{:.1}%",
                    (1.0 - compressed_size_b as f64 / raw_size_b as f64) * 100.0
                );

                let request_builder = transfer_manager
                    .upload()
                    .bucket(&bucket)
                    .key(to_upload.object_key())
                    .body(input_stream)
                    .metadata("record_count", record_count.to_string())
                    .metadata("raw_size_bytes", raw_size_b.to_string())
                    .metadata("compression_ratio", compression_ratio);

                let request_builder = if is_miniio {
                    request_builder
                } else {
                    request_builder
                        .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
                };

                request_builder
                    .initiate()
                    .map_err(|e| SinkError::S3Upload(e.to_string()))?
                    .join()
                    .await
                    .map_err(|e| SinkError::S3Upload(e.to_string()))?;

                Ok(())
            }
            .await;

            match result {
                Ok(()) => {
                    let (file_to_gc, offsets) = to_upload.into_parts();
                    UploadResult::success(file_to_gc, offsets)
                }
                Err(sink_error) => UploadResult::failure(to_upload, sink_error),
            }
        })
    }
}
