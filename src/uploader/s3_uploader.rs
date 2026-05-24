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

pub struct S3Upload {
    client: TransferClient,
    bucket: String,
}
impl S3Upload {
    pub async fn new(
        region: Region,
        endpoint_opt: Option<&str>,
        bucket: String,
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

        S3Upload { client, bucket }
    }
}

impl Uploader for S3Upload {
    fn upload(&self, to_upload: ToUpload) -> BoxFuture {
        let tm = self.client.clone();
        let bucket = self.bucket.clone();

        Box::pin(async move {
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

                tm.upload()
                    .bucket(&bucket)
                    .key(to_upload.object_key())
                    .body(input_stream)
                    // .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
                    .metadata("record_count", record_count.to_string())
                    .metadata("raw_size_bytes", raw_size_b.to_string())
                    .metadata("compression_ratio", compression_ratio)
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
                Err(se) => UploadResult::failure(to_upload, se),
            }
        })
    }
}
