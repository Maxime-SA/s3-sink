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

        // only difference between S3 & MinIO is that MinIO has an explicit endpoint
        if let Some(endpoint) = endpoint_opt {
            config_loader = config_loader.endpoint_url(endpoint);
        }

        let base_config = config_loader.load().await;

        let base_client = aws_sdk_s3::Client::new(&base_config);

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
            .client(base_client)
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

                tm.upload()
                    .bucket(&bucket)
                    .key(to_upload.object_key())
                    .body(input_stream)
                    .server_side_encryption(aws_sdk_s3::types::ServerSideEncryption::Aes256)
                    .acl(aws_sdk_s3::types::ObjectCannedAcl::BucketOwnerFullControl)
                    .metadata("raw_size_bytes", to_upload.raw_size_b().to_string())
                    .metadata("record_count", to_upload.record_count().to_string())
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
                Err(e) => UploadResult::failure(to_upload, e),
            }
        })
    }
}
