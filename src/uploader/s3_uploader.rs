use super::{BoxFuture, Uploader};
use crate::{
    Result, RouterStrategy,
    data_model::StreamId,
    envelopes::{ToUpload, UploadResult},
    error::SinkError,
};
use aws_config::Region;
use aws_sdk_s3_transfer_manager::{
    Client as TransferClient, Config as TransferConfig,
    io::InputStream,
    types::{ConcurrencyMode, PartSize},
};
use chrono::{DateTime, Utc};

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

    pub fn partition_spec(id: &StreamId) -> String {
        Self::partition_spec_inner(
            id,
            chrono::Utc::now(),
            &uuid::Uuid::new_v4().to_string()[..8],
        )
    }

    fn partition_spec_inner(id: &StreamId, now: DateTime<Utc>, uuid: &str) -> String {
        let mut parts: Vec<&str> = id.0.split(RouterStrategy::DELIMITER).collect();

        let suffix = format!(
            "ingest_year_month_day={}/{}-{}.jsonl.zst",
            now.format("%Y-%m-%d"),
            now.format("%Y-%m-%dT%H:%M:%SZ"),
            uuid
        );

        parts.push(suffix.as_str());

        parts.join("/")
    }
}

impl Uploader for S3Upload {
    fn upload(&self, to_upload: ToUpload) -> BoxFuture {
        let transfer_manager = self.client.clone();
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

                transfer_manager
                    .upload()
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
                Err(sink_error) => UploadResult::failure(to_upload, sink_error),
            }
        })
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;
    use std::rc::Rc;

    #[test]
    fn test_partition_spec() {
        let first_stream_id = StreamId(Rc::from(format!(
            "schema_name{}schema_version",
            RouterStrategy::DELIMITER
        )));

        let second_stream_id = StreamId(Rc::from(format!(
            "dlq{}schema_name{}schema_version{}status_code=400",
            RouterStrategy::DELIMITER,
            RouterStrategy::DELIMITER,
            RouterStrategy::DELIMITER
        )));

        let first_actual_result = S3Upload::partition_spec_inner(
            &first_stream_id,
            Utc.with_ymd_and_hms(2026, 5, 29, 14, 30, 0).unwrap(),
            "1234",
        );

        let second_actual_result = S3Upload::partition_spec_inner(
            &second_stream_id,
            Utc.with_ymd_and_hms(2026, 5, 29, 14, 30, 15).unwrap(),
            "5678",
        );

        assert_eq!(
            first_actual_result,
            "schema_name/schema_version/ingest_year_month_day=2026-05-29/2026-05-29T14:30:00Z-1234.jsonl.zst"
        );

        assert_eq!(
            second_actual_result,
            "dlq/schema_name/schema_version/status_code=400/ingest_year_month_day=2026-05-29/2026-05-29T14:30:15Z-5678.jsonl.zst"
        )
    }
}
