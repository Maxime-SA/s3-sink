use chrono::{DateTime, Utc};

use crate::{RouterStrategy, data_model::StreamId};

/*
Trait for generating an object key given a StreamId.
*/
pub trait KeyGenerator {
    fn key(&self, stream_id: &StreamId) -> String;
}

/*
Struct representing concrete S3 partitioner.
*/
pub struct S3Partitioner;

impl S3Partitioner {
    fn key_inner(id: &StreamId, now: DateTime<Utc>, uuid: &str) -> String {
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

impl KeyGenerator for S3Partitioner {
    fn key(&self, stream_id: &StreamId) -> String {
        Self::key_inner(
            stream_id,
            chrono::Utc::now(),
            &uuid::Uuid::new_v4().to_string()[..8],
        )
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use chrono::TimeZone;
    use std::rc::Rc;

    #[test]
    fn test_s3_partitioner() {
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

        let first_actual_result = S3Partitioner::key_inner(
            &first_stream_id,
            Utc.with_ymd_and_hms(2026, 5, 29, 14, 30, 0).unwrap(),
            "1234",
        );

        let second_actual_result = S3Partitioner::key_inner(
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
