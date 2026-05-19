use chrono::Utc;
use rdkafka::message::{Headers, Message};

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct FileId(String);



pub enum Partitioner {
    Regular,
    Dlq,
}

impl Partitioner {
    /*
    The partition spec is the object prefix in S3.
     */
    pub fn partition_spec<M: Message>(&self, record: &M) -> String {
        let schema_name = record.topic();
        let schema_version = Self::get_schema_version(record);
        let time_partition = format!("ingest_year_month_day={}", Utc::now().format("%Y-%m-%d"));

        match self {
            Partitioner::Regular => format!("{schema_name}/{schema_version}/{time_partition}"),
            Partitioner::Dlq => format!(
                "{schema_name}/{schema_version}/error={}/{time_partition}",
                Self::get_status_code(record)
            ),
        }
    }

    /*
    The active file id is the active file for buffering records before upload.
    For regular topics, there is a single file per topic and version.
    For DLQ topics, there is a single file per topic, version, and error code.
     */
    pub fn get_file_id<M: Message>(&self, record: &M) -> FileId {
        let schema_name = record.topic();
        let schema_version = Self::get_schema_version(record);

        FileId(match self {
            Partitioner::Regular => format!("{schema_name}.{schema_version}"),
            Partitioner::Dlq => format!(
                "{schema_name}.{schema_version}.{}",
                Self::get_status_code(record)
            ),
        })
    }

    fn get_status_code<M: Message>(record: &M) -> &str {
        Self::get_header(record, "status_code").unwrap_or("unknown_status_code")
    }

    fn get_schema_version<M: Message>(record: &M) -> &str {
        Self::get_header(record, "schema_version").unwrap_or("unknown_version")
    }

    fn get_header<'a, M: Message>(record: &'a M, key: &str) -> Option<&'a str> {
        record.headers()?.iter().find_map(|header| {
            if header.key == key {
                header.value.and_then(|val| str::from_utf8(val).ok())
            } else {
                None
            }
        })
    }
}
