use rdkafka::{Message, message::Headers};

/*

*/

pub struct FileHandle {
    path: PathBuf,
    writer: Encoder<'static, BufWriter<File>>,
    start_offset: i64,
    end_offset: i64,
    size_bytes: usize,
    record_count: usize,
    created_at: Instant,
}

pub struct GroupById(String);

trait RecordGroupBy<M: Message> {
    fn id(record: &M) -> GroupById;

    fn get_header<'a>(record: &'a M, key: &str) -> Option<&'a str> {
        record.headers()?.iter().find_map(|header| {
            if header.key == key {
                header.value.and_then(|val| str::from_utf8(val).ok())
            } else {
                None
            }
        })
    }
}

/*
Group by schema name and version
*/
struct GroupByTopicVersion;
impl<M> RecordGroupBy<M> for GroupByTopicVersion
where
    M: Message,
{
    fn id(record: &M) -> GroupById {
        let schema_name = Self::get_header(record, "schema_name").unwrap_or("unknown_schema_name");
        let schema_version =
            Self::get_header(record, "schema_version").unwrap_or("unknown_version");
        GroupById(format!("{schema_name}.{schema_version}"))
    }
}

/*
Assumptions:
- a 'status_code' record header exists

Group by schema name, version, and status code
*/
struct GroupByStatusCode;
impl<M> RecordGroupBy<M> for GroupByStatusCode
where
    M: Message,
{
    fn id(record: &M) -> GroupById {
        let schema_name = Self::get_header(record, "schema_name").unwrap_or("unknown_schema_name");
        let schema_version =
            Self::get_header(record, "schema_version").unwrap_or("unknown_version");
        let status_code = Self::get_header(record, "status_code").unwrap_or("unknown_status_code");

        GroupById(format!("{schema_name}.{schema_version}.{status_code}"))
    }
}
