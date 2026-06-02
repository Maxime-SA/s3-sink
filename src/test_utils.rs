use rdkafka::message::{OwnedHeaders, OwnedMessage};

pub fn make_owned_message(
    topic: Option<&str>,
    payload: Option<Vec<u8>>,
    headers: Option<OwnedHeaders>,
    partition: Option<i32>,
    offset: Option<i64>,
) -> OwnedMessage {
    OwnedMessage::new(
        payload,
        None,
        String::from(topic.unwrap_or("topic")),
        rdkafka::Timestamp::NotAvailable,
        partition.unwrap_or(0),
        offset.unwrap_or(0),
        headers,
    )
}

pub fn make_owned_headers(headers: Vec<(String, String)>) -> OwnedHeaders {
    let mut result = OwnedHeaders::new();

    for (key, value) in &headers {
        result = result.insert(rdkafka::message::Header {
            key: key,
            value: Some(value),
        });
    }
    result
}

pub fn make_default_owned_headers() -> OwnedHeaders {
    make_owned_headers(vec![
        ("header-A".into(), "value-A".into()),
        ("header-B".into(), "value-B".into()),
    ])
}
