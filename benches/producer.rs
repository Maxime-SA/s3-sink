use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::Duration;

const BOOTSTRAP_SERVERS: &str = "localhost:9092";
const NUM_TOPICS: usize = 10;
const TARGET_BYTES_PER_TOPIC: u64 = 50 * 1024 * 1024 * 1024; // 50GB

struct TopicProfile {
    topic: String,
    avg_payload_size: usize,
    schema_name: &'static str,
    schema_version: &'static str,
}

fn build_profiles() -> Vec<TopicProfile> {
    (1..=NUM_TOPICS)
        .map(|i| {
            let avg_size = match i {
                1..=3 => 1_024,   // 1KB - small payloads
                4..=6 => 60_000,  // 60KB - average payloads
                7..=9 => 500_000, // 500KB - large payloads
                _ => 2_000_000,   // 2MB - extra large
            };
            TopicProfile {
                topic: format!("topic-{i}"),
                avg_payload_size: avg_size,
                schema_name: &"test_schema",
                schema_version: &"1",
            }
        })
        .collect()
}

fn generate_payload(size: usize) -> Vec<u8> {
    // Simulate JsonSchema: 5 magic bytes + JSON body
    let mut payload = Vec::with_capacity(size);
    payload.extend_from_slice(b"00001"); // magic bytes

    // Generate a simple JSON payload to fill the rest
    payload.push(b'{');
    payload.extend_from_slice(b"\"event\":\"benchmark\"");

    // Pad with realistic-looking data
    let padding_needed = size.saturating_sub(payload.len() + 1);
    if padding_needed > 0 {
        payload.extend_from_slice(b",\"data\":\"");
        let fill_len = padding_needed.saturating_sub(10);
        payload.extend(std::iter::repeat(b'x').take(fill_len));
        payload.push(b'"');
    }

    payload.push(b'}');
    payload
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP_SERVERS)
        .set("message.max.bytes", "33554432") // 32MB
        .set("queue.buffering.max.kbytes", "2097152") // 2GB queue
        .set("queue.buffering.max.messages", "1000000")
        .set("batch.num.messages", "10000")
        .set("linger.ms", "50")
        .create()
        .expect("failed to create producer");

    let profiles = build_profiles();

    println!("=== Producing data to {} topics ===", NUM_TOPICS);
    println!(
        "Target: {} GB per topic",
        TARGET_BYTES_PER_TOPIC / (1024 * 1024 * 1024)
    );

    for profile in &profiles {
        let mut bytes_produced: u64 = 0;
        let mut records_produced: u64 = 0;
        let payload = generate_payload(profile.avg_payload_size);

        println!(
            "Filling {} (payload={}KB, target={}GB)...",
            profile.topic,
            profile.avg_payload_size / 1024,
            TARGET_BYTES_PER_TOPIC / (1024 * 1024 * 1024)
        );

        while bytes_produced < TARGET_BYTES_PER_TOPIC {
            let headers = OwnedHeaders::new()
                .insert(rdkafka::message::Header {
                    key: "schema_name",
                    value: Some(profile.schema_name.as_bytes()),
                })
                .insert(rdkafka::message::Header {
                    key: "schema_version",
                    value: Some(profile.schema_version.as_bytes()),
                });

            let record: FutureRecord<'_, str, Vec<u8>> = FutureRecord::to(&profile.topic)
                .payload(&payload)
                .headers(headers);

            // Fire and forget with bounded queue — will block if queue is full
            if let Err((err, _)) = producer.send(record, Duration::from_secs(30)).await {
                eprintln!("produce error: {err}");
                continue;
            }

            bytes_produced += payload.len() as u64;
            records_produced += 1;

            if records_produced % 10_000 == 0 {
                let gb = bytes_produced as f64 / (1024.0 * 1024.0 * 1024.0);
                println!(
                    "  {} - {:.2} GB ({} records)",
                    profile.topic, gb, records_produced
                );
            }
        }

        println!(
            "  {} done: {:.2} GB, {} records",
            profile.topic,
            bytes_produced as f64 / (1024.0 * 1024.0 * 1024.0),
            records_produced
        );
    }

    // Flush remaining messages
    println!("Flushing...");
    producer
        .flush(Duration::from_secs(60))
        .expect("flush failed");
    println!("=== Done ===");
}
