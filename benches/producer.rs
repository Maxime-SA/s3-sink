use futures::stream::{FuturesUnordered, StreamExt};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::io::Write;
use std::time::Duration;

const BOOTSTRAP_SERVERS: &str = "localhost:9092";
const NUM_TOPICS: usize = 50;
const ONE_KB: u64 = 1024;
const ONE_MB: u64 = ONE_KB * ONE_KB;
const TARGET_BYTES_PER_TOPIC: u64 = ONE_MB * ONE_KB * 2;
const NUM_PARTITIONS: i32 = 6;

struct TopicProfile {
    topic: String,
    avg_payload_size: usize,
    schema_name: String,
    schema_version: String,
}

fn build_profiles() -> Vec<TopicProfile> {
    let mut profiles: Vec<TopicProfile> = (1..=NUM_TOPICS)
        .map(|i| {
            let avg_size = match i {
                1..=25 => 32_096,
                26..=50 => 100_000,
                51..=75 => 500_000,
                76..=100 => 1_050_000,
                101..=125 => 10_000_000,
                126..=140 => 20_000_000,
                _ => 30_000_000,
            };
            TopicProfile {
                topic: format!("topic-{i}"),
                avg_payload_size: avg_size,
                schema_name: format!("topic-{i}"),
                schema_version: format!("version-{i}"),
            }
        })
        .collect();

    // // DLQ topic with properly formatted JSON payloads (no magic bytes)
    profiles.push(TopicProfile {
        topic: "dlq".to_string(),
        avg_payload_size: 4_096,
        schema_name: "dlq".to_string(),
        schema_version: "1".to_string(),
    });

    // Single-record topic with JsonSchema format (magic bytes + schema ID prefix)
    profiles.push(TopicProfile {
        topic: "topic-small".to_string(),
        avg_payload_size: 0, // sentinel: handled separately
        schema_name: "topic-small".to_string(),
        schema_version: "1".to_string(),
    });

    profiles
}

fn generate_payload(size: usize, record_id: u64) -> Vec<u8> {
    let mut payload = Vec::with_capacity(size);
    payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]);
    // Generate pseudo-random but valid JSON
    write!(
        payload,
        r#"{{"id":"{}","ts":"2025-01-01T00:00:00Z","value":{},"tags":[{},{},{}],"data":""#,
        uuid::Uuid::new_v4(),
        record_id,
        record_id % 100,
        record_id % 7,
        record_id * 31
    )
    .unwrap();
    // Fill with varied bytes instead of repeated 'x'
    let fill_len = size.saturating_sub(payload.len() + 2);
    for i in 0..fill_len {
        payload.push(b'A' + ((i + record_id as usize) % 26) as u8);
    }
    payload.extend_from_slice(b"\"}");
    payload
}

fn generate_dlq_payload(record_id: u64) -> Vec<u8> {
    let json = format!(
        r#"{{"error":"DeserializationException","message":"Failed to decode record","record_id":{},"topic":"topic-{}","partition":{},"offset":{},"timestamp":"2025-06-15T12:{}:{}Z","original_payload":"base64encodeddata{}","stack_trace":"org.apache.kafka.common.errors.SerializationException: Error deserializing..."}}"#,
        record_id,
        (record_id % 30) + 1,
        record_id % 6,
        record_id * 3,
        record_id % 60,
        record_id % 60,
        record_id
    );
    json.into_bytes()
}

fn generate_topic_small_payload() -> Vec<u8> {
    let mut payload = Vec::new();
    // Magic byte (0x00) + Schema ID (4 bytes, big-endian, ID=1)
    payload.extend_from_slice(&[0x00, 0x00, 0x00, 0x00, 0x00]);
    // JSON payload
    payload.extend_from_slice(br#"{"id":1,"name":"test","active":true}"#);
    payload
}

async fn create_topics(profiles: &[TopicProfile]) {
    let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP_SERVERS)
        .create()
        .expect("failed to create admin client");

    let new_topics: Vec<NewTopic> = profiles
        .iter()
        .map(|p| NewTopic::new(&p.topic, NUM_PARTITIONS, TopicReplication::Fixed(1)))
        .collect();

    let results = admin
        .create_topics(&new_topics, &AdminOptions::new())
        .await
        .expect("topic creation request failed");

    for result in results {
        match result {
            Ok(topic) => println!("  created {topic}"),
            Err((topic, err)) => {
                if err == rdkafka::types::RDKafkaErrorCode::TopicAlreadyExists {
                    println!("  {topic} already exists");
                } else {
                    panic!("failed to create {topic}: {err}");
                }
            }
        }
    }
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
async fn main() {
    let profiles = build_profiles();

    println!("=== Creating {} topics ===", NUM_TOPICS);
    create_topics(&profiles).await;

    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", BOOTSTRAP_SERVERS)
        .set("message.max.bytes", "33554432") // 32MB
        .set("queue.buffering.max.kbytes", "2097152") // 2GB queue
        .set("queue.buffering.max.messages", "1000000")
        .set("batch.num.messages", "10000")
        .set("linger.ms", "50")
        .create()
        .expect("failed to create producer");

    println!("=== Producing data to {} topics ===", NUM_TOPICS);
    println!("Target: {} MB per topic", TARGET_BYTES_PER_TOPIC / ONE_MB);

    let mut handles = Vec::with_capacity(profiles.len());

    for profile in profiles {
        let producer = producer.clone();
        handles.push(tokio::spawn(async move {
            produce_topic(&producer, &profile).await;
        }));
    }

    for handle in handles {
        handle.await.expect("task panicked");
    }

    // Flush remaining messages
    println!("Flushing...");
    producer
        .flush(Duration::from_secs(60))
        .expect("flush failed");
    println!("=== Done ===");
}

async fn produce_topic(producer: &FutureProducer, profile: &TopicProfile) {
    // topic-small: produce a single record and return
    if profile.topic == "topic-small" {
        let payload = generate_topic_small_payload();

        let headers = OwnedHeaders::new()
            .insert(rdkafka::message::Header {
                key: "schema_name",
                value: Some(profile.schema_name.as_bytes()),
            })
            .insert(rdkafka::message::Header {
                key: "schema_version",
                value: Some(profile.schema_version.as_bytes()),
            });

        let record: FutureRecord<'_, str, [u8]> = FutureRecord::to(&profile.topic)
            .payload(payload.as_slice())
            .headers(headers);

        match producer.send(record, Duration::from_secs(30)).await {
            Ok(_) => println!("  topic-small done: 1 record"),
            Err((err, _)) => eprintln!("  topic-small error: {err}"),
        }
        return;
    }

    let mut bytes_produced: u64 = 0;
    let mut records_produced: u64 = 0;

    // Pre-generate a pool of varied payloads to avoid lifetime issues with in-flight futures
    // while still giving zstd realistic (non-identical) data to compress
    // Scale pool size down for large payloads to avoid OOM
    let is_dlq = profile.topic == "dlq";
    let pool_size = match profile.avg_payload_size {
        0..=65_536 => 128,
        65_537..=1_000_000 => 32,
        1_000_001..=10_000_000 => 8,
        _ => 2,
    };
    let payload_pool: Vec<Vec<u8>> = (0..pool_size)
        .map(|i| {
            if is_dlq {
                generate_dlq_payload(i)
            } else {
                generate_payload(profile.avg_payload_size, i)
            }
        })
        .collect();

    let mut in_flight = FuturesUnordered::new();

    // Scale concurrency down for large payloads to avoid QueueFull
    let max_in_flight = match profile.avg_payload_size {
        0..=65_536 => 1_000,
        65_537..=1_000_000 => 200,
        _ => 20,
    };

    println!(
        "Filling {} (payload={}KB, target={}MB, concurrency={})...",
        profile.topic,
        profile.avg_payload_size / (ONE_KB as usize),
        TARGET_BYTES_PER_TOPIC / ONE_MB,
        max_in_flight
    );

    while bytes_produced < TARGET_BYTES_PER_TOPIC {
        let payload = &payload_pool[records_produced as usize % payload_pool.len()];

        let schema_name = if profile.schema_name != "dlq" {
            &profile.schema_name
        } else {
            "topic-A"
        };

        let headers = OwnedHeaders::new()
            .insert(rdkafka::message::Header {
                key: "schema_name",
                value: Some(schema_name.as_bytes()),
            })
            .insert(rdkafka::message::Header {
                key: "schema_version",
                value: Some(profile.schema_version.as_bytes()),
            });

        let record: FutureRecord<'_, str, [u8]> = FutureRecord::to(&profile.topic)
            .payload(payload.as_slice())
            .headers(headers);

        in_flight.push(producer.send(record, Duration::from_secs(30)));
        bytes_produced += payload.len() as u64;
        records_produced += 1;

        // Drain completed futures to bound memory
        while in_flight.len() >= max_in_flight {
            if let Some(Err((err, _))) = in_flight.next().await {
                eprintln!("produce error on {}: {err}", profile.topic);
            }
        }

        if records_produced % 10_000 == 0 {
            let mb = bytes_produced as f64 / ONE_MB as f64;
            println!(
                "  {} - {:.2} MB ({} records)",
                profile.topic, mb, records_produced
            );
        }
    }

    // Drain remaining
    while let Some(result) = in_flight.next().await {
        if let Err((err, _)) = result {
            eprintln!("produce error on {}: {err}", profile.topic);
        }
    }

    println!(
        "  {} done: {:.2} MB, {} records",
        profile.topic,
        bytes_produced as f64 / ONE_MB as f64,
        records_produced
    );
}
