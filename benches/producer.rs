use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::time::Duration;

const BOOTSTRAP_SERVERS: &str = "localhost:9092";
const NUM_TOPICS: usize = 10;
const ONE_KB: u64 = 1024;
const ONE_MB: u64 = ONE_KB * ONE_KB;
const TARGET_BYTES_PER_TOPIC: u64 = ONE_MB * 100;
const NUM_PARTITIONS: i32 = 6;

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
                1..=5 => 4_096,       // 4KB - small payloads
                5..=10 => 60_000,     // 60KB - average payloads
                11..=15 => 500_000,   // 500KB - large payloads
                15..=17 => 2_000_000, // 2MB - xlarge payloads
                _ => 10_000_000,      // 2MB - xxlarge payloads
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
    let mut bytes_produced: u64 = 0;
    let mut records_produced: u64 = 0;
    let payload = generate_payload(profile.avg_payload_size);

    println!(
        "Filling {} (payload={}KB, target={}MB)...",
        profile.topic,
        profile.avg_payload_size / (ONE_KB as usize),
        TARGET_BYTES_PER_TOPIC / ONE_MB
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

        if let Err((err, _)) = producer.send(record, Duration::from_secs(30)).await {
            eprintln!("produce error on {}: {err}", profile.topic);
            continue;
        }

        bytes_produced += payload.len() as u64;
        records_produced += 1;

        if records_produced % 10_000 == 0 {
            let mb = bytes_produced as f64 / ONE_MB as f64;
            println!(
                "  {} - {:.2} MB ({} records)",
                profile.topic, mb, records_produced
            );
        }
    }

    println!(
        "  {} done: {:.2} MB, {} records",
        profile.topic,
        bytes_produced as f64 / ONE_MB as f64,
        records_produced
    );
}
