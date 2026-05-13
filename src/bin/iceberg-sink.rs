struct IcebergConfiguration;

struct KafkaConfiguration;

struct Configuration {
    iceberg: IcebergConfiguration,
    kafka: KafkaConfiguration,
}

fn main() {
    println!("Starting Iceberg Sink")
}
