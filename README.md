# Data Platform S3 Sink
A single-threaded asynchronous Kafka S3 Sink written in Rust. The service has an at-least-once delivery semantic.

## Benchmark
This simple service is able to consume 700MB per seconds on 1vCPU and 4GB of RAM. The benchmark Kafka cluster had more than 500 topics amounting to 3000+ partitions. Input topics were highly heterogeneous, ranging from low volume, low throughput, to high throughput, high volume. Individual records ranged from a couple of bytes to more than 30 MBs.  
