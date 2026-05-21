use crate::{KafkaConfig, Result};
use rdkafka::{
    ClientConfig, ClientContext,
    config::FromClientConfigAndContext,
    consumer::{Consumer, ConsumerContext, StreamConsumer},
};
use tracing::info;

/*
Todo:
- Review unit tests
- Figure out what to do with partitions that will be revoked.
    - Do we need to flush and upload?
    - Simply discard work done so far and commit offsets?
    - ...
*/

pub struct SpecialContext;
impl ClientContext for SpecialContext {}
impl ConsumerContext for SpecialContext {
    fn pre_rebalance(
        &self,
        base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        info!("pre-rebalance");
        ()
    }
}

pub fn init_kafka_consumer(config: &KafkaConfig) -> Result<StreamConsumer<SpecialContext>> {
    let mut client_config = ClientConfig::new();

    for (key, value) in &config.consumer_properties {
        client_config.set(key, value);
    }

    let consumer: StreamConsumer<SpecialContext> =
        FromClientConfigAndContext::from_config_and_context(&client_config, SpecialContext)?;

    let topics_to_subscribe = config
        .input_topics
        .iter()
        .flat_map(|(_, topics)| topics.iter().map(|topic| topic.as_str()))
        .collect::<Vec<&str>>();

    consumer.subscribe(&topics_to_subscribe)?;

    Ok(consumer)
}
