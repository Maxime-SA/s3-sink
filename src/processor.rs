use futures::stream::FuturesUnordered;
use rdkafka::{Message, consumer::StreamConsumer, message::BorrowedMessage};

use crate::{
    BoxFuture, RecordDecoder, Result, TopicConfig, Uploader, error::SinkError,
    file_registry::FileRegistry, json_serializer::JsonSerializer, kafka_consumer::SpecialContext,
    record_router::RecordRouter,
};
use std::collections::HashMap;

pub struct Processor<'a, U: Uploader> {
    uploader: U,
    topics_config: HashMap<&'a str, &'a TopicConfig>,
    target_file_size_bytes: usize,
    max_concurrent_uploads: usize,
}

impl<'a, U: Uploader> Processor<'a, U> {
    pub fn new(
        uploader: U,
        input_topics: &'a Vec<(TopicConfig, Vec<String>)>,
        target_file_size_bytes: usize,
        max_concurrent_uploads: usize,
    ) -> Self {
        let topics_config =
            input_topics
                .iter()
                .fold(HashMap::new(), |mut acc, (configs, topics)| {
                    topics.iter().for_each(|topic| {
                        acc.insert(topic.as_str(), configs);
                    });
                    acc
                });

        Processor {
            uploader,
            topics_config,
            target_file_size_bytes,
            max_concurrent_uploads,
        }
    }

    fn topic_config(&self, topic_name: &str) -> Result<(&RecordDecoder, &RecordRouter)> {
        let config =
            self.topics_config
                .get(topic_name)
                .copied()
                .ok_or(SinkError::ConfigurationError(format!(
                    "missing topic configuration for '{topic_name}'"
                )))?;

        Ok((&config.decoder, &config.router))
    }

    pub fn process(
        &mut self,
        record: &BorrowedMessage<'_>,
        registry: &mut FileRegistry,
        upload_ftrs: &mut FuturesUnordered<BoxFuture>,
        serializer: &mut JsonSerializer,
    ) -> Result<()> {
        let (decoder, router) = self.topic_config(record.topic())?;

        if let Some(bytes) = serializer.serialize(record, decoder) {
            let file_id = router.id(record);

            let file = registry.get_active_file_or_create(&file_id)?;
            file.write_all(bytes)?;
            file.inc_record_count();

            if file.size() >= self.target_file_size_bytes
                && upload_ftrs.len() < self.max_concurrent_uploads
            {
                let sealed_file = registry.seal(&file_id)?;
                upload_ftrs.push(self.uploader.upload(sealed_file));
            }

            registry.add_offset(
                &file_id,
                record.topic(),
                record.partition(),
                record.offset(),
            )?;

            // topic ingestion budget management
        }

        Ok(())
    }

    pub fn reset_ingestion_budgets(&mut self, consumer: &StreamConsumer<SpecialContext>) {
        todo!()
    }
}
