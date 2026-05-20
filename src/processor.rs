use crate::{
    BoxFuture, Result, TopicConfig, Uploader, error::SinkError, files::FileRegistry,
    json_serializer::JsonSerializer, kafka_consumer::SpecialContext,
};
use futures::stream::FuturesUnordered;
use rdkafka::{Message, consumer::StreamConsumer, message::BorrowedMessage};
use std::collections::HashMap;

pub struct Processor<'a, U: Uploader> {
    uploader: U,
    serializer: JsonSerializer,
    topics_config: HashMap<&'a str, &'a TopicConfig>,
    target_file_size_b: usize,
    max_concurrent_uploads: usize,
}

impl<'a, U: Uploader> Processor<'a, U> {
    pub fn new(
        uploader: U,
        input_topics: &'a Vec<(TopicConfig, Vec<String>)>,
        target_file_size_b: usize,
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
            serializer: JsonSerializer::new(),
            topics_config,
            target_file_size_b,
            max_concurrent_uploads,
        }
    }

    /*
    Simple rate limiter for the number of in-flight uploads. We can limit the memory needed for these uploads.
     */
    fn can_upload(&self, raw_size_b: usize, in_flight_uploads: usize) -> bool {
        raw_size_b >= self.target_file_size_b && in_flight_uploads < self.max_concurrent_uploads
    }

    pub fn process_record(
        &mut self,
        record: &BorrowedMessage<'_>,
        registry: &mut FileRegistry,
        upload_ftrs: &mut FuturesUnordered<BoxFuture>,
    ) -> Result<()> {
        let topic_name = record.topic();

        let topic_config = self.topics_config.get(topic_name).copied().ok_or_else(|| {
            SinkError::ConfigurationError(format!("missing topic configuration for '{topic_name}'"))
        })?;

        let file_id = &topic_config.router.id(record);

        // serializer will return None for records with no payload and Err(...) for errors
        if let Some(bytes) = self.serializer.serialize(record, &topic_config.decoder)? {
            let file = registry.get_mut_active_file_or_create(&file_id)?;
            file.write_all(bytes)?;
            file.inc_record_count();

            if self.can_upload(file.raw_size_b(), upload_ftrs.len()) {
                let sealed_file = registry.seal(&file_id)?;
                upload_ftrs.push(self.uploader.upload(sealed_file));
            }
        }

        registry.add_offset(
            &file_id,
            record.topic(),
            record.partition(),
            record.offset(),
        )?;

        // TODO: fairness scheduler

        Ok(())
    }

    pub fn reset_ingestion_budgets(&mut self, consumer: &StreamConsumer<SpecialContext>) {
        todo!()
    }
}
