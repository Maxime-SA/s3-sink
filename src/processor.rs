use futures::stream::FuturesUnordered;
use rdkafka::{Message, consumer::StreamConsumer, message::BorrowedMessage};

use crate::{
    BoxFuture, Result, TopicConfig, UploadResult, Uploader, error::SinkError, file_io::FilesState,
    json_serializer::JsonSerializer, kafka_consumer::SpecialContext,
};
use std::collections::HashMap;

pub struct Processor<'a, U: Uploader> {
    serializer: JsonSerializer,
    uploader: U,
    topic_config: HashMap<&'a str, &'a TopicConfig>,
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
        let topic_config =
            input_topics
                .iter()
                .fold(HashMap::new(), |mut acc, (configs, topics)| {
                    topics.iter().for_each(|topic| {
                        acc.insert(topic.as_str(), configs);
                    });

                    acc
                });

        Processor {
            serializer: JsonSerializer::new(),
            uploader,
            topic_config,
            target_file_size_bytes,
            max_concurrent_uploads,
        }
    }

    /*
    Missing:
    - Check the topic allowed budget
    */
    pub fn process(
        &mut self,
        record: &BorrowedMessage<'_>,
        state: &mut FilesState,
        upload_ftrs: &mut FuturesUnordered<BoxFuture>,
    ) -> Result<()> {
        let topic_name = record.topic();

        

        let config = self
            .topic_config
            .get(topic_name)
            .copied()
            .ok_or(SinkError::KafkaError(format!(
                "missing record type and partitioner configuration for topic: '{topic_name}'"
            )))?;

            /*
                We have a stream of records from all topics:
                - We want to divide this stream into substreams.
                - Each substream id is a combination of properties from the record:
                    - topic name
                    - schema version
                    - status code 
             */

        if let Some(bytes) = self.serializer.serialize(record, &config.record_type) {
            let file_id = config.partitioner.get_file_id(record);

            let offset = record.offset();

            let file_handle = state.active_file(offset, &file_id)?;

            file_handle.write_all(bytes)?;
            file_handle.update_end_offset(offset);

            if file_handle.size() >= self.target_file_size_bytes
                && upload_ftrs.len() < self.max_concurrent_uploads
            {
                let sealed_file = state.seal_file(&file_id)?;
                upload_ftrs.push(self.uploader.upload(sealed_file));
            }
        }

        Ok(())
    }

    pub fn reset_ingestion_budgets(&mut self, consumer: &StreamConsumer<SpecialContext>) {
        todo!()
    }
}
