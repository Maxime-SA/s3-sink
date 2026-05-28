use crate::{
    RouterStrategy, TopicConfig,
    data_model::{RecordMetadata, StreamId, TopicName},
};
use rdkafka::Message;
use std::{
    collections::{HashMap, HashSet},
    rc::Rc,
};

pub struct Cache {
    buf: String,
    ids: HashSet<StreamId>, // memoization of StreamIds to prevent allocation on every record
    configs: HashMap<TopicName, TopicConfig>,
}
impl Cache {
    pub fn new(input_topics: &Vec<(TopicConfig, Vec<TopicName>)>) -> Self {
        let configs = input_topics
            .iter()
            .fold(HashMap::new(), |mut acc, (config, topics)| {
                topics.iter().for_each(|topic| {
                    acc.insert(topic.clone(), *config);
                });

                acc
            });

        Self {
            buf: String::new(),
            ids: HashSet::new(),
            configs,
        }
    }

    pub fn get_or_create_record_metadata<M: Message>(
        &mut self,
        record: &M,
    ) -> Option<RecordMetadata> {
        let topic_name_ref = record.topic();

        let (topic_name_ptr, &config) = self.configs.get_key_value(topic_name_ref)?;

        let topic_name = topic_name_ptr.clone();

        let stream_id = self.get_or_create_stream_id(record, &config.router);

        Some(RecordMetadata {
            topic_name,
            stream_id,
            config,
        })
    }

    fn get_or_create_stream_id<M: Message>(
        &mut self,
        record: &M,
        strategy: &RouterStrategy,
    ) -> StreamId {
        strategy.write_id(record, &mut self.buf);

        if let Some(cached) = self.ids.get(self.buf.as_str()) {
            cached.clone()
        } else {
            let id = StreamId(Rc::from(self.buf.as_str()));
            self.ids.insert(id.clone());
            id
        }
    }
}
