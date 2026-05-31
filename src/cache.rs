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

#[cfg(test)]
mod test {
    use super::*;
    use crate::{RecordDecoder, test_utils::make_owned_message};

    #[test]
    fn test_new_cache() {
        let topic_a = TopicName(Rc::from("topic-a"));
        let topic_b = TopicName(Rc::from("topic-b"));
        let topic_c = TopicName(Rc::from("topic-c"));

        let input_topics = vec![
            (
                TopicConfig {
                    decoder: RecordDecoder::JsonSchemaDecoder,
                    router: RouterStrategy::TopicVersion,
                },
                vec![topic_a.clone(), topic_b.clone()],
            ),
            (
                TopicConfig {
                    decoder: RecordDecoder::JsonStringDecoder,
                    router: RouterStrategy::Dlq,
                },
                vec![topic_c.clone()],
            ),
        ];

        let cache = Cache::new(&input_topics);

        assert_eq!(
            cache.configs.get(&topic_a).unwrap(),
            &TopicConfig {
                decoder: RecordDecoder::JsonSchemaDecoder,
                router: RouterStrategy::TopicVersion,
            }
        );

        assert_eq!(
            cache.configs.get(&topic_b).unwrap(),
            &TopicConfig {
                decoder: RecordDecoder::JsonSchemaDecoder,
                router: RouterStrategy::TopicVersion,
            }
        );

        assert_eq!(
            cache.configs.get(&topic_c).unwrap(),
            &TopicConfig {
                decoder: RecordDecoder::JsonStringDecoder,
                router: RouterStrategy::Dlq,
            }
        );

        assert!(cache.ids.is_empty());
    }

    #[test]
    fn get_or_create_record_metadata_with_topic_config() {
        let topic_name = TopicName(Rc::from("topic-a"));

        let input_topics = vec![(
            TopicConfig {
                decoder: RecordDecoder::JsonSchemaDecoder,
                router: RouterStrategy::TopicVersion,
            },
            vec![topic_name.clone()],
        )];

        let message = make_owned_message(Some("topic-a"), None, None);

        let mut buf = String::new();
        RouterStrategy::TopicVersion.write_id(&message, &mut buf);

        let mut cache = Cache::new(&input_topics);

        let metadata = cache.get_or_create_record_metadata(&message).unwrap();

        assert_eq!(metadata.topic_name, topic_name);

        assert_eq!(metadata.stream_id, StreamId(Rc::from(buf)));
    }

    #[test]
    fn get_or_create_record_metadata_without_topic_config() {
        let input_topics = vec![];

        let message = make_owned_message(Some("topic-a"), None, None);

        let mut cache = Cache::new(&input_topics);

        let metadata = cache.get_or_create_record_metadata(&message);

        assert_eq!(metadata, None);
    }
}
