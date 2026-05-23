use crate::{Result, RouterStrategy, SinkConfig, TopicConfig, error::SinkError};
use rdkafka::Message;
use std::{
    borrow::Borrow,
    collections::{HashMap, HashSet},
    fmt,
    rc::Rc,
};

/*
A Stream represents a flow of records from Kafka.
Each StreamId is derived from the record using a RouterStrategy.
The StreamId is used to route the record to its specific file and track offsets.
*/
#[derive(Eq, Hash, PartialEq, Clone)]
pub struct StreamId(pub Rc<str>);

impl fmt::Display for StreamId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl Borrow<str> for StreamId {
    fn borrow(&self) -> &str {
        &self.0
    }
}

#[derive(Eq, Hash, PartialEq, Clone, Debug)]
pub struct TopicName(pub Rc<str>);

impl Borrow<str> for TopicName {
    fn borrow(&self) -> &str {
        &self.0
    }
}

pub struct RecordMetadata {
    pub topic_name: TopicName,
    pub stream_id: StreamId,
    pub config: TopicConfig,
}

pub struct Cache {
    buf: String,
    topics: HashMap<TopicName, TopicConfig>,
    ids: HashSet<StreamId>, // memoization of StreamIds to prevent allocation on every record
    routers: HashMap<StreamId, RouterStrategy>, // for convenience when processing dormant files
}

impl Cache {
    pub fn new(config: &SinkConfig) -> Self {
        let mut topic_cache = HashMap::new();

        for (topic_config, topics) in &config.kafka.input_topics {
            for topic in topics {
                topic_cache.insert(topic.clone(), *topic_config);
            }
        }

        Self {
            buf: String::new(),
            topics: topic_cache,
            ids: HashSet::new(),
            routers: HashMap::new(),
        }
    }

    pub fn get_or_create_record_metadata<M: Message>(
        &mut self,
        record: &M,
    ) -> Result<RecordMetadata> {
        let topic_name_ref = record.topic();

        let (topic_name_ptr, &config) =
            self.topics.get_key_value(topic_name_ref).ok_or_else(|| {
                SinkError::Configuration(format!(
                    "missing topic configuration for '{topic_name_ref}'"
                ))
            })?;

        let topic_name = topic_name_ptr.clone();

        Ok(RecordMetadata {
            topic_name,
            stream_id: self.get_stream_id(record, &config.router),
            config,
        })
    }

    fn get_stream_id<M: Message>(&mut self, record: &M, strategy: &RouterStrategy) -> StreamId {
        strategy.write_id(record, &mut self.buf);

        if let Some(cached) = self.ids.get(self.buf.as_str()) {
            cached.clone()
        } else {
            let id = StreamId(Rc::from(self.buf.as_str()));
            self.routers.insert(id.clone(), *strategy);
            self.ids.insert(id.clone());
            id
        }
    }

    pub fn get_router(&mut self, id: &StreamId) -> Result<&RouterStrategy> {
        self.routers.get(id).ok_or_else(|| {
            SinkError::Configuration(format!("missing topic configuration for '{id}'"))
        })
    }
}
