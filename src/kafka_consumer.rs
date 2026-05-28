use crate::{KafkaConfig, Result};
use aws_config::Region;
use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
use rdkafka::{
    ClientConfig, ClientContext,
    client::OAuthToken,
    config::FromClientConfigAndContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer},
};
use std::borrow::Borrow;
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

/*
Todo:
- Review unit tests
*/

pub struct CustomContext {
    region: Region,
    lifetime_ms: i64,
    principal_name: String,
    tx: UnboundedSender<Vec<(String, i32)>>,
}
impl CustomContext {
    pub fn new(
        region: Region,
        lifetime_ms: i64,
        principal_name: String,
        tx: UnboundedSender<Vec<(String, i32)>>,
    ) -> Self {
        CustomContext {
            region,
            lifetime_ms,
            principal_name,
            tx,
        }
    }

    async fn generate_msk_iam_token(
        &self,
    ) -> std::result::Result<String, Box<dyn std::error::Error>> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;

        let (token, _expiry) = generate_auth_token_from_credentials_provider(
            self.region.clone(),
            config.credentials_provider().unwrap(),
        )
        .await?;

        Ok(token)
    }
}

impl ClientContext for CustomContext {
    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> std::prelude::v1::Result<rdkafka::client::OAuthToken, Box<dyn std::error::Error>> {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        let token = rt.block_on(self.generate_msk_iam_token())?;

        Ok(OAuthToken {
            token,
            principal_name: self.principal_name.clone(),
            lifetime_ms: self.lifetime_ms,
        })
    }
}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => info!("pre_rebalance: assigning {tpl:?}"),
            Rebalance::Revoke(tpl) => info!("pre_rebalance: revoking {tpl:?}"),
            Rebalance::Error(kafka_error) => warn!(
                "pre_rebalance: error {:?}",
                kafka_error.rdkafka_error_code()
            ),
        }
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => {
                info!("post_rebalance: assigned {tpl:?}");

                let partitions_assigned = tpl
                    .elements()
                    .iter()
                    .map(|element| (String::from(element.topic()), element.partition()))
                    .collect();

                if let Err(error) = self.tx.send(partitions_assigned) {
                    warn!("could not send partition assignment to event loop: {error:?}");
                };
            }
            Rebalance::Revoke(tpl) => info!("post_rebalance: revoked {tpl:?}"),
            Rebalance::Error(kafka_error) => warn!(
                "post_rebalance: error {:?}",
                kafka_error.rdkafka_error_code()
            ),
        }
    }

    fn commit_callback(
        &self,
        result: rdkafka::error::KafkaResult<()>,
        offsets: &rdkafka::TopicPartitionList,
    ) {
        match result {
            Ok(_) => info!("commit_callback: successfully committed {offsets:?}"),
            Err(kafka_error) => {
                warn!(
                    "commit_callback: error during commit phase {:?}",
                    kafka_error.rdkafka_error_code()
                );
            }
        }
    }
}

pub fn init_kafka_consumer(
    config: &KafkaConfig,
    tx: UnboundedSender<Vec<(String, i32)>>,
) -> Result<StreamConsumer<CustomContext>> {
    let mut client_config = ClientConfig::new();

    for (key, value) in &config.consumer_properties {
        client_config.set(key, value);
    }

    let custom_context = CustomContext::new(
        config.region.clone(),
        config.token_lifetime_ms,
        config.principal_name.clone(),
        tx,
    );

    let consumer: StreamConsumer<CustomContext> =
        FromClientConfigAndContext::from_config_and_context(&client_config, custom_context)?;

    let topics_to_subscribe = config
        .input_topics
        .iter()
        .flat_map(|(_, topics)| topics.iter().map(|topic| topic.borrow()))
        .collect::<Vec<&str>>();

    consumer.subscribe(&topics_to_subscribe)?;

    Ok(consumer)
}
