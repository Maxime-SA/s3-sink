use crate::{KafkaConfig, Result};
use aws_config::Region;
use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
use rdkafka::{
    ClientConfig, ClientContext,
    client::OAuthToken,
    config::FromClientConfigAndContext,
    consumer::{Consumer, ConsumerContext, StreamConsumer},
};
use std::borrow::Borrow;
use tracing::info;

/*
Todo:
- Review unit tests
- Figure out what to do with partitions that will be revoked.
    - Do we need to flush and upload?
    - Simply discard work done so far and commit offsets?
    - ...
*/

pub struct CustomContext {
    region: Region,
    lifetime_ms: i64,
    principal_name: String,
}
impl CustomContext {
    pub fn new(region: Region, lifetime_ms: i64, principal_name: String) -> Self {
        CustomContext {
            region,
            lifetime_ms,
            principal_name,
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
    fn pre_rebalance(
        &self,
        base_consumer: &rdkafka::consumer::BaseConsumer<Self>,
        rebalance: &rdkafka::consumer::Rebalance<'_>,
    ) {
        info!("pre-rebalance");
        ()
    }
}

pub fn init_kafka_consumer(config: &KafkaConfig) -> Result<StreamConsumer<CustomContext>> {
    let mut client_config = ClientConfig::new();

    for (key, value) in &config.consumer_properties {
        client_config.set(key, value);
    }

    let custom_context = CustomContext::new(
        config.region.clone(),
        config.token_lifetime_ms,
        config.principal_name.clone(),
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
