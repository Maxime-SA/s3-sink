use crate::{KafkaConfig, Result};
use aws_config::Region;
use aws_msk_iam_sasl_signer::generate_auth_token_from_credentials_provider;
use aws_sdk_s3::config::SharedCredentialsProvider;
use rdkafka::{
    ClientConfig, ClientContext,
    client::OAuthToken,
    config::FromClientConfigAndContext,
    consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance, StreamConsumer},
};
use std::{borrow::Borrow, collections::HashSet, time::Duration};
use tokio::sync::mpsc::UnboundedSender;
use tracing::{info, warn};

/*
Todo:
- Review unit tests
*/

pub struct CustomContext {
    region: Region,
    principal_name: String,
    tx: UnboundedSender<HashSet<(String, i32)>>,
    credentials_provider: SharedCredentialsProvider,
}
impl CustomContext {
    pub fn new(
        region: Region,
        principal_name: String,
        tx: UnboundedSender<HashSet<(String, i32)>>,
        credentials_provider: SharedCredentialsProvider,
    ) -> Self {
        CustomContext {
            region,
            principal_name,
            tx,
            credentials_provider,
        }
    }
}

impl ClientContext for CustomContext {
    const ENABLE_REFRESH_OAUTH_TOKEN: bool = true;

    fn generate_oauth_token(
        &self,
        _oauthbearer_config: Option<&str>,
    ) -> std::prelude::v1::Result<rdkafka::client::OAuthToken, Box<dyn std::error::Error>> {
        info!("generating MSK IAM token");

        let region = self.region.clone();
        let credentials_provider = self.credentials_provider.clone();

        let result = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();

            rt.block_on(async {
                tokio::time::timeout(
                    Duration::from_secs(10),
                    generate_auth_token_from_credentials_provider(region, credentials_provider),
                )
                .await
            })
        })
        .join()
        .map_err(|_| "MSK token thread panicked")??;

        let (token, expiration_time_ms) = result?;

        Ok(OAuthToken {
            token,
            principal_name: self.principal_name.clone(),
            lifetime_ms: expiration_time_ms,
        })
    }
}

impl ConsumerContext for CustomContext {
    fn pre_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => info!("pre_rebalance: assigning {} partitions", tpl.count()),
            Rebalance::Revoke(tpl) => {
                info!("pre_rebalance: revoking {} partitions", tpl.count());

                let partitions_revoked =
                    tpl.elements().iter().fold(HashSet::new(), |mut acc, next| {
                        let topic_name = String::from(next.topic());
                        let partition = next.partition();

                        acc.insert((topic_name, partition));
                        acc
                    });

                if let Err(error) = self.tx.send(partitions_revoked) {
                    warn!("could not send revoked partitions to event loop: {error:?}");
                };
            }
            Rebalance::Error(kafka_error) => warn!(
                "pre_rebalance: error {:?}",
                kafka_error.rdkafka_error_code()
            ),
        }
    }

    fn post_rebalance(&self, _: &BaseConsumer<Self>, rebalance: &Rebalance<'_>) {
        match rebalance {
            Rebalance::Assign(tpl) => info!("post_rebalance: assigned {} partitions", tpl.count()),
            Rebalance::Revoke(tpl) => info!("post_rebalance: revoked {} partitions", tpl.count()),
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
            Ok(_) => info!("commit_callback: successfully committed {offsets:?}",),
            Err(kafka_error) => {
                warn!(
                    "commit_callback: error during commit phase {:?}",
                    kafka_error.rdkafka_error_code()
                );
            }
        }
    }
}

pub async fn init_kafka_consumer(
    config: &KafkaConfig,
    tx: UnboundedSender<HashSet<(String, i32)>>,
) -> Result<StreamConsumer<CustomContext>> {
    let mut client_config = ClientConfig::new();

    for (key, value) in &config.consumer_properties {
        client_config.set(key, value);
    }

    let credentials_provider = aws_config::load_defaults(aws_config::BehaviorVersion::latest())
        .await
        .credentials_provider()
        .expect("no credentials provider");

    let custom_context = CustomContext::new(
        config.region.clone(),
        config.principal_name.clone(),
        tx,
        credentials_provider,
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
