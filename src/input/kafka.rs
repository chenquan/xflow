//! Kafka输入组件
//!
//! 从Kafka主题接收数据

use crate::input::{Ack, Input};
use crate::{Error, MessageBatch};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::Message as KafkaMessage;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::broadcast::error::RecvError;
use tokio::sync::{broadcast, oneshot, Mutex, RwLock};

/// Kafka输入配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaInputConfig {
    /// Kafka服务器地址列表
    pub brokers: Vec<String>,
    /// 订阅的主题
    pub topics: Vec<String>,
    /// 消费者组ID
    pub consumer_group: String,
    /// 客户端ID（可选）
    pub client_id: Option<String>,
    /// 从最的消息开始消费
    pub start_from_latest: bool,
}

/// Kafka输入组件
pub struct KafkaInput {
    config: KafkaInputConfig,
    consumer: Arc<RwLock<Option<StreamConsumer>>>,
    close_tx: broadcast::Sender<()>,
}

impl KafkaInput {
    /// 创建一个新的Kafka输入组件
    pub fn new(config: &KafkaInputConfig) -> Result<Self, Error> {
        let (close_tx, _) = broadcast::channel(1);
        Ok(Self {
            config: config.clone(),
            consumer: Arc::new(RwLock::new(None)),
            close_tx,
        })
    }
}

#[async_trait]
impl Input for KafkaInput {
    async fn connect(&self) -> Result<(), Error> {
        let mut client_config = ClientConfig::new();

        // 设置Kafka服务器地址
        client_config.set("bootstrap.servers", &self.config.brokers.join(","));

        // 设置消费者组ID
        client_config.set("group.id", &self.config.consumer_group);

        // 设置客户端ID
        if let Some(client_id) = &self.config.client_id {
            client_config.set("client.id", client_id);
        }

        // 设置偏移量重置策略
        if self.config.start_from_latest {
            client_config.set("auto.offset.reset", "latest");
        } else {
            client_config.set("auto.offset.reset", "earliest");
        }

        // 创建消费者
        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| Error::Connection(format!("无法创建Kafka消费者: {}", e)))?;

        // 订阅主题
        let x: Vec<&str> = self
            .config
            .topics
            .iter()
            .map(|topic| topic.as_str())
            .collect();
        consumer
            .subscribe(&x)
            .map_err(|e| Error::Connection(format!("无法订阅Kafka主题: {}", e)))?;

        // 更新消费者和连接状态
        let consumer_arc = self.consumer.clone();
        let mut consumer_guard = consumer_arc.write().await;
        *consumer_guard = Some(consumer);

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        let consumer_arc = self.consumer.clone();
        let consumer_guard = consumer_arc.read().await;
        if consumer_guard.is_none() {
            return Err(Error::Connection("输入未连接".to_string()));
        }
        let consumer = consumer_guard.as_ref().unwrap();

        let mut rx = self.close_tx.subscribe();

        tokio::select! {
            result = consumer.recv() => match result {
                Ok(kafka_message) => {
                    // 从Kafka消息创建内部消息
                    let payload = kafka_message.payload()
                        .ok_or_else(|| Error::Processing("Kafka消息没有内容".to_string()))?;

                    let mut binary_data = Vec::new();
                    binary_data.push(payload.to_vec());
                    let msg_batch = MessageBatch::new_binary(binary_data);

                    // 创建确认对象
                    let topic = kafka_message.topic().to_string();
                    let partition = kafka_message.partition();
                    let offset = kafka_message.offset();

                    let ack = KafkaAck {
                        consumer: self.consumer.clone(),
                        topic,
                        partition,
                        offset,
                    };

                    Ok((msg_batch, Arc::new(ack)))
                }
                Err(e) => {
                    Err(Error::Connection(format!("Kafka消息接收错误: {}", e)))
                }
            },
            _ =  rx.recv() => {
                Err(Error::Done)
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        let _ = self.close_tx.send(());

        // 安全地清理消费者资源
        let mut consumer_guard = self.consumer.write().await;
        if let Some(consumer) = consumer_guard.take() {
            if let Err(e) = consumer.unassign() {
                tracing::warn!("无法取消Kafka消费者分配: {}", e);
            }
        }
        Ok(())
    }
}

/// Kafka消息确认
pub struct KafkaAck {
    consumer: Arc<RwLock<Option<StreamConsumer>>>,
    topic: String,
    partition: i32,
    offset: i64,
}

#[async_trait]
impl Ack for KafkaAck {
    async fn ack(&self) {
        // 提交偏移量
        let consumer_mutex_guard = self.consumer.read().await;
        if let Some(v) = &*consumer_mutex_guard {
            if let Err(e) = v.store_offset(&self.topic, self.partition, self.offset) {
                tracing::error!("无法提交Kafka偏移量: {}", e);
            }
        }
    }
}
