//! Kafka输出组件
//!
//! 将处理后的数据发送到Kafka主题

use serde::{Deserialize, Serialize};

use crate::{Content, Error};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum CompressionType {
    None,
    Gzip,
    Snappy,
    Lz4,
}

impl std::fmt::Display for CompressionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CompressionType::None => write!(f, "none"),
            CompressionType::Gzip => write!(f, "gzip"),
            CompressionType::Snappy => write!(f, "snappy"),
            CompressionType::Lz4 => write!(f, "lz4"),
        }
    }
}

/// Kafka输出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaOutputConfig {
    /// Kafka服务器地址列表
    pub brokers: Vec<String>,
    /// 目标主题
    pub topic: String,
    /// 分区键（可选）
    pub key: Option<String>,
    /// 客户端ID
    pub client_id: Option<String>,
    /// 压缩类型
    pub compression: Option<CompressionType>,
    /// 确认级别（0=无确认, 1=领导者确认, all=所有副本确认）
    pub acks: Option<String>,
}

/// Kafka输出组件
pub struct KafkaOutput {
    config: KafkaOutputConfig,
    producer: Arc<RwLock<Option<FutureProducer>>>,
}

impl KafkaOutput {
    /// 创建一个新的Kafka输出组件
    pub fn new(config: &KafkaOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            producer: Arc::new(RwLock::new(None)),
        })
    }
}
use crate::{output::Output, MessageBatch};
use async_trait::async_trait;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;

#[async_trait]
impl Output for KafkaOutput {
    async fn connect(&self) -> Result<(), Error> {
        let mut client_config = ClientConfig::new();

        // 设置Kafka服务器地址
        client_config.set("bootstrap.servers", &self.config.brokers.join(","));

        // 设置客户端ID
        if let Some(client_id) = &self.config.client_id {
            client_config.set("client.id", client_id);
        }

        // 设置压缩类型
        if let Some(compression) = &self.config.compression {
            client_config.set("compression.type", compression.to_string().to_lowercase());
        }

        // 设置确认级别
        if let Some(acks) = &self.config.acks {
            client_config.set("acks", acks);
        }

        // 创建生产者
        let producer: FutureProducer = client_config
            .create()
            .map_err(|e| Error::Connection(format!("无法创建Kafka生产者: {}", e)))?;

        // 保存生产者实例
        let producer_arc = self.producer.clone();
        let mut producer_guard = producer_arc.write().await;
        *producer_guard = Some(producer);

        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        let producer_arc = self.producer.clone();
        let producer_guard = producer_arc.read().await;
        let producer = producer_guard
            .as_ref()
            .ok_or_else(|| Error::Connection("Kafka生产者未初始化".to_string()))?;

        let payloads = msg.as_string()?;
        if payloads.is_empty() {
            return Ok(());
        }

        match &msg.content {
            Content::Arrow(_) => return Err(Error::Processing("不支持arrow格式".to_string())),
            Content::Binary(v) => {
                for x in v {
                    // 创建记录
                    let mut record = FutureRecord::to(&self.config.topic).payload(&x);

                    // 如果有分区键，则设置
                    if let Some(key) = &self.config.key {
                        record = record.key(key);
                    }

                    // 获取生产者并发送消息
                    producer
                        .send(record, Duration::from_secs(5))
                        .await
                        .map_err(|(e, _)| Error::Processing(format!("发送Kafka消息失败: {}", e)))?;
                }
            }
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // 获取生产者并关闭
        let producer_arc = self.producer.clone();
        let mut producer_guard = producer_arc.write().await;

        if let Some(producer) = producer_guard.take() {
            // 等待所有消息发送完成
            producer
                .flush(Duration::from_secs(30))
                .map_err(|e| Error::Connection(format!("关闭Kafka生产者时刷新消息失败: {}", e)))?;
        }
        Ok(())
    }
}
