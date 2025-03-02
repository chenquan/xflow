//! Kafka输出组件
//!
//! 将处理后的数据发送到Kafka主题

use std::cell::RefCell;
use std::thread_local;

use async_trait::async_trait;
use rdkafka::producer::FutureProducer;
use serde::{Deserialize, Serialize};

use crate::{Error, Message, output::Output};

// 线程本地存储，用于保存Kafka生产者实例
thread_local! {
    static PRODUCER: RefCell<Option<FutureProducer>> = RefCell::new(None);
    static CONNECTED: RefCell<bool> = RefCell::new(false);
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
    /// 压缩类型（none, gzip, snappy, lz4）
    pub compression: Option<String>,
    /// 确认级别（0=无确认, 1=领导者确认, all=所有副本确认）
    pub acks: Option<String>,
}

/// Kafka输出组件
pub struct KafkaOutput {
    config: KafkaOutputConfig,
    // 在实际实现中，这里应该有一个Kafka生产者客户端
    // 例如：producer: Option<rdkafka::producer::FutureProducer>,
    connected: bool,
}

impl KafkaOutput {
    /// 创建一个新的Kafka输出组件
    pub fn new(config: &KafkaOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            // producer: None,
            connected: false,
        })
    }
}
#[async_trait]
impl Output for KafkaOutput {
    async fn connect(&self) -> Result<(), Error> {
        // 创建一个真正的Kafka生产者客户端
        use rdkafka::config::ClientConfig;
        use rdkafka::producer::FutureProducer;

        let mut client_config = ClientConfig::new();

        // 设置Kafka服务器地址
        client_config.set("bootstrap.servers", &self.config.brokers.join(","));

        // 设置客户端ID
        if let Some(client_id) = &self.config.client_id {
            client_config.set("client.id", client_id);
        }

        // 设置压缩类型
        if let Some(compression) = &self.config.compression {
            client_config.set("compression.type", compression);
        }

        // 设置确认级别
        if let Some(acks) = &self.config.acks {
            client_config.set("acks", acks);
        }

        // 创建生产者并存储在TLS中
        let producer: FutureProducer = client_config.create()
            .map_err(|e| Error::Connection(format!("无法创建Kafka生产者: {}", e)))?;

        // 使用线程本地存储来保存生产者实例
        PRODUCER.with(|cell| {
            let mut producer_ref = cell.borrow_mut();
            *producer_ref = Some(producer);
        });

        // 更新连接状态
        CONNECTED.with(|cell| {
            let mut connected_ref = cell.borrow_mut();
            *connected_ref = true;
        });

        Ok(())
    }

    async fn write(&self, msg: &Message) -> Result<(), Error> {
        // 检查连接状态
        let connected = CONNECTED.with(|cell| *cell.borrow());
        if !connected {
            return Err(Error::Connection("输出未连接".to_string()));
        }

        // 使用Kafka生产者发送消息
        use rdkafka::producer::FutureRecord;
        use std::time::Duration;

        // 从线程本地存储获取生产者
        let producer = PRODUCER.with(|cell| {
            cell.borrow().clone().ok_or_else(|| Error::Connection("Kafka生产者未初始化".to_string()))
        })?;

        let content = msg.content();

        // 创建记录
        let mut record = FutureRecord::to(&self.config.topic)
            .payload(content);

        // 如果有分区键，则设置
        if let Some(key) = &self.config.key {
            record = record.key(key);
        }

        // 发送消息
        producer.send(record, Duration::from_secs(5))
            .await
            .map_err(|(e, _)| Error::Processing(format!("发送Kafka消息失败: {}", e)))?;

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // 更新连接状态
        CONNECTED.with(|cell| {
            let mut connected_ref = cell.borrow_mut();
            *connected_ref = false;
        });

        // 清除生产者
        PRODUCER.with(|cell| {
            let mut producer_ref = cell.borrow_mut();
            *producer_ref = None;
        });

        Ok(())
    }
}