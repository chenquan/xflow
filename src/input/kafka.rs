//! Kafka输入组件
//!
//! 从Kafka主题接收数据

use std::cell::RefCell;
use std::thread_local;

use async_trait::async_trait;
use rdkafka::consumer::{Consumer, StreamConsumer};
use serde::{Deserialize, Serialize};

use crate::{Error, Message, input::Input};

// 线程本地存储，用于保存Kafka消费者实例
thread_local! {
    static CONSUMER: RefCell<Option<StreamConsumer>> = RefCell::new(None);
    static CONNECTED: RefCell<bool> = RefCell::new(false);
}

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
    /// 从最早的消息开始消费
    pub start_from_beginning: bool,
}

/// Kafka输入组件
pub struct KafkaInput {
    config: KafkaInputConfig,
    // 在实际实现中，这里应该有一个Kafka消费者客户端
    // 例如：consumer: Option<rdkafka::consumer::StreamConsumer>,
    connected: bool,
}

impl KafkaInput {
    /// 创建一个新的Kafka输入组件
    pub fn new(config: &KafkaInputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            // consumer: None,
            connected: false,
        })
    }
}

#[async_trait]
impl Input for KafkaInput {
    async fn connect(&self) -> Result<(), Error> {
        use rdkafka::config::ClientConfig;
        use rdkafka::consumer::{Consumer, StreamConsumer};

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
        if self.config.start_from_beginning {
            client_config.set("auto.offset.reset", "earliest");
        } else {
            client_config.set("auto.offset.reset", "latest");
        }

        // 创建消费者
        let consumer: StreamConsumer = client_config.create()
            .map_err(|e| Error::Connection(format!("无法创建Kafka消费者: {}", e)))?;

        // 订阅主题
        consumer.subscribe(self.config.topics.as_ref())
            .map_err(|e| Error::Connection(format!("无法订阅Kafka主题: {}", e)))?;

        // 使用线程本地存储来保存消费者实例
        CONSUMER.with(|cell| {
            let mut consumer_ref = cell.borrow_mut();
            *consumer_ref = Some(consumer);
        });

        // 更新连接状态
        CONNECTED.with(|cell| {
            let mut connected_ref = cell.borrow_mut();
            *connected_ref = true;
        });

        Ok(())
    }

    async fn read(&self) -> Result<Message, Error> {
        // 检查连接状态
        let connected = CONNECTED.with(|cell| *cell.borrow());
        if !connected {
            return Err(Error::Connection("输入未连接".to_string()));
        }

        use rdkafka::message::Message as KafkaMessage;
        use std::time::Duration;

        // 从线程本地存储获取消费者
        let consumer = CONSUMER.with(|cell| {
            cell.borrow().clone().ok_or_else(|| Error::Connection("Kafka消费者未初始化".to_string()))
        })?;

        // 接收消息，设置超时时间
        match tokio::time::timeout(Duration::from_secs(5), consumer.recv()).await {
            Ok(Ok(kafka_message)) => {
                // 从Kafka消息创建内部消息
                let payload = kafka_message.payload()
                    .ok_or_else(|| Error::Processing("Kafka消息没有内容".to_string()))?;

                let mut msg = Message::from_bytes(payload);

                // 添加元数据
                let metadata = msg.metadata_mut();

                // 添加主题信息
                if let Some(topic) = kafka_message.topic() {
                    metadata.insert("kafka_topic".to_string(), topic.to_string());
                }

                // 添加分区信息
                metadata.insert("kafka_partition".to_string(), kafka_message.partition().to_string());

                // 添加偏移量信息
                metadata.insert("kafka_offset".to_string(), kafka_message.offset().to_string());

                // 添加时间戳信息
                if let Some((ts_type, ts)) = kafka_message.timestamp() {
                    metadata.insert("kafka_timestamp".to_string(), ts.to_string());
                    metadata.insert("kafka_timestamp_type".to_string(), format!("{:?}", ts_type));
                }

                Ok(msg)
            }
            Ok(Err(e)) => Err(Error::Processing(format!("Kafka消息接收错误: {}", e))),
            Err(_) => Err(Error::Processing("接收Kafka消息超时".to_string())),
        }
    }

    async fn acknowledge(&self, msg: &Message) -> Result<(), Error> {
        // 从消息元数据中获取偏移量信息
        if let (Some(topic), Some(partition_str), Some(offset_str)) = (
            msg.metadata().get("kafka_topic"),
            msg.metadata().get("kafka_partition"),
            msg.metadata().get("kafka_offset")
        ) {
            let partition = partition_str.parse::<i32>().map_err(|e| {
                Error::Processing(format!("无法解析分区信息: {}", e))
            })?;

            let offset = offset_str.parse::<i64>().map_err(|e| {
                Error::Processing(format!("无法解析偏移量信息: {}", e))
            })?;

            // 从线程本地存储获取消费者
            let consumer = CONSUMER.with(|cell| {
                cell.borrow().clone().ok_or_else(|| Error::Connection("Kafka消费者未初始化".to_string()))
            })?;

            // 提交偏移量
            consumer.store_offset(topic, partition, offset + 1)
                .map_err(|e| Error::Processing(format!("无法提交偏移量: {}", e)))?;
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // 更新连接状态
        CONNECTED.with(|cell| {
            let mut connected_ref = cell.borrow_mut();
            *connected_ref = false;
        });

        // 清除消费者
        CONSUMER.with(|cell| {
            let mut consumer_ref = cell.borrow_mut();
            *consumer_ref = None;
        });

        Ok(())
    }
}