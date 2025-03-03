//! 输入组件模块
//!
//! 输入组件负责从各种源（如消息队列、文件系统、HTTP端点等）接收数据。

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, Message, MessageBatch};

pub mod file;
pub mod http;
pub mod kafka;
pub mod memory;
pub mod mqtt;

/// 输入组件的特征接口
#[async_trait]
pub trait Input: Send + Sync {
    /// 连接到输入源
    async fn connect(&self) -> Result<(), Error>;

    /// 从输入源读取消息
    async fn read(&self) -> Result<Message, Error>;

    /// 确认消息已被处理
    async fn acknowledge(&self, msg: &Message) -> Result<(), Error>;

    /// 关闭输入源连接
    async fn close(&self) -> Result<(), Error>;
}

#[async_trait]
pub trait InputBatch: Send + Sync {
    async fn connect(&self) -> Result<(), Error>;

    /// 从输入源读取消息
    async fn read(&self) -> Result<MessageBatch, Error>;

    /// 确认消息已被处理
    async fn acknowledge(&self, msg: &[Message]) -> Result<(), Error>;

    /// 关闭输入源连接
    async fn close(&self) -> Result<(), Error>;
}

#[async_trait]
impl<T> InputBatch for T
where
    T: Input,
{
    async fn connect(&self) -> Result<(), Error> {
        self.connect().await
    }

    async fn read(&self) -> Result<MessageBatch, Error> {
        let result = self.read().await?;
        Ok(MessageBatch::new_single(result))
    }

    async fn acknowledge(&self, msg: &[Message]) -> Result<(), Error> {
        for x in msg {
            self.acknowledge(x).await?
        }
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.close().await
    }
}

/// 输入配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum InputConfig {
    File(file::FileInputConfig),
    Http(http::HttpInputConfig),
    // Kafka(kafka::KafkaInputConfig),
    Memory(memory::MemoryInputConfig),
    Mqtt(mqtt::MqttInputConfig),
}

impl InputConfig {
    /// 根据配置构建输入组件
    pub fn build(&self) -> Result<Arc<dyn InputBatch>, Error> {
        match self {
            InputConfig::File(config) => Ok(Arc::new(file::FileInput::new(config)?)),
            InputConfig::Http(config) => Ok(Arc::new(http::HttpInput::new(config)?)),
            // InputConfig::Kafka(config) => Ok(Arc::new(kafka::KafkaInput::new(config)?)),
            InputConfig::Memory(config) => Ok(Arc::new(memory::MemoryInput::new(config)?)),
            InputConfig::Mqtt(config) => Ok(Arc::new(mqtt::MqttInput::new(config)?)),
        }
    }
}