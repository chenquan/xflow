//! 输入组件模块
//!
//! 输入组件负责从各种源（如消息队列、文件系统、HTTP端点等）接收数据。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{Error, MessageBatch};

pub mod file;
pub mod http;
pub mod kafka;
pub mod memory;
pub mod mqtt;
mod generate;

#[async_trait]
pub trait Ack: Send + Sync {
    async fn ack(&self);
}
/// 输入组件的特征接口
#[async_trait]
pub trait Input: Send + Sync {
    /// 连接到输入源
    async fn connect(&self) -> Result<(), Error>;

    /// 从输入源读取消息
    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error>;


    /// 关闭输入源连接
    async fn close(&self) -> Result<(), Error>;
}


pub struct NoopAck;

#[async_trait]
impl Ack for NoopAck {
    async fn ack(&self) {}
}

/// 输入配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum InputConfig {
    File(file::FileInputConfig),
    Http(http::HttpInputConfig),
    // Kafka(kafka::KafkaInputConfig),
    Generate(generate::GenerateConfig),
    Memory(memory::MemoryInputConfig),
    Mqtt(mqtt::MqttInputConfig),
}

impl InputConfig {
    /// 根据配置构建输入组件
    pub fn build(&self) -> Result<Arc<dyn Input>, Error> {
        match self {
            InputConfig::File(config) => Ok(Arc::new(file::FileInput::new(config)?)),
            InputConfig::Http(config) => Ok(Arc::new(http::HttpInput::new(config)?)),
            // InputConfig::Kafka(config) => Ok(Arc::new(kafka::KafkaInput::new(config)?)),
            InputConfig::Memory(config) => Ok(Arc::new(memory::MemoryInput::new(config)?)),
            InputConfig::Mqtt(config) => Ok(Arc::new(mqtt::MqttInput::new(config)?)),
            InputConfig::Generate(config) => Ok(Arc::new(generate::GenerateInput::new(config.clone())?)),
        }
    }
}

