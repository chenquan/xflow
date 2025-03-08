//! 输出组件模块
//!
//! 输出组件负责将处理后的数据发送到目标系统。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{Error, MessageBatch};

pub mod file;
pub mod http;
pub mod kafka;
pub mod mqtt;
pub mod redis;
pub mod stdout;

/// 输出组件的特征接口
#[async_trait]
pub trait Output: Send + Sync {
    /// 连接到输出目标
    async fn connect(&self) -> Result<(), Error>;

    /// 向输出目标写入消息
    async fn write(&self, msg: &MessageBatch) -> Result<(), Error>;

    /// 关闭输出目标连接
    async fn close(&self) -> Result<(), Error>;
}

/// 输出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OutputConfig {
    File(file::FileOutputConfig),
    Http(http::HttpOutputConfig),
    Kafka(kafka::KafkaOutputConfig),
    Mqtt(mqtt::MqttOutputConfig),
    // Redis(redis::RedisOutputConfig),
    Stdout(stdout::StdoutOutputConfig),
}

impl OutputConfig {
    /// 根据配置构建输出组件
    pub fn build(&self) -> Result<Arc<dyn Output>, Error> {
        match self {
            OutputConfig::File(config) => Ok(Arc::new(file::FileOutput::new(config)?)),
            OutputConfig::Http(config) => Ok(Arc::new(http::HttpOutput::new(config)?)),
            OutputConfig::Kafka(config) => Ok(Arc::new(kafka::KafkaOutput::new(config)?)),
            OutputConfig::Mqtt(config) => Ok(Arc::new(mqtt::MqttOutput::new(config)?)),
            // OutputConfig::Redis(config) => Ok(Arc::new(redis::RedisOutput::new(config)?)),
            OutputConfig::Stdout(config) => Ok(Arc::new(stdout::StdoutOutput::new(config)?)),
        }
    }
}
