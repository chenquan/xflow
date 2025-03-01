//! 处理器组件模块
//!
//! 处理器组件负责对数据进行转换、过滤、丰富等操作。

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, Message};

pub mod batch;
pub mod filter;
pub mod grok;
pub mod json;

/// 处理器组件的特征接口
#[async_trait]
pub trait Processor: Send + Sync {
    /// 处理消息
    async fn process(&self, msg: Message) -> Result<Vec<Message>, Error>;

    /// 关闭处理器
    async fn close(&self) -> Result<(), Error>;
}

/// 处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ProcessorConfig {
    Batch(batch::BatchProcessorConfig),
    Filter(filter::FilterProcessorConfig),
    Grok(grok::GrokProcessorConfig),
    Json(json::JsonProcessorConfig),
}

impl ProcessorConfig {
    /// 根据配置构建处理器组件
    pub fn build(&self) -> Result<Arc<dyn Processor>, Error> {
        match self {
            ProcessorConfig::Batch(config) => Ok(Arc::new(batch::BatchProcessor::new(config)?)),
            ProcessorConfig::Filter(config) => Ok(Arc::new(filter::FilterProcessor::new(config)?)),
            ProcessorConfig::Grok(config) => Ok(Arc::new(grok::GrokProcessor::new(config)?)),
            ProcessorConfig::Json(config) => Ok(Arc::new(json::JsonProcessor::new(config)?)),
        }
    }
}