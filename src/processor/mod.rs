//! 处理器组件模块
//!
//! 处理器组件负责对数据进行转换、过滤、丰富等操作。

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::{Error, MessageBatch};

pub mod batch;
pub mod json;
pub mod protobuf;
pub mod sql;
mod udf;

/// 处理器组件的特征接口
#[async_trait]
pub trait Processor: Send + Sync {
    /// 处理消息
    async fn process(&self, batch: MessageBatch) -> Result<Vec<MessageBatch>, Error>;

    /// 关闭处理器
    async fn close(&self) -> Result<(), Error>;
}

/// 处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ProcessorConfig {
    Batch(batch::BatchProcessorConfig),
    Sql(sql::SqlProcessorConfig),
    JsonToArrow,
    ArrowToJson,
    Protobuf(protobuf::ProtobufProcessorConfig),
}

impl ProcessorConfig {
    /// 根据配置构建处理器组件
    pub fn build(&self) -> Result<Arc<dyn Processor>, Error> {
        match self {
            ProcessorConfig::Batch(config) => Ok(Arc::new(batch::BatchProcessor::new(config)?)),
            ProcessorConfig::Sql(config) => Ok(Arc::new(sql::SqlProcessor::new(config)?)),
            ProcessorConfig::JsonToArrow => Ok(Arc::new(json::JsonToArrowProcessor)),
            ProcessorConfig::ArrowToJson => Ok(Arc::new(json::ArrowToJsonProcessor)),
            ProcessorConfig::Protobuf(config) => {
                Ok(Arc::new(protobuf::ProtobufProcessor::new(config)?))
            }
        }
    }
}
