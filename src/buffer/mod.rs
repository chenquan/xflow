//! 缓冲区组件模块
//!
//! 缓冲区组件在处理过程中提供临时存储，增强系统的弹性和性能。

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, Message};

pub mod memory;
pub mod disk;
pub mod redis;

/// 缓冲区组件的特征接口
#[async_trait]
pub trait Buffer: Send + Sync {
    /// 将消息推入缓冲区
    async fn push(&self, msg: &Message) -> Result<(), Error>;

    /// 从缓冲区弹出消息
    async fn pop(&self) -> Result<Option<Message>, Error>;

    /// 关闭缓冲区
    async fn close(&self) -> Result<(), Error>;
}

/// 缓冲区配置
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum BufferConfig {
    // Memory(memory::MemoryBufferConfig),
    // Disk(disk::DiskBufferConfig),
    // Redis(redis::RedisBufferConfig),
}

// impl BufferConfig {
//     /// 根据配置构建缓冲区组件
//     pub fn build(&self) -> Result<Arc<dyn Buffer>, Error> {
//         match self {
//             BufferConfig::Memory(config) => Ok(Arc::new(memory::MemoryBuffer::new(config)?)),
//             BufferConfig::Disk(config) => Ok(Arc::new(disk::DiskBuffer::new(config)?)),
//             BufferConfig::Redis(config) => Ok(Arc::new(redis::RedisBuffer::new(config)?)),
//         }
//     }
// }