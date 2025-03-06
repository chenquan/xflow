//! 批处理器组件
//!
//! 将多个消息批量处理为一个或多个消息

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use crate::{Error, MessageBatch, processor::Processor};

/// 批处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchProcessorConfig {
    /// 批处理大小
    pub count: usize,
    /// 批处理超时（毫秒）
    pub timeout_ms: u64,
    /// 批处理条件（可选，如特定字段匹配）
    pub condition: Option<String>,
}

/// 批处理器组件
pub struct BatchProcessor {
    config: BatchProcessorConfig,
    batch: Arc<Mutex<Vec<MessageBatch>>>,
    last_batch_time: Arc<Mutex<std::time::Instant>>,
}

// impl BatchProcessor {
//     /// 创建一个新的批处理器组件
//     pub fn new(config: &BatchProcessorConfig) -> Result<Self, Error> {
//         Ok(Self {
//             config: config.clone(),
//             batch: Arc::new(Mutex::new(Vec::with_capacity(config.count))),
//             last_batch_time: Arc::new(Mutex::new(std::time::Instant::now())),
//         })
//     }
//
//     /// 检查是否应该刷新批处理
//     async fn should_flush(&self) -> bool {
//         // 如果批处理已满，则刷新
//         let batch = self.batch.lock().await;
//         if batch.len() >= self.config.count {
//             return true;
//         }
//         let last_batch_time = self.last_batch_time.lock().await;
//         // 如果超过超时时间且批处理不为空，则刷新
//         if !batch.is_empty() && last_batch_time.elapsed().as_millis() >= self.config.timeout_ms as u128 {
//             return true;
//         }
//
//         false
//     }
//
//     /// 刷新批处理
//     async fn flush(&self) -> Result<Vec<Message>, Error> {
//         let mut batch = self.batch.lock().await;
//
//         if batch.is_empty() {
//             return Ok(vec![]);
//         }
//
//         // 创建一个新的批处理消息
//         let mut combined_content = Vec::new();
//
//         // 合并所有消息内容
//         for msg in batch.iter() {
//             combined_content.extend_from_slice(msg.content());
//             combined_content.push(b'\n'); // 添加换行符分隔
//         }
//
//         // 创建批处理消息
//         let mut batch_msg = Message::new(combined_content);
//
//         // 添加批处理元数据
//         let metadata = batch_msg.metadata_mut();
//         metadata.set("batch_size", &batch.len().to_string());
//
//         // 清空批处理并重置时间
//         let result = vec![batch_msg];
//         batch.clear();
//         let  mut last_batch_time = self.last_batch_time.lock().await;
//
//         *last_batch_time = std::time::Instant::now();
//
//         Ok(result)
//     }
// }

// #[async_trait]
// impl Processor for BatchProcessor {
//     async fn process(&self, msg: Message) -> Result<Vec<Message>, Error> {
//         let mut batch = self.batch.lock().await;
//
//         // 添加消息到批处理
//         batch.push(msg);
//
//         // 检查是否应该刷新批处理
//         if self.should_flush().await {
//             self.flush().await
//         } else {
//             // 如果不刷新，则返回空结果
//             Ok(vec![])
//         }
//     }
//
//     async fn close(&self) -> Result<(), Error> {
//         let mut batch = self.batch.lock().await;
//
//         batch.clear();
//         Ok(())
//     }
// }