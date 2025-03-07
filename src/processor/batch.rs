//! 批处理器组件
//!
//! 将多个消息批量处理为一个或多个消息

use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, RwLock};
use crate::{Error, MessageBatch, processor::Processor, Content};

/// 批处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchProcessorConfig {
    /// 批处理大小
    pub count: usize,
    /// 批处理超时（毫秒）
    pub timeout_ms: u64,
    /// 批处理处理器（可选，如特定字段匹配）
    pub data_type: String,
}

/// 批处理器组件
pub struct BatchProcessor {
    config: BatchProcessorConfig,
    batch: Arc<RwLock<Vec<MessageBatch>>>,
    last_batch_time: Arc<Mutex<std::time::Instant>>,
}

impl BatchProcessor {
    /// 创建一个新的批处理器组件
    pub fn new(config: &BatchProcessorConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            batch: Arc::new(RwLock::new(Vec::with_capacity(config.count))),
            last_batch_time: Arc::new(Mutex::new(std::time::Instant::now())),
        })
    }

    /// 检查是否应该刷新批处理
    async fn should_flush(&self) -> bool {
        // 如果批处理已满，则刷新
        let batch = self.batch.read().await;
        if batch.len() >= self.config.count {
            return true;
        }
        let last_batch_time = self.last_batch_time.lock().await;
        // 如果超过超时时间且批处理不为空，则刷新
        if !batch.is_empty() && last_batch_time.elapsed().as_millis() >= self.config.timeout_ms as u128 {
            return true;
        }

        false
    }

    /// 刷新批处理
    async fn flush(&self) -> Result<Vec<MessageBatch>, Error> {
        let mut batch = self.batch.write().await;

        if batch.is_empty() {
            return Ok(vec![]);
        }

        // 创建一个新的批处理消息
        let new_batch = match self.config.data_type.as_str() {
            "arrow" => {
                let mut combined_content = Vec::new();

                for msg in batch.iter() {
                    if let Content::Arrow(v) = &msg.content {
                        combined_content.push(v.clone());
                    }
                }
                let schema = combined_content[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &combined_content)
                    .map_err(|e| Error::Processing(format!("合并批次失败: {}", e)))?;
                Ok(vec![MessageBatch::new_arrow(batch)])
            }
            "binary" => {
                let mut combined_content = Vec::new();

                for msg in batch.iter() {
                    if let Content::Binary(v) = &msg.content {
                        combined_content.extend(v.clone());
                    }
                }
                Ok(vec![MessageBatch::new_binary(combined_content)])
            }
            _ => {
                Err(Error::Processing("Invalid data type".to_string()))
            }
        };

        batch.clear();
        let mut last_batch_time = self.last_batch_time.lock().await;

        *last_batch_time = std::time::Instant::now();

        new_batch
    }
}

#[async_trait]
impl Processor for BatchProcessor {
    async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        match &msg.content {
            Content::Arrow(_) => {
                if self.config.data_type != "arrow" {
                    return Err(Error::Processing("Invalid data type".to_string()));
                }
            }
            Content::Binary(_) => {
                if self.config.data_type != "binary" {
                    return Err(Error::Processing("Invalid data type".to_string()));
                }
            }
        }

        {
            let mut batch = self.batch.write().await;

            // 添加消息到批处理
            batch.push(msg);
        }

        // 检查是否应该刷新批处理
        if self.should_flush().await {
            self.flush().await
        } else {
            // 如果不刷新，则返回空结果
            Ok(vec![])
        }
    }

    async fn close(&self) -> Result<(), Error> {
        let mut batch = self.batch.write().await;

        batch.clear();
        Ok(())
    }
}