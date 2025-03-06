//! 内存缓冲区组件
//!
//! 在内存中提供临时消息存储

use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, MessageBatch, buffer::Buffer};
//
// /// 内存缓冲区配置
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct MemoryBufferConfig {
//     /// 缓冲区最大容量
//     pub max_size: usize,
// }
//
// /// 内存缓冲区组件
// pub struct MemoryBuffer {
//     config: MemoryBufferConfig,
//     queue: Mutex<VecDeque<Message>>,
// }
//
// impl MemoryBuffer {
//     /// 创建一个新的内存缓冲区组件
//     pub fn new(config: &MemoryBufferConfig) -> Result<Self, Error> {
//         Ok(Self {
//             config: config.clone(),
//             queue: Mutex::new(VecDeque::with_capacity(config.max_size)),
//         })
//     }
// }
//
// #[async_trait]
// impl Buffer for MemoryBuffer {
//     async fn push(&self, msg: &Message) -> Result<(), Error> {
//         let mut queue = self.queue.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//
//         // 检查队列是否已满
//         if queue.len() >= self.config.max_size {
//             return Err(Error::Processing("内存缓冲区已满".to_string()));
//         }
//
//         queue.push_back(msg.clone());
//         Ok(())
//     }
//
//     async fn pop(&self) -> Result<Option<Message>, Error> {
//         let mut queue = self.queue.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//         Ok(queue.pop_front())
//     }
//
//     async fn close(&self) -> Result<(), Error> {
//         let mut queue = self.queue.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//         queue.clear();
//         Ok(())
//     }
// }