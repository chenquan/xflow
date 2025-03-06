//! Redis缓冲区组件
//!
//! 在Redis中提供临时消息存储，支持分布式缓冲

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, MessageBatch, buffer::Buffer};
//
// /// Redis缓冲区配置
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct RedisBufferConfig {
//     /// Redis服务器地址
//     pub url: String,
//     /// 键前缀
//     pub key_prefix: String,
//     /// 最大缓冲区大小
//     pub max_size: usize,
//     /// 连接超时（毫秒）
//     pub timeout_ms: u64,
// }
//
// /// Redis缓冲区组件
// pub struct RedisBuffer {
//     config: RedisBufferConfig,
//     // 在实际实现中，这里应该有一个Redis客户端
//     // 例如：client: Option<redis::Client>,
//     connected: AtomicBool,
//     // 用于跟踪当前缓冲区大小
//     size: Mutex<usize>,
// }
//
// impl RedisBuffer {
//     /// 创建一个新的Redis缓冲区组件
//     pub fn new(config: &RedisBufferConfig) -> Result<Self, Error> {
//         Ok(Self {
//             config: config.clone(),
//             // client: None,
//             connected: AtomicBool::new(false),
//             size: Mutex::new(0),
//         })
//     }
//
//     /// 生成用于存储消息的键
//     fn generate_key(&self, index: usize) -> String {
//         format!("{}_msg_{}", self.config.key_prefix, index)
//     }
//
//     /// 生成用于存储读取索引的键
//     fn read_index_key(&self) -> String {
//         format!("{}_read_index", self.config.key_prefix)
//     }
//
//     /// 生成用于存储写入索引的键
//     fn write_index_key(&self) -> String {
//         format!("{}_write_index", self.config.key_prefix)
//     }
// }
//
// #[async_trait]
// impl Buffer for RedisBuffer {
//     async fn push(&self, msg: &Message) -> Result<(), Error> {
//         if !self.connected.load(Ordering::SeqCst) {
//             return Err(Error::Connection("缓冲区未连接".to_string()));
//         }
//
//         // 获取当前大小
//         let mut size = self.size.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//
//         // 检查是否已满
//         if *size >= self.config.max_size {
//             return Err(Error::Processing("Redis缓冲区已满".to_string()));
//         }
//
//         // 注意：这是一个模拟实现
//         // 在实际应用中，这里应该使用Redis客户端存储消息
//         // 例如：
//         /*
//         use redis::{Client, Commands};
//
//         let client = self.client.as_ref().unwrap();
//         let mut conn = client.get_connection()
//             .map_err(|e| Error::Connection(format!("无法获取Redis连接: {}", e)))?;
//
//         // 获取当前写入索引
//         let write_index: usize = conn.get(&self.write_index_key())
//             .unwrap_or(0);
//
//         // 序列化消息
//         let serialized = serde_json::to_string(msg)
//             .map_err(|e| Error::Serialization(e))?;
//
//         // 存储消息
//         let key = self.generate_key(write_index);
//         conn.set::<_, _, ()>(&key, &serialized)
//             .map_err(|e| Error::Processing(format!("Redis SET操作失败: {}", e)))?;
//
//         // 更新写入索引
//         conn.set::<_, _, ()>(&self.write_index_key(), write_index + 1)
//             .map_err(|e| Error::Processing(format!("Redis SET操作失败: {}", e)))?;
//         */
//
//         // 更新大小
//         *size += 1;
//
//         Ok(())
//     }
//
//     async fn pop(&self) -> Result<Option<Message>, Error> {
//         if !self.connected.load(Ordering::SeqCst) {
//             return Err(Error::Connection("缓冲区未连接".to_string()));
//         }
//
//         // 获取当前大小
//         let mut size = self.size.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//
//         // 检查是否为空
//         if *size == 0 {
//             return Ok(None);
//         }
//
//         // 注意：这是一个模拟实现
//         // 在实际应用中，这里应该使用Redis客户端获取消息
//         // 例如：
//         /*
//         use redis::{Client, Commands};
//
//         let client = self.client.as_ref().unwrap();
//         let mut conn = client.get_connection()
//             .map_err(|e| Error::Connection(format!("无法获取Redis连接: {}", e)))?;
//
//         // 获取当前读取索引
//         let read_index: usize = conn.get(&self.read_index_key())
//             .unwrap_or(0);
//
//         // 获取当前写入索引
//         let write_index: usize = conn.get(&self.write_index_key())
//             .unwrap_or(0);
//
//         // 检查是否有消息可读
//         if read_index >= write_index {
//             return Ok(None);
//         }
//
//         // 读取消息
//         let key = self.generate_key(read_index);
//         let serialized: Option<String> = conn.get(&key)
//             .map_err(|e| Error::Processing(format!("Redis GET操作失败: {}", e)))?;
//
//         // 如果没有找到消息，返回None
//         let serialized = match serialized {
//             Some(s) => s,
//             None => return Ok(None),
//         };
//
//         // 反序列化消息
//         let msg = serde_json::from_str(&serialized)
//             .map_err(|e| Error::Serialization(e))?;
//
//         // 删除已读取的消息
//         conn.del::<_, ()>(&key)
//             .map_err(|e| Error::Processing(format!("Redis DEL操作失败: {}", e)))?;
//
//         // 更新读取索引
//         conn.set::<_, _, ()>(&self.read_index_key(), read_index + 1)
//             .map_err(|e| Error::Processing(format!("Redis SET操作失败: {}", e)))?;
//         */
//
//         // 更新大小
//         *size -= 1;
//
//         // 模拟返回消息
//         Ok(Some(Message::from_string("模拟Redis缓冲区消息")))
//     }
//
//     async fn close(&self) -> Result<(), Error> {
//         self.connected.store(false, Ordering::SeqCst);
//         // self.client = None;
//
//         // 重置大小
//         let mut size = self.size.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//         *size = 0;
//
//         Ok(())
//     }
// }