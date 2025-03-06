//! 内存输入组件
//!
//! 从内存中的消息队列读取数据

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, MessageBatch, input::Input};
use crate::input::Ack;
use crate::input::NoopAck;

/// 内存输入配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInputConfig {
    /// 内存队列的初始消息
    pub messages: Option<Vec<String>>,
}

/// 内存输入组件
pub struct MemoryInput {
    queue: Arc<Mutex<VecDeque<MessageBatch>>>,
    connected: AtomicBool,
}

impl MemoryInput {
    /// 创建一个新的内存输入组件
    pub fn new(config: &MemoryInputConfig) -> Result<Self, Error> {
        let mut queue = VecDeque::new();

        // 如果配置中有初始消息，则添加到队列中
        if let Some(messages) = &config.messages {
            for msg_str in messages {
                queue.push_back(MessageBatch::from_string(msg_str));
            }
        }

        Ok(Self {
            queue: Arc::new(Mutex::new(queue)),
            connected: AtomicBool::new(false),
        })
    }

    /// 向内存输入添加消息
    pub async fn push(&self, msg: MessageBatch) -> Result<(), Error> {
        let mut queue = self.queue.lock().await;
        queue.push_back(msg);
        Ok(())
    }
}

// #[async_trait]
// impl Input for MemoryInput {
//     async fn connect(&self) -> Result<(), Error> {
//         self.connected.store(true, std::sync::atomic::Ordering::SeqCst);
//         Ok(())
//     }
//
//     async fn read(&self) -> Result<(Message, Arc<dyn Ack>), Error> {
//         if !self.connected.load(std::sync::atomic::Ordering::SeqCst) {
//             return Err(Error::Connection("输入未连接".to_string()));
//         }
//
//         // 尝试从队列中获取消息
//         let msg_option;
//         {
//             let mut queue = self.queue.lock().await;
//             msg_option = queue.pop_front();
//         }
//
//         if let Some(msg) = msg_option {
//             Ok((msg, Arc::new(NoopAck)))
//         } else {
//             Err(Error::Done)
//         }
//     }
//
//
//     async fn close(&self) -> Result<(), Error> {
//         self.connected.store(false, std::sync::atomic::Ordering::SeqCst);
//         Ok(())
//     }
// }
