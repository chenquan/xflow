//! 内存输入组件
//!
//! 从内存中的消息队列读取数据

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use tokio::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, Message, input::Input};

/// 内存输入配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryInputConfig {
    /// 内存队列的初始消息
    pub messages: Option<Vec<String>>,
}

/// 内存输入组件
pub struct MemoryInput {
    queue: Arc<Mutex<VecDeque<Message>>>,
    connected: AtomicBool,
}

impl MemoryInput {
    /// 创建一个新的内存输入组件
    pub fn new(config: &MemoryInputConfig) -> Result<Self, Error> {
        let mut queue = VecDeque::new();

        // 如果配置中有初始消息，则添加到队列中
        if let Some(messages) = &config.messages {
            for msg_str in messages {
                queue.push_back(Message::from_string(msg_str));
            }
        }

        Ok(Self {
            queue: Arc::new(Mutex::new(queue)),
            connected: AtomicBool::new(false),
        })
    }

    /// 向内存输入添加消息
    pub async fn push(&self, msg: Message) -> Result<(), Error> {
        let mut queue = self.queue.lock().await;
        queue.push_back(msg);
        Ok(())
    }
}

#[async_trait]
impl Input for MemoryInput {
    async fn connect(&self) -> Result<(), Error> {
        self.connected.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn read(&self) -> Result<Message, Error> {
        if !self.connected.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Connection("输入未连接".to_string()));
        }

        // 尝试从队列中获取消息
        let msg_option;
        {
            let mut queue = self.queue.lock().await;
            msg_option = queue.pop_front();
        }

        if let Some(msg) = msg_option {
            Ok(msg)
        } else {
            // 如果队列为空，则等待一段时间后返回错误
            // 在实际应用中，这里可能需要实现更复杂的等待机制
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            Err(Error::Done)
        }
    }

    async fn acknowledge(&self, _msg: &Message) -> Result<(), Error> {
        // 内存输入不需要确认机制
        Ok(())
    }

  async  fn close(&self) -> Result<(), Error> {
        self.connected.store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}