//! Rust流处理引擎
//!
//! 这个基于Rust的流处理引擎参考了Benthos的设计理念，
//! 旨在提供一个高性能、可靠且易于扩展的数据流处理系统。

use std::fmt;
use std::ops::Deref;
use std::sync::Arc;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub mod buffer;
pub mod config;
pub mod input;
pub mod output;
pub mod pipeline;
pub mod processor;
pub mod stream;
pub mod metrics;
pub mod plugin;

/// 表示流处理引擎中的错误
#[derive(Error, Debug)]
pub enum Error {
    #[error("IO错误: {0}")]
    Io(#[from] std::io::Error),

    #[error("序列化错误: {0}")]
    Serialization(#[from] serde_json::Error),

    #[error("配置错误: {0}")]
    Config(String),

    #[error("处理错误: {0}")]
    Processing(String),

    #[error("连接错误: {0}")]
    Connection(String),

    #[error("超时错误")]
    Timeout,

    #[error("未知错误: {0}")]
    Unknown(String),
    #[error("完成")]
    Done,
}

/// 消息元数据，存储消息的附加信息
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Metadata {
    fields: std::collections::HashMap<String, String>,
}

impl Metadata {
    /// 创建一个新的空元数据
    pub fn new() -> Self {
        Self {
            fields: std::collections::HashMap::new(),
        }
    }

    /// 设置元数据字段
    pub fn set(&mut self, key: &str, value: &str) {
        self.fields.insert(key.to_string(), value.to_string());
    }

    /// 获取元数据字段
    pub fn get(&self, key: &str) -> Option<&String> {
        self.fields.get(key)
    }

    /// 删除元数据字段
    pub fn remove(&mut self, key: &str) -> Option<String> {
        self.fields.remove(key)
    }
}

/// 表示流处理引擎中的消息
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message {
    /// 消息内容
    content: Vec<u8>,
    /// 消息元数据
    metadata: Metadata,
}

impl Message {
    /// 创建一个新消息
    pub fn new(content: Vec<u8>) -> Self {
        Self {
            content,
            metadata: Metadata::new(),
        }
    }

    /// 从字符串创建消息
    pub fn from_string(content: &str) -> Self {
        Self::new(content.as_bytes().to_vec())
    }

    /// 从JSON值创建消息
    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let content = serde_json::to_vec(value)?;
        Ok(Self::new(content))
    }

    /// 获取消息内容
    pub fn content(&self) -> &[u8] {
        &self.content
    }

    /// 获取可变消息内容
    pub fn content_mut(&mut self) -> &mut Vec<u8> {
        &mut self.content
    }

    /// 设置消息内容
    pub fn set_content(&mut self, content: Vec<u8>) {
        self.content = content;
    }

    /// 获取消息元数据
    pub fn metadata(&self) -> &Metadata {
        &self.metadata
    }

    /// 获取可变消息元数据
    pub fn metadata_mut(&mut self) -> &mut Metadata {
        &mut self.metadata
    }

    /// 将消息内容解析为JSON
    pub fn json<'a, T>(&'a self) -> Result<T, Error>
    where
        T: Deserialize<'a>,
    {
        serde_json::from_slice(&self.content).map_err(Error::from)
    }

    /// 将消息内容解析为字符串
    pub fn as_string(&self) -> Result<String, Error> {
        String::from_utf8(self.content.clone())
            .map_err(|e| Error::Processing(format!("无效的UTF-8序列: {}", e)))
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_string() {
            Ok(s) => write!(f, "{}", s),
            Err(_) => write!(f, "<二进制数据: {} 字节>", self.content.len()),
        }
    }
}

pub struct MessageBatch(Vec<Message>);

impl MessageBatch {
    pub fn new(messages: Vec<Message>) -> Self {
        Self(messages)
    }
    pub fn new_single(message: Message) -> Self {
        Self(vec![message])
    }
}
impl From<Vec<Message>> for MessageBatch {
    fn from(messages: Vec<Message>) -> Self {
        Self(messages)
    }
}

impl From<MessageBatch> for Vec<Message> {
    fn from(batch: MessageBatch) -> Self {
        batch.0
    }
}

impl Deref for MessageBatch {
    type Target = Vec<Message>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}