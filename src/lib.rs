//! Rust流处理引擎

use std::any::Any;
use std::fmt;
use std::fmt::Formatter;
use std::ops::Deref;
use datafusion::arrow::record_batch::RecordBatch;
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
#[derive(Clone, Debug)]
pub struct Message {
    /// 消息内容
    content: Content,
    /// 消息元数据
    metadata: Metadata,
}
#[derive(Clone, Debug)]
pub enum Content {
    Arrow(RecordBatch),
    Binary(Vec<u8>),
}

impl Message {
    pub fn new_binary(content: Vec<u8>) -> Self {
        Self {
            content: Content::Binary(content),
            metadata: Metadata::new(),
        }
    }
    pub fn new_arrow(content: RecordBatch) -> Self {
        assert!(content.num_rows() <= 1);
        Self {
            content: Content::Arrow(content),
            metadata: Metadata::new(),
        }
    }

    /// 从字符串创建消息
    pub fn from_string(content: &str) -> Self {
        Self::new_binary(content.as_bytes().to_vec())
    }

    /// 从JSON值创建消息
    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let content = serde_json::to_vec(value)?;
        Ok(Self::new_binary(content))
    }

    /// 获取消息内容
    pub fn content(&self) -> Result<&[u8], Error> {
        match &self.content {
            Content::Arrow(v_) => {
                Err(Error::Processing("Not serialized messages".to_string()))
            }
            Content::Binary(v) => {
                Ok(v)
            }
        }
    }


    /// 设置消息内容
    pub fn set_content(&mut self, content: Content) {
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


    /// 将消息内容解析为字符串
    pub fn as_string(&self) -> Result<String, Error> {
        match &self.content {
            Content::Arrow(v) => {
                Err(Error::Processing("无法解析为JSON".to_string()))
            }
            Content::Binary(v) => {
                String::from_utf8(v.clone())
                    .map_err(|e| Error::Processing(format!("无效的UTF-8序列: {}", e)))
            }
        }
    }
}

impl fmt::Display for Message {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self.as_string() {
            Ok(s) => write!(f, "{}", s),
            Err(_) => write!(f, "<二进制数据: {:?} 字节>", self.content),
        }
    }
}

#[derive(Clone, Debug)]
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

impl fmt::Display for MessageBatch {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let mut first = true;
        write!(f, "[")?;
        for x in &self.0 {
            if !first {
                write!(f, ", ")?; // 添加分隔符
            }
            first = false;
            write!(f, "{}", x)?; // 格式化每个元素
        }
        write!(f, "]")?;
        Ok(())
    }
}