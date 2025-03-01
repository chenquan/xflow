//! 过滤处理器组件
//!
//! 根据条件过滤消息

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, Message, processor::Processor};

/// 过滤处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FilterProcessorConfig {
    /// 过滤条件类型（content, metadata）
    pub condition_type: String,
    /// 过滤条件（如正则表达式或JSON路径）
    pub condition: String,
    /// 是否反转结果（即保留不匹配的消息）
    pub invert: bool,
}

/// 过滤处理器组件
pub struct FilterProcessor {
    config: FilterProcessorConfig,
}

impl FilterProcessor {
    /// 创建一个新的过滤处理器组件
    pub fn new(config: &FilterProcessorConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
        })
    }

    /// 检查消息是否匹配过滤条件
    fn matches(&self, msg: &Message) -> Result<bool, Error> {
        let matches = match self.config.condition_type.as_str() {
            "content" => {
                // 检查内容是否匹配条件
                // 这里使用简单的字符串包含检查，实际应用中可能需要更复杂的匹配逻辑
                let content = msg.as_string()?;
                content.contains(&self.config.condition)
            }
            "metadata" => {
                // 检查元数据是否匹配条件
                // 格式：key=value
                if let Some((key, value)) = self.config.condition.split_once('=') {
                    if let Some(meta_value) = msg.metadata().get(key) {
                        meta_value == value
                    } else {
                        false
                    }
                } else {
                    // 如果没有指定值，则检查键是否存在
                    msg.metadata().get(&self.config.condition).is_some()
                }
            }
            _ => return Err(Error::Config(format!("不支持的过滤条件类型: {}", self.config.condition_type))),
        };

        // 如果配置为反转结果，则取反
        Ok(if self.config.invert { !matches } else { matches })
    }
}

#[async_trait]
impl Processor for FilterProcessor {
    async fn process(&self, msg: Message) -> Result<Vec<Message>, Error> {
        // 检查消息是否匹配过滤条件
        if self.matches(&msg)? {
            // 如果匹配，则保留消息
            Ok(vec![msg])
        } else {
            // 如果不匹配，则丢弃消息
            Ok(vec![])
        }
    }

    async fn close(&self) -> Result<(), Error> {
        // 过滤处理器不需要特殊的关闭操作
        Ok(())
    }
}