//! JSON处理器组件
//!
//! 处理JSON格式的数据

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::{Error, Message, processor::Processor};

/// JSON处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonProcessorConfig {
    /// 操作类型（parse, format, query, set）
    pub operation: String,
    /// JSON路径（用于query和set操作）
    pub path: Option<String>,
    /// 设置值（用于set操作）
    pub value: Option<String>,
    /// 目标字段（可选，用于将结果存储到特定字段）
    pub target: Option<String>,
}

/// JSON处理器组件
pub struct JsonProcessor {
    config: JsonProcessorConfig,
}

impl JsonProcessor {
    /// 创建一个新的JSON处理器组件
    pub fn new(config: &JsonProcessorConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
        })
    }

    /// 解析JSON字符串
    fn parse_json(&self, content: &str) -> Result<Value, Error> {
        serde_json::from_str(content).map_err(Error::from)
    }

    /// 格式化JSON值为字符串
    fn format_json(&self, value: &Value) -> Result<String, Error> {
        serde_json::to_string(value).map_err(Error::from)
    }

    /// 查询JSON值
    fn query_json(&self, value: &Value, path: &str) -> Result<Value, Error> {
        // 简单的路径查询实现
        // 在实际应用中，可能需要更复杂的JSON路径查询库
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = value;

        for part in parts {
            if let Value::Object(map) = current {
                if let Some(next) = map.get(part) {
                    current = next;
                } else {
                    return Err(Error::Processing(format!("JSON路径不存在: {}", path)));
                }
            } else if let Value::Array(arr) = current {
                if let Ok(index) = part.parse::<usize>() {
                    if index < arr.len() {
                        current = &arr[index];
                    } else {
                        return Err(Error::Processing(format!("JSON数组索引越界: {}", index)));
                    }
                } else {
                    return Err(Error::Processing(format!("无效的JSON数组索引: {}", part)));
                }
            } else {
                return Err(Error::Processing(format!("无法在非对象或数组类型上使用路径: {}", path)));
            }
        }

        Ok(current.clone())
    }

    /// 设置JSON值
    fn set_json(&self, mut value: Value, path: &str, new_value: Value) -> Result<Value, Error> {
        // 简单的路径设置实现
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = &mut value;

        for (i, part) in parts.iter().enumerate() {
            if i == parts.len() - 1 {
                // 最后一部分，设置值
                if let Value::Object(map) = current {
                    map.insert(part.to_string(), new_value.clone());
                } else if let Value::Array(arr) = current {
                    if let Ok(index) = part.parse::<usize>() {
                        if index < arr.len() {
                            arr[index] = new_value.clone();
                        } else {
                            return Err(Error::Processing(format!("JSON数组索引越界: {}", index)));
                        }
                    } else {
                        return Err(Error::Processing(format!("无效的JSON数组索引: {}", part)));
                    }
                } else {
                    return Err(Error::Processing(format!("无法在非对象或数组类型上设置值: {}", path)));
                }
            } else {
                // 中间路径，导航到下一级
                if let Value::Object(map) = current {
                    if !map.contains_key(*part) {
                        map.insert(part.to_string(), Value::Object(serde_json::Map::new()));
                    }
                    current = map.get_mut(*part).unwrap();
                } else if let Value::Array(arr) = current {
                    if let Ok(index) = part.parse::<usize>() {
                        if index < arr.len() {
                            current = &mut arr[index];
                        } else {
                            return Err(Error::Processing(format!("JSON数组索引越界: {}", index)));
                        }
                    } else {
                        return Err(Error::Processing(format!("无效的JSON数组索引: {}", part)));
                    }
                } else {
                    return Err(Error::Processing(format!("无法在非对象或数组类型上导航: {}", path)));
                }
            }
        }

        Ok(value)
    }
}

#[async_trait]
impl Processor for JsonProcessor {
    async fn process(&self, mut msg: Message) -> Result<Vec<Message>, Error> {
        // 获取消息内容
        let content = msg.as_string()?;

        match self.config.operation.as_str() {
            "parse" => {
                // 解析JSON字符串
                let json_value = self.parse_json(&content)?;

                // 如果指定了目标字段，则将解析结果添加到元数据
                if let Some(target) = &self.config.target {
                    let json_str = self.format_json(&json_value)?;
                    msg.metadata_mut().set(target, &json_str);
                } else {
                    // 否则，将解析结果设置为消息内容
                    let json_bytes = serde_json::to_vec(&json_value).map_err(Error::from)?;
                    msg.set_content(json_bytes);
                }
            }
            "format" => {
                // 解析当前内容为JSON
                let json_value = self.parse_json(&content)?;

                // 格式化为字符串
                let formatted = self.format_json(&json_value)?;

                // 设置为新内容
                msg.set_content(formatted.into_bytes());
            }
            "query" => {
                // 需要路径参数
                let path = self.config.path.as_ref()
                    .ok_or_else(|| Error::Config("JSON查询操作需要指定path参数".to_string()))?;

                // 解析当前内容为JSON
                let json_value = self.parse_json(&content)?;

                // 查询JSON
                let result = self.query_json(&json_value, path)?;

                // 如果指定了目标字段，则将查询结果添加到元数据
                if let Some(target) = &self.config.target {
                    let result_str = self.format_json(&result)?;
                    msg.metadata_mut().set(target, &result_str);
                } else {
                    // 否则，将查询结果设置为消息内容
                    let result_bytes = serde_json::to_vec(&result).map_err(Error::from)?;
                    msg.set_content(result_bytes);
                }
            }
            "set" => {
                // 需要路径和值参数
                let path = self.config.path.as_ref()
                    .ok_or_else(|| Error::Config("JSON设置操作需要指定path参数".to_string()))?;
                let value_str = self.config.value.as_ref()
                    .ok_or_else(|| Error::Config("JSON设置操作需要指定value参数".to_string()))?;

                // 解析当前内容为JSON
                let json_value = self.parse_json(&content)?;

                // 解析要设置的值
                let new_value = self.parse_json(value_str)?;

                // 设置JSON值
                let result = self.set_json(json_value, path, new_value)?;

                // 将结果设置为消息内容
                let result_bytes = serde_json::to_vec(&result).map_err(Error::from)?;
                msg.set_content(result_bytes);
            }
            _ => return Err(Error::Config(format!("不支持的JSON操作类型: {}", self.config.operation))),
        }

        Ok(vec![msg])
    }

    async fn close(&self) -> Result<(), Error> {
        // JSON处理器不需要特殊的关闭操作
        Ok(())
    }
}