//! Arrow处理器组件
//!
//! 用于在二进制数据和Arrow格式之间进行转换的处理器

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow;
use serde_json::Value;

use crate::{Error, Message, Content, MessageBatch};
use crate::processor::{ProcessorBatch};

/// Arrow格式转换处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonProcessorConfig {
    /// 转换模式: "to_arrow" 或 "from_arrow"
    pub mode: String,
}

/// Arrow格式转换处理器
pub struct JsonProcessor {
    config: JsonProcessorConfig,
}

impl JsonProcessor {
    /// 创建一个新的Arrow格式转换处理器
    pub fn new(config: &JsonProcessorConfig) -> Result<Self, Error> {
        // 验证配置
        match config.mode.as_str() {
            "to_arrow" | "from_arrow" => {}
            _ => return Err(Error::Config(format!("不支持的转换模式: {}", config.mode))),
        }

        Ok(Self {
            config: config.clone(),
        })
    }

    /// 将JSON转换为Arrow格式
    fn json_to_arrow(&self, content: &str) -> Result<RecordBatch, Error> {
        // 解析JSON内容
        let json_value: Value = serde_json::from_str(content)
            .map_err(|e| Error::Processing(format!("JSON解析错误: {}", e)))?;

        match json_value {
            Value::Object(obj) => {
                // 单个对象转换为单行表
                let mut fields = Vec::new();
                let mut columns: Vec<ArrayRef> = Vec::new();

                // 提取所有字段和值
                for (key, value) in obj {
                    match value {
                        Value::Null => {
                            fields.push(Field::new(&key, DataType::Null, true));
                            // 空值列处理
                            columns.push(Arc::new(NullArray::new(1)));
                        }
                        Value::Bool(v) => {
                            fields.push(Field::new(&key, DataType::Boolean, false));
                            columns.push(Arc::new(BooleanArray::from(vec![v])));
                        }
                        Value::Number(v) => {
                            if v.is_i64() {
                                fields.push(Field::new(&key, DataType::Int64, false));
                                columns.push(Arc::new(Int64Array::from(vec![v.as_i64().unwrap()])));
                            } else if v.is_u64() {
                                fields.push(Field::new(&key, DataType::UInt64, false));
                                columns.push(Arc::new(UInt64Array::from(vec![v.as_u64().unwrap()])));
                            } else {
                                fields.push(Field::new(&key, DataType::Float64, false));
                                columns.push(Arc::new(Float64Array::from(vec![v.as_f64().unwrap_or(0.0)])));
                            }
                        }
                        Value::String(v) => {
                            fields.push(Field::new(&key, DataType::Utf8, false));
                            columns.push(Arc::new(StringArray::from(vec![v])));
                        }
                        Value::Array(v) => {
                            fields.push(Field::new(&key, DataType::Utf8, false));
                            if let Ok(x) = serde_json::to_string(&v) {
                                columns.push(Arc::new(StringArray::from(vec![x])));
                            } else {
                                columns.push(Arc::new(StringArray::from(vec!["[]".to_string()])));
                            }
                        }
                        Value::Object(v) => {
                            fields.push(Field::new(&key, DataType::Utf8, false));
                            if let Ok(x) = serde_json::to_string(&v) {
                                columns.push(Arc::new(StringArray::from(vec![x])));
                            } else {
                                columns.push(Arc::new(StringArray::from(vec!["{}".to_string()])));
                            }
                        }
                    };
                }

                // 创建schema和记录批次
                let schema = Arc::new(Schema::new(fields));
                RecordBatch::try_new(schema, columns)
                    .map_err(|e| Error::Processing(format!("创建Arrow记录批次失败: {}", e)))
            }
            _ => Err(Error::Processing("输入必须是JSON对象".to_string())),
        }
    }

    /// 将Arrow格式转换为JSON
    fn arrow_to_json(&self, batch: &RecordBatch) -> Result<Vec<u8>, Error> {
        // 使用Arrow的JSON序列化功能
        let mut buf = Vec::new();
        let mut writer = arrow::json::ArrayWriter::new(&mut buf);
        writer.write(batch)
            .map_err(|e| Error::Processing(format!("Arrow JSON序列化错误: {}", e)))?;
        writer.finish()
            .map_err(|e| Error::Processing(format!("Arrow JSON序列化完成错误: {}", e)))?;

        Ok(buf)
    }
}
#[async_trait]
impl ProcessorBatch for JsonProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let mut new_msg_batch = vec![];
        for msg in msg_batch.0 {
            match self.config.mode.as_str() {
                "to_arrow" => {
                    // 将二进制/JSON数据转换为Arrow格式
                    match &msg.content {
                        Content::Arrow(_) => {
                            // 已经是Arrow格式，直接返回
                            new_msg_batch.push(msg);
                        }
                        Content::Binary(data) => {
                            // 尝试将二进制数据解析为JSON，然后转换为Arrow
                            let json_str = String::from_utf8(data.clone())
                                .map_err(|e| Error::Processing(format!("无效的UTF-8序列: {}", e)))?;

                            let arrow_batch = self.json_to_arrow(&json_str)?;

                            // 创建新消息，保留原始元数据
                            let mut new_msg = Message::new_arrow(arrow_batch);
                            *new_msg.metadata_mut() = msg.metadata().clone();
                            new_msg_batch.push(new_msg);
                        }
                    }
                }
                "from_arrow" => {
                    // 将Arrow格式转换为JSON
                    match &msg.content {
                        Content::Arrow(batch) => {
                            let json_data = self.arrow_to_json(batch)?;

                            // 创建新消息，保留原始元数据
                            let mut new_msg = Message::new_binary(json_data);
                            *new_msg.metadata_mut() = msg.metadata().clone();

                            new_msg_batch.push(new_msg);
                        }
                        Content::Binary(_) => {
                            // 已经是二进制格式，直接返回
                            new_msg_batch.push(msg);
                        }
                    }
                }
                _ => {
                    return Err(Error::Processing(format!("无效的转换模式: {}", self.config.mode)));
                }
            };
        }
        Ok(vec![new_msg_batch.into()])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

// #[async_trait]
// impl Processor for JsonProcessor {
//     async fn process(&self, msg: Message) -> Result<Vec<Message>, Error> {}
//
//     async fn close(&self) -> Result<(), Error> {
//         // 无需特殊清理
//         Ok(())
//     }
// }