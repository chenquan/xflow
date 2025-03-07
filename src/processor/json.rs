//! Arrow处理器组件
//!
//! 用于在二进制数据和Arrow格式之间进行转换的处理器

use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, Int64Array, NullArray, StringArray, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use serde_json::Value;
use std::sync::Arc;

use crate::processor::Processor;
use crate::{Bytes, Content, Error, MessageBatch};

/// Arrow格式转换处理器配置

/// Arrow格式转换处理器
pub struct JsonProcessor {}

impl JsonProcessor {
    /// 创建一个新的Arrow格式转换处理器
    pub fn new() -> Result<Self, Error> {
        Ok(Self {})
    }

    /// 将JSON转换为Arrow格式
    fn json_to_arrow(&self, content: &Bytes) -> Result<RecordBatch, Error> {
        // 解析JSON内容
        let json_value: Value = serde_json::from_slice(content)
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
impl Processor for JsonProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        match msg_batch.content {
            Content::Arrow(v) => {
                let json_data = self.arrow_to_json(&v)?;
                Ok(vec![MessageBatch::new_binary(vec![json_data])])
            }
            Content::Binary(v) => {
                let mut batches = Vec::with_capacity(v.len());
                for x in v {
                    let record_batch = self.json_to_arrow(&x)?;
                    batches.push(record_batch)
                }
                if batches.is_empty() {
                    return Ok(vec![]);
                }

                let schema = batches[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| Error::Processing(format!("合并批次失败: {}", e)))?;
                Ok(vec![MessageBatch::new_arrow(batch)])
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

