//! SQL处理器组件
//!
//! 使用DataFusion执行SQL查询处理数据，支持静态SQL和流式SQL

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, BooleanArray, Float64Array, NullArray, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::arrow;
use arrow_json;

use datafusion::common::SchemaExt;
use serde_json::Value;
use crate::{Error, Message, MessageBatch, processor::{ProcessorBatch}};

const DEFAULT_TABLE_NAME: &str = "flow";
/// SQL处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlProcessorConfig {
    /// SQL查询语句
    pub query: String,

    /// 表名（用于SQL查询中引用）
    pub table_name: Option<String>,
}

/// SQL处理器组件
pub struct SqlProcessor {
    config: SqlProcessorConfig,
}

impl SqlProcessor {
    /// 创建一个新的SQL处理器组件
    pub fn new(config: &SqlProcessorConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
        })
    }

    /// 将消息内容解析为DataFusion表
    async fn parse_input(&self, content: &str) -> Result<RecordBatch, Error> {
        self.parse_json_input(content).await
    }

    /// 解析JSON输入
    async fn parse_json_input(&self, content: &str) -> Result<RecordBatch, Error> {
        // 解析JSON内容
        let json_value: serde_json::Value = serde_json::from_str(content)
            .map_err(|e| Error::Processing(format!("JSON解析错误: {}", e)))?;

        // 处理不同的JSON结构
        match json_value {
            Value::Object(obj) => {
                // 单个对象转换为单行表
                let mut fields = Vec::new();
                let mut columns: Vec<ArrayRef> = Vec::new();

                // 提取所有字段和值
                for (key, value) in obj {
                    let mut field;
                    let mut array: ArrayRef;
                    match value {
                        Value::Null => {
                            field = Field::new(&key, DataType::Null, true);
                            array = Arc::new(NullArray::new(1));
                        }
                        Value::Bool(v) => {
                            field = Field::new(&key, DataType::Boolean, true);
                            array = Arc::new(BooleanArray::from(vec![v]));
                        }
                        Value::Number(v) => {
                            field = Field::new(&key, DataType::Float64, true);
                            if let Some(x) = v.as_f64() {
                                array = Arc::new(Float64Array::from(vec![x]));
                            } else {
                                array = Arc::new(Float64Array::from(vec![0f64]));
                            }
                        }
                        Value::String(v) => {
                            field = Field::new(&key, DataType::Utf8, true);
                            array = Arc::new(StringArray::from(vec![v]));
                        }
                        Value::Array(v) => {
                            field = Field::new(&key, DataType::Utf8, true);

                            if let Ok(x) = serde_json::to_string(&v) {
                                array = Arc::new(StringArray::from(vec![x]));
                            } else {
                                array = Arc::new(StringArray::from(vec!["[]".to_string()]));
                            }
                        }
                        Value::Object(v) => {
                            field = Field::new(&key, DataType::Utf8, true);
                            if let Ok(x) = serde_json::to_string(&v) {
                                array = Arc::new(StringArray::from(vec![x]));
                            } else {
                                array = Arc::new(StringArray::from(vec!["{}".to_string()]));
                            }
                        }
                    };
                    fields.push(field);
                    columns.push(array);
                }

                // 创建schema和记录批次
                let schema = Arc::new(Schema::new(fields));
                RecordBatch::try_new(schema, columns)
                    .map_err(|e| Error::Processing(format!("创建记录批次失败: {}", e)))
            }
            Value::Array(_) => {
                Err(Error::Processing("不支持JSON数组".to_string()))
            }
            _ => Err(Error::Processing("输入必须是JSON对象或数组".to_string())),
        }
    }


    /// 执行SQL查询
    async fn execute_query(&self, batch: RecordBatch) -> Result<RecordBatch, Error> {
        // 创建会话上下文
        let ctx = SessionContext::new();

        // 注册表
        let table_name = self.config.table_name.as_deref()
            .unwrap_or(DEFAULT_TABLE_NAME);

        ctx.register_batch(table_name, batch)
            .map_err(|e| Error::Processing(format!("注册表失败: {}", e)))?;

        // 执行SQL查询并收集结果
        let df = ctx.sql(&self.config.query).await
            .map_err(|e| Error::Processing(format!("SQL查询错误: {}", e)))?;

        let result_batches = df.collect().await
            .map_err(|e| Error::Processing(format!("收集查询结果错误: {}", e)))?;

        if result_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        Ok(result_batches[0].clone())
    }

    /// 将查询结果格式化为输出
    fn format_output(&self, batch: &RecordBatch) -> Result<Vec<Message>, Error> {
        let mut messages = Vec::with_capacity(batch.num_rows());

        // 使用Arrow的JSON序列化功能
        let mut buf = Vec::new();
        let mut writer = arrow_json::ArrayWriter::new(&mut buf);
        writer.write_batches(&[batch])
            .map_err(|e| Error::Processing(format!("Arrow JSON序列化错误: {}", e)))?;
        writer.finish()
            .map_err(|e| Error::Processing(format!("Arrow JSON序列化完成错误: {}", e)))?;

        // 解析JSON数组并转换为消息
        let json_array: Vec<serde_json::Value> = serde_json::from_slice(&buf)
            .map_err(|e| Error::Processing(format!("JSON解析错误: {}", e)))?;

        for json_obj in json_array {
            let json_str = serde_json::to_string(&json_obj)
                .map_err(|e| Error::Processing(format!("JSON序列化错误: {}", e)))?;
            messages.push(Message::new(json_str.as_bytes().to_vec()));
        }

        Ok(messages)
    }

    /// 合并多个记录批次
    fn combine_batches(&self, batches: &[RecordBatch]) -> Result<RecordBatch, Error> {
        if batches.is_empty() {
            return Err(Error::Processing("没有批次可合并".to_string()));
        }

        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }

        let schema = batches[0].schema();
        arrow::compute::concat_batches(&schema, batches)
            .map_err(|e| Error::Processing(format!("合并批次失败: {}", e)))
    }
}

#[async_trait]
impl ProcessorBatch for SqlProcessor {
    async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        // 如果批次为空，直接返回空结果
        if msg.is_empty() {
            return Ok(vec![]);
        }

        // 批量处理多条消息
        let mut input_batches = Vec::with_capacity(msg.len());

        // 解析所有消息为DataFusion表
        for message in msg.iter() {
            let content = message.as_string()?;
            let batch = self.parse_input(&content).await?;
            input_batches.push(batch);
        }

        // 合并所有输入批次
        let combined_input = self.combine_batches(&input_batches)?;

        // 执行SQL查询
        let result_batch = self.execute_query(combined_input).await?;

        // 格式化结果
        let result_messages = self.format_output(&result_batch)?;

        Ok(vec![result_messages.into()])
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}