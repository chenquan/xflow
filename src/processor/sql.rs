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
use datafusion::common::SchemaExt;
use serde_json::Value;
use crate::{Error, Message, MessageBatch, processor::{ProcessorBatch}};

/// 窗口类型
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum WindowType {
    /// 滚动窗口（固定大小，不重叠）
    Tumbling,
    /// 滑动窗口（固定大小，可重叠）
    Sliding,
    /// 会话窗口（由不活动间隔定义）
    Session,
}

/// 窗口配置

/// SQL处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlProcessorConfig {
    /// SQL查询语句
    pub query: String,

    /// 表名（用于SQL查询中引用）
    pub table_name: String,

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
        ctx.register_batch(&self.config.table_name, batch)
            .map_err(|e| Error::Processing(format!("注册表失败: {}", e)))?;

        // 执行SQL查询
        let df = ctx.sql(&self.config.query).await
            .map_err(|e| Error::Processing(format!("SQL查询错误: {}", e)))?;

        // 收集结果
        let result_batches = df.collect().await
            .map_err(|e| Error::Processing(format!("收集查询结果错误: {}", e)))?;

        if result_batches.is_empty() {
            return Ok(RecordBatch::new_empty(Arc::new(Schema::empty())));
        }

        Ok(result_batches[0].clone())
    }

    /// 将查询结果格式化为输出
    fn format_output(&self, batch: &RecordBatch) -> Result<Vec<Message>, Error> {
        self.format_json_output(batch)
    }

    /// 格式化为JSON输出
    fn format_json_output(&self, batch: &RecordBatch) -> Result<Vec<Message>, Error> {
        let schema = batch.schema();
        let mut result = Vec::new();

        // 遍历每一行
        for row_idx in 0..batch.num_rows() {
            let mut row_obj = serde_json::Map::new();

            // 遍历每一列
            for col_idx in 0..batch.num_columns() {
                let column = batch.column(col_idx);
                let field_name = schema.field(col_idx).name();

                // 获取单元格值并转换为JSON值
                let value = if column.is_null(row_idx) {
                    serde_json::Value::Null
                } else {
                    // 提取字符串值
                    let display_value = if let Some(s) = format!("{:?}", column.as_ref()).strip_prefix("StringArray\n[") {
                        if let Some(end) = s.strip_suffix("]") {
                            let values: Vec<&str> = end.split(",").collect();
                            if row_idx < values.len() {
                                values[row_idx].trim().trim_matches('"').to_string()
                            } else {
                                "".to_string()
                            }
                        } else {
                            "".to_string()
                        }
                    } else {
                        // 尝试其他格式的数组
                        let array_str = format!("{:?}", column.as_ref());
                        if array_str.contains("[") && array_str.contains("]") {
                            let start_idx = array_str.find("[").unwrap_or(0) + 1;
                            let end_idx = array_str.find("]").unwrap_or(array_str.len());
                            if start_idx < end_idx {
                                let content = &array_str[start_idx..end_idx];
                                let values: Vec<&str> = content.split(",").collect();
                                if row_idx < values.len() {
                                    values[row_idx].trim().trim_matches('"').to_string()
                                } else {
                                    "".to_string()
                                }
                            } else {
                                "".to_string()
                            }
                        } else {
                            "".to_string()
                        }
                    };

                    // 尝试将值解析为JSON，如果失败则作为字符串处理
                    if display_value.starts_with('{') && display_value.ends_with('}') ||
                        display_value.starts_with('[') && display_value.ends_with(']') {
                        match serde_json::from_str(&display_value) {
                            Ok(json_value) => json_value,
                            Err(_) => serde_json::Value::String(display_value)
                        }
                    } else if display_value == "null" {
                        serde_json::Value::Null
                    } else if let Ok(num) = display_value.parse::<i64>() {
                        serde_json::Value::Number(serde_json::Number::from(num))
                    } else if let Ok(num) = display_value.parse::<f64>() {
                        match serde_json::Number::from_f64(num) {
                            Some(n) => serde_json::Value::Number(n),
                            None => serde_json::Value::String(display_value)
                        }
                    } else if display_value == "true" {
                        serde_json::Value::Bool(true)
                    } else if display_value == "false" {
                        serde_json::Value::Bool(false)
                    } else {
                        serde_json::Value::String(display_value)
                    }
                };

                row_obj.insert(field_name.clone(), value);
            }

            result.push(serde_json::Value::Object(row_obj));
        }

        let mut result_msg = vec![];

        for x in result {
            let msg_str = serde_json::to_string(&x)
                .map_err(|e| Error::Processing(format!("JSON序列化错误: {}", e)))?;
            result_msg.push(Message::new(msg_str.into_bytes()))
        }
        Ok(result_msg)
    }


    /// 合并多个记录批次
    fn combine_batches(&self, batches: &[RecordBatch]) -> Result<RecordBatch, Error> {
        if batches.is_empty() {
            return Err(Error::Processing("没有批次可合并".to_string()));
        }

        if batches.len() == 1 {
            return Ok(batches[0].clone());
        }

        // 使用第一个批次的schema
        let schema = batches[0].schema();

        // 为每一列创建合并数据
        let mut combined_columns: Vec<Vec<String>> = Vec::new();
        for _ in 0..schema.fields().len() {
            combined_columns.push(Vec::new());
        }

        // 合并所有批次的数据
        for batch in batches {
            if !batch.schema().logically_equivalent_names_and_types(&schema) {
                return Err(Error::Processing("批次schema不一致".to_string()));
            }

            for row_idx in 0..batch.num_rows() {
                for col_idx in 0..batch.num_columns() {
                    if col_idx >= combined_columns.len() {
                        // 安全检查，确保列索引有效
                        continue;
                    }

                    let column = batch.column(col_idx);
                    let value = if column.is_null(row_idx) {
                        "null".to_string()
                    } else {
                        if let Some(s) = format!("{:?}", column.as_ref()).strip_prefix("StringArray\n[") {
                            if let Some(end) = s.strip_suffix("]") {
                                let values: Vec<&str> = end.split(",").collect();
                                if row_idx < values.len() {
                                    values[row_idx].trim().trim_matches('"').to_string()
                                } else {
                                    "".to_string()
                                }
                            } else {
                                "".to_string()
                            }
                        } else {
                            // 尝试其他格式的数组
                            let array_str = format!("{:?}", column.as_ref());
                            if array_str.contains("[") && array_str.contains("]") {
                                let start_idx = array_str.find("[").unwrap_or(0) + 1;
                                let end_idx = array_str.find("]").unwrap_or(array_str.len());
                                if start_idx < end_idx {
                                    let content = &array_str[start_idx..end_idx];
                                    let values: Vec<&str> = content.split(",").collect();
                                    if row_idx < values.len() {
                                        values[row_idx].trim().trim_matches('"').to_string()
                                    } else {
                                        "".to_string()
                                    }
                                } else {
                                    "".to_string()
                                }
                            } else {
                                "".to_string()
                            }
                        }
                    };
                    combined_columns[col_idx].push(value);
                }
            }
        }

        // 创建Arrow列
        let arrow_columns: Vec<ArrayRef> = combined_columns.iter()
            .map(|col| Arc::new(StringArray::from(col.clone())) as ArrayRef)
            .collect();

        // 创建合并的记录批次
        RecordBatch::try_new(schema, arrow_columns)
            .map_err(|e| Error::Processing(format!("创建合并批次失败: {}", e)))
    }


    async fn close(&self) -> Result<(), Error> {
        // SQL处理器不需要特殊的关闭操作
        Ok(())
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
        // 复用Processor的close方法
        Ok(())
    }
}