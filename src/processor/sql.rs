//! SQL处理器组件
//!
//! 使用DataFusion执行SQL查询处理数据

use std::sync::Arc;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use datafusion::prelude::*;
use datafusion::arrow::array::{ArrayRef, StringArray};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;

use crate::{Error, Message, processor::Processor};

/// SQL处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlProcessorConfig {
    /// SQL查询语句
    pub query: String,
    /// 输入格式（json, csv）
    pub input_format: String,
    /// 表名（用于SQL查询中引用）
    pub table_name: String,
    /// 输出格式（json, csv）
    pub output_format: String,
    /// 目标字段（可选，用于将结果存储到特定字段）
    pub target: Option<String>,
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
        match self.config.input_format.as_str() {
            "json" => self.parse_json_input(content).await,
            "csv" => self.parse_csv_input(content).await,
            _ => Err(Error::Config(format!(
                "不支持的输入格式: {}",
                self.config.input_format
            ))),
        }
    }

    /// 解析JSON输入
    async fn parse_json_input(&self, content: &str) -> Result<RecordBatch, Error> {
        // 解析JSON内容
        let json_value: serde_json::Value = serde_json::from_str(content)
            .map_err(|e| Error::Processing(format!("JSON解析错误: {}", e)))?;

        // 处理不同的JSON结构
        match json_value {
            serde_json::Value::Object(obj) => {
                // 单个对象转换为单行表
                let mut fields = Vec::new();
                let mut columns: Vec<ArrayRef> = Vec::new();

                // 提取所有字段和值
                for (key, value) in obj {
                    fields.push(Field::new(&key, DataType::Utf8, false));

                    // 将值转换为字符串
                    let str_value = match value {
                        serde_json::Value::Null => "null".to_string(),
                        _ => value.to_string(),
                    };

                    // 创建列数据
                    let array = StringArray::from(vec![str_value]);
                    columns.push(Arc::new(array));
                }

                // 创建schema和记录批次
                let schema = Arc::new(Schema::new(fields));
                RecordBatch::try_new(schema, columns)
                    .map_err(|e| Error::Processing(format!("创建记录批次失败: {}", e)))
            }
            serde_json::Value::Array(arr) => {
                if arr.is_empty() {
                    // 返回一个空的记录批次而不是错误
                    let schema = Arc::new(Schema::new(vec![] as Vec<Field>));
                    return RecordBatch::try_new(schema, vec![]).map_err(|e| Error::Processing(format!("创建记录批次失败: {}", e)));
                }

                // 数组的第一个元素用于确定schema
                if let Some(serde_json::Value::Object(first_obj)) = arr.first() {
                    let mut fields = Vec::new();
                    let mut columns: Vec<Vec<String>> = Vec::new();

                    // 从第一个对象提取字段
                    for key in first_obj.keys() {
                        fields.push(Field::new(key, DataType::Utf8, false));
                        columns.push(Vec::with_capacity(arr.len()));
                    }

                    // 填充所有行的数据
                    for item in &arr {
                        if let serde_json::Value::Object(obj) = item {
                            let mut col_idx = 0;
                            for key in first_obj.keys() {
                                let value = obj.get(key).unwrap_or(&serde_json::Value::Null);
                                let str_value = match value {
                                    serde_json::Value::Null => "null".to_string(),
                                    _ => value.to_string(),
                                };
                                columns[col_idx].push(str_value);
                                col_idx += 1;
                            }
                        } else {
                            // 跳过非对象元素而不是返回错误
                            continue;
                        }
                    }

                    // 如果所有元素都被跳过，返回空的记录批次
                    if columns.first().map_or(true, |col| col.is_empty()) {
                        let schema = Arc::new(Schema::new(vec![] as Vec<Field>));
                        return RecordBatch::try_new(schema, vec![]).map_err(|e| Error::Processing(format!("创建记录批次失败: {}", e)));
                    }

                    // 创建Arrow列
                    let arrow_columns: Vec<ArrayRef> = columns.iter()
                        .map(|col| Arc::new(StringArray::from(col.clone())) as ArrayRef)
                        .collect();

                    // 创建schema和记录批次
                    let schema = Arc::new(Schema::new(fields));
                    RecordBatch::try_new(schema, arrow_columns)
                        .map_err(|e| Error::Processing(format!("创建记录批次失败: {}", e)))
                } else {
                    Err(Error::Processing("JSON数组的第一个元素不是对象".to_string()))
                }
            }
            _ => Err(Error::Processing("输入必须是JSON对象或数组".to_string())),
        }
    }

    /// 解析CSV输入
    async fn parse_csv_input(&self, content: &str) -> Result<RecordBatch, Error> {
        // 创建内存中的CSV阅读器
        let ctx = SessionContext::new();
        let options = CsvReadOptions::new()
            .has_header(true)
            .delimiter(b',');

        // 注册内存表
        ctx.register_csv("temp_csv", content, options).await
            .map_err(|e| Error::Processing(format!("CSV解析错误: {}", e)))?;

        // 执行简单查询获取数据
        let df = ctx.sql("SELECT * FROM temp_csv").await
            .map_err(|e| Error::Processing(format!("CSV查询错误: {}", e)))?;

        // 获取第一个批次
        let batches = df.collect().await
            .map_err(|e| Error::Processing(format!("收集CSV数据错误: {}", e)))?;

        if batches.is_empty() {
            return Err(Error::Processing("CSV数据为空".to_string()));
        }

        Ok(batches[0].clone())
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
            return Err(Error::Processing("查询结果为空".to_string()));
        }

        Ok(result_batches[0].clone())
    }

    /// 将查询结果格式化为输出
    fn format_output(&self, batch: &RecordBatch) -> Result<String, Error> {
        match self.config.output_format.as_str() {
            "json" => self.format_json_output(batch),
            "csv" => self.format_csv_output(batch),
            _ => Err(Error::Config(format!(
                "不支持的输出格式: {}",
                self.config.output_format
            ))),
        }
    }

    /// 格式化为JSON输出
    fn format_json_output(&self, batch: &RecordBatch) -> Result<String, Error> {
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
                    // 简单处理为字符串
                    // 使用 as_ref().to_string() 替代 value_to_string
                    let display_value = if let Some(s) = format!("{:?}", column.as_ref()).strip_prefix("StringArray\n[") {
                        if let Some(end) = s.strip_suffix("]") {
                            let values: Vec<&str> = end.split(",").collect();
                            if row_idx < values.len() {
                                values[row_idx].trim().to_string()
                            } else {
                                "".to_string()
                            }
                        } else {
                            "".to_string()
                        }
                    } else {
                        "".to_string()
                    };
                    serde_json::Value::String(display_value)
                };

                row_obj.insert(field_name.clone(), value);
            }

            result.push(serde_json::Value::Object(row_obj));
        }

        // 如果只有一行，返回对象而不是数组
        let final_result = if result.len() == 1 {
            result.pop().unwrap()
        } else {
            serde_json::Value::Array(result)
        };

        serde_json::to_string(&final_result)
            .map_err(|e| Error::Processing(format!("JSON序列化错误: {}", e)))
    }

    /// 格式化为CSV输出
    fn format_csv_output(&self, batch: &RecordBatch) -> Result<String, Error> {
        let schema = batch.schema();
        let mut result = String::new();

        // 添加标题行
        for (i, field) in schema.fields().iter().enumerate() {
            if i > 0 {
                result.push(',');
            }
            result.push_str(field.name());
        }
        result.push('\n');

        // 添加数据行
        for row_idx in 0..batch.num_rows() {
            for col_idx in 0..batch.num_columns() {
                if col_idx > 0 {
                    result.push(',');
                }

                let column = batch.column(col_idx);
                if column.is_null(row_idx) {
                    // 空值处理为空字符串
                    result.push_str("");
                } else {
                    // 获取值并处理引号和逗号
                    // 使用 as_ref().to_string() 替代 value_to_string
                    let value = if let Some(s) = format!("{:?}", column.as_ref()).strip_prefix("StringArray\n[") {
                        if let Some(end) = s.strip_suffix("]") {
                            let values: Vec<&str> = end.split(",").collect();
                            if row_idx < values.len() {
                                values[row_idx].trim().to_string()
                            } else {
                                "".to_string()
                            }
                        } else {
                            "".to_string()
                        }
                    } else {
                        "".to_string()
                    };
                    if value.contains(',') || value.contains('"') || value.contains('\n') {
                        // 需要引号包裹并转义内部引号
                        result.push('"');
                        for c in value.chars() {
                            if c == '"' {
                                result.push('"'); // 双引号转义
                            }
                            result.push(c);
                        }
                        result.push('"');
                    } else {
                        result.push_str(&value);
                    }
                }
            }
            result.push('\n');
        }

        Ok(result)
    }
}

#[async_trait]
impl Processor for SqlProcessor {
    async fn process(&self, mut msg: Message) -> Result<Vec<Message>, Error> {
        // 获取消息内容
        let content = msg.as_string()?;

        // 解析输入数据为DataFusion表
        let input_batch = self.parse_input(&content).await?;

        // 执行SQL查询
        let result_batch = self.execute_query(input_batch).await?;

        // 格式化结果
        let result_str = self.format_output(&result_batch)?;

        // 如果指定了目标字段，则将结果添加到元数据
        if let Some(target) = &self.config.target {
            msg.metadata_mut().set(target, &result_str);
        } else {
            // 否则，将结果设置为消息内容
            msg.set_content(result_str.into_bytes());
        }

        Ok(vec![msg])
    }

    async fn close(&self) -> Result<(), Error> {
        // SQL处理器不需要特殊的关闭操作
        Ok(())
    }
}