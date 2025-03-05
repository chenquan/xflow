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
use toml::value::Array;
use crate::{Error, Message, MessageBatch, processor::{ProcessorBatch}, Content};

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
    async fn parse_input(&self, message: Message) -> Result<RecordBatch, Error> {
        let x = match message.content {
            Content::Arrow(v) => {
                v.clone()
            }
            Content::Binary(_) => {
                return Err(Error::Processing("不支持的输入格式".to_string()))?;
            }
        };
        Ok(x)
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
        let num_rows = batch.num_rows();
        let mut mes_batch = Vec::new();

        for i in 0..num_rows {
            let columns: Vec<ArrayRef> = batch
                .columns()
                .iter()
                .map(|array| array.slice(i, 1))
                .collect();
            let schema = Arc::clone(&batch.schema());
            let new_batch = RecordBatch::try_new(schema, columns)
                .map_err(|e| Error::Processing(format!("创建新批次失败: {}", e)))?;
            mes_batch.push(Message::new_arrow(new_batch));
        }
        Ok(mes_batch)
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
        for message in msg.0 {
            let batch = self.parse_input(message).await?;
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