//! SQL处理器组件
//!
//! 使用DataFusion执行SQL查询处理数据，支持静态SQL和流式SQL

use arrow_json;
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::ArrayRef;
use datafusion::arrow::datatypes::Schema;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;


use crate::{ Content, Error, MessageBatch};
use crate::processor::Processor;

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
    async fn parse_input(&self, message: MessageBatch) -> Result<RecordBatch, Error> {
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
}

#[async_trait]
impl Processor for SqlProcessor {
    async fn process(&self, msg_batch: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        // 如果批次为空，直接返回空结果
        if msg_batch.is_empty() {
            return Ok(vec![]);
        }

        let batch: RecordBatch = match msg_batch.content {
            Content::Arrow(v) => {
                v
            }
            Content::Binary(_) => {
                return Err(Error::Processing("不支持的输入格式".to_string()))?;
            }
        };

        // 执行SQL查询
        let result_batch = self.execute_query(batch).await?;
        Ok(vec![MessageBatch::new_arrow(result_batch)])
    }


    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}