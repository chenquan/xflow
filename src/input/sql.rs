use crate::input::{Ack, Input, NoopAck};
use crate::{Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::{SQLOptions, SessionContext};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlConfig {
    select_sql: String,
    create_source_sql: String,
}

pub struct SqlInput {
    sql_config: SqlConfig,
    read: AtomicBool,
}

impl SqlInput {
    pub fn new(sql_config: &SqlConfig) -> Result<Self, Error> {
        Ok(Self {
            sql_config: sql_config.clone(),
            read: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Input for SqlInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        if self.read.load(Ordering::Acquire) {
            return Err(Error::Done);
        }

        let ctx = SessionContext::new();

        let sql_options = SQLOptions::new()
            .with_allow_ddl(true)
            .with_allow_dml(false)
            .with_allow_statements(false);
        ctx.sql_with_options(&self.sql_config.create_source_sql, sql_options)
            .await
            .map_err(|e| Error::Config(format!("Failed to execute SQL query: {}", e)))?;

        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(false)
            .with_allow_statements(false);
        let df = ctx
            .sql_with_options(&self.sql_config.select_sql, sql_options)
            .await
            .map_err(|e| Error::Reading(format!("Failed to execute SQL query: {}", e)))?;

        let result_batches = df
            .collect()
            .await
            .map_err(|e| Error::Reading(format!("Failed to collect data from SQL query: {}", e)))?;

        let x = if result_batches.is_empty() {
            RecordBatch::new_empty(Arc::new(Schema::empty()))
        } else {
            if result_batches.len() == 1 {
                result_batches[0].clone()
            } else {
                arrow::compute::concat_batches(&&result_batches[0].schema(), &result_batches)
                    .map_err(|e| Error::Processing(format!("合并批次失败: {}", e)))?
            }
        };
        self.read.store(true, Ordering::Release);
        Ok((MessageBatch::new_arrow(x), Arc::new(NoopAck)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
