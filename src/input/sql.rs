use crate::input::{Ack, Input, NoopAck};
use crate::{Error, MessageBatch};
use async_trait::async_trait;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::Schema;
use datafusion::prelude::{SQLOptions, SessionContext};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SqlConfig {
    sql: String,
}

pub struct SqlInput {
    sql_config: SqlConfig,
}

impl SqlInput {
    pub fn new(sql_config: &SqlConfig) -> Result<Self, Error> {
        Ok(Self {
            sql_config: sql_config.clone(),
        })
    }
}

#[async_trait]
impl Input for SqlInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        let sql = &self.sql_config.sql;
        let ctx = SessionContext::new();

        let sql_options = SQLOptions::new()
            .with_allow_ddl(false)
            .with_allow_dml(true)
            .with_allow_statements(true);
        let df = ctx
            .sql_with_options(sql, sql_options)
            .await
            .map_err(|e| Error::Config(format!("Failed to execute SQL query: {}", e)))?;

        let result_batches = df
            .collect()
            .await
            .map_err(|e| Error::Reading(format!("Failed to collect data from SQL query: {}", e)))?;

        let x = if result_batches.is_empty() {
            RecordBatch::new_empty(Arc::new(Schema::empty()))
        } else {
            result_batches[0].clone()
        };

        Ok((MessageBatch::new_arrow(x), Arc::new(NoopAck)))
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
