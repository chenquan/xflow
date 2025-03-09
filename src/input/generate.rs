use crate::input::{Ack, Input, NoopAck};
use crate::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Deserializer, Serialize};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenerateConfig {
    context: String,
    #[serde(deserialize_with = "deserialize_duration")]
    interval: Duration,
    count: Option<usize>,
    batch_size: Option<usize>,
}

pub struct GenerateInput {
    config: GenerateConfig,
    count: AtomicI64,
    batch_size: usize,
}
impl GenerateInput {
    pub fn new(config: GenerateConfig) -> Result<Self, Error> {
        let batch_size = config.batch_size.unwrap_or(1);

        Ok(Self {
            config,
            count: AtomicI64::new(0),
            batch_size,
        })
    }
}

#[async_trait]
impl Input for GenerateInput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        tokio::time::sleep(self.config.interval).await;

        if let Some(count) = self.config.count {
            if self.count.load(Ordering::SeqCst) >= count as i64 {
                return Err(Error::Done);
            }
        }
        let mut msgs = Vec::with_capacity(self.batch_size);
        for _ in 0..self.batch_size {
            let s = self.config.context.clone();
            msgs.push(s.into_bytes())
        }

        self.count
            .fetch_add(self.batch_size as i64, Ordering::SeqCst);

        Ok((MessageBatch::new_binary(msgs), Arc::new(NoopAck)))
    }
    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}

fn deserialize_duration<'de, D>(deserializer: D) -> Result<Duration, D::Error>
where
    D: Deserializer<'de>,
{
    let s: String = Deserialize::deserialize(deserializer)?;
    humantime::parse_duration(&s).map_err(serde::de::Error::custom)
}
