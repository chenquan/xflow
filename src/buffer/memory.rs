use crate::buffer::Buffer;
use crate::{Error, MessageBatch};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MemoryBufferConfig {}
pub struct MemoryBuffer {
    config: MemoryBufferConfig,
    queue: Arc<Mutex<VecDeque<MessageBatch>>>,
}
impl MemoryBuffer {
    pub fn new(config: &MemoryBufferConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            queue: Arc::new(Mutex::new(VecDeque::new())),
        })
    }
}
#[async_trait]
impl Buffer for MemoryBuffer {
    async fn push(&self, msg: &MessageBatch) -> Result<(), Error> {
        todo!()
    }

    async fn pop(&self) -> Result<Option<MessageBatch>, Error> {
        todo!()
    }

    async fn close(&self) -> Result<(), Error> {
        todo!()
    }
}
