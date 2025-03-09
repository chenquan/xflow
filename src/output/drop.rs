use crate::output::Output;
use crate::{Error, MessageBatch};
use async_trait::async_trait;

pub struct DropOutput;

#[async_trait]
impl Output for DropOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, _: &MessageBatch) -> Result<(), Error> {
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
