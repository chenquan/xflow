//! 管道组件模块
//!
//! 管道是处理器的有序集合，定义了数据如何从输入流向输出，经过一系列处理步骤。

use std::ops::Deref;
use std::sync::{Arc, Mutex};
use serde::{Deserialize, Serialize};

use crate::{Error, MessageBatch, processor::Processor,  };

/// 管道结构体，包含一系列处理器
pub struct Pipeline {
    processors: Vec<Arc<dyn Processor>>,
}

impl Pipeline {
    /// 创建一个新的管道
    pub fn new(processors: Vec<Arc<dyn Processor>>) -> Self {
        Self { processors }
    }

    /// 处理消息
    pub async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        let mut msgs = vec![msg];
        for processor in &self.processors {
            let mut new_msgs = Vec::new();
            for msg in msgs {
                match processor.process(msg).await {
                    Ok(processed) => new_msgs.extend(processed),
                    Err(e) => return Err(e),
                }
            }
            msgs = new_msgs;
        }
        Ok(msgs)
    }

    /// 关闭管道中的所有处理器
    pub async fn close(&self) -> Result<(), Error> {
        for processor in &self.processors {
            processor.close().await?
        }
        Ok(())
    }
}

/// 管道配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineConfig {
    pub thread_num: i32,
    pub processors: Vec<crate::processor::ProcessorConfig>,
}

impl PipelineConfig {
    /// 根据配置构建管道
    pub fn build(&self) -> Result<(Pipeline, i32), Error> {
        let mut processors = Vec::new();
        for processor_config in &self.processors {
            processors.push(processor_config.build()?);
        }
        Ok((Pipeline::new(processors), self.thread_num))
    }
}