//! 流组件模块
//!
//! 流是完整的数据处理单元，包含输入、管道和输出。

use std::sync::Arc;
use flume::RecvError;
use tokio::sync::Mutex;
use tracing::{debug, error, info};
use crate::{buffer::Buffer, input::Input, output::Output, pipeline::Pipeline, Error, Message, MessageBatch};
use crate::input::{Ack, InputBatch};
use crate::output::OutputBatch;

/// 流结构体，包含输入、管道、输出和可选的缓冲区
pub struct Stream {
    input: Arc<dyn InputBatch>,
    pipeline: Arc<Pipeline>,
    output: Arc<dyn OutputBatch>,
    buffer: Option<Arc<dyn Buffer>>,
    thread_num: i32,
}

impl Stream {
    /// 创建一个新的流
    pub fn new(
        input: Arc<dyn InputBatch>,
        pipeline: Pipeline,
        output: Arc<dyn OutputBatch>,
        buffer: Option<Arc<dyn Buffer>>,
        thread_num: i32,
    ) -> Self {
        Self {
            input,
            pipeline: Arc::new(pipeline),
            output,
            buffer,
            thread_num,
        }
    }

    /// 运行流处理
    pub async fn run(&mut self) -> Result<(), Error> {
        // 连接输入和输出
        self.input.connect().await?;
        self.output.connect().await?;;


        let (input_sender, input_receiver) = flume::bounded::<(MessageBatch, Arc<dyn Ack>)>(1000);
        let (output_sender, output_receiver) = flume::bounded::<(Vec<MessageBatch>, Arc<dyn Ack>)>(1000);
        let input = Arc::clone(&self.input);

        for i in 0..self.thread_num {
            let pipeline = self.pipeline.clone();
            let input_receiver = input_receiver.clone();
            let output_sender = output_sender.clone();

            tokio::spawn(async move {
                let i = i + 1;
                info!("Worker {} started", i);
                loop {
                    match input_receiver.recv_async().await {
                        Ok((msg, ack)) => {
                            // 通过管道处理消息
                            let processed = pipeline.process(msg).await;

                            // 处理结果消息
                            match processed {
                                Ok(msgs) => {
                                    if let Err(e) = output_sender.send_async((msgs, ack)).await {
                                        error!("Failed to send processed message: {}", e);
                                        break;
                                    }
                                }
                                Err(e) => {
                                    error!("{}", e)
                                }
                            }
                        }
                        Err(e) => {
                            return;
                        }
                    }
                }
                drop(output_sender);
                info!("Worker {} stopped", i);
            });
        }

        tokio::spawn(async move {
            loop {
                match input.read().await {
                    Ok(msg) => {

                        if let Err(e) = input_sender.send_async(msg).await {
                            error!("Failed to send input message: {}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        match e {
                            Error::Done => {
                                // 输入完成时，关闭发送端以通知所有工作线程
                                drop(input_sender);
                                return;
                            }
                            _ => {
                                error!("{}", e);
                                // 发生错误时，关闭发送端以通知所有工作线程
                                continue;
                            }
                        };
                    }
                };
            }
        });

        drop(output_sender);

        loop {
            // 通过管道处理消息
            let msg = match output_receiver.recv_async().await {
                Ok(msg) => msg,
                Err(_) => {
                    // 如果输出通道已关闭，退出循环
                    return Ok(());
                }
            };

            // TODO 如果有缓冲区，从缓冲区读取并写入输出
            // if let Some(buffer) = &mut self.buffer {
            //     while let Ok(Some(msg)) = buffer.pop().await {
            //         self.output.write(&msg).await?
            //     }
            // }

            for x in &msg.0 {
                self.output.write(x).await?
            }

            // 确认消息已成功处理
            msg.1.ack().await;
        }
    }

    /// 关闭流中的所有组件
    pub async fn close(&mut self) -> Result<(), Error> {
        // 关闭顺序：输入 -> 管道 -> 缓冲区 -> 输出
        self.input.close().await?;
        self.pipeline.close().await?;
        if let Some(buffer) = &mut self.buffer {
            buffer.close().await?;
        }
        self.output.close().await?;
        Ok(())
    }
}

/// 流配置
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StreamConfig {
    pub input: crate::input::InputConfig,
    pub pipeline: crate::pipeline::PipelineConfig,
    pub output: crate::output::OutputConfig,
    pub buffer: Option<crate::buffer::BufferConfig>,
}

impl StreamConfig {
    /// 根据配置构建流
    pub fn build(&self) -> Result<Stream, Error> {
        let input = self.input.build()?;
        let (pipeline, thread_num) = self.pipeline.build()?;
        let output = self.output.build()?;
        let buffer = if let Some(buffer_config) = &self.buffer {
            Some(buffer_config.build()?)
        } else {
            None
        };

        Ok(Stream::new(input, pipeline, output, buffer, thread_num))
    }
}
