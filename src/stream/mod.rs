//! 流组件模块
//!
//! 流是完整的数据处理单元，包含输入、管道和输出。

use crate::input::Ack;
use crate::{input::Input, output::Output, pipeline::Pipeline, Error, MessageBatch};
use std::sync::Arc;
use tokio::signal::unix::{signal, SignalKind};
use tracing::{debug, error, info};
use waitgroup::WaitGroup;

/// 流结构体，包含输入、管道、输出和可选的缓冲区
pub struct Stream {
    input: Arc<dyn Input>,
    pipeline: Arc<Pipeline>,
    output: Arc<dyn Output>,
    thread_num: i32,
}

impl Stream {
    /// 创建一个新的流
    pub fn new(
        input: Arc<dyn Input>,
        pipeline: Pipeline,
        output: Arc<dyn Output>,
        thread_num: i32,
    ) -> Self {
        Self {
            input,
            pipeline: Arc::new(pipeline),
            output,
            thread_num,
        }
    }

    /// 运行流处理
    pub async fn run(&mut self) -> Result<(), Error> {
        // 连接输入和输出
        self.input.connect().await?;
        self.output.connect().await?;

        // 设置信号处理器
        let mut sigint = signal(SignalKind::interrupt())?;
        let mut sigterm = signal(SignalKind::terminate())?;

        let (input_sender, input_receiver) = flume::bounded::<(MessageBatch, Arc<dyn Ack>)>(1000);
        let (output_sender, output_receiver) =
            flume::bounded::<(Vec<MessageBatch>, Arc<dyn Ack>)>(1000);
        let input = Arc::clone(&self.input);

        let wg = WaitGroup::new();

        for i in 0..self.thread_num {
            let pipeline = self.pipeline.clone();
            let input_receiver = input_receiver.clone();
            let output_sender = output_sender.clone();
            let worker = wg.worker();
            tokio::spawn(async move {
                let i = i + 1;
                info!("Worker {} started", i);
                loop {
                    match input_receiver.recv_async().await {
                        Ok((msg, ack)) => {
                            // 通过管道处理消息
                            debug!("Processing input message: {:?}", &msg.as_string());
                            let processed = pipeline.process(msg).await;

                            // 处理结果消息
                            match processed {
                                Ok(msgs) => {
                                    for x in &msgs {
                                        debug!("Processing output message: {:?}", x.as_string());
                                    }

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
                        Err(_e) => {
                            return;
                        }
                    }
                }
                drop(output_sender);
                info!("Worker {} stopped", i);
                drop(worker);
            });
        }

        let worker = wg.worker();

        let output_arc = self.output.clone();

        tokio::spawn(async move {
            loop {
                match input.read().await {
                    Ok(msg) => {
                        debug!("Received input message: {:?}", &msg.0.as_string());
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
                            Error::Disconnection => loop {
                                match output_arc.connect().await {
                                    Ok(_) => {
                                        info!("input reconnected");
                                        break;
                                    }
                                    Err(e) => {
                                        error!("{}", e);
                                        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
                                    }
                                };
                            },
                            Error::Config(e) => {
                                error!("{}", e);
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
            drop(worker)
        });

        drop(output_sender);
        loop {
            tokio::select! {
                // 监听中断信号
                _ = sigint.recv() => {
                    info!("Received SIGINT signal, initiating graceful shutdown");
                    self.close().await?;
                    break;
                }
                _ = sigterm.recv() => {
                    info!("Received SIGTERM signal, initiating graceful shutdown");
                    self.close().await?;
                     break;
                }
                msg = output_receiver.recv_async() => {
                    match msg {
                        Ok(msg) => {
                            let size = &msg.0.len();
                            let mut success_cnt = 0;
                            for x in &msg.0 {
                                match self.output.write(x).await {
                                    Ok(_) => {
                                        success_cnt = success_cnt + 1;
                                    }
                                    Err(e) => {
                                        error!("{}", e);
                                    }
                                }
                            }

                            // 确认消息已成功处理
                            if size == &success_cnt {
                                msg.1.ack().await;
                            }
                        }
                        Err(_) => {
                            return Ok(());
                        }
                    }
                }
            }
        }

        wg.wait();
        Ok(())
    }

    /// 关闭流中的所有组件
    pub async fn close(&mut self) -> Result<(), Error> {
        // 关闭顺序：输入 -> 管道 -> 缓冲区 -> 输出
        self.input.close().await?;
        self.pipeline.close().await?;
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
}

impl StreamConfig {
    /// 根据配置构建流
    pub fn build(&self) -> Result<Stream, Error> {
        let input = self.input.build()?;
        let (pipeline, thread_num) = self.pipeline.build()?;
        let output = self.output.build()?;

        Ok(Stream::new(input, pipeline, output, thread_num))
    }
}
