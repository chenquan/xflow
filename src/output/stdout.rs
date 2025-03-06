//! 标准输出组件
//!
//! 将处理后的数据输出到标准输出

use std::io::{self, Write};
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;

use async_trait::async_trait;
use datafusion::arrow;
use datafusion::arrow::array::RecordBatch;
use serde::{Deserialize, Serialize};

use crate::{Error, MessageBatch, output::Output, Content, Bytes};

/// 标准输出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StdoutOutputConfig {
    /// 是否在每条消息后添加换行符
    pub append_newline: Option<bool>,
}

/// 标准输出组件
pub struct StdoutOutput {
    config: StdoutOutputConfig,
    writer: Mutex<io::Stdout>,
}

impl StdoutOutput {
    /// 创建一个新的标准输出组件
    pub fn new(config: &StdoutOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            writer: Mutex::new(io::stdout()),
        })
    }
}

#[async_trait]
impl Output for StdoutOutput {
    async fn connect(&self) -> Result<(), Error> {
        Ok(())
    }

    async fn write(&self, batch: &MessageBatch) -> Result<(), Error> {
        match &batch.content {
            Content::Arrow(v) => {
                self.arrow_stdout(&v)
            }
            Content::Binary(v) => {
                self.binary_stdout(&v)
            }
        }
    }


    async fn close(&self) -> Result<(), Error> {
        Ok(())
    }
}
impl StdoutOutput {
    pub fn arrow_stdout(&self, msg: &RecordBatch) -> Result<(), Error> {
        let mut writer_std = self.writer.lock().map_err(|e| Error::Unknown(e.to_string()))?;

        // 使用Arrow的JSON序列化功能
        let mut buf = Vec::new();
        let mut writer = arrow::json::ArrayWriter::new(&mut buf);
        writer.write(msg)
            .map_err(|e| Error::Processing(format!("Arrow JSON序列化错误: {}", e)))?;
        writer.finish()
            .map_err(|e| Error::Processing(format!("Arrow JSON序列化完成错误: {}", e)))?;
        let string = String::from_utf8_lossy(&buf);

        if self.config.append_newline.unwrap_or(true) {
            writeln!(writer_std, "{}", string).map_err(Error::Io)?
        } else {
            write!(writer_std, "{}", string).map_err(Error::Io)?
        }

        writer_std.flush().map_err(Error::Io)?;
        Ok(())
    }
    pub fn binary_stdout(&self, msg: &[Bytes]) -> Result<(), Error> {
        let mut writer_std = self.writer.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        for x in msg {
            if self.config.append_newline.unwrap_or(true) {
                writeln!(writer_std, "{}", String::from_utf8_lossy(&x)).map_err(Error::Io)?
            } else {
                write!(writer_std, "{}", String::from_utf8_lossy(&x)).map_err(Error::Io)?
            }
        }
        Ok(())
    }
}