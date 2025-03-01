//! 标准输出组件
//!
//! 将处理后的数据输出到标准输出

use std::io::{self, Write};
use std::sync::atomic::AtomicBool;
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, Message, output::Output};

/// 标准输出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StdoutOutputConfig {
    /// 是否在每条消息后添加换行符
    pub append_newline: bool,
}

/// 标准输出组件
pub struct StdoutOutput {
    config: StdoutOutputConfig,
    writer: Mutex<io::Stdout>,
    connected: AtomicBool,
}

impl StdoutOutput {
    /// 创建一个新的标准输出组件
    pub fn new(config: &StdoutOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            writer: Mutex::new(io::stdout()),
            connected: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Output for StdoutOutput {
    async fn connect(&self) -> Result<(), Error> {
        self.connected.store(true, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: &Message) -> Result<(), Error> {
        if !self.connected.load(std::sync::atomic::Ordering::SeqCst) {
            return Err(Error::Connection("输出未连接".to_string()));
        }

        let content = msg.as_string()?;
        let mut writer = self.writer.lock().map_err(|e| Error::Unknown(e.to_string()))?;

        if self.config.append_newline {
            writeln!(writer, "{}", content).map_err(Error::Io)?
        } else {
            write!(writer, "{}", content).map_err(Error::Io)?
        }

        writer.flush().map_err(Error::Io)?;
        Ok(())
    }

    async fn close(&  self) -> Result<(), Error> {
        self.connected.store(false, std::sync::atomic::Ordering::SeqCst);
        Ok(())
    }
}