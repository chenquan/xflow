//! 文件输入组件
//!
//! 从文件系统读取数据

use std::fs::File;
use std::io::{BufRead, BufReader, Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, Message, input::Input};

/// 文件输入配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileInputConfig {
    /// 输入文件路径
    pub path: String,
    /// 是否在读取完成后关闭
    pub close_on_eof: bool,
    /// 是否从文件开头开始读取（否则从末尾开始）
    pub start_from_beginning: bool,
}

/// 文件输入组件
pub struct FileInput {
    config: FileInputConfig,
    reader: Arc<Mutex<Option<BufReader<File>>>>,
    connected: AtomicBool,
    eof_reached: AtomicBool,
}

impl FileInput {
    /// 创建一个新的文件输入组件
    pub fn new(config: &FileInputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            reader: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
            eof_reached: AtomicBool::new(false),
        })
    }
}

#[async_trait]
impl Input for FileInput {
    async fn connect(&self) -> Result<(), Error> {
        let path = Path::new(&self.config.path);

        // 打开文件
        let file = File::open(path).map_err(|e| {
            Error::Connection(format!("无法打开文件 {}: {}", self.config.path, e))
        })?;

        let mut reader = BufReader::new(file);

        // 如果不是从开头开始读取，则移动到文件末尾
        if !self.config.start_from_beginning {
            reader.seek(SeekFrom::End(0)).map_err(|e| {
                Error::Processing(format!("无法定位到文件末尾: {}", e))
            })?;
        }

        let reader_arc = self.reader.clone();
        reader_arc.lock().await.replace(reader);
        self.connected.store(true, Ordering::SeqCst);
        self.eof_reached.store(false, Ordering::SeqCst);
        Ok(())
    }

    async fn read(&self) -> Result<Message, Error> {
        let reader_arc = self.reader.clone();
        let mut reader_mutex = reader_arc.lock().await;
        if !self.connected.load(Ordering::SeqCst) || reader_mutex.is_none() {
            return Err(Error::Connection("输入未连接".to_string()));
        }

        if self.eof_reached.load(Ordering::SeqCst) && self.config.close_on_eof {
            return Err(Error::Processing("已到达文件末尾".to_string()));
        }


        // 使用作用域来限制锁的生命周期
        let bytes_read;
        let mut line = String::new();
        {
            let mut reader_mutex = reader_mutex.as_mut();
            if reader_mutex.is_none() {
                return Err(Error::Connection("输入未连接".to_string()));
            }

            let reader = reader_mutex.unwrap();
            // let mut reader = reader_mutex.lock().await;
            bytes_read = reader.read_line(&mut line).map_err(Error::Io)?;
        }

        if bytes_read == 0 {
            // 到达文件末尾
            self.eof_reached.store(true, Ordering::SeqCst);

            // 如果配置为关闭，则返回错误
            if self.config.close_on_eof {
                return Err(Error::Processing("已到达文件末尾".to_string()));
            }

            // 否则等待一段时间后重试（模拟tail -f行为）
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            return Err(Error::Processing("等待新数据".to_string()));
        }

        // 移除尾部的换行符
        if line.ends_with('\n') {
            line.pop();
            if line.ends_with('\r') {
                line.pop();
            }
        }

        Ok(Message::from_string(&line))
    }

    async fn acknowledge(&self, _msg: &Message) -> Result<(), Error> {
        // 文件输入不需要确认机制
        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::SeqCst);
        let reader_arc = self.reader.clone();
        let mut reader_mutex = reader_arc.lock().await;
        *reader_mutex = None;
        Ok(())
    }
}