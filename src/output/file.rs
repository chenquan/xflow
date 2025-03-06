//! 文件输出组件
//!
//! 将处理后的数据输出到文件

use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use crate::{Error, MessageBatch, output::Output};

/// 文件输出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileOutputConfig {
    /// 输出文件路径
    pub path: String,
    /// 是否在每条消息后添加换行符
    pub append_newline: Option<bool>,
    /// 是否追加到文件末尾（而不是覆盖）
    pub append: Option<bool>,
}

/// 文件输出组件
pub struct FileOutput {
    config: FileOutputConfig,
    writer: Arc<Mutex<Option<File>>>,
    connected: AtomicBool,
}

impl FileOutput {
    /// 创建一个新的文件输出组件
    pub fn new(config: &FileOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            writer: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
        })
    }
}

// #[async_trait]
// impl Output for FileOutput {
//     async fn connect(&self) -> Result<(), Error> {
//         let path = Path::new(&self.config.path);
//
//         // 确保目录存在
//         if let Some(parent) = path.parent() {
//             if !parent.exists() {
//                 std::fs::create_dir_all(parent).map_err(Error::Io)?
//             }
//         }
//         let append = self.config.append.unwrap_or(true);
//         // 打开文件
//         let file = OpenOptions::new()
//             .write(true)
//             .create(true)
//             .append(append)
//             .truncate(!append)
//             .open(path)
//             .map_err(Error::Io)?;
//         let writer_arc = self.writer.clone();
//         let mut writer_arc_guard = writer_arc.lock().await;
//         writer_arc_guard.replace(file);
//         self.connected.store(true, Ordering::SeqCst);
//         Ok(())
//     }
//
//     async fn write(&self, msg: &Message) -> Result<(), Error> {
//         let writer_arc = self.writer.clone();
//         let writer_arc_guard = writer_arc.lock().await;
//         if !self.connected.load(Ordering::SeqCst) || writer_arc_guard.is_none() {
//             return Err(Error::Connection("输出未连接".to_string()));
//         }
//
//         let content = msg.as_string()?;
//         let writer = writer_arc_guard.as_ref();
//         let mut file = writer.ok_or(Error::Connection("输出未连接".to_string()))?;
//
//         if self.config.append_newline.unwrap_or(true) {
//             writeln!(file, "{}", content).map_err(Error::Io)?
//         } else {
//             write!(file, "{}", content).map_err(Error::Io)?
//         }
//
//         file.flush().map_err(Error::Io)?;
//         Ok(())
//     }
//
//     async fn close(&self) -> Result<(), Error> {
//         self.connected.store(false, Ordering::SeqCst);
//         let writer_arc = self.writer.clone();
//         let mut writer_arc_mutex_guard = writer_arc.lock().await;
//         *writer_arc_mutex_guard = None;
//         Ok(())
//     }
// }