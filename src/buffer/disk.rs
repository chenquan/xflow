//! 磁盘缓冲区组件
//!
//! 在磁盘上提供临时消息存储，支持持久化

use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Mutex;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

use crate::{Error, MessageBatch, buffer::Buffer};

// /// 磁盘缓冲区配置
// #[derive(Debug, Clone, Serialize, Deserialize)]
// pub struct DiskBufferConfig {
//     /// 缓冲文件路径
//     pub path: String,
//     /// 最大文件大小（字节）
//     pub max_size: usize,
//     /// 是否在关闭时删除文件
//     pub delete_on_close: bool,
// }
//
// /// 磁盘缓冲区组件
// pub struct DiskBuffer {
//     config: DiskBufferConfig,
//     file_path: PathBuf,
//     reader: Mutex<Option<BufReader<File>>>,
//     writer: Mutex<Option<BufWriter<File>>>,
//     read_position: Mutex<u64>,
//     write_position: Mutex<u64>,
// }
//
// impl DiskBuffer {
//     /// 创建一个新的磁盘缓冲区组件
//     pub fn new(config: &DiskBufferConfig) -> Result<Self, Error> {
//         let file_path = PathBuf::from(&config.path);
//
//         // 确保目录存在
//         if let Some(parent) = file_path.parent() {
//             if !parent.exists() {
//                 std::fs::create_dir_all(parent).map_err(Error::Io)?
//             }
//         }
//
//         // 创建或打开文件
//         let file = OpenOptions::new()
//             .read(true)
//             .write(true)
//             .create(true)
//             .open(&file_path)
//             .map_err(Error::Io)?;
//
//         // 获取文件大小
//         let file_size = file.metadata().map_err(Error::Io)?.len();
//
//         // 创建读写器
//         let reader = BufReader::new(file.try_clone().map_err(Error::Io)?);
//         let writer = BufWriter::new(file);
//
//         Ok(Self {
//             config: config.clone(),
//             file_path,
//             reader: Mutex::new(Some(reader)),
//             writer: Mutex::new(Some(writer)),
//             read_position: Mutex::new(0),
//             write_position: Mutex::new(file_size),
//         })
//     }
//
//     /// 将消息序列化为字节
//     fn serialize_message(&self, msg: &Message) -> Result<Vec<u8>, Error> {
//         let content = msg.content().to_vec();
//         let content_len = content.len() as u32;
//
//         // 格式：内容长度(4字节) + 内容
//         let mut buffer = Vec::with_capacity(4 + content_len as usize);
//         buffer.extend_from_slice(&content_len.to_le_bytes());
//         buffer.extend_from_slice(&content);
//
//         Ok(buffer)
//     }
//
//     /// 从字节反序列化消息
//     fn deserialize_message(&self, mut bytes: &[u8]) -> Result<Message, Error> {
//         if bytes.len() < 4 {
//             return Err(Error::Processing("无效的消息格式".to_string()));
//         }
//
//         // 读取内容长度
//         let mut len_bytes = [0u8; 4];
//         bytes.read_exact(&mut len_bytes).map_err(Error::Io)?;
//         let content_len = u32::from_le_bytes(len_bytes) as usize;
//
//         // 读取内容
//         let mut content = vec![0u8; content_len];
//         bytes.read_exact(&mut content).map_err(Error::Io)?;
//
//         Ok(Message::new(content))
//     }
// }
//
// #[async_trait]
// impl Buffer for DiskBuffer {
//     async fn push(&self, msg: &Message) -> Result<(), Error> {
//         // 序列化消息
//         let data = self.serialize_message(msg)?;
//
//         // 获取写入位置
//         let mut write_pos = self.write_position.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//
//         // 检查是否超过最大大小
//         if (*write_pos as usize) + data.len() > self.config.max_size {
//             return Err(Error::Processing("磁盘缓冲区已满".to_string()));
//         }
//
//         // 获取写入器
//         let mut writer_guard = self.writer.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//         let writer = writer_guard.as_mut().ok_or_else(|| Error::Connection("缓冲区未连接".to_string()))?;
//
//         // 定位到写入位置
//         writer.seek(SeekFrom::Start(*write_pos)).map_err(Error::Io)?;
//
//         // 写入数据
//         writer.write_all(&data).map_err(Error::Io)?;
//         writer.flush().map_err(Error::Io)?;
//
//         // 更新写入位置
//         *write_pos += data.len() as u64;
//
//         Ok(())
//     }
//
//     async fn pop(&self) -> Result<Option<Message>, Error> {
//         // 获取读取和写入位置
//         let mut read_pos = self.read_position.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//         let write_pos = self.write_position.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//
//         // 检查是否有数据可读
//         if *read_pos >= *write_pos {
//             return Ok(None);
//         }
//
//         // 获取读取器
//         let mut reader_guard = self.reader.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//         let reader = reader_guard.as_mut().ok_or_else(|| Error::Connection("缓冲区未连接".to_string()))?;
//
//         // 定位到读取位置
//         reader.seek(SeekFrom::Start(*read_pos)).map_err(Error::Io)?;
//
//         // 读取消息长度
//         let mut len_bytes = [0u8; 4];
//         reader.read_exact(&mut len_bytes).map_err(Error::Io)?;
//         let content_len = u32::from_le_bytes(len_bytes) as usize;
//
//         // 读取消息内容
//         let mut content = vec![0u8; content_len];
//         reader.read_exact(&mut content).map_err(Error::Io)?;
//
//         // 更新读取位置
//         *read_pos += (4 + content_len) as u64;
//
//         // 创建消息
//         let msg = Message::new(content);
//
//         Ok(Some(msg))
//     }
//
//     async fn close(&self) -> Result<(), Error> {
//         // 释放读写器
//         let mut reader_guard = self.reader.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//         let mut writer_guard = self.writer.lock().map_err(|e| Error::Unknown(e.to_string()))?;
//
//         *reader_guard = None;
//         *writer_guard = None;
//
//         // 如果配置为关闭时删除文件，则删除文件
//         if self.config.delete_on_close && Path::new(&self.file_path).exists() {
//             std::fs::remove_file(&self.file_path).map_err(Error::Io)?;
//         }
//
//         Ok(())
//     }
// }