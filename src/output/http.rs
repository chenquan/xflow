//! HTTP输出组件
//!
//! 将处理后的数据发送到HTTP端点

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use async_trait::async_trait;
use reqwest::{Client, header};
use serde::{Deserialize, Serialize};
use tokio::sync::Mutex;
use crate::{Error, MessageBatch, output::Output};

/// HTTP输出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpOutputConfig {
    /// 目标URL
    pub url: String,
    /// HTTP方法
    pub method: String,
    /// 超时时间（毫秒）
    pub timeout_ms: u64,
    /// 重试次数
    pub retry_count: u32,
    /// 请求头
    pub headers: Option<std::collections::HashMap<String, String>>,
}

/// HTTP输出组件
pub struct HttpOutput {
    config: HttpOutputConfig,
    client: Arc<Mutex<Option<Client>>>,
    connected: AtomicBool,
}

impl HttpOutput {
    /// 创建一个新的HTTP输出组件
    pub fn new(config: &HttpOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            client: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
        })
    }
}


#[async_trait]
impl Output for HttpOutput {
    async fn connect(&self) -> Result<(), Error> {
        // 创建HTTP客户端
        let mut client_builder = Client::builder()
            .timeout(std::time::Duration::from_millis(self.config.timeout_ms));
        let client_arc = self.client.clone();
        client_arc.lock().await.replace(client_builder.build().map_err(|e| {
            Error::Connection(format!("无法创建HTTP客户端: {}", e))
        })?);

        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        let client_arc = self.client.clone();
        let client_arc_guard = client_arc.lock().await;
        if !self.connected.load(Ordering::SeqCst) || client_arc_guard.is_none() {
            return Err(Error::Connection("输出未连接".to_string()));
        }


        let client = client_arc_guard.as_ref().unwrap();
        let content = msg.as_string()?;
        if content.is_empty() {
            return Ok(());
        }
        let body;
        if content.len() == 1 {
            body = content[0].clone();
        } else {
            body = serde_json::to_string(&content).map_err(|_| Error::Processing("无法序列化消息".to_string()))?;
        }

        // 构建请求
        let mut request_builder = match self.config.method.to_uppercase().as_str() {
            "GET" => client.get(&self.config.url),
            "POST" => client.post(&self.config.url).body(body),
            "PUT" => client.put(&self.config.url).body(body),
            "DELETE" => client.delete(&self.config.url),
            "PATCH" => client.patch(&self.config.url).body(body),
            _ => return Err(Error::Config(format!("不支持的HTTP方法: {}", self.config.method))),
        };

        // 添加请求头
        if let Some(headers) = &self.config.headers {
            for (key, value) in headers {
                request_builder = request_builder.header(key, value);
            }
        }

        // 添加内容类型头（如果没有指定）
        if self.config.headers.as_ref().map_or(true, |h| !h.contains_key("Content-Type")) {
            request_builder = request_builder.header(header::CONTENT_TYPE, "application/json");
        }

        // 发送请求
        let mut retry_count = 0;
        let mut last_error = None;

        while retry_count <= self.config.retry_count {
            match request_builder.try_clone().unwrap().send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        return Ok(());
                    } else {
                        let status = response.status();
                        let body = response.text().await.unwrap_or_else(|_| "<无法读取响应体>".to_string());
                        last_error = Some(Error::Processing(
                            format!("HTTP请求失败: 状态码 {}, 响应: {}", status, body)
                        ));
                    }
                }
                Err(e) => {
                    last_error = Some(Error::Connection(format!("HTTP请求错误: {}", e)));
                }
            }

            retry_count += 1;
            if retry_count <= self.config.retry_count {
                // 指数退避重试
                tokio::time::sleep(std::time::Duration::from_millis(
                    100 * 2u64.pow(retry_count - 1)
                )).await;
            }
        }

        Err(last_error.unwrap_or_else(|| Error::Unknown("未知HTTP错误".to_string())))
    }

    async fn close(&self) -> Result<(), Error> {
        self.connected.store(false, Ordering::SeqCst);
        let mut guard = self.client.lock().await;
        *guard = None;
        Ok(())
    }
}