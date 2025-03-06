//! HTTP输入组件
//!
//! 从HTTP端点接收数据

use std::net::SocketAddr;
use std::sync::Arc;
use tokio::sync::Mutex;
use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, Ordering};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use axum::{Router, routing::post, extract::State, http::StatusCode};

use crate::{Error, MessageBatch, input::Input};
use crate::input::{Ack, NoopAck};

/// HTTP输入配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpInputConfig {
    /// 监听地址
    pub address: String,
    /// 路径
    pub path: String,
    /// 是否启用CORS
    pub cors_enabled: Option<bool>,
}

/// HTTP输入组件
pub struct HttpInput {
    config: HttpInputConfig,
    queue: Arc<Mutex<VecDeque<MessageBatch>>>,
    server_handle: Arc<Mutex<Option<tokio::task::JoinHandle<Result<(), Error>>>>>,
    connected: AtomicBool,
}

/// 共享状态
type AppState = Arc<Mutex<VecDeque<MessageBatch>>>;

impl HttpInput {
    /// 创建一个新的HTTP输入组件
    pub fn new(config: &HttpInputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            server_handle: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
        })
    }

    /// 处理HTTP请求
    async fn handle_request(State(state): State<AppState>, body: axum::extract::Json<serde_json::Value>) -> StatusCode {
        let msg = match MessageBatch::from_json(&body.0) {
            Ok(msg) => msg,
            Err(_) => return StatusCode::BAD_REQUEST,
        };

        let mut queue = state.lock().await;
        queue.push_back(msg);
        StatusCode::OK
    }
}

#[async_trait]
impl Input for HttpInput {
    async fn connect(&self) -> Result<(), Error> {
        if self.connected.load(Ordering::SeqCst) {
            return Ok(());
        }

        let queue = self.queue.clone();
        let path = self.config.path.clone();
        let address = self.config.address.clone();

        // 创建HTTP服务器
        let app = Router::new()
            .route(&path, post(Self::handle_request))
            .with_state(queue);

        // 解析地址
        let addr: SocketAddr = address.parse().map_err(|e| {
            Error::Config(format!("无效的地址 {}: {}", address, e))
        })?;

        // 启动服务器
        let server_handle = tokio::spawn(async move {
            axum::Server::bind(&addr)
                .serve(app.into_make_service())
                .await
                .map_err(|e| Error::Connection(format!("HTTP服务器错误: {}", e)))
        });

        let server_handle_arc = self.server_handle.clone();
        let mut server_handle_arc_mutex = server_handle_arc.lock().await;
        *server_handle_arc_mutex = Some(server_handle);
        self.connected.store(true, std::sync::atomic::Ordering::SeqCst);

        Ok(())
    }

    async fn read(&self) -> Result<(MessageBatch, Arc<dyn Ack>), Error> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Connection("输入未连接".to_string()));
        }

        // 尝试从队列中获取消息
        let msg_option;
        {
            let mut queue = self.queue.lock().await;
            msg_option = queue.pop_front();
        }

        if let Some(msg) = msg_option {
            Ok((msg, Arc::new(NoopAck)))
        } else {
            // 如果队列为空，则等待一段时间后返回错误
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            Err(Error::Processing("队列为空".to_string()))
        }
    }


    async fn close(&self) -> Result<(), Error> {
        let mut server_handle_guard = self.server_handle.lock().await;
        if let Some(handle) = server_handle_guard.take() {
            handle.abort();
        }

        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }
}