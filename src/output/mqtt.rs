//! MQTT输出组件
//!
//! 将处理后的数据发送到MQTT代理

use crate::{output::Output, Error, MessageBatch};
use async_trait::async_trait;
use rumqttc::{AsyncClient, MqttOptions, QoS};
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use tokio::sync::Mutex;
use tracing::info;

/// MQTT输出配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttOutputConfig {
    /// MQTT代理地址
    pub host: String,
    /// MQTT代理端口
    pub port: u16,
    /// 客户端ID
    pub client_id: String,
    /// 用户名（可选）
    pub username: Option<String>,
    /// 密码（可选）
    pub password: Option<String>,
    /// 发布的主题
    pub topic: String,
    /// 服务质量等级 (0, 1, 2)
    pub qos: Option<u8>,
    /// 是否清除会话
    pub clean_session: Option<bool>,
    /// 保持连接的时间间隔（秒）
    pub keep_alive: Option<u64>,
    /// 是否保留消息
    pub retain: Option<bool>,
}

/// MQTT输出组件
pub struct MqttOutput {
    config: MqttOutputConfig,
    client: Arc<Mutex<Option<AsyncClient>>>,
    connected: AtomicBool,
    eventloop_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl MqttOutput {
    /// 创建一个新的MQTT输出组件
    pub fn new(config: &MqttOutputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            client: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
            eventloop_handle: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl Output for MqttOutput {
    async fn connect(&self) -> Result<(), Error> {
        // 创建MQTT选项
        let mut mqtt_options =
            MqttOptions::new(&self.config.client_id, &self.config.host, self.config.port);

        // 设置认证信息
        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            mqtt_options.set_credentials(username, password);
        }

        // 设置保持连接时间
        if let Some(keep_alive) = self.config.keep_alive {
            mqtt_options.set_keep_alive(std::time::Duration::from_secs(keep_alive));
        }

        // 设置清除会话
        if let Some(clean_session) = self.config.clean_session {
            mqtt_options.set_clean_session(clean_session);
        }

        // 创建MQTT客户端
        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 10);

        // 保存客户端
        let client_arc = self.client.clone();
        let mut client_guard = client_arc.lock().await;
        *client_guard = Some(client);

        // 启动事件循环处理线程（保持连接活跃）
        let eventloop_handle = tokio::spawn(async move {
            while let Ok(_) = eventloop.poll().await {
                // 只需保持事件循环运行，不需要处理事件
            }
        });

        // 保存事件循环处理线程句柄
        let eventloop_handle_arc = self.eventloop_handle.clone();
        let mut eventloop_handle_guard = eventloop_handle_arc.lock().await;
        *eventloop_handle_guard = Some(eventloop_handle);

        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn write(&self, msg: &MessageBatch) -> Result<(), Error> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Connection("输出未连接".to_string()));
        }

        let client_arc = self.client.clone();
        let client_guard = client_arc.lock().await;
        let client = client_guard
            .as_ref()
            .ok_or_else(|| Error::Connection("MQTT客户端未初始化".to_string()))?;

        // 获取消息内容
        let payloads = match msg.as_string() {
            Ok(v) => v.to_vec(),
            Err(e) => {
                return Err(e);
            }
        };

        for payload in payloads {
            info!(
                "Send message: {}",
                &String::from_utf8_lossy((&payload).as_ref())
            );

            // 确定QoS级别
            let qos_level = match self.config.qos {
                Some(0) => QoS::AtMostOnce,
                Some(1) => QoS::AtLeastOnce,
                Some(2) => QoS::ExactlyOnce,
                _ => QoS::AtLeastOnce, // 默认为QoS 1
            };

            // 确定是否保留消息
            let retain = self.config.retain.unwrap_or(false);

            // 发布消息
            client
                .publish(&self.config.topic, qos_level, retain, payload)
                .await
                .map_err(|e| Error::Processing(format!("MQTT发布失败: {}", e)))?;
        }

        Ok(())
    }

    async fn close(&self) -> Result<(), Error> {
        // 停止事件循环处理线程
        let mut eventloop_handle_guard = self.eventloop_handle.lock().await;
        if let Some(handle) = eventloop_handle_guard.take() {
            handle.abort();
        }

        // 断开MQTT连接
        let client_arc = self.client.clone();
        let client_guard = client_arc.lock().await;
        if let Some(client) = &*client_guard {
            // 尝试断开连接，但不等待结果
            let _ = client.disconnect().await;
        }

        self.connected.store(false, Ordering::SeqCst);
        Ok(())
    }
}
