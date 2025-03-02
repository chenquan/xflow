//! MQTT输入组件
//!
//! 从MQTT代理接收数据

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::collections::VecDeque;
use tokio::sync::Mutex;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use rumqttc::{AsyncClient, MqttOptions, QoS, Event, Packet};

use crate::{Error, Message, input::Input};

/// MQTT输入配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttInputConfig {
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
    /// 订阅的主题列表
    pub topics: Vec<String>,
    /// 服务质量等级 (0, 1, 2)
    pub qos: Option<u8>,
    /// 是否清除会话
    pub clean_session: Option<bool>,
    /// 保持连接的时间间隔（秒）
    pub keep_alive: Option<u64>,
}

/// MQTT输入组件
pub struct MqttInput {
    config: MqttInputConfig,
    client: Arc<Mutex<Option<AsyncClient>>>,
    queue: Arc<Mutex<VecDeque<Message>>>,
    connected: AtomicBool,
    eventloop_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
}

impl MqttInput {
    /// 创建一个新的MQTT输入组件
    pub fn new(config: &MqttInputConfig) -> Result<Self, Error> {
        Ok(Self {
            config: config.clone(),
            client: Arc::new(Mutex::new(None)),
            queue: Arc::new(Mutex::new(VecDeque::new())),
            connected: AtomicBool::new(false),
            eventloop_handle: Arc::new(Mutex::new(None)),
        })
    }
}

#[async_trait]
impl Input for MqttInput {
    async fn connect(&self) -> Result<(), Error> {
        if self.connected.load(Ordering::SeqCst) {
            return Ok(());
        }

        // 创建MQTT选项
        let mut mqtt_options = MqttOptions::new(
            &self.config.client_id,
            &self.config.host,
            self.config.port,
        );

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
        
        // 订阅主题
        let qos_level = match self.config.qos {
            Some(0) => QoS::AtMostOnce,
            Some(1) => QoS::AtLeastOnce,
            Some(2) => QoS::ExactlyOnce,
            _ => QoS::AtLeastOnce, // 默认为QoS 1
        };

        for topic in &self.config.topics {
            client.subscribe(topic, qos_level).await
                .map_err(|e| Error::Connection(format!("无法订阅MQTT主题 {}: {}", topic, e)))?;
        }

        // 保存客户端
        let client_arc = self.client.clone();
        let mut client_guard = client_arc.lock().await;
        *client_guard = Some(client);

        // 启动事件循环处理线程
        let queue = self.queue.clone();
        let connected = self.connected.load(Ordering::SeqCst);
        let eventloop_handle = tokio::spawn(async move {
            while connected {
                match eventloop.poll().await {
                    Ok(event) => {
                        if let Event::Incoming(Packet::Publish(publish)) = event {
                            let payload = publish.payload.to_vec();
                            let mut msg = Message::new(payload);
                            
                            // // 添加元数据
                            // let metadata = msg.metadata_mut();
                            // metadata.set("mqtt_topic", &publish.topic);
                            
                            // metadata.set("mqtt_qos", &publish.qos.to_string());
                            // if let Some(pkid) = publish.pkid {
                            //     metadata.set("mqtt_packet_id", &pkid.to_string());
                            // }
                            
                            // 将消息添加到队列
                            let mut queue_guard = queue.lock().await;
                            queue_guard.push_back(msg);
                        }
                    },
                    Err(e) => {
                        // 记录错误并尝试短暂等待后继续
                        eprintln!("MQTT事件循环错误: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    }
                }
            }
        });

        // 保存事件循环处理线程句柄
        let eventloop_handle_arc = self.eventloop_handle.clone();
        let mut eventloop_handle_guard = eventloop_handle_arc.lock().await;
        *eventloop_handle_guard = Some(eventloop_handle);

        self.connected.store(true, Ordering::SeqCst);
        Ok(())
    }

    async fn read(&self) -> Result<Message, Error> {
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
            Ok(msg)
        } else {
            // 如果队列为空，则等待一段时间后返回错误
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
            Err(Error::Processing("队列为空".to_string()))
        }
    }

    async fn acknowledge(&self, _msg: &Message) -> Result<(), Error> {
        // MQTT输入不需要确认机制，因为QoS已经在MQTT协议层处理
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