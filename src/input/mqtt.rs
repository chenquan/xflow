//! MQTT输入组件
//!
//! 从MQTT代理接收数据

use async_trait::async_trait;
use rumqttc::{AsyncClient, ClientError, Event, MqttOptions, Packet, Publish, QoS};
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::future::Future;
use std::string::FromUtf8Error;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use flume::{Receiver, RecvError, SendError, Sender};
use tokio::sync::Mutex;
use tracing::{error, info};
use crate::{input::Input, Content, Error, Message};
use crate::input::Ack;

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
    connected: AtomicBool,
    eventloop_handle: Arc<Mutex<Option<tokio::task::JoinHandle<()>>>>,
    sender: Arc<Sender<Publish>>,
    receiver: Arc<Receiver<Publish>>,
}

impl MqttInput {
    /// 创建一个新的MQTT输入组件
    pub fn new(config: &MqttInputConfig) -> Result<Self, Error> {
        let (sender, receiver) = flume::bounded::<Publish>(1000);
        Ok(Self {
            config: config.clone(),
            client: Arc::new(Mutex::new(None)),
            connected: AtomicBool::new(false),
            eventloop_handle: Arc::new(Mutex::new(None)),
            sender: Arc::new(sender),
            receiver: Arc::new(receiver),
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
        let mut mqtt_options =
            MqttOptions::new(&self.config.client_id, &self.config.host, self.config.port);
        mqtt_options.set_manual_acks(true);
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
            client
                .subscribe(topic, qos_level)
                .await
                .map_err(|e| Error::Connection(format!("无法订阅MQTT主题 {}: {}", topic, e)))?;
        }

        self.connected.store(true, Ordering::SeqCst);


        // 保存客户端
        let client_arc = self.client.clone();
        let mut client_guard = client_arc.lock().await;
        *client_guard = Some(client);

        // 启动事件循环处理线程
        let connected = self.connected.load(Ordering::SeqCst);
        let sender_arc = self.sender.clone();
        let eventloop_handle = tokio::spawn(async move {
            while connected {
                match eventloop.poll().await {
                    Ok(event) => {
                        if let Event::Incoming(Packet::Publish(publish)) = event {

                            // 将消息添加到队列
                            match sender_arc.send_async(publish).await {
                                Ok(_) => {}
                                Err(e) => {
                                    error!("{}",e)
                                }
                            };
                        }
                    }
                    Err(e) => {
                        // 记录错误并尝试短暂等待后继续
                        error!("MQTT事件循环错误: {}", e);
                        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;
                    }
                }
            }
            drop(sender_arc);
        });

        // 保存事件循环处理线程句柄
        let eventloop_handle_arc = self.eventloop_handle.clone();
        let mut eventloop_handle_guard = eventloop_handle_arc.lock().await;
        *eventloop_handle_guard = Some(eventloop_handle);

        Ok(())
    }

    async fn read(&self) -> Result<(Message, Arc<dyn Ack>), Error> {
        if !self.connected.load(Ordering::SeqCst) {
            return Err(Error::Connection("输入未连接".to_string()));
        }


        match self.receiver.recv() {
            Ok(publish) => {
                let payload = publish.payload.to_vec();
                let msg = Message::new_binary(payload);
                Ok((msg, Arc::new(MqttAck {
                    client: self.client.clone(),
                    publish,
                })))
            }
            Err(_) => {
                Err(Error::Done)
            }
        }
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


struct MqttAck {
    client: Arc<Mutex<Option<AsyncClient>>>,
    publish: Publish,
}
#[async_trait]
impl Ack for MqttAck {
    async fn ack(&self) {
        let mutex_guard = self.client.lock().await;
        if let Some(client) = &*mutex_guard {
            if let Err(e) = client.ack(&self.publish).await {
                error!("{}",e);
            }
        }
    }
}