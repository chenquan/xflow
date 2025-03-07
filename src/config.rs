//! 配置模块
//!
//! 提供流处理引擎的配置管理功能

use serde::{Deserialize, Serialize};
use toml;

use crate::{
    stream::StreamConfig


    ,
    Error,
};

/// 配置文件格式
#[derive(Debug, Clone, Copy)]
pub enum ConfigFormat {
    /// YAML格式
    YAML,
    /// JSON格式
    JSON,
    /// TOML格式
    TOML,
}

/// 日志配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoggingConfig {
    /// 日志级别
    pub level: String,
    /// 是否输出到文件
    pub file_output: Option<bool>,
    /// 日志文件路径
    pub file_path: Option<String>,
}

/// 引擎配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EngineConfig {
    /// 流配置
    pub streams: Vec<StreamConfig>,
    /// 全局HTTP服务器配置（可选）
    pub http: Option<HttpServerConfig>,
    /// 指标收集配置（可选）
    pub metrics: Option<MetricsConfig>,
    /// 日志配置（可选）
    pub logging: Option<LoggingConfig>,
}

/// HTTP服务器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpServerConfig {
    /// 监听地址
    pub address: String,
    /// 是否启用CORS
    pub cors_enabled: bool,
    /// 是否启用健康检查端点
    pub health_enabled: bool,
}

/// 指标收集配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MetricsConfig {
    /// 是否启用指标收集
    pub enabled: bool,
    /// 指标类型（prometheus, statsd等）
    pub type_name: String,
    /// 指标前缀
    pub prefix: Option<String>,
    /// 指标标签（键值对）
    pub tags: Option<std::collections::HashMap<String, String>>,
}

impl EngineConfig {
    /// 从文件加载配置
    pub fn from_file(path: &str) -> Result<Self, Error> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| Error::Config(format!("无法读取配置文件: {}", e)))?;
        
        // 根据文件后缀名判断格式
        if let Some(format) = get_format_from_path(path) {
            match format {
                ConfigFormat::YAML => {
                    return serde_yaml::from_str(&content)
                        .map_err(|e| Error::Config(format!("YAML解析错误: {}", e)));
                },
                ConfigFormat::JSON => {
                    return serde_json::from_str(&content)
                        .map_err(|e| Error::Config(format!("JSON解析错误: {}", e)));
                },
                ConfigFormat::TOML => {
                    return toml::from_str(&content)
                        .map_err(|e| Error::Config(format!("TOML解析错误: {}", e)));
                },
            }
        }
        
        // 如果无法从文件名判断格式，则尝试所有格式
        Self::from_string(&content)
    }
    
    /// 从字符串加载配置
    pub fn from_string(content: &str) -> Result<Self, Error> {
        // 尝试解析为YAML
        if let Ok(config) = serde_yaml::from_str(content) {
            return Ok(config);
        }
        
        // 尝试解析为TOML
        if let Ok(config) = toml::from_str(content) {
            return Ok(config);
        }
        
        // 尝试解析为JSON
        if let Ok(config) = serde_json::from_str(content) {
            return Ok(config);
        }
        
        // 所有格式都解析失败
        Err(Error::Config(format!("无法解析配置文件，支持的格式为：YAML、TOML、JSON")))
    }
    
    /// 保存配置到文件
    pub fn save_to_file(&self, path: &str) -> Result<(), Error> {
        let content = toml::to_string_pretty(self)
            .map_err(|e| Error::Config(format!("无法序列化配置: {}", e)))?;
        
        std::fs::write(path, content)
            .map_err(|e| Error::Config(format!("无法写入配置文件: {}", e)))
    }
}

/// 创建默认配置
pub fn default_config() -> EngineConfig {
    EngineConfig {
        streams: vec![],
        http: Some(HttpServerConfig {
            address: "0.0.0.0:8000".to_string(),
            cors_enabled: false,
            health_enabled: true,
        }),
        metrics: Some(MetricsConfig {
            enabled: true,
            type_name: "prometheus".to_string(),
            prefix: Some("benthos".to_string()),
            tags: None,
        }),
        logging: Some(LoggingConfig {
            level: "info".to_string(),
            file_output: None,
            file_path: None,
        }),
    }
}

/// 从文件路径获取配置格式
fn get_format_from_path(path: &str) -> Option<ConfigFormat> {
    let path = path.to_lowercase();
    if path.ends_with(".yaml") || path.ends_with(".yml") {
        Some(ConfigFormat::YAML)
    } else if path.ends_with(".json") {
        Some(ConfigFormat::JSON)
    } else if path.ends_with(".toml") {
        Some(ConfigFormat::TOML)
    } else {
        None
    }
}