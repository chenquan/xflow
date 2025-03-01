//! 插件系统模块
//!
//! 提供流处理引擎的插件扩展功能

use std::any::Any;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use crate::{
    Error,
    buffer::Buffer,
    input::Input,
    output::Output,
    processor::Processor,
};

/// 插件类型
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PluginType {
    /// 输入插件
    Input,
    /// 输出插件
    Output,
    /// 处理器插件
    Processor,
    /// 缓冲区插件
    Buffer,
}

/// 插件注册表
pub struct PluginRegistry {
    inputs: Mutex<HashMap<String, Box<dyn Fn() -> Box<dyn Input> + Send + Sync>>>,
    outputs: Mutex<HashMap<String, Box<dyn Fn() -> Box<dyn Output> + Send + Sync>>>,
    processors: Mutex<HashMap<String, Box<dyn Fn() -> Box<dyn Processor> + Send + Sync>>>,
    buffers: Mutex<HashMap<String, Box<dyn Fn() -> Box<dyn Buffer> + Send + Sync>>>,
}

impl PluginRegistry {
    /// 创建一个新的插件注册表
    pub fn new() -> Self {
        Self {
            inputs: Mutex::new(HashMap::new()),
            outputs: Mutex::new(HashMap::new()),
            processors: Mutex::new(HashMap::new()),
            buffers: Mutex::new(HashMap::new()),
        }
    }
    
    /// 注册输入插件
    pub fn register_input<F>(&self, name: &str, factory: F) -> Result<(), Error>
    where
        F: Fn() -> Box<dyn Input> + Send + Sync + 'static,
    {
        let mut inputs = self.inputs.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        if inputs.contains_key(name) {
            return Err(Error::Config(format!("输入插件 '{}' 已存在", name)));
        }
        inputs.insert(name.to_string(), Box::new(factory));
        Ok(())
    }
    
    /// 注册输出插件
    pub fn register_output<F>(&self, name: &str, factory: F) -> Result<(), Error>
    where
        F: Fn() -> Box<dyn Output> + Send + Sync + 'static,
    {
        let mut outputs = self.outputs.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        if outputs.contains_key(name) {
            return Err(Error::Config(format!("输出插件 '{}' 已存在", name)));
        }
        outputs.insert(name.to_string(), Box::new(factory));
        Ok(())
    }
    
    /// 注册处理器插件
    pub fn register_processor<F>(&self, name: &str, factory: F) -> Result<(), Error>
    where
        F: Fn() -> Box<dyn Processor> + Send + Sync + 'static,
    {
        let mut processors = self.processors.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        if processors.contains_key(name) {
            return Err(Error::Config(format!("处理器插件 '{}' 已存在", name)));
        }
        processors.insert(name.to_string(), Box::new(factory));
        Ok(())
    }
    
    /// 注册缓冲区插件
    pub fn register_buffer<F>(&self, name: &str, factory: F) -> Result<(), Error>
    where
        F: Fn() -> Box<dyn Buffer> + Send + Sync + 'static,
    {
        let mut buffers = self.buffers.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        if buffers.contains_key(name) {
            return Err(Error::Config(format!("缓冲区插件 '{}' 已存在", name)));
        }
        buffers.insert(name.to_string(), Box::new(factory));
        Ok(())
    }
    
    /// 创建输入插件实例
    pub fn create_input(&self, name: &str) -> Result<Box<dyn Input>, Error> {
        let inputs = self.inputs.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        if let Some(factory) = inputs.get(name) {
            Ok(factory())
        } else {
            Err(Error::Config(format!("输入插件 '{}' 不存在", name)))
        }
    }
    
    /// 创建输出插件实例
    pub fn create_output(&self, name: &str) -> Result<Box<dyn Output>, Error> {
        let outputs = self.outputs.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        if let Some(factory) = outputs.get(name) {
            Ok(factory())
        } else {
            Err(Error::Config(format!("输出插件 '{}' 不存在", name)))
        }
    }
    
    /// 创建处理器插件实例
    pub fn create_processor(&self, name: &str) -> Result<Box<dyn Processor>, Error> {
        let processors = self.processors.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        if let Some(factory) = processors.get(name) {
            Ok(factory())
        } else {
            Err(Error::Config(format!("处理器插件 '{}' 不存在", name)))
        }
    }
    
    /// 创建缓冲区插件实例
    pub fn create_buffer(&self, name: &str) -> Result<Box<dyn Buffer>, Error> {
        let buffers = self.buffers.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        if let Some(factory) = buffers.get(name) {
            Ok(factory())
        } else {
            Err(Error::Config(format!("缓冲区插件 '{}' 不存在", name)))
        }
    }
    
    /// 获取所有已注册的插件名称
    pub fn list_plugins(&self) -> Result<HashMap<PluginType, Vec<String>>, Error> {
        let mut result = HashMap::new();
        
        let inputs = self.inputs.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        result.insert(PluginType::Input, inputs.keys().cloned().collect());
        
        let outputs = self.outputs.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        result.insert(PluginType::Output, outputs.keys().cloned().collect());
        
        let processors = self.processors.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        result.insert(PluginType::Processor, processors.keys().cloned().collect());
        
        let buffers = self.buffers.lock().map_err(|e| Error::Unknown(e.to_string()))?;
        result.insert(PluginType::Buffer, buffers.keys().cloned().collect());
        
        Ok(result)
    }
}

/// 全局插件注册表实例
lazy_static::lazy_static! {
    static ref GLOBAL_REGISTRY: Arc<PluginRegistry> = Arc::new(PluginRegistry::new());
}

/// 获取全局插件注册表
pub fn global_registry() -> Arc<PluginRegistry> {
    GLOBAL_REGISTRY.clone()
}

/// 插件特征，所有插件必须实现此特征
pub trait Plugin: Any + Send + Sync {
    /// 获取插件名称
    fn name(&self) -> &str;
    
    /// 获取插件类型
    fn plugin_type(&self) -> PluginType;
    
    /// 注册插件到注册表
    fn register(&self, registry: &PluginRegistry) -> Result<(), Error>;
}