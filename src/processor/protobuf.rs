//! Protobuf处理器组件
//!
//! 用于在Protobuf数据和Arrow格式之间进行转换的处理器

use std::{fs, io};
use std::path::Path;
use std::sync::Arc;
use async_trait::async_trait;
use datafusion::arrow;
use serde::{Deserialize, Serialize};
use datafusion::arrow::array::{Array, ArrayRef, BinaryArray, BooleanArray, Float32Array, Float64Array, Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::parquet::data_type::AsBytes;
use prost_reflect::{DynamicMessage, Kind, MessageDescriptor, Value};
use prost_reflect::prost::Message;
use prost_reflect::prost_types::FileDescriptorSet;

use protobuf::Message as ProtobufMessage;
use tracing::info;
use crate::{Error, MessageBatch, Content};
use crate::processor::Processor;

/// Protobuf格式转换处理器配置
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProtobufProcessorConfig {
    /// Protobuf消息类型描述符文件路径
    pub proto_inputs: Vec<String>,
    pub proto_includes: Option<Vec<String>>,
    /// Protobuf消息类型名称
    pub message_type: String,
}

/// Protobuf格式转换处理器
pub struct ProtobufProcessor {
    config: ProtobufProcessorConfig,
    descriptor: MessageDescriptor,
}

impl ProtobufProcessor {
    /// 创建一个新的Protobuf格式转换处理器
    pub fn new(config: &ProtobufProcessorConfig) -> Result<Self, Error> {

        // 检查文件扩展名，判断是proto文件还是二进制描述符文件
        let file_descriptor_set = Self::parse_proto_file(&config)?;

        let descriptor_pool = prost_reflect::DescriptorPool::from_file_descriptor_set(file_descriptor_set)
            .map_err(|e| Error::Config(format!("无法创建Protobuf描述符池: {}", e)))?;

        let message_descriptor = descriptor_pool.get_message_by_name(&config.message_type)
            .ok_or_else(|| Error::Config(format!("找不到消息类型: {}", config.message_type)))?;

        Ok(Self {
            config: config.clone(),
            descriptor: message_descriptor,
        })
    }

    /// 从.proto文件解析并生成FileDescriptorSet
    fn parse_proto_file(c: &&ProtobufProcessorConfig) -> Result<FileDescriptorSet, Error> {
        let mut proto_inputs: Vec<String> = vec![];
        for x in &c.proto_inputs {
            let files_in_dir_result = list_files_in_dir(x).map_err(|e| Error::Config(format!("列出proto文件失败: {}", e)))?;
            proto_inputs.extend(
                files_in_dir_result.iter()
                    .filter(|path| path.ends_with(".proto"))
                    .map(|path| x.to_string() + path).collect::<Vec<_>>()
            )
        }
        let proto_includes = c.proto_includes.clone().unwrap_or(c.proto_inputs.clone());

        // 使用protobuf_parse库解析proto文件
        let file_descriptor_protos = protobuf_parse::Parser::new()
            .pure()
            .inputs(proto_inputs)
            .includes(proto_includes)
            .parse_and_typecheck()
            .map_err(|e| Error::Config(format!("解析proto文件失败: {}", e)))?
            .file_descriptors;

        if file_descriptor_protos.is_empty() {
            return Err(Error::Config("解析proto文件未产生任何描述符".to_string()));
        }

        // 将FileDescriptorProto转换为FileDescriptorSet
        let mut file_descriptor_set = prost_reflect::prost_types::FileDescriptorSet {
            file: Vec::new(),
        };

        for proto in file_descriptor_protos {
            // 将protobuf库的FileDescriptorProto转换为prost_types的FileDescriptorProto
            let proto_bytes = proto.write_to_bytes()
                .map_err(|e| Error::Config(format!("序列化FileDescriptorProto失败: {}", e)))?;

            let prost_proto = prost_reflect::prost_types::FileDescriptorProto::decode(proto_bytes.as_slice())
                .map_err(|e| Error::Config(format!("转换FileDescriptorProto失败: {}", e)))?;

            file_descriptor_set.file.push(prost_proto);
        }

        Ok(file_descriptor_set)
    }

    /// 将Protobuf数据转换为Arrow格式
    fn protobuf_to_arrow(&self, data: &[u8]) -> Result<RecordBatch, Error> {
        // 解析Protobuf消息
        let proto_msg = DynamicMessage::decode(self.descriptor.clone(), data)
            .map_err(|e| Error::Processing(format!("Protobuf消息解析失败: {}", e)))?;

        // 构建Arrow Schema
        let mut fields = Vec::new();
        let mut columns: Vec<ArrayRef> = Vec::new();

        // 遍历Protobuf消息的所有字段
        for field in self.descriptor.fields() {
            let field_name = field.name();

            let field_value_opt = proto_msg.get_field_by_name(field_name);
            if field_value_opt.is_none() {
                continue;
            }
            let field_value = field_value_opt.unwrap();
            match field_value.as_ref() {
                Value::Bool(value) => {
                    fields.push(Field::new(field_name, DataType::Boolean, false));
                    columns.push(Arc::new(BooleanArray::from(vec![value.clone()])));
                }
                Value::I32(value) => {
                    fields.push(Field::new(field_name, DataType::Int32, false));
                    columns.push(Arc::new(Int32Array::from(vec![value.clone()])));
                }
                Value::I64(value) => {
                    fields.push(Field::new(field_name, DataType::Int64, false));
                    columns.push(Arc::new(Int64Array::from(vec![value.clone()])));
                }
                Value::U32(value) => {
                    fields.push(Field::new(field_name, DataType::UInt32, false));
                    columns.push(Arc::new(UInt32Array::from(vec![value.clone()])));
                }
                Value::U64(value) => {
                    fields.push(Field::new(field_name, DataType::UInt64, false));
                    columns.push(Arc::new(UInt64Array::from(vec![value.clone()])));
                }
                Value::F32(value) => {
                    fields.push(Field::new(field_name, DataType::Float32, false));
                    columns.push(Arc::new(Float32Array::from(vec![value.clone()])))
                }
                Value::F64(value) => {
                    fields.push(Field::new(field_name, DataType::Float64, false));
                    columns.push(Arc::new(Float64Array::from(vec![value.clone()])));
                }
                Value::String(value) => {
                    fields.push(Field::new(field_name, DataType::Utf8, false));
                    columns.push(Arc::new(StringArray::from(vec![value.clone()])));
                }
                Value::Bytes(value) => {
                    fields.push(Field::new(field_name, DataType::Binary, false));
                    columns.push(Arc::new(BinaryArray::from(vec![value.as_bytes()])));
                }
                Value::EnumNumber(value) => {
                    fields.push(Field::new(field_name, DataType::Int32, false));
                    columns.push(Arc::new(Int32Array::from(vec![value.clone()])));
                }
                _ => {
                    return Err(Error::Processing(format!("不支持的字段类型: {}", field_name)));
                }
                // Value::Message(_) => {}
                // Value::List(_) => {}
                // Value::Map(_) => {}
            }
        }

        // 创建RecordBatch
        let schema = Arc::new(Schema::new(fields));
        RecordBatch::try_new(schema, columns)
            .map_err(|e| Error::Processing(format!("创建Arrow记录批次失败: {}", e)))
    }

    /// 将Arrow格式转换为Protobuf
    fn arrow_to_protobuf(&self, batch: &RecordBatch) -> Result<Vec<u8>, Error> {
        // 创建一个新的动态消息
        let mut proto_msg = DynamicMessage::new(self.descriptor.clone());

        // 获取Arrow的schema
        let schema = batch.schema();

        // 确保只有一行数据
        if batch.num_rows() != 1 {
            return Err(Error::Processing("只支持单行Arrow数据转换为Protobuf".to_string()));
        }

        // 遍历所有列，将数据填充到Protobuf消息中
        for (i, field) in schema.fields().iter().enumerate() {
            let field_name = field.name();

            // 检查Protobuf消息是否有对应的字段
            if let Some(proto_field) = self.descriptor.get_field_by_name(field_name) {
                let column = batch.column(i);

                match proto_field.kind() {
                    prost_reflect::Kind::Bool => {
                        if let Some(value) = column.as_any().downcast_ref::<BooleanArray>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::Bool(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Int32 | prost_reflect::Kind::Sint32 | prost_reflect::Kind::Sfixed32 => {
                        if let Some(value) = column.as_any().downcast_ref::<Int32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::I32(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Int64 | prost_reflect::Kind::Sint64 | prost_reflect::Kind::Sfixed64 => {
                        if let Some(value) = column.as_any().downcast_ref::<Int64Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::I64(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Uint32 | prost_reflect::Kind::Fixed32 => {
                        if let Some(value) = column.as_any().downcast_ref::<UInt32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::U32(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Uint64 | prost_reflect::Kind::Fixed64 => {
                        if let Some(value) = column.as_any().downcast_ref::<UInt64Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::U64(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Float => {
                        if let Some(value) = column.as_any().downcast_ref::<Float32Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::F32(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::Double => {
                        if let Some(value) = column.as_any().downcast_ref::<Float64Array>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::F64(value.value(0)));
                            }
                        }
                    }
                    prost_reflect::Kind::String => {
                        if let Some(value) = column.as_any().downcast_ref::<StringArray>() {
                            if value.len() > 0 {
                                proto_msg.set_field_by_name(field_name, Value::String(value.value(0).to_string()));
                            }
                        }
                    }
                    // 对于其他类型，可以根据需要添加更多处理
                    _ => return Err(Error::Processing(format!("不支持的Protobuf类型: {:?}", proto_field.kind()))),
                    // Kind::Bytes => {}
                    // Kind::Message(_) => {}
                    // Kind::Enum(_) => {}
                }
            }
        }

        // 编码Protobuf消息
        let mut buf = Vec::new();
        proto_msg.encode(&mut buf)
            .map_err(|e| Error::Processing(format!("Protobuf编码失败: {}", e)))?;

        Ok(buf)
    }
}


#[async_trait]
impl Processor for ProtobufProcessor {
    async fn process(&self, msg: MessageBatch) -> Result<Vec<MessageBatch>, Error> {
        if msg.is_empty() {
            return Ok(vec![]);
        }
        match msg.content {
            Content::Arrow(v) => {
                // 将Arrow格式转换为Protobuf
                let proto_data = self.arrow_to_protobuf(&v)?;

                // 创建新消息，保留原始元数据
                let new_msg = MessageBatch::new_binary(vec![proto_data]);

                Ok(vec![new_msg])
            }
            Content::Binary(v) => {
                if v.is_empty() {
                    return Ok(vec![]);
                }
                let mut batches = Vec::with_capacity(v.len());
                for x in v {
                    // 将Protobuf消息转换为Arrow格式
                    let batch = self.protobuf_to_arrow(&x)?;
                    batches.push(batch)
                }

                let schema = batches[0].schema();
                let batch = arrow::compute::concat_batches(&schema, &batches)
                    .map_err(|e| Error::Processing(format!("合并批次失败: {}", e)))?;
                Ok(vec![MessageBatch::new_arrow(batch)])
            }
        }
    }

    async fn close(&self) -> Result<(), Error> {
        // 无需特殊清理
        Ok(())
    }
}

fn list_files_in_dir<P: AsRef<Path>>(dir: P) -> io::Result<Vec<String>> {
    let mut files = Vec::new();
    if dir.as_ref().is_dir() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.is_file() {
                if let Some(file_name) = path.file_name() {
                    if let Some(file_name_str) = file_name.to_str() {
                        files.push(file_name_str.to_string());
                    }
                }
            }
        }
    }
    Ok(files)
}