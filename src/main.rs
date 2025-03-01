//! Rust流处理引擎示例程序

use tracing::{error, info};
use rtflow::{
    input::{memory::MemoryInputConfig, InputConfig},
    output::{stdout::StdoutOutputConfig, OutputConfig},
    pipeline::PipelineConfig,
    stream::StreamConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 设置日志
    tracing_subscriber::fmt::init();
    
    println!("启动Rust流处理引擎示例...");
    
    // 创建内存输入配置
    let input_config = InputConfig::Memory(MemoryInputConfig {
        messages: Some(vec![
            "{\"name\":\"张三\",\"age\":30}".to_string(),
            "{\"name\":\"李四\",\"age\":25}".to_string(),
            "{\"name\":\"王五\",\"age\":35}".to_string(),
        ]),
    });
    
    // 创建标准输出配置
    let output_config = OutputConfig::Stdout(StdoutOutputConfig {
        append_newline: true,
    });
    
    // 创建管道配置（暂时为空，没有处理器）
    let pipeline_config = PipelineConfig {
        thread_num: 0,
        processors: vec![],
    };
    
    // 创建流配置
    let stream_config = StreamConfig {
        input: input_config,
        pipeline: pipeline_config,
        output: output_config,
        buffer: None,
    };
    
    // 构建流
    let mut stream = stream_config.build()?;
    
    // 运行流处理
    info!("开始处理消息...");
    match stream.run().await {
        Ok(_) => info!("流处理成功完成"),
        Err(e) => error!("流处理出错: {}", e),
    }
    
    // 关闭流
    stream.close().await?;
    
    println!("流处理引擎示例结束");
    Ok(())
}
