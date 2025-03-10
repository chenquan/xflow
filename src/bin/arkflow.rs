//! 流处理引擎启动器
//!
//! 这个模块提供了一个命令行工具，用于根据配置文件启动流处理引擎。
//! 支持从YAML、JSON或TOML格式的配置文件加载配置。

use std::process;

use clap::{Arg, Command};
use tracing::{error, info, Level};
use tracing_subscriber::FmtSubscriber;

use arkflow::config::EngineConfig;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 解析命令行参数
    let matches = Command::new("Rust流处理引擎")
        .version("0.1.0")
        .author("Rust流处理引擎团队")
        .about("高性能、可靠且易于扩展的数据流处理系统")
        .arg(
            Arg::new("config")
                .short('c')
                .long("config")
                .value_name("FILE")
                .help("指定配置文件路径")
                .required(true),
        )
        .arg(
            Arg::new("validate")
                .short('v')
                .long("validate")
                .help("仅验证配置文件，不启动引擎")
                .action(clap::ArgAction::SetTrue),
        )
        .get_matches();

    // 获取配置文件路径
    let config_path = matches.get_one::<String>("config").unwrap();

    // 加载配置
    let config = match EngineConfig::from_file(config_path) {
        Ok(config) => {
            println!("成功加载配置文件: {}", config_path);
            config
        }
        Err(e) => {
            println!("加载配置文件失败: {}", e);
            process::exit(1);
        }
    };

    // 如果只是验证配置，则退出
    if matches.get_flag("validate") {
        info!("配置文件验证通过");
        return Ok(());
    }

    // 初始化日志系统
    init_logging(&config);

    // 创建并运行所有流
    let mut streams = Vec::new();
    let mut handles = Vec::new();

    for (i, stream_config) in config.streams.iter().enumerate() {
        info!("Initializing flow #{}", i + 1);

        match stream_config.build() {
            Ok(stream) => {
                streams.push(stream);
            }
            Err(e) => {
                error!("Initializing flow #{} error: {}", i + 1, e);
                process::exit(1);
            }
        }
    }

    // 启动所有流
    for (i, mut stream) in streams.into_iter().enumerate() {
        info!("Starting flow #{}", i + 1);

        // 在新的任务中运行流
        let handle = tokio::spawn(async move {
            match stream.run().await {
                Ok(_) => info!("Flow #{} completed successfully", i + 1),
                Err(e) => {
                    error!("Stream #{} ran with error: {}", i + 1, e)
                }
            }
        });

        handles.push(handle);
    }

    // 等待所有流完成
    for handle in handles {
        handle.await?;
    }

    info!("All flow tasks have been complete");
    Ok(())
}

/// 初始化日志系统
fn init_logging(config: &EngineConfig) -> () {
    let log_level = if let Some(logging) = &config.logging {
        match logging.level.as_str() {
            "trace" => Level::TRACE,
            "debug" => Level::DEBUG,
            "info" => Level::INFO,
            "warn" => Level::WARN,
            "error" => Level::ERROR,
            _ => Level::INFO,
        }
    } else {
        Level::INFO
    };

    let subscriber = FmtSubscriber::builder().with_max_level(log_level).finish();

    tracing::subscriber::set_global_default(subscriber)
        .expect("You can't set a global default log subscriber");
}
