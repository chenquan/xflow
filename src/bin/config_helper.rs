//! 配置助手工具
//!
//! 提供命令行工具，帮助用户生成、验证和管理配置文件

use std::path::Path;
use std::process;

use clap::{Parser, Subcommand};
use colored::Colorize;

use rust_stream_engine::config::{ConfigError, ConfigFormat, EngineConfig};

#[derive(Parser)]
#[command(name = "配置助手")]
#[command(author = "Chen Quan")]
#[command(version = "0.1.0")]
#[command(about = "Rust流处理引擎配置助手工具", long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// 生成示例配置文件
    Generate {
        /// 输出文件路径
        #[arg(short, long)]
        output: String,
        
        /// 输出格式 (yaml, json, toml)
        #[arg(short, long, default_value = "yaml")]
        format: String,
    },
    
    /// 验证配置文件
    Validate {
        /// 配置文件路径
        #[arg(short, long)]
        config: String,
    },
    
    /// 转换配置文件格式
    Convert {
        /// 输入配置文件路径
        #[arg(short, long)]
        input: String,
        
        /// 输出配置文件路径
        #[arg(short, long)]
        output: String,
        
        /// 输出格式 (yaml, json, toml)
        #[arg(short, long)]
        format: String,
    },
}

fn main() {
    let cli = Cli::parse();
    
    match &cli.command {
        Commands::Generate { output, format } => {
            if let Err(e) = generate_config(output, format) {
                eprintln!("{}: {}", "错误".red().bold(), e);
                process::exit(1);
            }
        },
        Commands::Validate { config } => {
            if let Err(e) = validate_config(config) {
                eprintln!("{}: {}", "错误".red().bold(), e);
                process::exit(1);
            }
        },
        Commands::Convert { input, output, format } => {
            if let Err(e) = convert_config(input, output, format) {
                eprintln!("{}: {}", "错误".red().bold(), e);
                process::exit(1);
            }
        },
    }
}

/// 生成示例配置文件
fn generate_config(output: &str, format_str: &str) -> Result<(), ConfigError> {
    println!("正在生成示例配置文件: {}", output);
    
    let config = EngineConfig::generate_example();
    let format = parse_format(format_str)?;
    
    config.save_to_file(output, format)?;
    
    println!("{} 示例配置已生成: {}", "成功".green().bold(), output);
    Ok(())
}

/// 验证配置文件
fn validate_config(config_path: &str) -> Result<(), ConfigError> {
    println!("正在验证配置文件: {}", config_path);
    
    let config = EngineConfig::from_file(config_path)?;
    
    // 配置已经在加载过程中验证过了，如果能走到这一步，说明配置有效
    println!("{} 配置文件有效", "成功".green().bold());
    
    // 打印配置摘要
    println!("\n配置摘要:");
    println!("  流数量: {}", config.streams.len());
    
    for (i, stream) in config.streams.iter().enumerate() {
        println!("  流 #{}", i + 1);
        
        // 打印输入类型
        match &stream.input {
            rust_stream_engine::input::InputConfig::File(_) => println!("    输入: 文件"),
            rust_stream_engine::input::InputConfig::Http(_) => println!("    输入: HTTP"),
            rust_stream_engine::input::InputConfig::Kafka(_) => println!("    输入: Kafka"),
            rust_stream_engine::input::InputConfig::Memory(_) => println!("    输入: 内存"),
        }
        
        // 打印处理器数量
        println!("    处理器数量: {}", stream.pipeline.processors.len());
        
        // 打印输出类型
        match &stream.output {
            rust_stream_engine::output::OutputConfig::File(_) => println!("    输出: 文件"),
            rust_stream_engine::output::OutputConfig::Http(_) => println!("    输出: HTTP"),
            rust_stream_engine::output::OutputConfig::Kafka(_) => println!("    输出: Kafka"),
            rust_stream_engine::output::OutputConfig::Redis(_) => println!("    输出: Redis"),
            rust_stream_engine::output::OutputConfig::Stdout(_) => println!("    输出: 标准输出"),
        }
        
        // 打印是否有缓冲区
        if stream.buffer.is_some() {
            println!("    缓冲区: 是");
        } else {
            println!("    缓冲区: 否");
        }
    }
    
    Ok(())
}

/// 转换配置文件格式
fn convert_config(input: &str, output: &str, format_str: &str) -> Result<(), ConfigError> {
    println!("正在转换配置文件: {} -> {}", input, output);
    
    let config = EngineConfig::from_file(input)?;
    let format = parse_format(format_str)?;
    
    config.save_to_file(output, format)?;
    
    println!("{} 配置已转换: {}", "成功".green().bold(), output);
    Ok(())
}

/// 解析格式字符串
fn parse_format(format_str: &str) -> Result<ConfigFormat, ConfigError> {
    match format_str.to_lowercase().as_str() {
        "yaml" | "yml" => Ok(ConfigFormat::YAML),
        "json" => Ok(ConfigFormat::JSON),
        "toml" => Ok(ConfigFormat::TOML),
        _ => Err(ConfigError::UnknownFormat(format!(
            "未知格式: {}，支持的格式: yaml, json, toml", format_str
        ))),
    }
}