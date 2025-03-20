use crate::errors::AppError;
use std::fs::OpenOptions;
use std::io::Write;
use chrono::Local;

pub fn init_logger() {
    std::fs::create_dir_all("logs").expect("无法创建日志目录");
}

pub fn log_error(err: &AppError) {
    let log_file = match err {
        AppError::DatabaseError(_) => "logs/database_errors.log",
        AppError::ApiError(_) => "logs/api_errors.log",
        AppError::TokenBanned(_) => {
            println!("控制台提示: {}", err);
            "logs/token_banned.log"
        }
        AppError::ConfigError(_) => "logs/config_errors.log",
        AppError::FileError(_) => "logs/file_errors.log",
        AppError::TimeoutError(_) => "logs/timeout_errors.log",
        AppError::InvalidFormat(_) => "logs/invalid_format.log", // 新增
    };

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(log_file)
        .expect("无法打开日志文件");

    let time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let log_entry = match err {
        AppError::DatabaseError(msg) => format!("错误类型: 数据库错误 | 库: tokio_postgres | 详情: {} | 时间: {}\n", msg, time),
        AppError::ApiError(msg) => format!("错误类型: API错误 | 库: reqwest | 详情: {} | 时间: {}\n", msg, time),
        AppError::TokenBanned(msg) => format!("错误类型: Token被禁用 | 库: 无 | 详情: {} | 时间: {}\n", msg, time),
        AppError::ConfigError(msg) => format!("错误类型: 配置错误 | 库: 无 | 详情: {} | 时间: {}\n", msg, time),
        AppError::FileError(msg) => format!("错误类型: 文件错误 | 库: 无 | 详情: {} | 时间: {}\n", msg, time),
        AppError::TimeoutError(msg) => format!("错误类型: 超时错误 | 库: 无 | 详情: {} | 时间: {}\n", msg, time),
        AppError::InvalidFormat(msg) => format!("错误类型: 格式错误 | 库: 无 | 详情: {} | 时间: {}\n", msg, time), // 新增
    };

    file.write_all(log_entry.as_bytes()).expect("无法写入日志");
}

pub fn log_stats(success_requests: usize, available_tokens: usize) {
    let time = Local::now().format("%Y-%m-%d %H:%M:%S").to_string();
    let log_entry = format!(
        "[{}] 每分钟统计 - 成功请求数: {}, 可用Token数: {}\n",
        time, success_requests, available_tokens
    );

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open("logs/stats.log")
        .expect("无法打开统计日志文件");

    file.write_all(log_entry.as_bytes()).expect("无法写入统计日志");
}