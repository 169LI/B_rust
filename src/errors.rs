use std::fmt;
use deadpool_postgres::CreatePoolError;

#[allow(dead_code)]
#[derive(Debug)]
pub enum AppError {
    DatabaseError(String),
    ConfigError(String),
    FileError(String),
    ApiError(String),
    TimeoutError(String),
    TokenBanned(String),
    InvalidFormat(String),
}

impl AppError {
    pub fn retry_after(&self) -> Option<u64> {
        match self {
            AppError::ApiError(msg) => {
                if msg.contains("速率限制，需等待") {
                    msg.split_whitespace().nth(2).and_then(|s| s.parse::<u64>().ok())
                } else if msg.contains("主要速率限制，需等待") {
                    msg.split_whitespace().nth(2).and_then(|s| s.parse::<u64>().ok())
                } else if msg.contains("次要速率限制") {
                    Some(60) // 默认 1 分钟
                } else {
                    None
                }
            }
            _ => None,
        }
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            AppError::DatabaseError(msg) => write!(f, "数据库错误: {}", msg),
            AppError::ConfigError(msg) => write!(f, "配置错误: {}", msg),
            AppError::FileError(msg) => write!(f, "文件错误: {}", msg),
            AppError::ApiError(msg) => write!(f, "API错误: {}", msg),
            AppError::TimeoutError(msg) => write!(f, "超时错误: {}", msg),
            AppError::TokenBanned(msg) => write!(f, "Token被禁用: {}", msg),
            AppError::InvalidFormat(msg) => write!(f, "格式错误: {}", msg),
        }
    }
}

impl std::error::Error for AppError {}

impl From<std::num::ParseIntError> for AppError {
    fn from(err: std::num::ParseIntError) -> Self {
        AppError::ConfigError(format!("解析错误: {}", err))
    }
}

impl From<CreatePoolError> for AppError {
    fn from(err: CreatePoolError) -> Self {
        AppError::DatabaseError(format!("连接池错误: {}", err))
    }
}

impl From<std::io::Error> for AppError {
    fn from(err: std::io::Error) -> Self {
        AppError::FileError(format!("文件错误: {}", err))
    }
}

impl From<tokio_postgres::Error> for AppError {
    fn from(err: tokio_postgres::Error) -> Self {
        AppError::DatabaseError(err.to_string())
    }
}