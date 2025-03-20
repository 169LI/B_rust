use std::fmt;
use deadpool_postgres::CreatePoolError;
#[allow(dead_code)]
#[derive(Debug)]
pub enum AppError {
    DatabaseError(String),  // 数据库相关错误
    ConfigError(String),    // 配置相关错误
    FileError(String),      // 文件相关错误
    ApiError(String),       // API 相关错误
    TimeoutError(String),   // 超时错误
    TokenBanned(String),    // Token 被禁用错误
    InvalidFormat(String),  // 无效格式错误（新增）
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
            AppError::InvalidFormat(msg) => write!(f, "格式错误: {}", msg), // 新增
        }
    }
}

impl std::error::Error for AppError {}

// 实现 From traits
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