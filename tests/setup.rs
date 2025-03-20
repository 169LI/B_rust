use std::fs::{self, File};
use std::io::{self};
use std::path::Path;
use tokio_postgres::NoTls;
use std::env;

// 检查文件是否存在
fn check_file_exists(file_path: &str) -> bool {
    Path::new(file_path).exists()
}

// 清除日志文件内容
fn clear_log_files(log_dir: &str) -> io::Result<()> {
    let log_dir = Path::new(log_dir);
    if !log_dir.exists() {
        fs::create_dir_all(log_dir)?;
    }

    let log_files = [
        "logs/api_errors.log",
        "logs/database_errors.log",
        "logs/token_banned.log",
        "logs/config_errors.log",
        "logs/file_errors.log",
        "logs/timeout_errors.log",
        "logs/invalid_format.log",
        "logs/stats.log",
    ];

    for log_file in log_files.iter() {
        let path = Path::new(log_file);
        if path.exists() {
            File::create(path)?.set_len(0)?;
            println!("已清空日志文件: {}", log_file);
        } else {
            File::create(path)?;
            println!("已创建空日志文件: {}", log_file);
        }
    }
    Ok(())
}

// 测试文件和日志清理
#[test]
fn test_environment_setup() {
    assert!(
        check_file_exists(".env"),
        "错误: .env 文件不存在，请确保项目根目录下有 .env 文件"
    );
    assert!(
        check_file_exists("repositories.data"),
        "错误: repositories.data 文件不存在，请确保项目根目录下有 repositories.data 文件"
    );
    clear_log_files("logs").expect("清理日志文件失败");
    println!("环境检查和日志清理完成");
}

// 测试数据库连接
#[tokio::test]
async fn test_database_connection() {
    // 加载 .env 文件
    dotenv::dotenv().ok();

    // 从环境变量获取数据库配置
    let host = env::var("DB_HOST").unwrap_or("localhost".to_string());
    let port = env::var("DB_PORT").unwrap_or("5432".to_string());
    let user = env::var("DB_USER").unwrap_or("postgres".to_string());
    let password = env::var("DB_PASSWORD").expect("DB_PASSWORD 未设置");
    let dbname = env::var("DB_NAME").unwrap_or("github_db".to_string());

    // 构建连接字符串
    let conn_str = format!(
        "host={} port={} user={} password={} dbname={}",
        host, port, user, password, dbname
    );

    // 尝试连接数据库
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
        .await
        .expect("数据库连接失败");

    // 启动连接任务
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("数据库连接错误: {}", e);
        }
    });

    // 执行简单查询以验证连接
    let row = client
        .query_one("SELECT 1 AS test", &[])
        .await
        .expect("执行测试查询失败");

    let result: i32 = row.get(0);
    assert_eq!(result, 1, "数据库查询结果不正确");
    println!("数据库连接测试成功: SELECT 1 返回 {}", result);
}