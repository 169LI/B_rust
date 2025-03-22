use std::env;
use std::fs;
use std::io::Write;
use std::sync::Arc;
use dotenv::dotenv;
use tokio;
use tokio::sync::Semaphore;
use futures::future::join_all;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use rand::Rng;
use crate::errors::AppError;
use crate::logger::{log_error, log_stats};

mod db;
mod errors;
mod logger;

// 全局配置
#[allow(dead_code)]
struct Config {
    concurrency: usize,        // 并发请求数
    max_db_retries: u32,      // 数据库连接最大重试次数
    db_retry_interval: u64,   // 数据库重试间隔（秒）
    request_timeout: u64,     // 请求超时时间（秒）
    tokens: Vec<String>,      // GitHub Token 列表
}

impl Config {
    fn new() -> Self {
        Config {
            concurrency: env::var("CONCURRENCY")
                .unwrap_or("50".to_string())
                .parse()
                .unwrap_or(50),
            max_db_retries: env::var("MAX_DB_RETRIES")
                .unwrap_or("5".to_string())
                .parse()
                .unwrap_or(5),
            db_retry_interval: env::var("DB_RETRY_INTERVAL")
                .unwrap_or("5".to_string())
                .parse()
                .unwrap_or(5),
            request_timeout: env::var("REQUEST_TIMEOUT")
                .unwrap_or("30".to_string())
                .parse()
                .unwrap_or(30),
            tokens: vec![
                env::var("GITHUB_TOKEN_1").expect("GITHUB_TOKEN_1 not set"),
                env::var("GITHUB_TOKEN_2").expect("GITHUB_TOKEN_2 not set"),
                env::var("GITHUB_TOKEN_3").expect("GITHUB_TOKEN_3 not set"),
                env::var("GITHUB_TOKEN_4").expect("GITHUB_TOKEN_4 not set"),
                env::var("GITHUB_TOKEN_5").expect("GITHUB_TOKEN_5 NOT set"),
                env::var("GITHUB_TOKEN_6").expect("GITHUB_TOKEN_6 NOT set"),
                env::var("GITHUB_TOKEN_7").expect("GITHUB_TOKEN_7 NOT set"),
                env::var("GITHUB_TOKEN_8").expect("GITHUB_TOKEN_8 NOT set"),
                env::var("GITHUB_TOKEN_9").expect("GITHUB_TOKEN_9 NOT set"),
            ].into_iter().filter(|t| !t.is_empty()).collect(),
        }
    }
}

pub fn validate_repo_format(repo: &str) -> Result<(String, String), AppError> {
    let parts: Vec<&str> = repo.split('/').collect();
    let owner = parts[3].to_string();
    let name_raw = parts[4];
    if (parts[0]!="https:" && parts[0]!="http:")||parts.len() < 5|| owner.is_empty() || name_raw.is_empty() {
        return Err(AppError::InvalidFormat(format!("无效的仓库格式: {}", repo)));
    }
    let name = name_raw.split('.').next().unwrap_or(name_raw).to_string();
    if name.is_empty() {
        return Err(AppError::InvalidFormat(format!("仓库名称为空: {}", repo)));
    }
    Ok((owner, name))
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    dotenv().ok();
    let config = Config::new();
    logger::init_logger();

    let tokens = Arc::new(config.tokens);
    let available_tokens = Arc::new(tokio::sync::Mutex::new(tokens.len()));
    let success_count = Arc::new(AtomicUsize::new(0));
    let banned_tokens = Arc::new(tokio::sync::Mutex::new(HashMap::<String, Instant>::new()));

    let mut pg_client = db::connect_postgres().await?;
    db::init_db(&mut pg_client).await?;

    let pool = db::create_pool().await?;

    let repos_content = fs::read_to_string("repositories.data")?;
    let repos: Vec<String> = repos_content.lines().map(|s| s.to_string()).collect();

    let success_count_clone = success_count.clone();
    let available_tokens_clone = available_tokens.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let count = success_count_clone.swap(0, Ordering::Relaxed);
            let available = *available_tokens_clone.lock().await;
            log_stats(count, available);
        }
    });

    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let mut handles = Vec::new();

    for repo in repos {
        let repo = repo.trim().to_string();
        match validate_repo_format(&repo) {
            Ok((owner, name)) => {
                let pool = pool.clone();
                let tokens = tokens.clone();
                let mut token_index = rand::thread_rng().gen_range(0..tokens.len());
                let mut token = tokens[token_index].clone();
                let _permit = semaphore.clone().acquire_owned().await.unwrap();
                let success_count = success_count.clone();
                let banned_tokens = banned_tokens.clone();

                handles.push(tokio::spawn(async move {
                    let db_client = pool.get().await.unwrap();
                    let mut retries = 0;
                    const MAX_RETRIES: usize = 5;

                    loop {
                        match tokio::time::timeout(
                            Duration::from_secs(config.request_timeout),
                            fetch_repo_data(&owner, &name, &token),
                        ).await {
                            Ok(Ok(response)) => {
                                match db::store_repo_data(&db_client, &owner, &name, response).await {
                                    Ok(()) => {
                                        success_count.fetch_add(1, Ordering::Relaxed);
                                        break;
                                    }
                                    Err(e) => {
                                        log_error(&e);
                                        return Err(e);
                                    }
                                }
                            }
                            Ok(Err(e)) => {
                                log_error(&e);
                                if retries >= MAX_RETRIES {
                                    let err = AppError::TokenBanned(format!("Token {} 超出重试次数", token));
                                    log_error(&err);
                                    banned_tokens.lock().await.insert(token.clone(), Instant::now());
                                    break;
                                }
                                if let Some(wait_time) = e.retry_after() {
                                    tokio::time::sleep(Duration::from_secs(wait_time)).await;
                                    retries += 1;
                                } else {
                                    let wait_time = 60 * (1 << retries); // 指数退避
                                    tokio::time::sleep(Duration::from_secs(wait_time)).await;
                                    retries += 1;
                                    token_index = (token_index + 1) % tokens.len();
                                    token = tokens[token_index].clone();
                                }
                            }
                            Err(_) => {
                                let err = AppError::TimeoutError("请求超时".to_string());
                                log_error(&err);
                                break;
                            }
                        }
                    }
                    Ok(())
                }));
            }
            Err(err) => {
                log_error(&err);
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open("invalid_repos.txt")?
                    .write_all(format!("{}\n", repo).as_bytes())?;
            }
        }
    }
    join_all(handles).await;
    Ok(())
}

async fn fetch_repo_data(
    owner: &str,
    name: &str,
    token: &str,
) -> Result<serde_json::Value, AppError> {
    let client = reqwest::Client::new();
    let query = format!(
        r#"query {{
            repository(owner: "{}", name: "{}") {{
                stargazers {{ totalCount }}
                forks {{ totalCount }}
                defaultBranchRef {{
                    target {{
                        ... on Commit {{
                            history {{ totalCount }}
                        }}
                    }}
                }}
                issues {{ totalCount }}
                pullRequests(states: MERGED) {{ totalCount }}
            }}
        }}"#,
        owner, name
    );

    let response = client
        .post("https://api.github.com/graphql")
        .header("User-Agent", "get-data-test")
        .bearer_auth(token)
        .json(&serde_json::json!({ "query": query }))
        .send()
        .await
        .map_err(|e| AppError::ApiError(format!("请求发送失败: {}", e)))?;

    let status = response.status();
    if status == reqwest::StatusCode::FORBIDDEN || status == reqwest::StatusCode::TOO_MANY_REQUESTS {
        let headers = response.headers();
        let remaining = headers.get("x-ratelimit-remaining").and_then(|v| v.to_str().ok().and_then(|s| s.parse::<i32>().ok()));
        let retry_after = headers.get("retry-after").and_then(|v| v.to_str().ok().and_then(|s| s.parse::<u64>().ok()));
        let reset_at = headers.get("x-ratelimit-reset").and_then(|v| v.to_str().ok().and_then(|s| s.parse::<u64>().ok()));

        if let Some(seconds) = retry_after {
            return Err(AppError::ApiError(format!("速率限制，需等待 {} 秒", seconds)));
        } else if let (Some(0), Some(reset_at)) = (remaining, reset_at) {
            let now = Instant::now().elapsed().as_secs();
            let wait_time = reset_at.saturating_sub(now);
            return Err(AppError::ApiError(format!("主要速率限制，需等待 {} 秒", wait_time)));
        } else {
            return Err(AppError::ApiError("次要速率限制，需指数退避".to_string()));
        }
    }

    if !status.is_success() {
        let text = response.text().await.map_err(|e| AppError::ApiError(format!("响应解析失败: {}", e)))?;
        return Err(AppError::ApiError(format!(
            "请求失败，状态码: {}, 响应: {}",
            status, text
        )));
    }

    let json = response.json::<serde_json::Value>().await.map_err(|e| AppError::ApiError(format!("JSON解析失败: {}", e)))?;
    if json.get("data").and_then(|d| d.get("repository")).is_none() {
        return Err(AppError::ApiError(format!(
            "API响应中没有仓库数据: owner={}, name={}", owner, name
        )));
    }
    Ok(json)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io;
    use std::path::Path;
    use tokio_postgres::NoTls;

    fn check_file_exists(file_path: &str) -> bool {
        Path::new(file_path).exists()
    }

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
    #[test]
    fn test_all_repos_format() {
        let repos_content = fs::read_to_string("repositories.data")
            .expect("无法读取 repositories.data 文件");
        let repos: Vec<String> = repos_content.lines().map(|s| s.trim().to_string()).collect();

        let mut invalid_repos = Vec::new();
        for repo in repos {
            match validate_repo_format(&repo) {
                Ok(_) => {},
                Err(err) => {
                    log_error(&err);
                    invalid_repos.push(repo);
                }
            }
        }

        if !invalid_repos.is_empty() {
            println!("发现以下格式错误的仓库地址: {:?}", invalid_repos);
            panic!("测试失败：存在 {} 个格式错误的仓库地址", invalid_repos.len());
        }
        println!("所有仓库地址格式检查通过");
    }


    #[tokio::test]
    async fn test_database_connection() {
        dotenv::dotenv().ok();

        let host = env::var("DB_HOST").unwrap_or("localhost".to_string());
        let port = env::var("DB_PORT").unwrap_or("5432".to_string());
        let user = env::var("DB_USER").unwrap_or("postgres".to_string());
        let password = env::var("DB_PASSWORD").expect("DB_PASSWORD 未设置");
        let dbname = env::var("DB_NAME").unwrap_or("github_db".to_string());

        let conn_str = format!(
            "host={} port={} user={} password={} dbname={}",
            host, port, user, password, dbname
        );

        let (client, connection) = tokio_postgres::connect(&conn_str, NoTls)
            .await
            .expect("数据库连接失败");

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("数据库连接错误: {}", e);
            }
        });

        let row = client
            .query_one("SELECT 1 AS test", &[])
            .await
            .expect("执行测试查询失败");

        let result: i32 = row.get(0);
        assert_eq!(result, 1, "数据库查询结果不正确");
        println!("数据库连接测试成功: SELECT 1 返回 {}", result);
    }
}