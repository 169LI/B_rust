use std::env;
use std::fs;
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
            ].into_iter().filter(|t| !t.is_empty()).collect(),
        }
    }
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
        let parts: Vec<&str> = repo.split('/').collect();
        if parts.len() >= 5 {
            let owner = parts[3].to_string();
            let name_raw = parts[4];
            if owner.is_empty() || name_raw.is_empty() {
                let err = AppError::InvalidFormat(format!("无效的仓库格式: {}", repo));
                log_error(&err);
                continue;
            }
            let name = name_raw.split('.').next().unwrap_or(name_raw).to_string();
            if name.is_empty() {
                let err = AppError::InvalidFormat(format!("仓库名称为空: {}", repo));
                log_error(&err);
                continue;
            }

            let pool = pool.clone();
            let tokens = tokens.clone();
            let mut token_index = rand::thread_rng().gen_range(0..tokens.len());
            let mut token = tokens[token_index].clone();
            let _permit = semaphore.clone().acquire_owned().await.unwrap();
            let success_count = success_count.clone();
            let banned_tokens = banned_tokens.clone();

            handles.push(tokio::spawn(async move {
                let db_client = pool.get().await.unwrap();
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
                                    // let err = AppError::DatabaseError(e.to_string());
                                    log_error(&e);
                                    return Err(Box::new(e));
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            let err_str = e.to_string();
                            let is_403 = err_str.contains("403");
                            let is_429 = err_str.contains("429");
                            let err = if is_403 {
                                AppError::TokenBanned(format!("Token {} 被禁用: {}", token, err_str))
                            } else if is_429 {
                                AppError::ApiError(format!("速率限制超出: {}", err_str))
                            } else {
                                AppError::ApiError(err_str)
                            };
                            log_error(&err);
                            if is_429 || is_403 {
                                let mut banned = banned_tokens.lock().await;
                                banned.insert(token.clone(), Instant::now());
                                token_index = (token_index + 1) % tokens.len();
                                token = tokens[token_index].clone();
                                tokio::time::sleep(Duration::from_secs(120)).await;
                            }
                        }
                        Err(_) => {
                            let err = AppError::TimeoutError("请求超时".to_string());
                            log_error(&err);
                        }
                    }
                }
                Ok(())
            }));
        } else {
            let err = AppError::InvalidFormat(repo);
            log_error(&err);
        }
    }
    join_all(handles).await;
    Ok(())
}

async fn fetch_repo_data(
    owner: &str,
    name: &str,
    token: &str,
) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
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
        .await?;

    let status = response.status();
    if !status.is_success() {
        let text = response.text().await?;
        return Err(Box::new(AppError::ApiError(format!(
            "请求失败，状态码: {}, 响应: {}",
            status, text
        ))));
    }

    let json = response.json::<serde_json::Value>().await?;
    if json.get("data").and_then(|d| d.get("repository")).is_none() {
        return Err(Box::new(AppError::ApiError(format!(
            "API响应中没有仓库数据: owner={}, name={}", owner, name
        ))));
    }
    Ok(json)
}