use std::env;
use std::fs;
use std::sync::Arc;
use dotenv::dotenv;
use tokio;
use tokio::sync::Semaphore;
use futures::future::join_all;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering}; // 修正导入

mod db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv().ok();

    let tokens = vec![

        env::var("GITHUB_TOKEN_1").expect("GITHUB_TOKEN_1 not set in .env"),
        env::var("GITHUB_TOKEN_2").expect("GITHUB_TOKEN_2 not set in .env"),
        env::var("GITHUB_TOKEN_3").expect("GITHUB_TOKEN_3 not set in .env"),
        env::var("GITHUB_TOKEN_4").expect("GITHUB_TOKEN_4 not set in .env"),
        //TODO 可以添加更多 token，例如 GITHUB_TOKEN_5 到 GITHUB_TOKEN_9
    ];
    let tokens = Arc::new(tokens);
    let available_tokens = Arc::new(tokio::sync::Mutex::new(tokens.len()));
    let banned_tokens = Arc::new(tokio::sync::Mutex::new(HashMap::<String, Instant>::new()));
    let success_count = Arc::new(AtomicUsize::new(0));

    let pg_client = db::connect_postgres().await?;
    db::init_db(&pg_client).await?;

    let pool = db::create_pool().await?;

    let repos_content = fs::read_to_string("repositories.data")?;
    let repos: Vec<&str> = repos_content.lines().collect();

    let success_count_clone = success_count.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let count = success_count_clone.swap(0, Ordering::Relaxed);
            println!("Successes in the last minute: {}", count);
        }
    });
    //TODO 用户可以设置并发数量，需要考虑二级速率限制
    let semaphore = Arc::new(Semaphore::new(50));

    let mut handles = Vec::new();

    for (i, repo) in repos.iter().enumerate() {
        let repo = repo.trim();
        let parts: Vec<&str> = repo.split('/').collect();
        if parts.len() >= 4  {
            let owner = parts[3].to_string();
            let name = parts[4].split('.').next().unwrap_or(parts[4]).to_string();
            let pool = pool.clone();
            let tokens = tokens.clone();
            let mut token = tokens[i % tokens.len()].clone();
            let _permit = semaphore.clone().acquire_owned().await.unwrap();
            let available_tokens = available_tokens.clone();
            let banned_tokens = banned_tokens.clone();
            let success_count = success_count.clone();

            handles.push(tokio::spawn(async move {
                let db_client = pool.get().await.unwrap();
                //println!("Processing: {}/{}", owner, name);
                let mut retry_count = 0;
                let mut token_index = i % tokens.len();

                loop {
                    {
                        let mut banned = banned_tokens.lock().await;
                        if let Some(ban_time) = banned.get(&token) {
                            if ban_time.elapsed() >= Duration::from_secs(3600) {
                                banned.remove(&token);
                                let mut available = available_tokens.lock().await;
                                *available += 1;
                                eprintln!("Token {} recovered, remaining tokens: {}", token, *available);
                            }
                        }
                    }

                    match tokio::time::timeout(Duration::from_secs(30), fetch_repo_data(&owner, &name, &token)).await {
                        Ok(Ok(response)) => {
                            match db::store_repo_data(&db_client, &owner, &name, response).await {
                                Ok(()) => {
                                    success_count.fetch_add(1, Ordering::Relaxed);
                                    db::delete_error_log(&owner, &name);
                                    break;
                                }
                                Err(e) => {
                                    let error_msg = format!("Store error: {}", e);
                                    eprintln!("Store failed for {}/{}", owner, name);
                                    db::log_error(&owner, &name, &error_msg).await.unwrap();
                                }
                            }
                        }
                        Ok(Err(e)) => {
                            let error_msg = format!("Fetch error: {}", e);
                            eprintln!("Fetch failed for {}/{}", owner, name);
                            db::log_error(&owner, &name, &error_msg).await.unwrap();
                            if error_msg.contains("429") || error_msg.contains("rate limit") {
                                let mut available = available_tokens.lock().await;
                                *available -= 1;
                                let mut banned = banned_tokens.lock().await;
                                banned.insert(token.clone(), Instant::now());
                                db::log_token_ban(&token, &owner, &name, *available).await.unwrap();
                                token_index = (token_index + 1) % tokens.len();
                                token = tokens[token_index].clone();
                                eprintln!("Switched to new token for {}/{}", owner, name);
                                tokio::time::sleep(Duration::from_secs(60)).await;
                                continue;
                            }
                        }
                        Err(_) => {
                            retry_count += 1;
                            if retry_count >= 3 {
                                let mut available = available_tokens.lock().await;
                                *available -= 1;
                                let mut banned = banned_tokens.lock().await;
                                banned.insert(token.clone(), Instant::now());
                                db::log_token_ban(&token, &owner, &name, *available).await.unwrap();
                                token_index = (token_index + 1) % tokens.len();
                                token = tokens[token_index].clone();
                                eprintln!("Switched to new token for {}/{} after timeout", owner, name);
                                retry_count = 0;
                                continue;
                            } else {
                                let timeout_msg = format!("Request timed out after 30s (attempt {})", retry_count);
                                eprintln!("Timeout for {}/{}", owner, name);
                                db::log_error(&owner, &name, &timeout_msg).await.unwrap();
                            }
                        }
                    }
                    tokio::time::sleep(Duration::from_secs(2)).await;
                }
            }));
        } else {
            let error_msg = format!("Invalid repo format: {}", repo);
            eprintln!("{}", error_msg);
            db::log_invalid_format(repo).await.unwrap();
        }
    }

    join_all(handles).await;
    Ok(())
}

async fn fetch_repo_data(owner: &str, name: &str, token: &str) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let client = reqwest::Client::new();
    let query = format!(
        r#"query {{
            repository(owner: "{}", name: "{}") {{
                name
                stargazers {{ totalCount }}
                forks {{ totalCount }}
                defaultBranchRef {{
                    target {{
                        ... on Commit {{
                            history {{
                                totalCount
                            }}
                        }}
                    }}
                }}
                issues(states: CLOSED) {{ totalCount }}
                pullRequests(states: MERGED) {{ totalCount }}
            }}
        }}"#,
        owner, name
    );

    let request_body = serde_json::json!({ "query": query });
    let response = client
        .post("https://api.github.com/graphql")
        .header("Authorization", format!("Bearer {}", token))
        .header("User-Agent", "get-data-test")
        .json(&request_body)
        .send()
        .await?
        .json::<serde_json::Value>()
        .await?;

    Ok(response)
}