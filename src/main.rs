use std::env;
use std::fs;
use dotenv::dotenv;
use tokio;
use futures::future::join_all;

mod db;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    dotenv().ok();

    let pg_client = db::connect_postgres().await?;
    db::init_db(&pg_client).await?;

    let pool = db::create_pool().await?;

    let repos_content = fs::read_to_string("repositories.data")?;
    let repos: Vec<&str> = repos_content.lines().collect();

    let mut handles = Vec::new();
    for repo in repos {
        let repo = repo.trim();
        let parts: Vec<&str> = repo.split('/').collect();
        if parts.len() >= 4 && parts[0] == "https:" && parts[2] == "github.com" {
            let owner = parts[3].to_string();
            let name = parts[4].split('.').next().unwrap_or(parts[4]).to_string();
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let db_client = pool.get().await.unwrap();
                println!("Processing: {}/{}", owner, name);
                loop {
                    match fetch_repo_data(&owner, &name).await {
                        Ok(response) => {
                            if db::store_repo_data(&db_client, &owner, &name, response).await.is_ok() {
                                db::delete_error_log(&owner, &name); // 成功后删除错误日志
                                break; // 成功则退出循环
                            }
                        }
                        Err(e) => {
                            eprintln!("Error for {}/{}: {}", owner, name, e);
                            db::log_error(&owner, &name, &e.to_string()).await.unwrap();
                            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await; // 等待5秒后重试
                        }
                    }
                }
            }));
        } else {
            eprintln!("Invalid repo format: {}", repo);
        }
    }

    join_all(handles).await;
    Ok(())
}

async fn fetch_repo_data(owner: &str, name: &str) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
    let token = env::var("GITHUB_TOKEN")?;
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