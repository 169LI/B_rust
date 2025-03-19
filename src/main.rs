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

    let repos_content = fs::read_to_string("repos.txt")?;
    let repos: Vec<&str> = repos_content.lines().collect();

    let mut handles = Vec::new();
    for repo in repos {
        let repo = repo.trim();
        let parts: Vec<&str> = repo.split('/').collect();
        if parts.len() >= 4 && parts[0] == "https:" && parts[2] == "github.com" {
            let owner = parts[3].to_string();
            // 提取 name 并移除可能的 .git 或其他后缀
            let raw_name = parts[4];
            let name = raw_name
                .split('.')  // 以点分割，去掉 .git 或 .rs 等
                .next()      // 取第一个部分
                .unwrap_or(raw_name)  // 如果没有分割结果，用原始值
                .to_string();
            let pool = pool.clone();
            handles.push(tokio::spawn(async move {
                let db_client = match pool.get().await {
                    Ok(client) => client,
                    Err(e) => {
                        eprintln!("Failed to get DB client for {}/{}: {}", owner, name, e);
                        return;
                    }
                };
                println!("Processing: {}/{}", owner, name);
                match fetch_repo_data(&owner, &name).await {
                    Ok(response) => {
                        if let Err(e) = db::store_repo_data(&db_client, &owner, &name, response).await {
                            eprintln!("Failed to store data for {}/{}: {}", owner, name, e);
                        }
                    }
                    Err(e) => eprintln!("Failed to fetch data for {}/{}: {}", owner, name, e),
                }
            }));
        } else {
            eprintln!("Invalid repo format: {}", repo);
        }
    }

    join_all(handles).await;

    println!("---");
    Ok(())
}

/// 从 GitHub API 获取仓库数据
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