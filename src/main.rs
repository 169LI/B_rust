use reqwest::Client;
use serde_json::json;
use dotenv::dotenv;
use std::env;
use std::fs;
use std::error::Error;

fn parse_repo_url(url: &str) -> Option<(String, String)> {
    let parts: Vec<&str> = url.split('/').collect();
    if parts.len() >= 5 && parts[2] == "github.com" {
        Some((parts[3].to_string(), parts[4].to_string()))
    } else {
        None
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    dotenv().ok();
    let token = env::var("GITHUB_TOKEN").expect("GITHUB_TOKEN not set in .env file");
    let client = Client::builder()
        .user_agent("graphql-rust/0.1.0")
        .build()?;
   /*
   name:仓库名字
   stargazers:star数量
   forks:fork数量
   watchers:关注者数量

    */
    let query = r#"
        query($owner: String!, $name: String!) {
          repository(owner: $owner, name: $name) {
            name
            stargazers { totalCount }
            forks { totalCount }
            watchers { totalCount }
            defaultBranchRef {
              target {
                ... on Commit {
                  history(first: 100) {
                    totalCount
                    edges {
                      node {
                        committedDate
                      }
                    }
                  }
                }
              }
            }
            pullRequests(states: MERGED) { totalCount }
            issues(states: CLOSED) { totalCount }
          }
        }
    "#;

    let contents = fs::read_to_string("repos.txt")?;
    let repo_urls: Vec<&str> = contents.lines().collect();

    for url in repo_urls {
        if let Some((owner, name)) = parse_repo_url(url) {
            println!("Processing: {}/{}", owner, name);

            let variables = json!({
                "owner": owner,
                "name": name
            });

            let request_body = json!({
                "query": query,
                "variables": variables
            });

            let response = client
                .post("https://api.github.com/graphql")
                .bearer_auth(&token)
                .json(&request_body)
                .send()
                .await?
                .json::<serde_json::Value>()
                .await?;

            println!("Response: {}", serde_json::to_string_pretty(&response)?);
            println!("---");
        } else {
            println!("Invalid URL: {}", url);
        }
    }

    Ok(())
}