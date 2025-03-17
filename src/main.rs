use reqwest::header::{HeaderMap, HeaderValue, AUTHORIZATION, USER_AGENT};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fs::File;
use std::io::Write;

#[derive(Serialize, Deserialize, Debug)]
struct RepoInfo {
    stargazers_count: Option<i32>,
    forks_count: Option<i32>,
    updated_at: Option<String>,
    open_issues_count: Option<i32>,
}

#[derive(Serialize, Deserialize, Debug)]
struct Commit {
    sha: String,
    commit: CommitDetails,
}

#[derive(Serialize, Deserialize, Debug)]
struct CommitDetails {
    author: Author,
}

#[derive(Serialize, Deserialize, Debug)]
struct Author {
    date: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let token = "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"; // 替换为你的有效Token
    let repo = "dtolnay/syn";
    let base_url = "https://api.github.com/repos/";

    // 设置请求头
    let mut headers = HeaderMap::new();
    headers.insert(AUTHORIZATION, HeaderValue::from_str(&format!("token {}", token))?);
    headers.insert("Accept", HeaderValue::from_static("application/vnd.github.v3+json"));
    headers.insert(USER_AGENT, HeaderValue::from_static("get-data-test/1.0 (your_email@example.com)"));

    let client = reqwest::Client::builder().default_headers(headers).build()?;

    // 获取仓库信息
    let response = client.get(format!("{}{}", base_url, repo)).send().await?;
    if response.status().is_success() {
        let repo_info: RepoInfo = response.json().await?;
        println!("仓库信息: {:?}", repo_info);

        // 保存到JSON文件
        let repo_json = serde_json::to_string_pretty(&repo_info)?;
        let mut file = File::create("repo_info.json")?;
        file.write_all(repo_json.as_bytes())?;
        println!("仓库信息已保存到 repo_info.json");
    } else {
        println!("Error: Status {}, Body: {}", response.status(), response.text().await?);
    }

    // 获取最近10次提交
    let response = client.get(format!("{}{}/commits?per_page=10", base_url, repo)).send().await?;
    if response.status().is_success() {
        let commits: Vec<Commit> = response.json().await?;
        println!("最近提交: {:?}", commits);

        // 保存到JSON文件
        let commits_json = serde_json::to_string_pretty(&commits)?;
        let mut file = File::create("commits.json")?;
        file.write_all(commits_json.as_bytes())?;
        println!("提交记录已保存到 commits.json");
    } else {
        println!("Error: Status {}, Body: {}", response.status(), response.text().await?);
    }

    Ok(())
}