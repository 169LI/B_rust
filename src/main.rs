

use std::io;
use std::env;
use std::fs;
use std::io::{Write};
use std::sync::Arc;
use dotenv::dotenv;
use tokio;
use tokio::sync::Semaphore;
use futures::future::join_all;
use std::time::{Duration, Instant};
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use crate::errors::AppError;
use crate::logger::{log_error, log_stats};

mod db; //数据库（连接、初始化、存储数据）
mod errors;//自定义错误类型
mod logger;
mod split;
//日志记录功能

/// 配置管理(config结构体)
#[allow(dead_code)]
struct Config {
    concurrency: usize,  // 并发请求数
    max_db_retries: u32, // 数据库连接最大重试次数
    db_retry_interval: u64, // 数据库重试间隔（秒）
    request_timeout: u64, // 请求超时时间（秒）
    tokens: Vec<String>, // GitHub Token 列表
    data_file: String,
    failed_file: String,
}

impl Config {
    fn new() -> Self {
        Config {
            concurrency: env::var("CONCURRENCY").unwrap_or("50".to_string()).parse().unwrap_or(50),
            max_db_retries: env::var("MAX_DB_RETRIES").unwrap_or("5".to_string()).parse().unwrap_or(5),
            db_retry_interval: env::var("DB_RETRY_INTERVAL").unwrap_or("5".to_string()).parse().unwrap_or(5),
            request_timeout: env::var("REQUEST_TIMEOUT").unwrap_or("30".to_string()).parse().unwrap_or(30),
            tokens: vec![
                // 需要测试5000/小时 是如何计算的？0-10min把Token用完了，10-20会恢复吗
                env::var("GITHUB_TOKEN_1").expect("GITHUB_TOKEN_1 not set"),
                // env::var("GITHUB_TOKEN_2").expect("GITHUB_TOKEN_2 not set"),
                env::var("GITHUB_TOKEN_3").expect("GITHUB_TOKEN_3 not set"),
                // env::var("GITHUB_TOKEN_4").expect("GITHUB_TOKEN_4 not set"),
                env::var("GITHUB_TOKEN_5").expect("GITHUB_TOKEN_5 NOT set"),
                env::var("GITHUB_TOKEN_6").expect("GITHUB_TOKEN_6 NOT set"),
                env::var("GITHUB_TOKEN_7").expect("GITHUB_TOKEN_7 NOT set"),
                env::var("GITHUB_TOKEN_8").expect("GITHUB_TOKEN_8 NOT set"),
                env::var("GITHUB_TOKEN_9").expect("GITHUB_TOKEN_9 NOT set"),
                env::var("GITHUB_TOKEN_10").expect("GITHUB_TOKEN_9 NOT set"),
                env::var("GITHUB_TOKEN_11").expect("GITHUB_TOKEN_9 NOT set"),
            ].into_iter().filter(|t| !t.is_empty()).collect(),
            data_file: env::var("DATA_FILE").unwrap_or("repositories.data".to_string()),
            failed_file: env::var("FAILED_FILE").unwrap_or("failed_repos.data".to_string()),}
    }
}
///检查数据格式  返回(owner,name)
///
///格式http://github.com/owner/name 或https://github.com/owner/name.git
///
///name不合规则的返回AppError::InvalidFormat
pub fn validate_repo_format(repo: &str) -> Result<(String,String, String), AppError> {
    let part: Vec<&str> = repo.split(',').collect();
    if part.len() != 2 {
        return Err(AppError::InvalidFormat(format!("无效的格式: {}", repo)));
    }

    let cratesname = part[0].trim().to_string();
    let url = part[1].trim();

    let parts: Vec<&str> = url.split('/').collect();

    if parts.len() < 5 || (parts[0] != "https:" && parts[0] != "http:") {
        return Err(AppError::InvalidFormat(format!("无效的仓库格式: {}", repo)));
    }
    let owner = parts[3].to_string();
    let name_raw = parts[4];
    if owner.is_empty() || name_raw.is_empty() {
        return Err(AppError::InvalidFormat(format!("无效的仓库格式: {}", repo)));
    }
    let name = name_raw.split('.').next().unwrap_or(name_raw).to_string();
    if name.is_empty() {
        return Err(AppError::InvalidFormat(format!("仓库名称为空: {}", repo)));
    }
    Ok((cratesname,owner, name))
}

#[tokio::main]
async fn main() -> Result<(), AppError> {
    dotenv().ok();
    let config = Arc::new(Config::new());
    logger::init_logger();

    let initial_tokens = config.tokens.clone(); // 初始 token 列表

    let token_counter = Arc::new(AtomicUsize::new(0)); // 用于轮询
    let available_tokens = Arc::new(tokio::sync::Mutex::new(initial_tokens.len()));
    let success_count = Arc::new(AtomicUsize::new(0));
    let banned_tokens = Arc::new(tokio::sync::Mutex::new(HashMap::<String, Instant>::new()));

    // 初始化 HTTP 客户端
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(config.request_timeout))
        .pool_max_idle_per_host(config.concurrency)
        .build()
        .map_err(|e| AppError::ConfigError(format!("构建 HTTP 客户端失败: {}", e)))?;
    let client = Arc::new(client);

    // 测试所有 token 并获取有效 token 列表
    println!("开始测试 token 有效性...");
    let valid_tokens = test_tokens(&client, &initial_tokens, &banned_tokens).await?;
    if valid_tokens.is_empty() {
        return Err(AppError::ConfigError("没有有效的 token，无法继续".to_string()));
    }
    let tokens = Arc::new(valid_tokens); // 更新全局 tokens 为有效 token
    *available_tokens.lock().await = tokens.len(); // 更新可用 token 数量


    let mut pg_client = db::connect_postgres().await?;
    db::init_db(&mut pg_client).await?;
    let pool = db::create_pool().await?;

    let repos_content = fs::read_to_string(&config.data_file)
        .map_err(|e| AppError::FileError(format!("无法读取数据文件 {}: {}", config.data_file, e)))?;
    let repos: Vec<String> = repos_content.lines().map(|s| s.to_string()).collect();

    let success_count_clone = success_count.clone();
    let available_tokens_clone = available_tokens.clone();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
            let count = success_count_clone.swap(0, Ordering::Relaxed);
            let available = *available_tokens_clone.lock().await;
            println!("成功: {}, 可用令牌: {}", count, available);
            log_stats(count, available);
        }
    });

    let semaphore = Arc::new(Semaphore::new(config.concurrency));
    let mut handles = Vec::new();

    for repo in repos {
        let repo = repo.trim().to_string();
        match validate_repo_format(&repo) {
            Ok((cratesname,owner, name)) => {
                let pool = pool.clone();
                let tokens = tokens.clone();
                let config = config.clone();
                let client = client.clone();
                let token_counter = token_counter.clone();
                let success_count = success_count.clone();
                let banned_tokens = banned_tokens.clone();
                let available_tokens = available_tokens.clone(); // 关键改动：克隆 Arc
                let token_index = token_counter.fetch_add(1, Ordering::Relaxed) % tokens.len();//Token轮询机制
                let token = tokens[token_index].clone();
                let _permit = semaphore.clone().acquire_owned().await.unwrap();

                let mut db_retries = 0;
                let max_db_retries = config.max_db_retries as usize; // 重用 Config 中的 max_db_retries

                handles.push(tokio::spawn(async move {
                    let db_client = pool.get().await.unwrap();
                    let mut retries = 0;
                    let max_retries = config.max_db_retries as usize;
                    let mut current_token = token;

                    loop {
                        match tokio::time::timeout(Duration::from_secs(config.request_timeout), fetch_repo_data(&client, &owner, &name, &current_token), ).await {
                            Ok(Ok(response)) => {
                                loop {
                                    match db::store_repo_data(&db_client, &cratesname, &owner, &name, response.clone()).await {
                                        Ok(_) => {
                                            success_count.fetch_add(1, Ordering::Relaxed);
                                            break; // 成功写入数据库，退出数据库写入循环
                                        }
                                        Err(e) => {
                                            log_error(&e);
                                            if db_retries >= max_db_retries {
                                                // 重试次数用尽，记录此条数据失败并退出
                                                fs::OpenOptions::new()
                                                    .create(true)
                                                    .append(true)
                                                    .open("write_failed")
                                                    .map_err(|e| AppError::FileError(format!("写入失败 write_failed: {}", e)))?
                                                    .write_all(format!("{},https://github.com/{}/{}\n", cratesname, owner, name).as_bytes())?;
                                                return Err(e); // 5次写也写不进去，不写了，直接退出本次tokio::spawn
                                            }
                                            db_retries += 1; // 立即重试，也可以加一个间隔
                                        }
                                    }
                                }
                                break;//退出本次任务，本分支的tokio::spawn结束，不然又重新请求本条数据了
                            }
                            Ok(Err(e)) => {
                                log_error(&e);
                                let is_token_banned = matches!(e, AppError::TokenBanned(_));//否匹配 AppError::TokenBanned 变体
                                // 如果是 TokenBanned 或重试次数用尽，禁用当前 token
                                if is_token_banned || retries >= max_retries {
                                    let err_msg = if is_token_banned {
                                        e.to_string() // 直接用 TokenBanned 的消息
                                    } else {
                                        format!("Token {} 超出重试次数", current_token)
                                    };
                                    let err = AppError::TokenBanned(err_msg);
                                    log_error(&err);
                                    banned_tokens.lock().await.insert(current_token.clone(), Instant::now());
                                    *available_tokens.lock().await -= 1;
                                    // 检查是否还有可用 token
                                    if retries >= tokens.len() - 1 { // 如果所有 token 都试过了
                                        let err = AppError::TokenBanned("所有 token 均已耗尽".to_string());
                                        log_error(&err);
                                        fs::OpenOptions::new()
                                            .create(true)
                                            .append(true)
                                            .open("write_failed")
                                            .map_err(|e| AppError::FileError(format!("无法写入失败文件 write_failed: {}", e)))?
                                            .write_all(format!("{},https://github.com/{}/{}\n", cratesname, owner, name).as_bytes())?;
                                        break;
                                    }
                                }
                                // 如果不是立即禁用，固定等待 2 秒
                                if !is_token_banned {
                                    tokio::time::sleep(Duration::from_secs(2)).await;
                                }
                                // 切换到下一个 token
                                retries += 1;
                                let next_index = (token_index + retries) % tokens.len();
                                current_token = tokens[next_index].clone();
                            }
                            Err(_) => {
                                println!("请求超时 401触发了？？");
                                let err = AppError::TimeoutError("请求超时".to_string());
                                log_error(&err);
                                fs::OpenOptions::new()
                                    .create(true)
                                    .append(true)
                                    .open("write_failed")
                                    .map_err(|e| AppError::FileError(format!("无法写入失败文件 write_failed: {}", e)))?
                                    .write_all(format!("{},https://github.com/{}/{}\n", cratesname,owner, name).as_bytes())?;
                                break;
                            }
                        }
                    }
                    Ok(())
                }));
            }
            Err(err) => {
                //TODO  需要与name拼接  这里只处理格式规范的
                log_error(&err);
                fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&config.failed_file)//此处是由于格式不规范引起的问题，不经过处理无法，再次重试也是报错 需要另存信息
                    .map_err(|e| AppError::FileError(format!("无法写入失败文件 {}: {}", config.failed_file, e)))?
                    .write_all(format!("{}\n", repo).as_bytes())?;
            }
        }
    }
    join_all(handles).await;
    Ok(())
}

async fn fetch_repo_data(client: &reqwest::Client,owner: &str, name: &str, token: &str) -> Result<serde_json::Value, AppError> {
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


    if !status.is_success() {//200 Ok 不会进入
        let text = response.text().await.map_err(|e| AppError::ApiError(format!("响应解析失败: {}", e)))?;
        println!("请求失败，状态码: {}, 响应: {}", status, text);
        return Err(AppError::ApiError(format!("请求失败，状态码: {}, 响应: {}", status, text)));
    }

    let json = response.json::<serde_json::Value>().await.map_err(|e| AppError::ApiError(format!("JSON解析失败: {}", e)))?;
    // 检查 RATE_LIMITED
    if let Some(errors) = json.get("errors").and_then(|e| e.as_array()) {
        for error in errors {
            if let Some(error_type) = error.get("type").and_then(|t| t.as_str()) {
                if error_type == "RATE_LIMITED" {
                    return Err(AppError::TokenBanned(format!("Token {} 已达速率限制", token)));
                }
            }
        }
    }

    if json.get("data").and_then(|d| d.get("repository")).is_none() {
        return Err(AppError::ApiError(format!("API响应中没有仓库数据: owner={}, name={}", owner, name)));
    }
    Ok(json)
}
/// 测试所有 token 的有效性，返回有效的 token 数量
async fn test_tokens(
    client: &reqwest::Client,
    tokens: &[String],
    banned_tokens: &tokio::sync::Mutex<HashMap<String, Instant>>,
) -> Result<Vec<String>, AppError> {
    let mut valid_tokens = Vec::new();
    let mut total_remaining = 0;
    for token in tokens {
        let query = r#"query {
            rateLimit {
                limit
                remaining
                used
            }
        }"#;

        let response = client
            .post("https://api.github.com/graphql")
            .header("User-Agent", "get-data-test")
            .bearer_auth(token)
            .json(&serde_json::json!({ "query": query }))
            .send()
            .await
            .map_err(|e| AppError::ApiError(format!("测试 token {} 失败: {}", token, e)))?;

        let status = response.status();
        if !status.is_success() {
            println!("Token {} 无效，状态码: {}", token, status);
            banned_tokens.lock().await.insert(token.clone(), Instant::now());
            continue;
        }

        let json = response
            .json::<serde_json::Value>()
            .await
            .map_err(|e| AppError::ApiError(format!("解析 token {} 响应失败: {}", token, e)))?;

        // 检查 rateLimit 数据
        if let Some(rate_limit) = json.get("data").and_then(|d| d.get("rateLimit")) {
            let remaining = rate_limit
                .get("remaining")
                .and_then(|r| r.as_u64())
                .unwrap_or(0);
            let limit = rate_limit
                .get("limit")
                .and_then(|l| l.as_u64())
                .unwrap_or(0);
            println!(
                "Token {}: 限制={}, 剩余={}",
                token, limit, remaining
            );
            if remaining > 0 {
                valid_tokens.push(token.clone());
                total_remaining += remaining; // 累加剩余请求次数
            } else {
                banned_tokens.lock().await.insert(token.clone(), Instant::now());
            }
        } else if let Some(errors) = json.get("errors") {
            println!("Token {} 查询失败: {:?}", token, errors);
            banned_tokens.lock().await.insert(token.clone(), Instant::now());
        } else {
            println!("Token {} 返回意外响应: {:?}", token, json);
            banned_tokens.lock().await.insert(token.clone(), Instant::now());
        }
    }

    println!("有效 token 数量: {}, 总剩余请求次数: {}", valid_tokens.len(), total_remaining);

    // 检查总请求次数并提示用户
    if total_remaining > 40000 {
        println!(
            "警告：所有 token 的总剩余请求次数为 {}，超过 40000，是否继续运行？(y/n)",
            total_remaining
        );
        let mut input = String::new();
        io::stdin()
            .read_line(&mut input)
            .map_err(|e| AppError::ConfigError(format!("读取用户输入失败: {}", e)))?;
        let input = input.trim().to_lowercase();
        if input != "y" && input != "yes" {
            println!("用户选择不继续，返回空 token 列表");
            return Ok(Vec::new()); // 返回空列表，表示不继续
        }
        println!("用户确认继续运行");
    }
    Ok(valid_tokens)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, File};
    use std::io;
    use std::path::Path;
    use tokio_postgres::NoTls;
    use crate::split::split_file;
    //ToDo 需要添加测试，检查Token有效性

    #[test]
    fn test_split_data() -> Result<(), io::Error> {
        let input_file = "crates_with_address.data";
        let lines_per_file = 40_000;

        if !Path::new(input_file).exists() {
            eprintln!("错误：输入文件 {} 不存在", input_file);
            return Err(io::Error::new(io::ErrorKind::NotFound, "输入文件不存在"));
        }

        split_file(input_file, lines_per_file)?;

        let base_name = Path::new(input_file)
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap();
        let ext = Path::new(input_file)
            .extension()
            .map_or("", |e| e.to_str().unwrap());

        let mut part_index = 1;
        let mut total_lines = 0;
        loop {
            let part_file = format!("{}_part_{}.{}", base_name, part_index, ext);
            match fs::read_to_string(&part_file) {
                Ok(content) => {
                    let line_count = content.lines().count();
                    println!("{}: {} 行", part_file, line_count);
                    total_lines += line_count;
                    part_index += 1;
                }
                Err(_) => break,
            }
        }

        assert_eq!(total_lines, 143924, "分割后的总行数应为 170,000");
        println!("总行数验证通过: {}", total_lines);

        Ok(())
    }


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

        // 加载 .env 文件
        dotenv::dotenv().ok();

        // 检查 .env 文件是否存在
        assert!(
            check_file_exists(".env"),
            "错误: .env 文件不存在，请确保项目根目录下有 .env 文件"
        );

        // 从环境变量中获取 DATA_FILE 和 FAILED_FILE 的值，并提供默认值
        let data_file = env::var("DATA_FILE").unwrap_or("repositories.data".to_string());
        let failed_file = env::var("FAILED_FILE").unwrap_or("failed_repos.data".to_string());

        // 检查 DATA_FILE 指定的文件是否存在
        assert!(
            check_file_exists(&data_file),
            "错误: {} 文件不存在，请确保项目根目录下有该文件",
            data_file
        );

        // 检查 FAILED_FILE 是否可创建（不要求必须存在，但需要确保目录可写）
        let failed_dir = Path::new(&failed_file).parent().unwrap_or(Path::new("."));
        if !failed_dir.exists() {
            fs::create_dir_all(failed_dir).expect("无法创建 FAILED_FILE 的目录");
        }

        // 清理日志文件
        clear_log_files("logs").expect("清理日志文件失败");
        println!("环境检查和日志清理完成");
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use std::fs;

        #[test]
        fn test_all_repos_format() {
            logger::init_logger();
            dotenv::dotenv().ok();

            let data_file = env::var("DATA_FILE").unwrap_or("repositories.data".to_string());
            let repos_content = fs::read_to_string(&data_file)
                .map_err(|e| {
                    let err = AppError::FileError(format!("无法读取数据文件 {}: {}", data_file, e));
                    log_error(&err);
                    err
                })
                .expect("读取文件失败");

            let repos: Vec<String> = repos_content.lines().map(|s| s.trim().to_string()).collect();

            for repo in repos {
                match validate_repo_format(&repo) {
                    Ok((cratesname, owner, name)) => {
                        println!("Valid repo: {} -> cratesname: {}, owner: {}, name: {}", repo, cratesname, owner, name);
                    }
                    Err(err) => log_error(&err),
                }
            }
        }
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