use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use chrono::Utc;
use std::error::Error;

/// 创建 PostgreSQL 连接池
pub async fn create_pool() -> Result<Pool, Box<dyn Error + Send + Sync>> {
    let mut cfg = Config::new();
    cfg.host = Some(env::var("DB_HOST").expect("DB_HOST not set"));
    cfg.port = Some(env::var("DB_PORT").expect("DB_PORT not set").parse()?);
    cfg.user = Some(env::var("DB_USER").expect("DB_USER not set"));
    cfg.password = Some(env::var("DB_PASSWORD").expect("DB_PASSWORD not set"));
    cfg.dbname = Some(env::var("DB_NAME").expect("DB_NAME not set"));
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
    Ok(pool)
}

/// 直接连接到 PostgreSQL 数据库
pub async fn connect_postgres() -> Result<tokio_postgres::Client, tokio_postgres::Error> {
    let host = env::var("DB_HOST").expect("DB_HOST not set");
    let port = env::var("DB_PORT").expect("DB_PORT not set");
    let password = env::var("POSTGRES_PASSWORD").expect("POSTGRES_PASSWORD not set");

    let conn_str = format!(
        "host={} port={} user=postgres password={}",
        host, port, password
    );

    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Initial database connection error: {}", e);
        }
    });

    Ok(client)
}

/// 记录 SQL 语句到日志文件
fn log_sql(query: &str, context: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let log_dir = "sql_logs";
    fs::create_dir_all(log_dir)?;

    let timestamp = Utc::now().format("%Y-%m-%d_%H-%M-%S").to_string();
    let filename = format!("{}/{}_{}.sql", log_dir, context, timestamp);
    let mut file = File::create(&filename)?;

    writeln!(file, "-- SQL executed at {}", Utc::now().to_rfc3339())?;
    writeln!(file, "{}", query)?;
    println!("Logged SQL to {}", filename);

    Ok(())
}

/// 初始化数据库和表结构
pub async fn init_db(pg_client: &tokio_postgres::Client) -> Result<(), Box<dyn Error + Send + Sync>> {
    let password = env::var("DB_PASSWORD").expect("DB_PASSWORD not set");
    let create_user = format!("CREATE ROLE github_user WITH LOGIN PASSWORD '{}'", password);
    if let Err(e) = pg_client.execute(&create_user, &[]).await {
        if !e.to_string().contains("already exists") {
            return Err(Box::new(e));
        }
    }
    log_sql(&create_user, "init_user")?;

    let create_db = "CREATE DATABASE github_db OWNER github_user";
    if let Err(e) = pg_client.execute(create_db, &[]).await {
        if !e.to_string().contains("already exists") {
            return Err(Box::new(e));
        }
    }
    log_sql(create_db, "init_db")?;

    let pool = create_pool().await?;
    let github_client = pool.get().await?;

    // 删除旧表（如果存在）
    let drop_repositories = "DROP TABLE IF EXISTS repositories CASCADE";
    github_client.execute(drop_repositories, &[]).await?;
    log_sql(drop_repositories, "drop_repositories")?;

    // 创建新表
    let create_repositories = r#"
        CREATE TABLE IF NOT EXISTS repositories (
            id SERIAL PRIMARY KEY,
            owner VARCHAR(255) NOT NULL,
            name VARCHAR(255) NOT NULL,
            stars INTEGER NOT NULL,
            forks INTEGER NOT NULL,
            commits INTEGER NOT NULL,
            issues INTEGER NOT NULL,
            merged_prs INTEGER NOT NULL,
            UNIQUE(owner, name)
        )"#;
    github_client.execute(create_repositories, &[]).await?;
    log_sql(create_repositories, "init_repositories")?;

    println!("Database and tables initialized");
    Ok(())
}

/// 存储仓库数据到数据库
pub async fn store_repo_data(
    pg_client: &deadpool_postgres::Client,
    owner: &str,
    name: &str,
    response: serde_json::Value,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    if let Some(repo) = response.get("data").and_then(|d| d.get("repository")) {
        let stars = repo["stargazers"]["totalCount"].as_i64().unwrap_or(0) as i32;
        let forks = repo["forks"]["totalCount"].as_i64().unwrap_or(0) as i32;
        let commits = repo["defaultBranchRef"]["target"]["history"]["totalCount"]
            .as_i64()
            .unwrap_or(0) as i32;
        let issues = repo["issues"]["totalCount"].as_i64().unwrap_or(0) as i32;
        let merged_prs = repo["pullRequests"]["totalCount"].as_i64().unwrap_or(0) as i32;

        let query = r#"
            INSERT INTO repositories (owner, name, stars, forks, commits, issues, merged_prs)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (owner, name) DO UPDATE
            SET stars = $3, forks = $4, commits = $5, issues = $6, merged_prs = $7
        "#;

        pg_client
            .execute(
                query,
                &[&owner, &name, &stars, &forks, &commits, &issues, &merged_prs],
            )
            .await?;
        log_sql(query, &format!("{}_{}", owner, name))?;

        println!("Stored data for {}/{}", owner, name);
    } else if let Some(errors) = response.get("errors") {
        println!("Error for {}/{}: {:?}", owner, name, errors);
    } else {
        println!("No repository data for {}/{}", owner, name);
    }

    Ok(())
}