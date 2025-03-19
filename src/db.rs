use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;
use std::env;
use std::fs::{self, File};
use std::io::Write;
use chrono::Utc;
use std::error::Error;

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

pub async fn log_error(owner: &str, name: &str, error: &str) -> Result<(), Box<dyn Error + Send + Sync>> {
    let log_dir = "error_logs";
    fs::create_dir_all(log_dir)?;

    let timestamp = Utc::now().format("%Y-%m-%d_%H-%M-%S").to_string();
    let filename = format!("{}/{}_{}_{}.log", log_dir, owner, name, timestamp);
    let mut file = File::create(&filename)?;
    writeln!(file, "Error at {}: {}", Utc::now().to_rfc3339(), error)?;
    Ok(())
}

pub fn delete_error_log(owner: &str, name: &str) {
    let log_dir = "error_logs";
    if let Ok(entries) = fs::read_dir(log_dir) {
        for entry in entries.flatten() {
            let path = entry.path();
            if let Some(filename) = path.file_name().and_then(|n| n.to_str()) {
                if filename.starts_with(&format!("{}_{}", owner, name)) {
                    let _ = fs::remove_file(path);
                }
            }
        }
    }
}

pub async fn init_db(pg_client: &tokio_postgres::Client) -> Result<(), Box<dyn Error + Send + Sync>> {
    let password = env::var("DB_PASSWORD").expect("DB_PASSWORD not set");
    let create_user = format!("CREATE ROLE github_user WITH LOGIN PASSWORD '{}'", password);
    if let Err(e) = pg_client.execute(&create_user, &[]).await {
        if !e.to_string().contains("already exists") {
            return Err(Box::new(e));
        }
    }

    let create_db = "CREATE DATABASE github_db OWNER github_user";
    if let Err(e) = pg_client.execute(create_db, &[]).await {
        if !e.to_string().contains("already exists") {
            return Err(Box::new(e));
        }
    }

    let pool = create_pool().await?;
    let github_client = pool.get().await?;

    let drop_repositories = "DROP TABLE IF EXISTS repositories CASCADE";
    github_client.execute(drop_repositories, &[]).await?;

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

    Ok(())
}

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
        Ok(())
    } else {
        Err("No repository data or error in response".into())
    }
}