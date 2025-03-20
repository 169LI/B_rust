use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;
use std::env;
use crate::errors::AppError;

pub async fn connect_postgres() -> Result<tokio_postgres::Client, AppError> {
    let host = env::var("DB_HOST").unwrap_or("localhost".to_string());
    let port = env::var("DB_PORT").unwrap_or("5432".to_string());
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

pub async fn init_db(pg_client: &mut tokio_postgres::Client) -> Result<(), AppError> {
    if pg_client.is_closed() {
        return Err(AppError::DatabaseError("数据库连接已关闭".to_string()));
    }

    let db_exists = pg_client
        .query_one("SELECT 1 FROM pg_database WHERE datname='github_db'", &[])
        .await
        .is_ok();
    if !db_exists {
        pg_client
            .execute("CREATE DATABASE github_db", &[])
            .await?;
    }

    let host = env::var("DB_HOST").unwrap_or("localhost".to_string());
    let port = env::var("DB_PORT").unwrap_or("5432".to_string());
    let password = env::var("POSTGRES_PASSWORD").expect("POSTGRES_PASSWORD not set");
    let conn_str = format!(
        "host={} port={} user=postgres password={} dbname=github_db",
        host, port, password
    );
    let (pg_client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("Database connection error: {}", e);
        }
    });

    let table_exists = pg_client
        .query_one(
            "SELECT 1 FROM information_schema.tables WHERE table_name='repositories'",
            &[],
        )
        .await
        .is_ok();
    if table_exists {
        let column_type = pg_client
            .query_one(
                "SELECT data_type FROM information_schema.columns WHERE table_name='repositories' AND column_name='stars'",
                &[],
            )
            .await?;
        if column_type.get::<_, String>(0) != "bigint" {
            pg_client.execute("DROP TABLE repositories", &[]).await?;
        } else {
            return Ok(());
        }
    }

    pg_client
        .execute(
            r#"
            CREATE TABLE IF NOT EXISTS repositories (
                id SERIAL PRIMARY KEY,
                owner VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                stars BIGINT NOT NULL,
                forks BIGINT NOT NULL,
                commits BIGINT NOT NULL,
                issues BIGINT NOT NULL,
                merged_prs BIGINT NOT NULL,
                UNIQUE(owner, name)
            )
            "#,
            &[],
        )
        .await?;

    Ok(())
}

pub async fn create_pool() -> Result<Pool, AppError> {
    let mut cfg = Config::new();
    cfg.host = Some(env::var("DB_HOST").unwrap_or("localhost".to_string()));
    cfg.port = Some(env::var("DB_PORT").unwrap_or("5432".to_string()).parse()?);
    cfg.user = Some(env::var("DB_USER").unwrap_or("postgres".to_string()));
    cfg.password = Some(env::var("DB_PASSWORD").unwrap_or("github@123".to_string()));
    cfg.dbname = Some(env::var("DB_NAME").unwrap_or("github_db".to_string()));
    cfg.pool = Some(deadpool_postgres::PoolConfig::new(100));
    cfg.manager = Some(ManagerConfig {
        recycling_method: RecyclingMethod::Fast,
    });

    let pool = cfg.create_pool(Some(Runtime::Tokio1), NoTls)?;
    Ok(pool)
}

pub async fn store_repo_data(
    pg_client: &deadpool_postgres::Client,
    owner: &str,
    name: &str,
    response: serde_json::Value,
) -> Result<(), AppError> {
    if let Some(repo) = response.get("data").and_then(|d| d.get("repository")) {
        let stars = repo["stargazers"]["totalCount"].as_i64().unwrap_or(0);
        let forks = repo["forks"]["totalCount"].as_i64().unwrap_or(0);
        let commits = repo["defaultBranchRef"]["target"]["history"]["totalCount"]
            .as_i64()
            .unwrap_or(0);
        let issues = repo["issues"]["totalCount"].as_i64().unwrap_or(0);
        let merged_prs = repo["pullRequests"]["totalCount"].as_i64().unwrap_or(0);

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
        Err(AppError::ApiError("API响应中没有仓库数据".to_string()))
    }
}