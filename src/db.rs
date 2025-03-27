use deadpool_postgres::{Config, ManagerConfig, Pool, RecyclingMethod, Runtime};
use tokio_postgres::NoTls;
use std::env;
use crate::errors::AppError;
use crate::logger::log_error;

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

pub async fn connect_postgres() -> Result<tokio_postgres::Client, AppError> {
    let host = env::var("DB_HOST").expect("DB_HOST not set");
    let port = env::var("DB_PORT").expect("DB_PORT not set");
    let user = env::var("DB_USER").expect("DB_USER not set");
    let password = env::var("POSTGRES_PASSWORD").expect("POSTGRES_PASSWORD not set");
    let conn_str = format!(
        "host={} port={} user={} password={}",
        host, port, user, password
    );
    let (client, connection) = tokio_postgres::connect(&conn_str, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            log_error(&AppError::DatabaseError(format!("数据库已断开连接: {}", e))); // 使用 log_error
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
            log_error(&AppError::DatabaseError(format!("数据库连接错误: {}", e)));
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
        let column_exists = pg_client
            .query_one(
                "SELECT 1 FROM information_schema.columns WHERE table_name='repositories' AND column_name='cratesname'",
                &[],
            )
            .await
            .is_ok();
        if !column_exists {
            pg_client.execute("DROP TABLE repositories", &[]).await?;
        }
    }

    pg_client
        .execute(
            r#"
            CREATE TABLE IF NOT EXISTS repositories (
                cratesname VARCHAR(255) PRIMARY KEY,
                owner VARCHAR(255) NOT NULL,
                name VARCHAR(255) NOT NULL,
                stars BIGINT NOT NULL,
                forks BIGINT NOT NULL,
                commits BIGINT NOT NULL,
                issues BIGINT NOT NULL,
                merged_prs BIGINT NOT NULL
            )
            "#,
            &[],
        )
        .await?;

    Ok(())
}

pub async fn store_repo_data(
    pg_client: &deadpool_postgres::Client,
    cratesname: &str,
    owner: &str,
    name: &str,
    response: serde_json::Value,
) -> Result<bool, AppError> {
    if let Some(repo) = response.get("data").and_then(|d| d.get("repository")) {
        let stars = repo["stargazers"]["totalCount"].as_i64().unwrap_or(0);
        let forks = repo["forks"]["totalCount"].as_i64().unwrap_or(0);
        let commits = repo["defaultBranchRef"]["target"]["history"]["totalCount"]
            .as_i64()
            .unwrap_or(0);
        let issues = repo["issues"]["totalCount"].as_i64().unwrap_or(0);
        let merged_prs = repo["pullRequests"]["totalCount"].as_i64().unwrap_or(0);

        let query = r#"
            INSERT INTO repositories (cratesname, owner, name, stars, forks, commits, issues, merged_prs)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (cratesname) DO UPDATE
            SET owner = $2, name = $3, stars = $4, forks = $5, commits = $6, issues = $7, merged_prs = $8
        "#;

        let rows_affected = pg_client
            .execute(
                query,
                &[
                    &cratesname, &owner, &name, &stars, &forks, &commits, &issues, &merged_prs,
                ],
            )
            .await?;
        Ok(rows_affected == 1) // 新插入返回 true，更新返回 false
    } else {
        Err(AppError::ApiError("API响应中没有仓库数据".to_string()))
    }
}