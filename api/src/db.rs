use deadpool_postgres::{Config, Pool, Runtime};
use std::env;
use std::sync::Arc;
use tokio_postgres::{Client, NoTls};
use url::Url;

pub async fn create_pool() -> Pool {
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let db_url = Url::parse(&database_url).expect("Invalid database URL");

    let mut config = Config::new();
    config.host = db_url.host_str().map(ToOwned::to_owned);
    config.port = db_url.port();
    config.user = Some(db_url.username().to_owned());
    config.password = db_url.password().map(ToOwned::to_owned);
    config.dbname = db_url.path().strip_prefix('/').map(ToOwned::to_owned);

    config
        .create_pool(Some(Runtime::Tokio1), NoTls)
        .expect("Failed to create pool")
}

pub async fn get_tables(client: &Client) -> Vec<String> {
    let rows = client
        .query(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'",
            &[],
        )
        .await
        .expect("Failed to fetch tables");

    rows.into_iter()
        .map(|row| row.get::<_, String>("table_name"))
        .collect()
}

pub async fn initialize_db() -> (Pool, Arc<Vec<String>>) {
    let pool = create_pool().await;
    let client = pool.get().await.expect("Failed to get client from pool");
    let tables = Arc::new(get_tables(&client).await);
    (pool, tables)
}
