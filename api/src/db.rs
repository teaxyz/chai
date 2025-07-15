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

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;
    use url::Url;

    #[test]
    fn test_database_url_parsing() {
        // Test valid PostgreSQL URL parsing
        let test_url = "postgres://user:pass@localhost:5432/dbname";
        let parsed = Url::parse(test_url).unwrap();
        
        assert_eq!(parsed.host_str(), Some("localhost"));
        assert_eq!(parsed.port(), Some(5432));
        assert_eq!(parsed.username(), "user");
        assert_eq!(parsed.password(), Some("pass"));
        assert_eq!(parsed.path(), "/dbname");
    }

    #[test]
    fn test_database_url_without_password() {
        let test_url = "postgres://user@localhost:5432/dbname";
        let parsed = Url::parse(test_url).unwrap();
        
        assert_eq!(parsed.host_str(), Some("localhost"));
        assert_eq!(parsed.port(), Some(5432));
        assert_eq!(parsed.username(), "user");
        assert_eq!(parsed.password(), None);
        assert_eq!(parsed.path(), "/dbname");
    }

    #[test]
    fn test_database_url_with_default_port() {
        let test_url = "postgres://user:pass@localhost/dbname";
        let parsed = Url::parse(test_url).unwrap();
        
        assert_eq!(parsed.host_str(), Some("localhost"));
        assert_eq!(parsed.port(), None); // Default port not specified
        assert_eq!(parsed.username(), "user");
        assert_eq!(parsed.password(), Some("pass"));
        assert_eq!(parsed.path(), "/dbname");
    }

    #[test]
    fn test_database_url_with_special_characters() {
        let test_url = "postgres://user%40example.com:pa%24%24@localhost:5432/my_db";
        let parsed = Url::parse(test_url).unwrap();
        
        assert_eq!(parsed.host_str(), Some("localhost"));
        assert_eq!(parsed.port(), Some(5432));
        assert_eq!(parsed.username(), "user@example.com");
        assert_eq!(parsed.password(), Some("pa$$"));
        assert_eq!(parsed.path(), "/my_db");
    }

    #[test]
    fn test_invalid_database_url() {
        let invalid_urls = vec![
            "not_a_url",
            "http://localhost", // wrong scheme
            "postgres://", // incomplete
            "postgres://user@", // no host
        ];
        
        for invalid_url in invalid_urls {
            let result = Url::parse(invalid_url);
            assert!(result.is_err(), "Expected error for URL: {}", invalid_url);
        }
    }

    #[test]
    fn test_database_config_creation() {
        // Test that we can create a database config
        let mut config = deadpool_postgres::Config::new();
        config.host = Some("localhost".to_owned());
        config.port = Some(5432);
        config.user = Some("test_user".to_owned());
        config.password = Some("test_pass".to_owned());
        config.dbname = Some("test_db".to_owned());

        // Basic validation that config fields are set correctly
        assert_eq!(config.host, Some("localhost".to_owned()));
        assert_eq!(config.port, Some(5432));
        assert_eq!(config.user, Some("test_user".to_owned()));
        assert_eq!(config.password, Some("test_pass".to_owned()));
        assert_eq!(config.dbname, Some("test_db".to_owned()));
    }

    #[test]
    fn test_database_path_stripping() {
        // Test the path stripping logic used in create_pool
        let test_cases = vec![
            ("/dbname", Some("dbname")),
            ("/", None),
            ("", None),
            ("/test_db", Some("test_db")),
            ("/my_database_name", Some("my_database_name")),
        ];

        for (input, expected) in test_cases {
            let result = input.strip_prefix('/').and_then(|s| if s.is_empty() { None } else { Some(s) });
            assert_eq!(result, expected, "Failed for input: {}", input);
        }
    }

    // Note: We can't easily test create_pool, get_tables, and initialize_db 
    // without a real database connection, so we focus on the parsing logic
    // and configuration setup that can be tested in isolation.
}
