use deadpool_postgres::Pool;
use std::sync::Arc;

pub struct AppState {
    pub pool: Pool,
    pub tables: Arc<Vec<String>>,
}
