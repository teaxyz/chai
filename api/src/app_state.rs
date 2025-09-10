use dashmap::DashMap;
use deadpool_postgres::Pool;
use serde_json::Value;
use std::sync::Arc;
use std::time::{Duration, Instant};
use uuid::Uuid;

const TTL: Duration = Duration::from_secs(3600); // 1 hour

#[derive(Clone)]
pub struct ProjectCacheEntry {
    pub data: Arc<Value>,
    pub created_at: Instant,
}

impl ProjectCacheEntry {
    pub fn new(data: Value) -> Self {
        Self {
            data: Arc::new(data),
            created_at: Instant::now(),
        }
    }

    pub fn is_expired(&self) -> bool {
        self.created_at.elapsed() > TTL
    }
}

pub struct AppState {
    pub pool: Pool,
    pub tables: Arc<Vec<String>>,
    pub project_cache: Arc<DashMap<Uuid, ProjectCacheEntry>>,
}
