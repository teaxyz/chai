mod app_state;
mod db;
mod handlers;
mod logging;
mod utils;

use actix_web::{web, App, HttpServer};
use dashmap::DashMap;
use dotenv::dotenv;
use std::env;
use std::sync::Arc;

use crate::app_state::AppState;
use crate::handlers::{
    get_leaderboard, get_project, get_table, get_table_row, heartbeat, list_projects_by_id,
    list_projects_by_name, list_tables,
};
use crate::logging::setup_logger;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    setup_logger();

    let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_address = format!("{host}:{port}");

    let (pool, tables) = db::initialize_db().await;
    // Cache for project data to reduce database load on leaderboard routes
    let project_cache = Arc::new(DashMap::new());

    log::info!("Available tables: {tables:?}");
    log::info!("Starting server at http://{bind_address}");

    HttpServer::new(move || {
        App::new()
            .wrap(logging::Logger::default())
            .app_data(web::Data::new(AppState {
                pool: pool.clone(),
                tables: Arc::clone(&tables),
                project_cache: Arc::clone(&project_cache),
            }))
            // HEALTH
            .service(heartbeat)
            // SIMPLE CRUD OPERATIONS
            .service(list_tables)
            .service(get_table)
            .service(get_table_row)
            // BUSINESS LOGIC
            .service(get_leaderboard)
            .service(get_project)
            .service(list_projects_by_id)
            .service(list_projects_by_name)
    })
    .bind(&bind_address)?
    .run()
    .await
}
