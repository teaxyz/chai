mod app_state;
mod db;
mod handlers;
mod logging;
mod utils;

use actix_web::{web, App, HttpServer};
use dotenv::dotenv;
use std::env;
use std::sync::Arc;

use crate::app_state::AppState;
use crate::db::create_db_client;
use crate::handlers::{get_table, get_table_row, heartbeat, list_tables};
use crate::logging::setup_logger;

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv().ok();
    setup_logger();

    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let host = env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());
    let port = env::var("PORT").unwrap_or_else(|_| "8080".to_string());
    let bind_address = format!("{}:{}", host, port);

    let client = create_db_client(&database_url).await;
    let tables = Arc::new(db::get_tables(&client).await);

    log::info!("Available tables: {:?}", tables);
    log::info!("Starting server at http://{}", bind_address);

    HttpServer::new(move || {
        App::new()
            .wrap(logging::Logger::default())
            .app_data(web::Data::new(AppState {
                client: Arc::clone(&client),
                tables: Arc::clone(&tables),
            }))
            .service(list_tables)
            .service(heartbeat)
            .service(get_table)
            .service(get_table_row)
    })
    .bind(&bind_address)?
    .run()
    .await
}
