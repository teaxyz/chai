use actix_web::{get, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio_postgres::error::SqlState;
use uuid::Uuid;

use crate::app_state::AppState;
use crate::utils::{get_column_names, rows_to_json, Pagination};

#[derive(Deserialize)]
pub struct PaginationParams {
    pub page: Option<i64>,
    pub limit: Option<i64>,
}

#[derive(Serialize)]
struct PaginatedResponse {
    table: String,
    total_count: i64,
    page: i64,
    limit: i64,
    total_pages: i64,
    columns: Vec<String>,
    data: Vec<Value>,
}

pub fn check_table_exists(table: &str, tables: &[String]) -> Option<HttpResponse> {
    if !tables.contains(&table.to_string()) {
        Some(HttpResponse::NotFound().json(json!({
            "error": format!("Table '{}' not found", table),
            "valid_tables": tables,
            "help": "Refer to the API documentation for valid table names."
        })))
    } else {
        None
    }
}

#[get("/tables")]
pub async fn list_tables(
    query: web::Query<PaginationParams>,
    data: web::Data<AppState>,
) -> impl Responder {
    let total_count = data.tables.len() as i64;
    let pagination = Pagination::new(query, total_count);

    let start = pagination.offset as usize;
    let end = (start + pagination.limit as usize).min(data.tables.len());

    let paginated_tables = &data.tables[start..end];

    HttpResponse::Ok().json(json!({
        "total_count": total_count,
        "page": pagination.page,
        "limit": pagination.limit,
        "total_pages": pagination.total_pages,
        "data": paginated_tables,
    }))
}

#[get("/heartbeat")]
pub async fn heartbeat(data: web::Data<AppState>) -> impl Responder {
    match data.pool.get().await {
        Ok(client) => match client.query_one("SELECT 1", &[]).await {
            Ok(_) => HttpResponse::Ok().body("OK - Database connection is healthy"),
            Err(e) => {
                log::error!("Database query failed: {}", e);
                HttpResponse::InternalServerError().body("Database query failed")
            }
        },
        Err(e) => {
            log::error!("Failed to get database connection: {}", e);
            HttpResponse::InternalServerError().body("Failed to get database connection")
        }
    }
}

#[get("/{table}")]
pub async fn get_table(
    path: web::Path<String>,
    query: web::Query<PaginationParams>,
    data: web::Data<AppState>,
) -> impl Responder {
    let table = path.into_inner();
    if let Some(response) = check_table_exists(&table, &data.tables) {
        return response;
    }

    let count_query = format!("SELECT COUNT(*) FROM {}", table);
    match data.pool.get().await {
        Ok(client) => match client.query_one(&count_query, &[]).await {
            Ok(count_row) => {
                let total_count: i64 = count_row.get(0);
                let pagination = Pagination::new(query, total_count);

                let data_query = format!("SELECT * FROM {} LIMIT $1 OFFSET $2", table);
                match client
                    .query(&data_query, &[&pagination.limit, &pagination.offset])
                    .await
                {
                    Ok(rows) => {
                        let columns = get_column_names(&rows);
                        let data = rows_to_json(&rows);
                        let response = PaginatedResponse {
                            table,
                            total_count,
                            page: pagination.page,
                            limit: pagination.limit,
                            total_pages: pagination.total_pages,
                            columns,
                            data,
                        };
                        HttpResponse::Ok().json(response)
                    }
                    Err(e) => {
                        log::error!("Database query error: {}", e);
                        HttpResponse::InternalServerError().json(json!({
                            "error": "An error occurred while querying the database"
                        }))
                    }
                }
            }
            Err(e) => {
                log::error!("Database count query error: {}", e);
                HttpResponse::InternalServerError().json(json!({
                    "error": "An error occurred while counting rows in the database"
                }))
            }
        },
        Err(e) => {
            log::error!("Failed to get database connection: {}", e);
            HttpResponse::InternalServerError().body("Failed to get database connection")
        }
    }
}

#[get("/{table}/{id}")]
pub async fn get_table_row(
    path: web::Path<(String, Uuid)>,
    data: web::Data<AppState>,
) -> impl Responder {
    let (table_name, id) = path.into_inner();

    if let Some(response) = check_table_exists(&table_name, &data.tables) {
        return response;
    }

    let query = format!("SELECT * FROM {} WHERE id = $1", table_name);

    match data.pool.get().await {
        Ok(client) => match client.query_one(&query, &[&id]).await {
            Ok(row) => {
                let json = rows_to_json(&[row]);
                let value = json.first().unwrap();
                HttpResponse::Ok().json(value)
            }
            Err(e) => {
                if e.as_db_error()
                    .is_some_and(|db_err| db_err.code() == &SqlState::UNDEFINED_TABLE)
                {
                    HttpResponse::NotFound().json(json!({
                        "error": format!("Table '{}' not found", table_name)
                    }))
                } else if e
                    .as_db_error()
                    .is_some_and(|e| e.code() == &SqlState::NO_DATA_FOUND)
                {
                    HttpResponse::NotFound().json(json!({
                        "error": format!("No row found with id '{}' in table '{}'", id, table_name)
                    }))
                } else {
                    HttpResponse::InternalServerError().json(json!({
                        "error": format!("Database error: {}", e)
                    }))
                }
            }
        },
        Err(e) => {
            log::error!("Failed to get database connection: {}", e);
            HttpResponse::InternalServerError().body("Failed to get database connection")
        }
    }
}
