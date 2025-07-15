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
                log::error!("Database query failed: {e}");
                HttpResponse::InternalServerError().body("Database query failed")
            }
        },
        Err(e) => {
            log::error!("Failed to get database connection: {e}");
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

    let count_query = format!("SELECT COUNT(*) FROM {table}");
    match data.pool.get().await {
        Ok(client) => match client.query_one(&count_query, &[]).await {
            Ok(count_row) => {
                let total_count: i64 = count_row.get(0);
                let pagination = Pagination::new(query, total_count);

                let data_query = format!("SELECT * FROM {table} LIMIT $1 OFFSET $2");
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
                        log::error!("Database query error: {e}");
                        HttpResponse::InternalServerError().json(json!({
                            "error": "An error occurred while querying the database"
                        }))
                    }
                }
            }
            Err(e) => {
                log::error!("Database count query error: {e}");
                HttpResponse::InternalServerError().json(json!({
                    "error": "An error occurred while counting rows in the database"
                }))
            }
        },
        Err(e) => {
            log::error!("Failed to get database connection: {e}");
            HttpResponse::InternalServerError().body("Failed to get database connection")
        }
    }
}

#[get("/project/{id}")]
pub async fn get_projects(path: web::Path<Uuid>, data: web::Data<AppState>) -> impl Responder {
    // Check if the table exists
    let id = path.into_inner();

    // Construct the query
    let query = r#"
        SELECT DISTINCT ON (c.id)
            c.id AS "projectId",
            u_homepage.url AS homepage,
            c.name,
            u_source.url AS source,
            COALESCE(tr.rank,'0') AS "teaRank",
            tr.created_at AS "teaRankCalculatedAt",
            (
                SELECT ARRAY_AGG(DISTINCT s.type)
                FROM canon_packages cp2
                JOIN packages p2 ON cp2.package_id = p2.id
                JOIN package_managers pm2 ON p2.package_manager_id = pm2.id
                JOIN sources s ON pm2.source_id = s.id
                WHERE cp2.canon_id = c.id
            ) AS "packageManagers"
        FROM canons c
        JOIN urls u_homepage ON c.url_id = u_homepage.id 
        JOIN canon_packages cp ON cp.canon_id = c.id
        JOIN package_urls pu ON pu.package_id = cp.package_id
        JOIN urls u_source ON pu.url_id = u_source.id
        JOIN url_types ut_source ON ut_source.id = u_source.url_type_id
        LEFT JOIN tea_ranks tr ON tr.canon_id = c.id
        WHERE 
            c.id = $1 
            AND ut_source.name = 'source'
        ORDER BY c.id, tr.created_at DESC, u_source.url;"#;

    match data.pool.get().await {
        Ok(client) => match client.query_one(query, &[&id]).await {
            Ok(row) => {
                let json = rows_to_json(&[row]);
                let value = json.first().unwrap();
                HttpResponse::Ok().json(value)
            }
            Err(e) => {
                if e.as_db_error()
                    .is_some_and(|e| e.code() == &SqlState::NO_DATA_FOUND)
                {
                    HttpResponse::NotFound().json(json!({
                        "error": format!("No row found with id '{:?}' in table canons", id)
                    }))
                } else {
                    HttpResponse::InternalServerError().json(json!({
                        "error": format!("Database error: {}", e)
                    }))
                }
            }
        },
        Err(e) => {
            log::error!("Failed to get database connection: {e}");
            HttpResponse::InternalServerError().body("Failed to get database connection")
        }
    }
}

#[get("/project/batch/{ids}")]
pub async fn get_projects_batch(
    path: web::Path<String>,
    data: web::Data<AppState>,
) -> impl Responder {
    let ids_str = path.into_inner();

    // Parse comma-separated UUIDs
    let ids: Result<Vec<Uuid>, _> = ids_str
        .split(',')
        .map(|s| s.trim().parse::<Uuid>())
        .collect();

    let Ok(ids) = ids else {
        return HttpResponse::BadRequest()
            .json(json!({"error": format!("Invalid UUID format in project IDs")}));
    };

    if ids.is_empty() {
        return HttpResponse::BadRequest().json(json!({
            "error": "No project IDs provided"
        }));
    }

    // Construct the query
    let query = r#"
        SELECT DISTINCT ON (c.id)
            c.id AS "projectId",
            u_homepage.url AS homepage,
            c.name,
            u_source.url AS source,
            COALESCE(tr.rank,'0') AS "teaRank",
            tr.created_at AS "teaRankCalculatedAt",
            (
                SELECT ARRAY_AGG(DISTINCT s.type)
                FROM canon_packages cp2
                JOIN packages p2 ON cp2.package_id = p2.id
                JOIN package_managers pm2 ON p2.package_manager_id = pm2.id
                JOIN sources s ON pm2.source_id = s.id
                WHERE cp2.canon_id = c.id
            ) AS "packageManagers"
        FROM canons c
        JOIN urls u_homepage ON u_homepage.id = c.url_id 
        JOIN canon_packages cp ON cp.canon_id = c.id
        JOIN package_urls pu ON pu.package_id = cp.package_id
        JOIN urls u_source ON pu.url_id = u_source.id
        JOIN url_types ut ON ut.id = u_source.url_type_id
        LEFT JOIN tea_ranks tr ON tr.canon_id = c.id
        WHERE c.id = ANY($1::uuid[]) AND ut.name = 'source'
        ORDER BY c.id, tr.created_at DESC, u_source.url;"#;

    match data.pool.get().await {
        Ok(client) => match client.query(query, &[&ids]).await {
            Ok(rows) => {
                let json = rows_to_json(&rows);
                HttpResponse::Ok().json(json)
            }
            Err(e) => {
                log::error!("Database query error: {e}");
                HttpResponse::InternalServerError().json(json!({
                    "error": format!("Database error: {}", e)
                }))
            }
        },
        Err(e) => {
            log::error!("Failed to get database connection: {e}");
            HttpResponse::InternalServerError().body("Failed to get database connection")
        }
    }
}

#[get("/project/search/{name}")]
pub async fn search_projects(path: web::Path<String>, data: web::Data<AppState>) -> impl Responder {
    let name = path.into_inner();

    if name.trim().is_empty() {
        return HttpResponse::BadRequest().json(json!({
            "error": "Search name cannot be empty"
        }));
    }

    let wildcard = format!("%{name}%");

    // Construct the query
    let query = r#"
        SELECT *
        FROM (
            SELECT DISTINCT ON (c.id)
                c.id AS "projectId",
                u_homepage.url AS homepage,
                c.name,
                u_source.url AS source,
                (
                    SELECT ARRAY_AGG(DISTINCT s.type)
                    FROM canon_packages cp2
                    JOIN packages p2 ON cp2.package_id = p2.id
                    JOIN package_managers pm2 ON p2.package_manager_id = pm2.id
                    JOIN sources s ON pm2.source_id = s.id
                    WHERE cp2.canon_id = c.id
                ) AS "packageManagers"
            FROM canons c
            JOIN urls u_homepage ON c.url_id = u_homepage.id
            JOIN canon_packages cp ON cp.canon_id = c.id
            JOIN package_urls pu ON pu.package_id = cp.package_id
            JOIN urls u_source ON pu.url_id = u_source.id
            JOIN url_types ut_source ON ut_source.id = u_source.url_type_id
            WHERE ut_source.name = 'source' AND (c.name ILIKE $1)
            ORDER BY c.id
        ) sub
        ORDER BY LENGTH(name), name
        LIMIT 10;"#;

    match data.pool.get().await {
        Ok(client) => match client.query(query, &[&wildcard]).await {
            Ok(rows) => {
                let json = rows_to_json(&rows);
                HttpResponse::Ok().json(json)
            }
            Err(e) => {
                log::error!("Database query error: {e}");
                HttpResponse::InternalServerError().json(json!({
                    "error": format!("Database error: {e}")
                }))
            }
        },
        Err(e) => {
            log::error!("Failed to get database connection: {e}");
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

    let query = format!("SELECT * FROM {table_name} WHERE id = $1");

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
            log::error!("Failed to get database connection: {e}");
            HttpResponse::InternalServerError().body("Failed to get database connection")
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::test;
    use actix_web::web::Query;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_check_table_exists_with_valid_table() {
        let tables = vec!["users".to_string(), "posts".to_string(), "comments".to_string()];
        let result = check_table_exists("users", &tables);
        assert!(result.is_none());
    }

    #[test]
    fn test_check_table_exists_with_invalid_table() {
        let tables = vec!["users".to_string(), "posts".to_string(), "comments".to_string()];
        let result = check_table_exists("invalid_table", &tables);
        assert!(result.is_some());
        
        let response = result.unwrap();
        assert_eq!(response.status(), 404);
    }

    #[test]
    fn test_check_table_exists_with_empty_tables() {
        let tables = vec![];
        let result = check_table_exists("any_table", &tables);
        assert!(result.is_some());
        
        let response = result.unwrap();
        assert_eq!(response.status(), 404);
    }

    #[test]
    fn test_check_table_exists_case_sensitivity() {
        let tables = vec!["users".to_string(), "Posts".to_string()];
        
        // Should not find "Users" when table is "users"
        let result = check_table_exists("Users", &tables);
        assert!(result.is_some());
        
        // Should find exact match
        let result = check_table_exists("users", &tables);
        assert!(result.is_none());
        
        // Should find exact match for "Posts"
        let result = check_table_exists("Posts", &tables);
        assert!(result.is_none());
    }

    #[test]
    fn test_pagination_params_deserialization() {
        // Test default values
        let query = Query::from_query("").unwrap();
        let params: &PaginationParams = &query;
        assert_eq!(params.page, None);
        assert_eq!(params.limit, None);
        
        // Test with values
        let query = Query::from_query("page=2&limit=50").unwrap();
        let params: &PaginationParams = &query;
        assert_eq!(params.page, Some(2));
        assert_eq!(params.limit, Some(50));
        
        // Test with only page
        let query = Query::from_query("page=3").unwrap();
        let params: &PaginationParams = &query;
        assert_eq!(params.page, Some(3));
        assert_eq!(params.limit, None);
        
        // Test with only limit
        let query = Query::from_query("limit=100").unwrap();
        let params: &PaginationParams = &query;
        assert_eq!(params.page, None);
        assert_eq!(params.limit, Some(100));
    }

    #[test]
    fn test_pagination_params_with_invalid_values() {
        // Test with invalid page value (should be handled gracefully)
        let query_result = Query::from_query("page=invalid");
        assert!(query_result.is_err());
        
        // Test with invalid limit value
        let query_result = Query::from_query("limit=invalid");
        assert!(query_result.is_err());
    }

    #[test]
    fn test_pagination_params_with_negative_values() {
        // Test with negative page
        let query = Query::from_query("page=-1").unwrap();
        let params: &PaginationParams = &query;
        assert_eq!(params.page, Some(-1));
        
        // Test with negative limit
        let query = Query::from_query("limit=-10").unwrap();
        let params: &PaginationParams = &query;
        assert_eq!(params.limit, Some(-10));
    }

    #[test]
    fn test_paginated_response_serialization() {
        let response = PaginatedResponse {
            table: "test_table".to_string(),
            total_count: 100,
            page: 2,
            limit: 25,
            total_pages: 4,
            columns: vec!["id".to_string(), "name".to_string()],
            data: vec![
                json!({"id": 1, "name": "Alice"}),
                json!({"id": 2, "name": "Bob"}),
            ],
        };
        
        let json_string = serde_json::to_string(&response).unwrap();
        assert!(json_string.contains("test_table"));
        assert!(json_string.contains("100"));
        assert!(json_string.contains("Alice"));
        assert!(json_string.contains("Bob"));
    }

    #[test]
    fn test_paginated_response_structure() {
        let response = PaginatedResponse {
            table: "users".to_string(),
            total_count: 50,
            page: 1,
            limit: 10,
            total_pages: 5,
            columns: vec!["id".to_string(), "email".to_string()],
            data: vec![json!({"id": 1, "email": "test@example.com"})],
        };
        
        // Test that all fields are accessible
        assert_eq!(response.table, "users");
        assert_eq!(response.total_count, 50);
        assert_eq!(response.page, 1);
        assert_eq!(response.limit, 10);
        assert_eq!(response.total_pages, 5);
        assert_eq!(response.columns.len(), 2);
        assert_eq!(response.data.len(), 1);
    }
}
