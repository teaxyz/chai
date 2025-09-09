use actix_web::{get, post, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::sync::Arc;
use tokio_postgres::error::SqlState;
use uuid::Uuid;

use crate::app_state::AppState;
use crate::utils::{get_cached_projects, get_column_names, rows_to_json, Pagination};

const RESPONSE_LIMIT: i64 = 1000;

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

#[derive(Deserialize)]
pub struct LeaderboardRequest {
    #[serde(rename = "projectIds")]
    pub project_ids: Option<Vec<Uuid>>,
    pub limit: i64,
}

#[derive(Deserialize)]
pub struct ProjectBatchRequest {
    #[serde(rename = "projectIds")]
    pub project_ids: Vec<Uuid>,
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

#[get("/tables/{table}")]
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

#[get("/tables/{table}/{id}")]
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

#[get("/project/{id}")]
pub async fn get_project(path: web::Path<Uuid>, data: web::Data<AppState>) -> impl Responder {
    // Check if the table exists
    let id = path.into_inner();

    // Construct the query
    let query = r#"
        WITH base AS MATERIALIZED (
            SELECT
                c.id,
                u_homepage.url AS homepage,
                c.name,
                COALESCE(tr_latest.rank, '0') AS "teaRank",
                tr_latest.created_at AS "teaRankCalculatedAt",
                (
                SELECT ARRAY_AGG(DISTINCT s.type)
                FROM canon_packages cp2
                JOIN packages p2           ON cp2.package_id = p2.id
                JOIN package_managers pm2  ON p2.package_manager_id = pm2.id
                JOIN sources s             ON pm2.source_id = s.id
                WHERE cp2.canon_id = c.id
                ) AS "packageManagers",
                (
                SELECT COUNT(*)::bigint
                FROM legacy_dependencies ld
                JOIN canon_packages cp_out ON cp_out.package_id = ld.package_id
                WHERE cp_out.canon_id = c.id
                ) AS "dependenciesCount",
                (
                SELECT COUNT(*)::bigint
                FROM legacy_dependencies ld
                JOIN canon_packages cp_in ON cp_in.package_id = ld.dependency_id
                WHERE cp_in.canon_id = c.id
                ) AS "dependentsCount"
            FROM canons c
            JOIN urls u_homepage ON c.url_id = u_homepage.id
            LEFT JOIN LATERAL (
                SELECT tr.rank, tr.created_at
                FROM tea_ranks tr
                WHERE tr.canon_id = c.id
                ORDER BY tr.created_at DESC
                LIMIT 1
            ) tr_latest ON TRUE
            WHERE c.id = $1
        )
        SELECT DISTINCT ON (b.id)
            b.id                AS "projectId",
            b.homepage,
            b.name,
            u_source.url        AS source,
            b."teaRank",
            b."teaRankCalculatedAt",
            b."packageManagers",
            b."dependenciesCount",
            b."dependentsCount"
        FROM base b
        JOIN canon_packages cp ON cp.canon_id = b.id
        JOIN package_urls pu   ON pu.package_id = cp.package_id
        JOIN urls u_source     ON pu.url_id = u_source.id
        JOIN url_types ut      ON ut.id = u_source.url_type_id
        WHERE ut.name = 'source'
        ORDER BY b.id, b."teaRankCalculatedAt" DESC, u_source.url;"#;

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

#[post("/project/batch")]
pub async fn list_projects_by_id(
    req: web::Json<ProjectBatchRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    if req.project_ids.is_empty() {
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
        Ok(client) => match client.query(query, &[&req.project_ids]).await {
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
pub async fn list_projects_by_name(
    path: web::Path<String>,
    data: web::Data<AppState>,
) -> impl Responder {
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

#[post("/leaderboard")]
pub async fn get_leaderboard(
    req: web::Json<LeaderboardRequest>,
    data: web::Data<AppState>,
) -> impl Responder {
    let limit = req.limit.clamp(1, RESPONSE_LIMIT);

    let Some(project_ids) = req.project_ids.as_deref() else {
        return get_top_projects(data, limit).await;
    };

    if project_ids.len() > RESPONSE_LIMIT as usize {
        return HttpResponse::BadRequest().json(json!({
            "error": format!("Too many project IDs (maximum {} allowed)", RESPONSE_LIMIT)
        }));
    }

    // Get cached projects and identify missing ones
    let (cached_projects, missing_ids) =
        get_cached_projects(data.project_cache.clone(), project_ids);

    // If we have all projects cached, return them sorted
    if missing_ids.is_empty() {
        return sort_truncate_and_return(cached_projects, limit);
    }

    // Query for missing projects
    let query = r#"
        SELECT *
        FROM (
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
                    AND cp2.canon_id = ANY($1::uuid[])
                ) AS "packageManagers"
            FROM canons c
            JOIN urls u_homepage ON c.url_id = u_homepage.id
            JOIN canon_packages cp ON cp.canon_id = c.id
            JOIN package_urls pu ON pu.package_id = cp.package_id
            JOIN urls u_source ON pu.url_id = u_source.id
            JOIN url_types ut_source ON ut_source.id = u_source.url_type_id
            LEFT JOIN tea_ranks tr ON tr.canon_id = c.id
            WHERE
            c.id = ANY($1::uuid[])
            AND ut_source.name = 'source'
            AND CAST(tr.rank AS NUMERIC) > 0
            ORDER BY c.id, tr.created_at DESC, u_source.url
        ) sub
        ORDER BY CAST("teaRank" AS NUMERIC) DESC NULLS LAST
        LIMIT $2"#;

    match data.pool.get().await {
        Ok(client) => match client.query(query, &[&missing_ids, &limit]).await {
            Ok(rows) => {
                let fresh_projects = rows_to_json(&rows);

                // Cache the fresh projects
                for project in &fresh_projects {
                    if let Some(project_id) = project.get("projectId").and_then(|v| v.as_str()) {
                        if let Ok(uuid) = Uuid::parse_str(project_id) {
                            data.project_cache.insert(
                                uuid,
                                crate::app_state::ProjectCacheEntry::new(project.clone()),
                            );
                        } else {
                            log::info!("Failed to parse project ID as UUID: {}", project_id);
                        }
                    } else {
                        log::info!("No projectId found in project: {:?}", project);
                    }
                }

                // Combine cached and fresh projects - keep Arc<Value> for cached ones
                let mut all_projects: Vec<Arc<Value>> = cached_projects;

                // Convert fresh projects to Arc<Value> to match the type
                let fresh_arcs: Vec<Arc<Value>> =
                    fresh_projects.into_iter().map(Arc::new).collect();
                all_projects.extend(fresh_arcs);

                sort_truncate_and_return(all_projects, limit)
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

// Helper function to sort, truncate, and return the final response
fn sort_truncate_and_return(projects: Vec<Arc<Value>>, limit: i64) -> actix_web::HttpResponse {
    let mut projects = projects;

    // Sort projects by teaRank (descending) - Arc<Value> derefs to Value
    projects.sort_by(|a, b| {
        let rank_a = a.get("teaRank").and_then(|v| v.as_str()).unwrap_or("0");
        let rank_b = b.get("teaRank").and_then(|v| v.as_str()).unwrap_or("0");
        rank_b.cmp(rank_a)
    });

    // Apply limit
    projects.truncate(limit as usize);

    // Convert to Vec<Value> only for the final response - Arc<Value> doesn't implement Serialize
    let final_projects: Vec<Value> = projects
        .into_iter()
        .map(|arc_val| (*arc_val).clone())
        .collect();
    actix_web::HttpResponse::Ok().json(final_projects)
}

async fn get_top_projects(data: web::Data<AppState>, limit: i64) -> HttpResponse {
    // get client
    let Ok(client) = data.pool.get().await else {
        return HttpResponse::InternalServerError().body("Failed to get database connection");
    };

    // get latest run id
    let run_query = r#"SELECT MAX(run) from tea_rank_runs"#;
    let Ok(run_row) = client.query_one(run_query, &[]).await else {
        return HttpResponse::InternalServerError().body("Failed to get latest run");
    };
    let run: i32 = run_row.get(0);

    // get top projects (1-RESPONSE_LIMIT)
    let top_ranks_query = r#"SELECT
            canon_id as "projectId",
            name,
            rank as "teaRank",
            (
                SELECT ARRAY_AGG(DISTINCT s.type)
                FROM canon_packages cp2
                JOIN packages p2 ON cp2.package_id = p2.id
                JOIN package_managers pm2 ON p2.package_manager_id = pm2.id
                JOIN sources s ON pm2.source_id = s.id
                WHERE cp2.canon_id = canon_id
            ) AS "packageManagers"
        FROM
            tea_ranks
            JOIN canons ON canon_id = canons.id
        WHERE
            tea_rank_run = $1
        ORDER BY
            rank DESC
        LIMIT $2"#;
    let Ok(top_ranks) = client
        .query(top_ranks_query, &[&run, &limit.clamp(1, RESPONSE_LIMIT)])
        .await
    else {
        return HttpResponse::InternalServerError().json(json!({
            "error": "Failed to fetch top ranks"
        }));
    };
    let json = rows_to_json(&top_ranks);
    HttpResponse::Ok().json(json)
}
