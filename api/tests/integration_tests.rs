use actix_web::{test, App, web};
use chai_api::{handlers, app_state::AppState};
use deadpool_postgres::Config;
use serde_json::json;
use std::sync::Arc;
use tokio_postgres::NoTls;

// Mock data for testing
fn create_mock_app_state() -> web::Data<AppState> {
    // Create a minimal config for testing
    let config = Config::new();
    
    // We can't create a real pool without a database, 
    // so these tests will focus on the parts that don't require DB
    let mock_tables = Arc::new(vec![
        "packages".to_string(),
        "dependencies".to_string(),
        "urls".to_string(),
        "canons".to_string(),
    ]);
    
    // Note: This will panic if we try to use the actual pool
    // These tests should focus on handler logic that doesn't require DB access
    let pool = config.create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)
        .expect("Failed to create mock pool");
    
    web::Data::new(AppState {
        pool,
        tables: mock_tables,
    })
}

#[actix_web::test]
async fn test_list_tables_endpoint() {
    let app_state = create_mock_app_state();
    
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .service(handlers::list_tables)
    ).await;
    
    let req = test::TestRequest::get()
        .uri("/tables")
        .to_request();
    
    let resp = test::call_service(&app, req).await;
    
    assert!(resp.status().is_success());
    
    let body: serde_json::Value = test::read_body_json(resp).await;
    
    // Verify the response structure
    assert!(body.get("table").is_some());
    assert!(body.get("total_count").is_some());
    assert!(body.get("page").is_some());
    assert!(body.get("limit").is_some());
    assert!(body.get("total_pages").is_some());
    assert!(body.get("columns").is_some());
    assert!(body.get("data").is_some());
    
    // Verify the table is "tables"
    assert_eq!(body["table"], "tables");
    
    // Verify we have the expected number of tables
    assert_eq!(body["total_count"], 4);
    
    // Verify the data contains our mock tables
    let data = body["data"].as_array().unwrap();
    let table_names: Vec<String> = data.iter()
        .map(|item| item["table_name"].as_str().unwrap().to_string())
        .collect();
    
    assert!(table_names.contains(&"packages".to_string()));
    assert!(table_names.contains(&"dependencies".to_string()));
    assert!(table_names.contains(&"urls".to_string()));
    assert!(table_names.contains(&"canons".to_string()));
}

#[actix_web::test]
async fn test_list_tables_with_pagination() {
    let app_state = create_mock_app_state();
    
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .service(handlers::list_tables)
    ).await;
    
    let req = test::TestRequest::get()
        .uri("/tables?page=1&limit=2")
        .to_request();
    
    let resp = test::call_service(&app, req).await;
    
    assert!(resp.status().is_success());
    
    let body: serde_json::Value = test::read_body_json(resp).await;
    
    // Verify pagination parameters
    assert_eq!(body["page"], 1);
    assert_eq!(body["limit"], 2);
    assert_eq!(body["total_pages"], 2); // 4 tables / 2 per page = 2 pages
    
    // Verify we only get 2 tables in the response
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 2);
}

#[actix_web::test]
async fn test_list_tables_with_invalid_pagination() {
    let app_state = create_mock_app_state();
    
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .service(handlers::list_tables)
    ).await;
    
    // Test with invalid page parameter
    let req = test::TestRequest::get()
        .uri("/tables?page=invalid")
        .to_request();
    
    let resp = test::call_service(&app, req).await;
    
    // Should return a 400 Bad Request for invalid query parameters
    assert_eq!(resp.status(), 400);
}

#[actix_web::test]
async fn test_empty_tables_list() {
    // Create app state with empty tables
    let empty_tables = Arc::new(vec![]);
    let config = Config::new();
    let pool = config.create_pool(Some(deadpool_postgres::Runtime::Tokio1), NoTls)
        .expect("Failed to create mock pool");
    
    let app_state = web::Data::new(AppState {
        pool,
        tables: empty_tables,
    });
    
    let app = test::init_service(
        App::new()
            .app_data(app_state)
            .service(handlers::list_tables)
    ).await;
    
    let req = test::TestRequest::get()
        .uri("/tables")
        .to_request();
    
    let resp = test::call_service(&app, req).await;
    
    assert!(resp.status().is_success());
    
    let body: serde_json::Value = test::read_body_json(resp).await;
    
    assert_eq!(body["total_count"], 0);
    assert_eq!(body["total_pages"], 0);
    
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 0);
}

#[actix_web::test]
async fn test_table_check_functionality() {
    let app_state = create_mock_app_state();
    
    // Test that check_table_exists works with our mock tables
    let tables = vec!["packages".to_string(), "dependencies".to_string()];
    
    // Valid table should return None
    let result = chai_api::handlers::check_table_exists("packages", &tables);
    assert!(result.is_none());
    
    // Invalid table should return Some(HttpResponse)
    let result = chai_api::handlers::check_table_exists("invalid_table", &tables);
    assert!(result.is_some());
    
    let response = result.unwrap();
    assert_eq!(response.status(), 404);
}

// Test basic application setup
#[actix_web::test]
async fn test_app_configuration() {
    let app_state = create_mock_app_state();
    
    let app = test::init_service(
        App::new()
            .app_data(app_state.clone())
            .service(handlers::list_tables)
    ).await;
    
    // Test that the app is properly configured
    let req = test::TestRequest::get()
        .uri("/tables")
        .to_request();
    
    let resp = test::call_service(&app, req).await;
    
    // Should not return 404 (endpoint not found)
    assert_ne!(resp.status(), 404);
    
    // Should return success or some other expected status
    assert!(resp.status().is_success() || resp.status().is_client_error());
}

#[cfg(test)]
mod unit_tests {
    use super::*;
    use chai_api::handlers::PaginationParams;
    
    #[test]
    fn test_pagination_params_defaults() {
        // Test that PaginationParams can be created with default values
        let params = PaginationParams {
            page: None,
            limit: None,
        };
        
        assert_eq!(params.page, None);
        assert_eq!(params.limit, None);
    }
    
    #[test]
    fn test_pagination_params_with_values() {
        let params = PaginationParams {
            page: Some(2),
            limit: Some(50),
        };
        
        assert_eq!(params.page, Some(2));
        assert_eq!(params.limit, Some(50));
    }
}