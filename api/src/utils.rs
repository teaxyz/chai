use actix_web::web::Query;
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde_json::{json, Value};
use tokio_postgres::{types::Type, Row};
use uuid::Uuid;

use crate::handlers::PaginationParams;

pub fn get_column_names(rows: &[Row]) -> Vec<String> {
    if let Some(row) = rows.first() {
        row.columns()
            .iter()
            .map(|col| col.name().to_string())
            .collect()
    } else {
        vec![]
    }
}

pub fn convert_optional_to_json<T, E>(result: Result<Option<T>, E>) -> Value
where
    T: serde::Serialize,
{
    match result {
        Ok(Some(val)) => json!(val),
        _ => Value::Null,
    }
}

pub fn rows_to_json(rows: &[Row]) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (i, column) in row.columns().iter().enumerate() {
                let value: Value = match *column.type_() {
                    Type::INT2 => convert_optional_to_json(row.try_get::<_, Option<i16>>(i)),
                    Type::INT4 => convert_optional_to_json(row.try_get::<_, Option<i32>>(i)),
                    Type::INT8 => convert_optional_to_json(row.try_get::<_, Option<i64>>(i)),
                    Type::FLOAT4 => convert_optional_to_json(row.try_get::<_, Option<f32>>(i)),
                    Type::FLOAT8 => convert_optional_to_json(row.try_get::<_, Option<f64>>(i)),
                    Type::BOOL => convert_optional_to_json(row.try_get::<_, Option<bool>>(i)),
                    Type::VARCHAR | Type::TEXT | Type::BPCHAR => {
                        convert_optional_to_json(row.try_get::<_, Option<String>>(i))
                    }
                    Type::TIMESTAMP => {
                        convert_optional_to_json(row.try_get::<_, Option<NaiveDateTime>>(i))
                    }
                    Type::TIMESTAMPTZ => {
                        convert_optional_to_json(row.try_get::<_, Option<DateTime<Utc>>>(i))
                    }
                    Type::DATE => convert_optional_to_json(row.try_get::<_, Option<NaiveDate>>(i)),
                    Type::JSON | Type::JSONB => {
                        convert_optional_to_json(row.try_get::<_, Option<serde_json::Value>>(i))
                    }
                    Type::UUID => convert_optional_to_json(row.try_get::<_, Option<Uuid>>(i)),
                    Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
                        convert_optional_to_json(row.try_get::<_, Option<Vec<String>>>(i))
                    }
                    _ => {
                        // For unsupported types, try to convert to string
                        convert_optional_to_json(row.try_get::<_, Option<String>>(i))
                    }
                };
                map.insert(column.name().to_string(), value);
            }
            Value::Object(map)
        })
        .collect()
}

pub struct Pagination {
    pub page: i64,
    pub limit: i64,
    pub offset: i64,
    pub total_pages: i64,
}

impl Pagination {
    pub fn new(query: Query<PaginationParams>, total_count: i64) -> Self {
        let limit = query.limit.unwrap_or(200).clamp(1, 1000);
        let total_pages = (total_count as f64 / limit as f64).ceil() as i64;

        let page = query.page.unwrap_or(1).clamp(1, total_pages);

        let offset = (page - 1) * limit;
        Self {
            page,
            limit,
            offset,
            total_pages,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use actix_web::web::Query;
    use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
    use serde_json::json;
    use std::collections::HashMap;
    use tokio_postgres::types::Type;
    use tokio_postgres::{Column, Row};
    use uuid::Uuid;

    // Helper function to create a mock row - this is complex due to tokio_postgres internals
    // We'll focus on testing the conversion logic with simpler approaches

    #[test]
    fn test_convert_optional_to_json_with_some_value() {
        let result: Result<Option<i32>, &str> = Ok(Some(42));
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, json!(42));
    }

    #[test]
    fn test_convert_optional_to_json_with_none() {
        let result: Result<Option<i32>, &str> = Ok(None);
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, serde_json::Value::Null);
    }

    #[test]
    fn test_convert_optional_to_json_with_error() {
        let result: Result<Option<i32>, &str> = Err("some error");
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, serde_json::Value::Null);
    }

    #[test]
    fn test_convert_optional_to_json_with_string() {
        let result: Result<Option<String>, &str> = Ok(Some("hello".to_string()));
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, json!("hello"));
    }

    #[test]
    fn test_convert_optional_to_json_with_bool() {
        let result: Result<Option<bool>, &str> = Ok(Some(true));
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, json!(true));
    }

    #[test]
    fn test_convert_optional_to_json_with_float() {
        let result: Result<Option<f64>, &str> = Ok(Some(3.14));
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, json!(3.14));
    }

    #[test]
    fn test_convert_optional_to_json_with_uuid() {
        let uuid = Uuid::new_v4();
        let result: Result<Option<Uuid>, &str> = Ok(Some(uuid));
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, json!(uuid));
    }

    #[test]
    fn test_convert_optional_to_json_with_datetime() {
        let datetime = Utc::now();
        let result: Result<Option<DateTime<Utc>>, &str> = Ok(Some(datetime));
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, json!(datetime));
    }

    #[test]
    fn test_convert_optional_to_json_with_naive_datetime() {
        let naive_datetime = NaiveDateTime::from_timestamp_opt(1234567890, 0).unwrap();
        let result: Result<Option<NaiveDateTime>, &str> = Ok(Some(naive_datetime));
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, json!(naive_datetime));
    }

    #[test]
    fn test_convert_optional_to_json_with_naive_date() {
        let naive_date = NaiveDate::from_ymd_opt(2023, 1, 1).unwrap();
        let result: Result<Option<NaiveDate>, &str> = Ok(Some(naive_date));
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, json!(naive_date));
    }

    #[test]
    fn test_convert_optional_to_json_with_json_value() {
        let json_obj = json!({"key": "value"});
        let result: Result<Option<serde_json::Value>, &str> = Ok(Some(json_obj.clone()));
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, json_obj);
    }

    #[test]
    fn test_convert_optional_to_json_with_string_array() {
        let vec_strings = vec!["hello".to_string(), "world".to_string()];
        let result: Result<Option<Vec<String>>, &str> = Ok(Some(vec_strings.clone()));
        let json_value = convert_optional_to_json(result);
        assert_eq!(json_value, json!(vec_strings));
    }

    #[test]
    fn test_get_column_names_with_empty_rows() {
        let rows: Vec<Row> = vec![];
        let column_names = get_column_names(&rows);
        assert_eq!(column_names, Vec::<String>::new());
    }

    // Test Pagination struct
    #[test]
    fn test_pagination_new_with_defaults() {
        let mut params = HashMap::new();
        let query = Query::from_query("").unwrap();
        let pagination = Pagination::new(query, 100);
        
        assert_eq!(pagination.page, 1);
        assert_eq!(pagination.limit, 200);
        assert_eq!(pagination.offset, 0);
        assert_eq!(pagination.total_pages, 1);
    }

    #[test]
    fn test_pagination_new_with_custom_values() {
        let query = Query::from_query("page=2&limit=50").unwrap();
        let pagination = Pagination::new(query, 150);
        
        assert_eq!(pagination.page, 2);
        assert_eq!(pagination.limit, 50);
        assert_eq!(pagination.offset, 50);
        assert_eq!(pagination.total_pages, 3);
    }

    #[test]
    fn test_pagination_new_with_zero_total() {
        let query = Query::from_query("").unwrap();
        let pagination = Pagination::new(query, 0);
        
        assert_eq!(pagination.page, 1);
        assert_eq!(pagination.limit, 200);
        assert_eq!(pagination.offset, 0);
        assert_eq!(pagination.total_pages, 0);
    }

    #[test]
    fn test_pagination_limit_clamping() {
        // Test minimum limit
        let query = Query::from_query("limit=0").unwrap();
        let pagination = Pagination::new(query, 100);
        assert_eq!(pagination.limit, 1);

        // Test maximum limit
        let query = Query::from_query("limit=2000").unwrap();
        let pagination = Pagination::new(query, 100);
        assert_eq!(pagination.limit, 1000);
    }

    #[test]
    fn test_pagination_page_clamping() {
        // Test minimum page
        let query = Query::from_query("page=0").unwrap();
        let pagination = Pagination::new(query, 100);
        assert_eq!(pagination.page, 1);

        // Test page beyond total pages
        let query = Query::from_query("page=10").unwrap();
        let pagination = Pagination::new(query, 100);
        assert_eq!(pagination.page, 1); // Should clamp to max available page (1 in this case)
    }

    #[test]
    fn test_pagination_offset_calculation() {
        let query = Query::from_query("page=3&limit=25").unwrap();
        let pagination = Pagination::new(query, 100);
        
        assert_eq!(pagination.offset, 50); // (3-1) * 25 = 50
        assert_eq!(pagination.total_pages, 4); // ceil(100/25) = 4
    }

    #[test]
    fn test_pagination_total_pages_calculation() {
        // Test exact division
        let query = Query::from_query("limit=10").unwrap();
        let pagination = Pagination::new(query, 100);
        assert_eq!(pagination.total_pages, 10);

        // Test with remainder
        let query = Query::from_query("limit=7").unwrap();
        let pagination = Pagination::new(query, 100);
        assert_eq!(pagination.total_pages, 15); // ceil(100/7) = 15
    }
}
