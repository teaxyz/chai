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
