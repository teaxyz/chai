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

pub fn rows_to_json(rows: &[Row]) -> Vec<Value> {
    rows.iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (i, column) in row.columns().iter().enumerate() {
                let value: Value = match *column.type_() {
                    Type::INT2 => match row.try_get::<_, Option<i16>>(i) {
                        Ok(Some(val)) => json!(val),
                        _ => Value::Null,
                    },
                    Type::INT4 => match row.try_get::<_, Option<i32>>(i) {
                        Ok(Some(val)) => json!(val),
                        _ => Value::Null,
                    },
                    Type::INT8 => match row.try_get::<_, Option<i64>>(i) {
                        Ok(Some(val)) => json!(val),
                        _ => Value::Null,
                    },
                    Type::FLOAT4 => match row.try_get::<_, Option<f32>>(i) {
                        Ok(Some(val)) => json!(val),
                        _ => Value::Null,
                    },
                    Type::FLOAT8 => match row.try_get::<_, Option<f64>>(i) {
                        Ok(Some(val)) => json!(val),
                        _ => Value::Null,
                    },
                    Type::BOOL => match row.try_get::<_, Option<bool>>(i) {
                        Ok(Some(val)) => json!(val),
                        _ => Value::Null,
                    },
                    Type::VARCHAR | Type::TEXT | Type::BPCHAR => {
                        match row.try_get::<_, Option<String>>(i) {
                            Ok(Some(val)) => json!(val),
                            _ => Value::Null,
                        }
                    }
                    Type::TIMESTAMP => match row.try_get::<_, Option<NaiveDateTime>>(i) {
                        Ok(Some(val)) => json!(val.to_string()),
                        _ => Value::Null,
                    },
                    Type::TIMESTAMPTZ => match row.try_get::<_, Option<DateTime<Utc>>>(i) {
                        Ok(Some(val)) => json!(val.to_rfc3339()),
                        _ => Value::Null,
                    },
                    Type::DATE => match row.try_get::<_, Option<NaiveDate>>(i) {
                        Ok(Some(val)) => json!(val.to_string()),
                        _ => Value::Null,
                    },
                    Type::JSON | Type::JSONB => {
                        match row.try_get::<_, Option<serde_json::Value>>(i) {
                            Ok(Some(val)) => val,
                            _ => Value::Null,
                        }
                    }
                    Type::UUID => match row.try_get::<_, Option<Uuid>>(i) {
                        Ok(Some(val)) => json!(val.to_string()),
                        _ => Value::Null,
                    },
                    Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
                        match row.try_get::<_, Option<Vec<String>>>(i) {
                            Ok(Some(val)) => json!(val),
                            _ => Value::Null,
                        }
                    }
                    _ => Value::Null,
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
