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
                    Type::INT2 => json!(row.get::<_, i16>(i)),
                    Type::INT4 => json!(row.get::<_, i32>(i)),
                    Type::INT8 => json!(row.get::<_, i64>(i)),
                    Type::FLOAT4 => json!(row.get::<_, f32>(i)),
                    Type::FLOAT8 => json!(row.get::<_, f64>(i)),
                    Type::BOOL => json!(row.get::<_, bool>(i)),
                    Type::VARCHAR | Type::TEXT | Type::BPCHAR => json!(row.get::<_, String>(i)),
                    Type::TIMESTAMP => {
                        let ts: NaiveDateTime = row.get(i);
                        json!(ts.to_string())
                    }
                    Type::TIMESTAMPTZ => {
                        let ts: DateTime<Utc> = row.get(i);
                        json!(ts.to_rfc3339())
                    }
                    Type::DATE => {
                        let date: NaiveDate = row.get(i);
                        json!(date.to_string())
                    }
                    Type::JSON | Type::JSONB => {
                        let json_value: serde_json::Value = row.get(i);
                        json_value
                    }
                    Type::UUID => {
                        let uuid: Uuid = row.get(i);
                        json!(uuid.to_string())
                    }
                    Type::TEXT_ARRAY | Type::VARCHAR_ARRAY => {
                        let array: Vec<String> = row.get(i);
                        json!(array)
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
