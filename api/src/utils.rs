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

macro_rules! get_json_option {
    ($row:expr, $idx:expr, $type:ty) => {
        {
            let v: Option<$type> = $row.try_get($idx).unwrap_or(None);
            json!(v)
        }
    };
}

pub fn rows_to_json(rows: &[Row]) -> Vec<Value> {
    if rows.is_empty() {
        return vec![];
    }

    let columns = rows[0].columns();
    let column_types: Vec<&Type> = columns.iter().map(|col| col.type_()).collect();
    let column_names: Vec<&str> = columns.iter().map(|col| col.name()).collect();

    rows.iter()
        .map(|row| {
            let mut map = serde_json::Map::new();
            for (i, &column_type) in column_types.iter().enumerate() {
                let column_name = column_names[i];

                let value: Value = match *column_type {
                    Type::INT2 => get_json_option!(row, i, i16),
                    Type::INT4 => get_json_option!(row, i, i32),
                    Type::INT8 => get_json_option!(row, i, i64),
                    Type::FLOAT4 => get_json_option!(row, i, f32),
                    Type::FLOAT8 => get_json_option!(row, i, f64),
                    Type::BOOL => get_json_option!(row, i, bool),
                    Type::VARCHAR | Type::TEXT | Type::BPCHAR => get_json_option!(row, i, String),
                    Type::TIMESTAMP => {
                        let v: Option<NaiveDateTime> = row.try_get(i).unwrap_or(None);
                        v.map(|ts| json!(ts.to_string())).unwrap_or(Value::Null)
                    }
                    Type::TIMESTAMPTZ => {
                        let v: Option<DateTime<Utc>> = row.try_get(i).unwrap_or(None);
                        v.map(|ts| json!(ts.to_rfc3339())).unwrap_or(Value::Null)
                    }
                    Type::DATE => {
                        let v: Option<NaiveDate> = row.try_get(i).unwrap_or(None);
                        v.map(|date| json!(date.to_string())).unwrap_or(Value::Null)
                    }
                    Type::JSON | Type::JSONB => {
                        let v: Option<Value> = row.try_get(i).unwrap_or(None);
                        v.unwrap_or(Value::Null)
                    }
                    Type::UUID => {
                        let v: Option<Uuid> = row.try_get(i).unwrap_or(None);
                        v.map(|uuid| json!(uuid.to_string())).unwrap_or(Value::Null)
                    }
                    _ => {
                        log::warn!("Unhandled column type: {:?}", column_type);
                        Value::Null
                    }
                };
                map.insert(column_name.to_string(), value);
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
        let total_pages = ((total_count as f64 / limit as f64).ceil() as i64).max(1);

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
