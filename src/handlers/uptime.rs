use axum::Json;
use serde_json::{Value, json};

pub async fn uptime() -> Json<Value> {
    Json(json!({"ok": true}))
}
