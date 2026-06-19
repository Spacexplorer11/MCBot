use axum::Json;
use axum::routing::post;
use serde_json::json;
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let router = axum::Router::new().route("/slack/events", post(handle_event));
    let listener = TcpListener::bind("0.0.0.0:4598").await.unwrap();

    axum::serve(listener, router).await.expect("Uh oh");
}

async fn handle_event(Json(payload): Json<serde_json::Value>) -> Json<serde_json::Value> {
    #[cfg(debug_assertions)]
    println!("{:#?}", payload);
    if payload["type"] == "url_verification" {
        Json(json!({"challenge": payload["challenge"]}))
    } else {
        Json(json!({"response":"ok"}))
    }
}
