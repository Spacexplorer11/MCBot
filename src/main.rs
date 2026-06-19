use axum::{Json, extract::State, routing::post};
use dotenvy::dotenv;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::env;
use tokio::net::TcpListener;

#[derive(Clone)]
struct AppState {
    client: Client,
    bot_token: String,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum SlackPayload {
    #[serde(rename = "url_verification")]
    UrlVerification { challenge: String },

    #[serde(rename = "event_callback")]
    EventCallback { event: SlackEvent },
}

#[derive(Deserialize)]
struct SlackEvent {
    #[serde(rename = "type")]
    event_type: String,
    channel: String,
    text: String,
    user: String,
    ts: String,
}

#[tokio::main]
async fn main() {
    if !dotenv().is_ok() {
        eprintln!(".env file NOT LOADED")
    }

    let bot_token = env::var("SLACK_BOT_TOKEN").expect("Bot Token NOT FOUND");
    let app_token = env::var("SLACK_APP_TOKEN").expect("App Token NOT FOUND");

    let state = AppState {
        client: Client::new(),
        bot_token,
    };

    let router = axum::Router::new()
        .route("/slack/events", post(handle_event))
        .with_state(state);
    let listener = TcpListener::bind("0.0.0.0:4598").await.unwrap();

    axum::serve(listener, router).await.expect("Uh oh");
}

async fn handle_event(
    State(state): State<AppState>,
    Json(payload): Json<SlackPayload>,
) -> Json<serde_json::Value> {
    match payload {
        SlackPayload::UrlVerification { challenge } => {
            println!("Url Verification challenge received");
            Json(json!({"challenge": challenge}))
        }

        SlackPayload::EventCallback { event } => {
            #[cfg(debug_assertions)]
            println!("Received event");
            match state.client.post("https://slack.com/api/chat.postMessage").bearer_auth(state.bot_token).json(&json!({"channel": format!("{}", event.channel), "text": "if this works, you deserve to proud of yourself :)"})).send().await {
                Ok(response) => {
                    #[cfg(debug_assertions)]
                    println!("{:#?}", response)
                },
                Err(e) => eprintln!("Something went wrong with sending a message, {e}")
            };
            Json(json!({"ok":"true"}))
        }
    }
}
