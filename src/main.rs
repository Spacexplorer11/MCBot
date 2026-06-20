use axum::{Form, Json, extract::State, routing::post};
use dotenvy::dotenv;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashSet;
use std::{env, io::Cursor};
use tokio::{net::TcpListener, sync::mpsc};

enum Task {
    Recipe {
        item_name: String,
        response_url: String,
    },
}

#[derive(Clone)]
struct AppState {
    client: Client,
    bot_token: String,
    mpsc: mpsc::Sender<Task>,
    valid_recipes: std::sync::Arc<HashSet<String>>,
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

#[derive(Deserialize)]
struct SlackSlashCommand {
    command: String,
    text: String,
    channel_id: String,
    user_id: String,
    response_url: String,
    trigger_id: String,
    team_id: String,
}

#[tokio::main]
async fn main() {
    if !dotenv().is_ok() {
        eprintln!(".env file NOT LOADED")
    }

    let bot_token = env::var("SLACK_BOT_TOKEN").expect("Bot Token NOT FOUND");
    let (queue_input, mut queue_output) = mpsc::channel::<Task>(2000);

    let mut valid_recipes = HashSet::new();

    {
        let client = Client::new();

        let response = client
            .get("https://api.github.com/repos/misode/mcmeta/git/trees/data?recursive=1")
            .header("User-Agent", "mcbot") // GitHub API requires a User-Agent header
            .send()
            .await
            .expect("Failed to fetch recipe list from mcmeta");

        let tree: serde_json::Value = response
            .json()
            .await
            .expect("Invalid JSON from GitHub tree API");

        if let Some(entries) = tree["tree"].as_array() {
            for entry in entries {
                if let Some(name) = entry["path"].as_str() {
                    if let Some(name) = name.strip_prefix("data/minecraft/recipe/") {
                        if let Some(item_name) = name.strip_suffix(".json") {
                            valid_recipes.insert(item_name.to_owned());
                        }
                    }
                }
            }
        }
    }

    let shared_recipes = std::sync::Arc::new(valid_recipes);

    let state = AppState {
        client: Client::new(),
        bot_token,
        mpsc: queue_input,
        valid_recipes: shared_recipes,
    };

    tokio::spawn(async move {
        while let Some(task) = queue_output.recv().await {
            // 3. The Execution Engine
            match task {
                Task::Recipe {
                    item_name: recipe_name,
                    response_url,
                } => {}
                // Add more later obvs
            }
        }
    });

    let router = axum::Router::new()
        .route("/slack/events", post(handle_event))
        .route("/slack/commands", post(handle_command))
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

async fn handle_command(
    State(state): State<AppState>,
    Form(payload): Form<SlackSlashCommand>,
) -> Json<serde_json::Value> {
    match payload.command.as_str() {
        "/recipe" => {
            if state.valid_recipes.contains(&payload.text) {
                Json(
                    json!({"response_type": "ephemeral", "text": "Gathering images and sewing 'em up, hang on a second!"}),
                )
            } else {
                Json(json!({"response_type": "ephemeral", "text": "Your recipe was invalid."}))
            }
        }
        _ => Json(
            json!({"response_type": "ephemeral", "text": "Sorry that command isn't supported as of right now. (If you got this, let @<U08D22QNUVD> know)"}),
        ), // only registered slash commands should even come, this shouldn't trigger anyway
    }
}
