pub mod data;
pub mod recipes;

use crate::Task::Recipe;
use crate::data::fetch_client_jar;
use crate::recipes::RecipeData;
use axum::{Form, Json, extract::State, routing::post};
use dotenvy::dotenv;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, env};
use tokio::{net::TcpListener, sync::mpsc};

enum Task {
    Recipe {
        item_name: String,
        response_url: String,
        channel_id: String,
    },
}

#[derive(Clone)]
struct AppState {
    client: Client,
    bot_token: String,
    mpsc: mpsc::Sender<Task>,
    valid_recipes: std::sync::Arc<HashMap<String, usize>>,
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
    if dotenv().is_err() {
        eprintln!(".env file NOT LOADED")
    }

    let bot_token = env::var("SLACK_BOT_TOKEN").expect("Bot Token NOT FOUND");
    let (queue_input, mut queue_output) = mpsc::channel::<Task>(2000);

    let client = Client::new();

    let mut client_jar_zip = fetch_client_jar(&client).await;
    let mut recipe_data = RecipeData::default();
    println!("Now adding recipes, items & tags to memory");
    recipe_data
        .fetch_recipes_and_more(&mut client_jar_zip)
        .await;

    /* Delete this later but
    TODO: Implement for stuff for shapeless recipes & transmute and search if any other recipes exist / test and handle them
    TODO: Fix torch and if there's anything like it
    TODO: Add more memory management things and extract some stuff to functions and move other stuff to other files
     */

    let state = AppState {
        client: Client::new(),
        bot_token,
        mpsc: queue_input,
        valid_recipes: std::sync::Arc::new(recipe_data.valid_recipes.clone()),
    };

    tokio::spawn(async move {
        let bot_token = env::var("SLACK_BOT_TOKEN").expect("Bot Token NOT FOUND");

        while let Some(task) = queue_output.recv().await {
            match task {
                Recipe {
                    item_name,
                    response_url,
                    channel_id,
                } => {
                    recipe_data
                        .make_and_send_recipe_image(
                            item_name,
                            &client,
                            &bot_token,
                            channel_id,
                            &mut client_jar_zip,
                        )
                        .await;
                }
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
            if state.valid_recipes.contains_key(&payload.text) {
                match state
                    .mpsc
                    .send(Recipe {
                        item_name: payload.text,
                        response_url: payload.response_url,
                        channel_id: payload.channel_id,
                    })
                    .await
                {
                    Ok(..) => Json(
                        json!({"response_type": "in_channel", "text": "Gathering images and sewing 'em up, hang on a second!"}),
                    ),
                    Err(e) => {
                        eprintln!("Error occurred sending task to generate image: {e}");
                        Json(
                            json!({"response_type": "ephemeral", "text": "I wasn't able to start generating your image. Please try again."}),
                        )
                    }
                }
            } else {
                Json(json!({"response_type": "ephemeral", "text": "Your recipe was invalid."}))
            }
        }
        _ => Json(
            json!({"response_type": "ephemeral", "text": "Sorry that command isn't supported as of right now. (If you got this, let @<U08D22QNUVD> know)"}),
        ), // only registered slash commands should even come, this shouldn't trigger anyway
    }
}
