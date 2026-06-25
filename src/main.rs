pub mod data;
pub mod recipes;

use crate::Task::Recipe;
use crate::data::fetch_client_jar;
use crate::recipes::RecipeData;
use axum::{Form, Json, extract::State, routing::post};
use dotenvy::dotenv;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::Resource;
use opentelemetry_sdk::trace::SdkTracerProvider;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, env};
use tokio::{net::TcpListener, sync::mpsc};
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

enum Task {
    Recipe {
        item_name: String,
        response_url: String,
        channel_id: String,
        user_id: String,
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
        warn!(".env file NOT LOADED")
    }

    let appsignal_api_key =
        env::var("APPSIGNAL_PUSH_API_KEY").expect("APPSIGNAL_PUSH_API_KEY must be set in .env");

    let appsignal_url = "https://m1lxp90w.eu-central.appsignal-collector.net/v1/traces";

    let mut headers = HashMap::new();
    headers.insert("X-AppSignal-ApiKey".to_string(), appsignal_api_key.clone());

    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .with_protocol(Protocol::HttpBinary)
        .with_endpoint(appsignal_url)
        .with_headers(headers)
        .build()
        .expect("Failed to create OpenTelemetry span exporter");

    let resource = Resource::builder()
        .with_attributes(vec![
            KeyValue::new("service.name", "MCBot"),
            KeyValue::new("appsignal.config.name", "MCBot"),
            KeyValue::new("appsignal.config.push_api_key", appsignal_api_key),
            KeyValue::new("appsignal.config.language_integration", "rust"),
            KeyValue::new("appsignal.config.environment", "development"),
        ])
        .build();

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    global::set_tracer_provider(tracer_provider.clone());

    let tracer = global::tracer("mc-bot-tracer");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,mcbot=debug,opentelemetry_sdk=off,opentelemetry-otlp=off")
    });

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(telemetry_layer)
        .with(filter)
        .init();

    let bot_token = env::var("SLACK_BOT_TOKEN").expect("Bot Token NOT FOUND");
    let (queue_input, mut queue_output) = mpsc::channel::<Task>(2000);

    let client = Client::new();

    let mut client_jar_zip = fetch_client_jar(&client).await;
    let mut recipe_data = RecipeData::default();
    info!("Now adding recipes, items & tags to memory");
    recipe_data
        .fetch_recipes_and_more(&mut client_jar_zip)
        .await
        .expect("Failed to fetch recipes");

    /* Delete this later but
    TODO: Implement for stuff for shapeless recipes & transmute and search if any other recipes exist / test and handle them
    TODO: Fix torch and if there's anything like it
     */

    let state = AppState {
        client: Client::new(),
        bot_token: bot_token.clone(),
        mpsc: queue_input,
        valid_recipes: std::sync::Arc::new(recipe_data.valid_recipes.clone()),
    };

    tokio::spawn(async move {
        while let Some(task) = queue_output.recv().await {
            match task {
                Recipe {
                    item_name,
                    response_url,
                    channel_id,
                    user_id,
                } => {
                    match recipe_data
                        .make_and_send_recipe_image(
                            item_name,
                            &client,
                            &bot_token,
                            channel_id,
                            user_id,
                            &mut client_jar_zip,
                        )
                        .await
                    {
                        Ok(..) => (),
                        Err(e) => {
                            error!(error = ?e, "Failed to fulfill recipe task processing pipeline");

                            let polite_msg = json!({
                                "response_type": "ephemeral",
                                "text": "Uh oh, something went wrong! If this persists, please contact me."
                            });
                            let mut response =
                                client.post(&response_url).json(&polite_msg).send().await;
                            while response.is_err() {
                                error!(error = ?response.err().unwrap(), "The generic error message failed to send to the user");
                                response =
                                    client.post(&response_url).json(&polite_msg).send().await;
                            }
                        }
                    };
                }
            }
        }
    });

    let router = axum::Router::new()
        .route("/slack/events", post(handle_event))
        .route("/slack/commands", post(handle_command))
        .with_state(state);
    let listener = TcpListener::bind("0.0.0.0:4598")
        .await
        .expect("Unable to bind the TcpList");

    axum::serve(listener, router)
        .await
        .expect("Unable to serve the axum server");
}

async fn handle_event(
    State(state): State<AppState>,
    Json(payload): Json<SlackPayload>,
) -> Json<serde_json::Value> {
    match payload {
        SlackPayload::UrlVerification { challenge } => {
            info!("Url Verification challenge received");
            Json(json!({"challenge": challenge}))
        }

        SlackPayload::EventCallback { event } => {
            #[cfg(debug_assertions)]
            info!("Received event");
            match state.client.post("https://slack.com/api/chat.postMessage").bearer_auth(state.bot_token).json(&json!({"channel": format!("{}", event.channel), "text": "if this works, you deserve to proud of yourself :)"})).send().await {
                Ok(response) => {
                    #[cfg(debug_assertions)]
                    info!("{:#?}", response)
                },
                Err(e) => error!("Something went wrong with sending a message, {e}")
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
        "/mcrecipe" => {
            if state.valid_recipes.contains_key(&payload.text) {
                match state
                    .mpsc
                    .send(Recipe {
                        item_name: payload.text,
                        response_url: payload.response_url,
                        channel_id: payload.channel_id,
                        user_id: payload.user_id,
                    })
                    .await
                {
                    Ok(..) => Json(
                        json!({"response_type": "ephemeral", "text": "Gathering images and sewing 'em up, hang on a second!"}),
                    ),
                    Err(e) => {
                        error!("Error occurred sending task to generate image: {e}");
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
