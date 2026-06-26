pub mod data;
pub mod recipes;

use crate::Task::Recipe;
use crate::data::fetch_client_jar;
use crate::recipes::{RecipeData, fix_recipe, fix_recipe_typo};
use axum::{Form, Json, extract::State, routing::post};
use chrono::Utc;
use dotenvy::dotenv;
use opentelemetry::{KeyValue, global};
use opentelemetry_otlp::{Protocol, WithExportConfig, WithHttpConfig};
use opentelemetry_sdk::{Resource, trace::SdkTracerProvider};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::{collections::HashMap, env};
use tokio::{net::TcpListener, sync::mpsc};
use tracing::{Level, error, info, warn};
use tracing_subscriber::{EnvFilter, Layer, layer::SubscriberExt, util::SubscriberInitExt};

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

#[derive(serde::Serialize)]
struct LogPayload {
    timestamp: String,
    group: String,
    severity: String,
    message: String,
    hostname: String,
}

struct LogVisitor {
    message: String,
}

impl tracing::field::Visit for LogVisitor {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        if field.name() == "message" {
            self.message = format!("{:?}", value);
        }
    }
}

struct HttpLogger {
    client: Client,
    url: String,
}

impl<S: tracing::Subscriber> Layer<S> for HttpLogger {
    fn on_event(
        &self,
        event: &tracing::Event<'_>,
        _ctx: tracing_subscriber::layer::Context<'_, S>,
    ) {
        let mut visitor = LogVisitor {
            message: String::new(),
        };
        event.record(&mut visitor);

        let severity = match *event.metadata().level() {
            Level::ERROR => "error",
            Level::WARN => "warn",
            Level::INFO => "info",
            Level::DEBUG => "debug",
            Level::TRACE => "trace",
        };

        let payload = LogPayload {
            timestamp: Utc::now().to_rfc3339(),
            group: "MCBot".to_string(),
            severity: severity.to_string(),
            message: visitor.message,
            hostname: hostname::get()
                .ok()
                .and_then(|h| h.into_string().ok())
                .unwrap_or_else(|| "unknown".to_string()),
        };

        let client = self.client.clone();
        let url = self.url.clone();
        tokio::spawn(async move {
            let _ = client.post(url).json(&payload).send().await;
        });
    }
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
        .with_protocol(Protocol::HttpJson)
        .with_endpoint(appsignal_url)
        .with_headers(headers.clone())
        .build()
        .expect("Failed to create OpenTelemetry span exporter");

    let resource = Resource::builder()
        .with_attributes(vec![
            KeyValue::new("service.name", "MCBot"),
            KeyValue::new("appsignal.config.name", "MCBot"),
            KeyValue::new("appsignal.config.language_integration", "rust"),
            KeyValue::new("appsignal.config.environment", "development"), // Change when I deploy
        ])
        .build();

    let tracer_provider = SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource.clone())
        .build();

    global::set_tracer_provider(tracer_provider.clone());

    let tracer = global::tracer("mc-bot-tracer");
    let telemetry_layer = tracing_opentelemetry::layer().with_tracer(tracer);

    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| {
        EnvFilter::new("info,mcbot=debug,opentelemetry_sdk=off,opentelemetry-otlp=off")
    });

    let logs_url = env::var("APPSIGNAL_LOGS_URL").expect("No appsignal logs url found");

    let client = Client::new();

    tracing_subscriber::registry()
        .with(telemetry_layer)
        .with(filter)
        .with(HttpLogger {
            client: client.clone(),
            url: logs_url,
        })
        .init();

    let bot_token = env::var("SLACK_BOT_TOKEN").expect("Bot Token NOT FOUND");
    let (queue_input, mut queue_output) = mpsc::channel::<Task>(2000);

    let mut client_jar_zip = fetch_client_jar(&client).await;
    let mut recipe_data = RecipeData::default();
    info!("Now adding recipes, items & tags to memory");
    recipe_data
        .fetch_recipes_and_more(&mut client_jar_zip)
        .await
        .expect("Failed to fetch recipes");

    /* Delete this later but
    TODO: Add typo detection and regex for sanitising input so users dont have to put a strict item_name.
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
                        .process_recipe(
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

                            let polite_msg = if e
                                .to_string()
                                .eq("Unable to convert the json to MCRecipe type")
                            {
                                json!({
                                    "response_type": "ephemeral",
                                    "text": "Uh oh, that type of recipe isn't supported! This bot currently only supports crafting recipes. If that was supposed to work, please contact @Akaalroop or email akaal@akaalroop.com"
                                })
                            } else {
                                json!({
                                    "response_type": "ephemeral",
                                    "text": format!("Uh oh, something went wrong! If this persists, please contact @Akaalroop on slack or email akaal@akaalroop.com. Error: {e}")
                                })
                            };
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
        .expect("Unable to bind the TcpListener");

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
            let requested_recipe = fix_recipe(&payload.text);
            if state.valid_recipes.contains_key(&requested_recipe) {
                match state
                    .mpsc
                    .send(Recipe {
                        item_name: requested_recipe.clone(),
                        response_url: payload.response_url,
                        channel_id: payload.channel_id,
                        user_id: payload.user_id.clone(),
                    })
                    .await
                {
                    Ok(..) => {
                        info!(
                            "Started processing recipe for {} from {}",
                            requested_recipe, payload.user_id
                        );
                        Json(
                            json!({"response_type": "ephemeral", "text": "Gathering images and sewing 'em up, hang on a second!"}),
                        )
                    }
                    Err(e) => {
                        error!("Error occurred sending task to generate image: {e}");
                        Json(
                            json!({"response_type": "ephemeral", "text": "I wasn't able to start generating your image. Please try again."}),
                        )
                    }
                }
            } else {
                match fix_recipe_typo(&state.valid_recipes, &requested_recipe) {
                    Some(fixed_requested_recipe) => {
                        match state
                            .mpsc
                            .send(Recipe {
                                item_name: fixed_requested_recipe.clone(),
                                response_url: payload.response_url,
                                channel_id: payload.channel_id,
                                user_id: payload.user_id.clone(),
                            })
                            .await
                        {
                            Ok(..) => {
                                info!(
                                    "Started processing recipe for {} from {}",
                                    fixed_requested_recipe, payload.user_id
                                );
                                Json(
                                    json!({"response_type": "ephemeral", "text": format!("Gathering images and sewing 'em up, hang on a second! (Assumed you meant {})", fixed_requested_recipe.replace('_', " "))}),
                                )
                            }
                            Err(e) => {
                                error!("Error occurred sending task to generate image: {e}");
                                Json(
                                    json!({"response_type": "ephemeral", "text": "I wasn't able to start generating your image. Please try again."}),
                                )
                            }
                        }
                    }
                    None => {
                        info!(
                            "User {} tried to get recipe {} but it was invalid",
                            payload.user_id, requested_recipe
                        );
                        Json(
                            json!({"response_type": "ephemeral", "text": "Sorry your recipe was invalid."}),
                        )
                    }
                }
            }
        }
        _ => {
            warn!(
                "User {} ran an unsupported command {}",
                payload.user_id, payload.command
            );
            Json(
                json!({"response_type": "ephemeral", "text": "Sorry that command isn't supported as of right now."}),
            )
        } // only registered slash commands should even come, this shouldn't trigger anyway
    }
}
