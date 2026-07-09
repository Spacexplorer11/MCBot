pub mod data;
pub mod font;
pub mod logging;
pub mod recipes;

use crate::logging::initialise_logging;
use crate::{
    Task::Recipe,
    data::fetch_client_jar,
    recipes::{RecipeData, fix_recipe, fix_recipe_typo},
};
use axum::{
    Form, Json,
    body::Body,
    extract::{Request, State},
    middleware,
    middleware::Next,
    response::Response,
    routing::{get, post},
};
use chrono::Utc;
use dotenvy::dotenv;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::json;
use std::{collections::HashMap, env, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{mpsc, mpsc::error::TrySendError},
};
use tracing::{debug, error, info, trace, warn};

enum Task {
    Recipe {
        item_name: String,
        response_url: Option<String>,
        channel_id: String,
        user_id: String,
        thread_ts: Option<String>,
        bot_token: String,
    },
}

#[derive(Clone)]
struct AppState {
    client: Client,
    bot_token: String,
    mpsc: mpsc::Sender<Task>,
    valid_recipes: HashMap<String, usize>,
}

#[derive(Clone)]
struct SlackSignatureVerifierState {
    verifier: slack_http_verifier::SlackVerifier,
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

#[expect(dead_code)]
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

pub struct SlackMessageContext<'a> {
    client: &'a Client,
    bot_token: &'a str,
    channel_id: &'a str,
    user_id: &'a str,
    thread_ts: Option<&'a str>,
}

#[tokio::main]
async fn main() {
    trace!("Loading .env");
    if dotenv().is_err() {
        warn!(".env file NOT LOADED");
    }

    debug!("Initialising logging");
    initialise_logging();

    let client = Client::new();

    let bot_token = env::var("SLACK_BOT_TOKEN").expect("MCBot Bot Token NOT FOUND");
    let mcrecipes_bot_token =
        env::var("SLACK_BOT_TOKEN_MCRECIPES").expect("MCRecipes Bot Token NOT FOUND");

    let signing_secret = env::var("SLACK_SIGNING_SECRET").expect("MCBot Signing Secret NOT FOUND");
    let mcrecipes_signing_secret =
        env::var("SLACK_SIGNING_SECRET_MCRECIPES").expect("MCRecipes Bot Token NOT FOUND");

    let verifier = slack_http_verifier::SlackVerifier::new(signing_secret)
        .expect("Unable to make a slack http verifier instance using MCBOT's signing secret");
    let mcrecipes_verifier = slack_http_verifier::SlackVerifier::new(mcrecipes_signing_secret)
        .expect("Unable to make a slack http verifier instance using MCRECIPES' signing secret");

    let (queue_input, mut queue_output) = mpsc::channel::<Task>(128);

    let mut client_jar_zip = fetch_client_jar(&client).await;
    let mut recipe_data = RecipeData::default();
    info!("Now adding recipes, items & tags to memory");
    recipe_data
        .fetch_recipes_and_more(&mut client_jar_zip)
        .await
        .expect("Failed to fetch recipes");

    let state = Arc::new(AppState {
        client: Client::new(),
        bot_token: bot_token.clone(),
        mpsc: queue_input.clone(),
        valid_recipes: recipe_data.valid_recipes.clone(),
    });

    let mcrecipes_state = Arc::new(AppState {
        client: Client::new(),
        bot_token: mcrecipes_bot_token,
        mpsc: queue_input,
        valid_recipes: recipe_data.valid_recipes.clone(),
    });

    let slack_signature_verifier_state = SlackSignatureVerifierState { verifier };
    let mcrecipes_slack_signature_verifier_state = SlackSignatureVerifierState {
        verifier: mcrecipes_verifier,
    };

    tokio::spawn(async move {
        while let Some(task) = queue_output.recv().await {
            trace!("Received task in async thread");
            match task {
                Recipe {
                    item_name,
                    response_url,
                    channel_id,
                    user_id,
                    thread_ts,
                    bot_token,
                } => {
                    let ctx = SlackMessageContext {
                        client: &client,
                        bot_token: &bot_token,
                        channel_id: &channel_id,
                        user_id: &user_id,
                        thread_ts: thread_ts.as_deref(),
                    };
                    match recipe_data
                        .process_recipe(item_name.as_str(), ctx, &mut client_jar_zip)
                        .await
                    {
                        Ok(..) => debug!("Recipe successfully processed"),
                        Err(e) => {
                            error!(error = ?e, "Failed to fulfill recipe task processing pipeline");

                            if let Some(response_url) = response_url {
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
                                        "text": format!("Uh oh, something went wrong! Please try again! If this persists, please contact @Akaalroop on slack or email akaal@akaalroop.com. Error: {e}")
                                    })
                                };
                                let mut response =
                                    client.post(&response_url).json(&polite_msg).send().await;
                                if response.is_err() {
                                    for _ in 0..=3 {
                                        error!(error = ?response.err().unwrap(), "The generic error message failed to send to the user");
                                        response = client
                                            .post(&response_url)
                                            .json(&polite_msg)
                                            .send()
                                            .await;
                                        if response.is_ok() {
                                            break;
                                        }
                                    }
                                }
                            } else if let Some(thread_ts) = thread_ts {
                                let polite_msg = if e
                                    .to_string()
                                    .eq("Unable to convert the json to MCRecipe type")
                                {
                                    json!({
                                        "channel": channel_id, "thread_ts": thread_ts,
                                        "text": "Uh oh, that type of recipe isn't supported! This bot currently only supports crafting recipes. If that was supposed to work, please contact @Akaalroop or email akaal@akaalroop.com"
                                    })
                                } else {
                                    json!({
                                        "channel": channel_id, "thread_ts": thread_ts,
                                        "text": format!("Uh oh, something went wrong! Please try again! If this persists, please contact @Akaalroop on slack or email akaal@akaalroop.com. Error: {e}")
                                    })
                                };
                                let mut response =
                                    send_message(&polite_msg, &client, &bot_token).await;
                                if response.is_err() {
                                    for _ in 0..=3 {
                                        error!(error = ?response.err().unwrap(), "The generic error message failed to send to the user");
                                        response =
                                            send_message(&polite_msg, &client, &bot_token).await;
                                        if response.is_ok() {
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                    };
                }
            }
        }
    });

    let mcbot_router = axum::Router::new()
        .route("/slack/events", post(handle_event))
        .route("/slack/commands", post(handle_command))
        .route_layer(middleware::from_fn_with_state(
            slack_signature_verifier_state,
            verify_slack_signature,
        ))
        .with_state(state);

    let mcrecipes_router = axum::Router::new()
        .route("/slack/mcrecipes", post(handle_mcrecipes))
        .route_layer(middleware::from_fn_with_state(
            mcrecipes_slack_signature_verifier_state,
            verify_slack_signature,
        ))
        .with_state(mcrecipes_state);

    let uptime_router = axum::Router::new().route("/status/uptime", get(uptime));

    let listener = TcpListener::bind("0.0.0.0:4598")
        .await
        .expect("Unable to bind the TcpListener");

    let router = axum::Router::new()
        .merge(mcbot_router)
        .merge(mcrecipes_router)
        .merge(uptime_router);

    axum::serve(listener, router)
        .await
        .expect("Unable to serve the axum server");
}

async fn handle_event(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SlackPayload>,
) -> Json<serde_json::Value> {
    trace!("Received an event at /slack/events");
    match payload {
        SlackPayload::UrlVerification { challenge } => {
            info!("Url Verification challenge received");
            Json(json!({"challenge": challenge}))
        }

        SlackPayload::EventCallback { event } => {
            trace!(event_type = event.event_type, "Received event");
            match state.client.post("https://slack.com/api/chat.postMessage")
                .bearer_auth(state.bot_token.clone())
                .json(&json!({"channel": event.channel, "text": "Hi! I'm MCBot! :) \nUse /mcrecipe to get crafting recipes!", "thread_ts": event.ts}))
                .send().await {
                Ok(..) => (),
                Err(e) => error!("Something went wrong with sending a message, {e}")
            };
            Json(json!({"ok":true}))
        }
    }
}

async fn handle_command(
    State(state): State<Arc<AppState>>,
    Form(payload): Form<SlackSlashCommand>,
) -> Json<serde_json::Value> {
    trace!("Received command at /slack/commands");
    match payload.command.as_str() {
        "/mcrecipe" => {
            trace!(
                "Received /mcrecipe command for {recipe}",
                recipe = &payload.text
            );
            if payload.text.is_empty() || payload.text.eq(" ") {
                return Json(
                    json!({"response_type": "ephemeral", "text": "You didn't enter a recipe!"}),
                );
            }
            let requested_recipe = fix_recipe(&payload.text);
            if state.valid_recipes.contains_key(&requested_recipe) {
                match state.mpsc.try_send(Recipe {
                    item_name: requested_recipe.clone(),
                    response_url: Some(payload.response_url),
                    channel_id: payload.channel_id,
                    user_id: payload.user_id.clone(),
                    thread_ts: None,
                    bot_token: state.bot_token.clone(),
                }) {
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
                        match e {
                            TrySendError::Full(..) => Json(
                                json!({"response_type": "ephemeral", "text": "Too many people have requested recipes at the moment. Please try again later."}),
                            ),
                            _ => Json(
                                json!({"response_type": "ephemeral", "text": "I wasn't able to start generating your image. Please try again."}),
                            ),
                        }
                    }
                }
            } else {
                match fix_recipe_typo(&state.valid_recipes, &requested_recipe) {
                    Some(fixed_requested_recipe) => {
                        match state.mpsc.try_send(Recipe {
                            item_name: fixed_requested_recipe.clone(),
                            response_url: Some(payload.response_url),
                            channel_id: payload.channel_id,
                            user_id: payload.user_id.clone(),
                            thread_ts: None,
                            bot_token: state.bot_token.clone(),
                        }) {
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
                                match e {
                                    TrySendError::Full(..) => Json(
                                        json!({"response_type": "ephemeral", "text": "Too many people have requested recipes at the moment. Please try again later."}),
                                    ),
                                    _ => Json(
                                        json!({"response_type": "ephemeral", "text": "I wasn't able to start generating your image. Please try again."}),
                                    ),
                                }
                            }
                        }
                    }
                    None => {
                        warn!(
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

async fn handle_mcrecipes(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SlackPayload>,
) -> Json<serde_json::Value> {
    trace!("Received an event at /slack/mcrecipes");
    match payload {
        SlackPayload::UrlVerification { challenge } => {
            info!("Url Verification challenge received for MCRecipes");
            Json(json!({"challenge": challenge}))
        }

        SlackPayload::EventCallback { event } => {
            let requested_recipe = fix_recipe(event.text.strip_prefix("<@U0A5X0FV9V4>").unwrap());
            if state.valid_recipes.contains_key(&requested_recipe) {
                match state.mpsc.try_send(Recipe {
                    item_name: requested_recipe.clone(),
                    response_url: None,
                    channel_id: event.channel.clone(),
                    user_id: event.user.clone(),
                    thread_ts: Some(event.ts.clone()),
                    bot_token: state.bot_token.clone(),
                }) {
                    Ok(..) => {
                        info!(
                            "Started processing recipe for {} from {}",
                            requested_recipe, event.user
                        );
                        match send_message(
                            &json!({"channel": event.channel, "thread_ts": event.ts, "text": "This bot now uses <@U0B8ER7U1S5>'s backend for responses, as it has been replaced by it. You can also use /mcrecipe to get the recipe!\nGathering images and sewing 'em up, hang on a second!"}),
                            &state.client,
                            &state.bot_token
                        ).await {
                            Ok(..) =>   Json(json!({"ok": true})),
                            Err(e) => {error!("Error occurred sending message: {e}"); Json(json!({"ok": false}))}
                        }
                    }
                    Err(e) => {
                        error!("Error occurred sending task to generate image: {e}");
                        match e {
                            TrySendError::Full(..) => {
                                match send_message(
                                    &json!({"channel": event.channel, "thread_ts": event.ts, "text": "Too many people have requested recipes at the moment. Please try again later."}),
                                    &state.client,
                                    &state.bot_token
                                ).await {
                                    Ok(..) => Json(json!({"ok": true})),
                                    Err(e) => {
                                        error!("Error occurred sending message: {e}");
                                        Json(json!({"ok": false}))
                                    }
                                }
                            },
                            _ => {
                                match send_message(
                                    &json!({"channel": event.channel, "thread_ts": event.ts, "text": "An error occurred when trying to send the task to generate your image. Please try again!"}),
                                    &state.client,
                                    &state.bot_token
                                ).await {
                                    Ok(..) => Json(json!({"ok": true})),
                                    Err(e) => {
                                        error!("Error occurred sending message: {e}");
                                        Json(json!({"ok": false}))
                                    }
                                }
                            }
                        }
                    }
                }
            } else {
                match fix_recipe_typo(&state.valid_recipes, &requested_recipe) {
                    Some(fixed_requested_recipe) => {
                        match state.mpsc.try_send(Recipe {
                            item_name: fixed_requested_recipe.clone(),
                            response_url: None,
                            channel_id: event.channel.clone(),
                            user_id: event.user.clone(),
                            thread_ts: Some(event.ts.clone()),
                            bot_token: state.bot_token.clone(),
                        }) {
                            Ok(..) => {
                                info!(
                                    "Started processing recipe for {} from {}",
                                    fixed_requested_recipe, event.user
                                );
                                match send_message(
                                    &json!({"channel": event.channel, "thread_ts": event.ts, "text": format!("This bot now uses <@U0B8ER7U1S5>'s backend for responses, as it has been replaced by it. You can also use /mcrecipe to get the recipe!\nGathering images and sewing 'em up, hang on a second! (Assumed you meant {})", fixed_requested_recipe.replace('_', " "))}),
                                    &state.client,
                                    &state.bot_token
                                ).await {
                                 Ok(..) =>   Json(json!({"ok": true})),
                                    Err(e) => {error!("Error occurred sending message: {e}"); Json(json!({"ok": false}))}
                                }
                            }
                            Err(e) => {
                                error!("Error occurred sending task to generate image: {e}");
                                match e {
                                    TrySendError::Full(..) => {
                                        match send_message(
                                            &json!({"channel": event.channel, "thread_ts": event.ts, "text": "Too many people have requested recipes at the moment. Please try again later."}),
                                            &state.client,
                                            &state.bot_token
                                        ).await {
                                            Ok(..) =>   Json(json!({"ok": true})),
                                            Err(e) => {error!("Error occurred sending message: {e}"); Json(json!({"ok": false}))}
                                        }
                                    },
                                    _ => {
                                        match send_message(
                                            &json!({"channel": event.channel, "thread_ts": event.ts, "text": "An error occurred when trying to send the task to generate your image. Please try again!"}),
                                            &state.client,
                                            &state.bot_token
                                        ).await {
                                            Ok(..) => Json(json!({"ok": true})),
                                            Err(e) => {
                                                error!("Error occurred sending message: {e}");
                                                Json(json!({"ok": false}))
                                            }
                                        }
                                    },
                                }
                            }
                        }
                    }
                    None => {
                        warn!(
                            "User {} tried to get recipe {} but it was invalid",
                            event.user, requested_recipe
                        );
                        match send_message(
                            &json!({"channel": event.channel, "thread_ts": event.ts, "text": "Sorry your recipe was invalid."}),
                            &state.client,
                            &state.bot_token
                        ).await {
                            Ok(..) =>   Json(json!({"ok": true})),
                            Err(e) => {error!("Error occurred sending message: {e}"); Json(json!({"ok": false}))}
                        }
                    }
                }
            }
        }
    }
}

async fn uptime() -> Json<serde_json::Value> {
    Json(json!({"ok": true}))
}

async fn verify_slack_signature(
    State(slack_signature_verifier_state): State<SlackSignatureVerifierState>,
    request: Request,
    next: Next,
) -> Response {
    trace!("Received request to verify signature");
    let (parts, body) = request.into_parts();

    let request_bytes = match axum::body::to_bytes(body, 1024 * 16).await {
        Ok(bytes) => bytes,
        Err(e) => {
            error!("Failed to read request body: {e}");
            return Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body("Failed to read request body".into())
                .unwrap();
        }
    };
    let timestamp = match parts.headers.get("x-slack-request-timestamp") {
        Some(ts) => {
            let ts = match ts.to_str() {
                Ok(s) => s,
                Err(..) => {
                    error!("Slack request timestamp header not a string");
                    return Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body("Slack request timestamp header not a string".into())
                        .unwrap();
                }
            };
            let ts = match ts.parse::<i64>() {
                Ok(s) => s,
                Err(..) => {
                    error!("Slack request timestamp header not a number");
                    return Response::builder()
                        .status(StatusCode::UNAUTHORIZED)
                        .body("Slack request timestamp is not a number".into())
                        .unwrap();
                }
            };
            let now = Utc::now().timestamp();
            let allowed_skew = 60 * 5;
            if ts < now - allowed_skew || ts > now + allowed_skew {
                error!("Slack request timestamp is too old");
                return Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body("Slack request timestamp is too old".into())
                    .unwrap();
            }
            ts.to_string()
        }
        None => {
            error!("Slack request timestamp header not found");
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body("Slack request timestamp header not found".into())
                .unwrap();
        }
    };
    let signature = match parts.headers.get("x-slack-signature") {
        Some(sig) => match sig.to_str() {
            Ok(s) => s,
            Err(..) => {
                error!("Slack signature header not a string");
                return Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body("Slack signature header not a string".into())
                    .unwrap();
            }
        },
        None => {
            error!("Slack signature header not found");
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body("Slack signature header not found".into())
                .unwrap();
        }
    };

    let request_string = match str::from_utf8(request_bytes.as_ref()) {
        Ok(s) => s,
        Err(e) => {
            error!("Slack request body not valid utf-8: {e}");
            return Response::builder()
                .status(StatusCode::BAD_REQUEST)
                .body("Slack request body not valid utf-8".into())
                .unwrap();
        }
    };

    match slack_signature_verifier_state.verifier.verify(
        timestamp.as_str(),
        request_string,
        signature,
    ) {
        Ok(..) => {
            trace!("Slack signature verification successful");
            next.run(Request::from_parts(parts, Body::from(request_bytes)))
                .await
        }
        Err(e) => {
            warn!("Slack signature verification failed: {e}");
            Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body("Slack signature verification failed".into())
                .unwrap()
        }
    }
}

async fn send_message(
    json: &serde_json::Value,
    client: &Client,
    bot_token: &String,
) -> anyhow::Result<()> {
    client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(bot_token)
        .json(json)
        .send()
        .await?;
    Ok(())
}
