#![allow(linker_messages)]

pub mod data;
pub mod font;
pub mod logging;
pub mod recipes;

use crate::{
    Task::{Recipe, Subscriptions},
    data::fetch_client_jar,
    logging::initialise_logging,
    recipes::{RecipeData, validate_recipe},
};
use anyhow::{Context, anyhow};
use axum::response::IntoResponse;
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
use hmac::{KeyInit, Mac};
use reqwest::{Client, StatusCode};
use sentry::integrations::anyhow::capture_anyhow;
use sentry::integrations::tower::{NewSentryLayer, SentryHttpLayer};
use sentry::metrics::{counter, distribution};
use sentry::protocol::Unit;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{query, query_as};
use std::time::Duration;
use std::{collections::HashMap, env, io, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{mpsc, mpsc::error::TrySendError},
};
use tower::ServiceBuilder;
use tracing::{debug, error, info, trace, warn};

type HmacSha256 = hmac::Hmac<sha2::Sha256>;

enum Task {
    Recipe {
        item_name: String,
        response_url: Option<String>,
        channel_id: String,
        user_id: String,
        thread_ts: Option<String>,
        bot_token: Arc<str>,
    },
    Subscriptions {
        user_id: String,
        trigger_id: String,
        bot_token: Arc<str>,
    },
}

struct Subscription {
    id: i64,
    target_id: String,
    active: bool,
    mc_usernames: Vec<String>,
}

#[derive(Clone)]
struct AppState {
    client: Client,
    bot_token: Arc<str>,
    mpsc: mpsc::Sender<Task>,
    valid_recipes: HashMap<String, usize>,
    sqlx_pool: sqlx::PgPool,
    flipped_language_mappings: HashMap<String, String>,
    hackclub_api_key: Arc<str>,
}

#[derive(Clone)]
struct MCRecipesAppState {
    client: Client,
    bot_token: Arc<str>,
    mpsc: mpsc::Sender<Task>,
    valid_recipes: HashMap<String, usize>,
    flipped_language_mappings: HashMap<String, String>,
}

#[derive(Deserialize, Serialize)]
struct SubsPageMetadata {
    page: i64,
    page_size: i64,
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
struct SlackInteractionPayload {
    payload: String,
}

#[derive(Deserialize)]
struct SlackChannel {
    id: String,
}

#[derive(Deserialize)]
struct OpenConversationResponse {
    ok: bool,
    channel: SlackChannel,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum SlackInteraction {
    #[serde(rename = "block_actions")]
    BlockActions {
        user: SlackUser,
        view: Option<SlackView>,
        actions: Vec<SlackActions>,
        trigger_id: String,
        response_url: Option<String>,
    },
    #[serde(rename = "view_submission")]
    ViewSubmission { user: SlackUser, view: SlackView },
}

#[derive(Deserialize)]
struct SlackView {
    id: String,
    callback_id: CallbackId,
    private_metadata: Option<String>,
    hash: String,
    blocks: Vec<Value>,
    state: Option<ViewState>,
}

#[derive(Deserialize)]
struct ViewState {
    values: HashMap<String, HashMap<String, StateElements>>,
}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum StateElements {
    #[serde(rename = "users_select")]
    UserSelect { selected_user: String },
}

#[derive(Deserialize)]
struct SlackActions {
    #[serde(flatten)]
    action_id: ActionId,
}

#[derive(Deserialize, Debug)]
#[serde(tag = "action_id")]
enum ActionId {
    #[serde(rename = "subscribe_new_person")]
    SubscribeNewPerson,
    #[serde(rename = "remove_subscription")]
    RemoveSubscription { value: String },
    #[serde(rename = "subs_page_prev")]
    SubsPagePrev,
    #[serde(rename = "subs_page_next")]
    SubsPageNext,
    #[serde(rename = "users_select")]
    UserSelect { selected_user: String },
    #[serde(rename = "approve_subscription")]
    ApproveSubscription { value: String },
    #[serde(rename = "decline_subscription")]
    DeclineSubscription { value: String },
    #[serde(other)]
    Other,
}

impl ActionId {
    fn as_metric_name(&self) -> &'static str {
        match self {
            ActionId::SubscribeNewPerson => "subscribe_new_person",
            ActionId::RemoveSubscription { .. } => "remove_subscription",
            ActionId::SubsPagePrev => "subs_page_prev",
            ActionId::SubsPageNext => "subs_page_next",
            ActionId::UserSelect { .. } => "users_select",
            ActionId::ApproveSubscription { .. } => "approve_subscription",
            ActionId::DeclineSubscription { .. } => "decline_subscription",
            ActionId::Other => "other",
        }
    }
}

#[derive(Deserialize)]
struct MinecraftPlayerData {
    nick: MinecraftPlayerNick,
}

#[derive(Deserialize)]
struct MinecraftPlayerNick {
    name: String,
}

#[derive(Deserialize)]
enum CallbackId {
    #[serde(rename = "configure_subs_modal")]
    ConfigureSubsModal,
    #[serde(rename = "input_new_sub_user")]
    InputNewSubUser,
}

#[derive(Deserialize)]
struct SlackUser {
    id: String,
}

#[derive(Deserialize, Debug)]
struct SlackEvent {
    #[serde(rename = "type")]
    event_type: SlackEvents,
    channel: String,
    text: String,
    user: Option<String>,
    ts: String,
    bot_id: Option<String>,
    username: UsefulUsernames,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
enum UsefulUsernames {
    Join,
    Leave,
    Nickname,
    #[serde(other)]
    Irrelevant,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
enum SlackEvents {
    AppMention,
    Message,
}

#[derive(Deserialize)]
struct SlackSlashCommand {
    command: String,
    text: String,
    channel_id: String,
    user_id: String,
    response_url: String,
    trigger_id: String,
}

pub struct SlackMessageContext<'a> {
    client: &'a Client,
    bot_token: &'a str,
    channel_id: &'a str,
    user_id: &'a str,
    thread_ts: Option<&'a str>,
}

fn main() -> io::Result<()> {
    trace!("Loading .env");

    if dotenv().is_err() {
        warn!(".env file NOT LOADED");
    }

    let sentry_url = env::var("SENTRY_URL").expect("SENTRY URL NOT FOUND");

    // Sentry MUST be initialised before the Tokio runtime starts.
    let _guard = sentry::init(
        sentry::ClientOptions::new()
            .dsn(&sentry_url)
            .maybe_release(sentry::release_name!())
            .enable_logs(true)
            .enable_metrics(true)
            .traces_sample_rate(1.0)
            .auto_session_tracking(true)
            .session_mode(sentry::SessionMode::Request),
    );

    debug!("Initialising logging");
    initialise_logging();

    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?
        .block_on(async {
            let client = Client::new();

            let bot_token =
                env::var("SLACK_BOT_TOKEN").expect("MCBot Bot Token NOT FOUND");

            let mcrecipes_bot_token = env::var("SLACK_BOT_TOKEN_MCRECIPES")
                .expect("MCRecipes Bot Token NOT FOUND");

            let signing_secret = Arc::new(
                env::var("SLACK_SIGNING_SECRET")
                    .expect("MCBot Signing Secret NOT FOUND"),
            );

            let mcrecipes_signing_secret = Arc::new(
                env::var("SLACK_SIGNING_SECRET_MCRECIPES")
                    .expect("MCRecipes Bot Token NOT FOUND"),
            );

            let hackclub_api_key =
                env::var("HACKCLUB_API_KEY").expect("HACKCLUB API KEY NOT FOUND");

            let task_queue_capacity = 128usize;
            let (queue_input, mut queue_output) = mpsc::channel::<Task>(task_queue_capacity);
            debug!(capacity = task_queue_capacity, "MPSC task queue created");

            let mut client_jar_zip = fetch_client_jar(&client).await;
            let mut recipe_data = RecipeData::default();

            info!("Now adding recipes, items & tags to memory");

            match recipe_data
                .fetch_recipes_and_more(&mut client_jar_zip)
                .await
            {
                Ok(()) => info!("All startup assets loaded successfully"),
                Err(e) => {
                    capture_anyhow(&e);
                    panic!("Failed to fetch recipes: {e:?}");
                }
            }

            let sqlx_pool = sqlx::Pool::connect(
                &env::var("DATABASE_URL").expect("DATABASE_URL NOT FOUND"),
            )
                .await
                .expect("Failed to connect to database");
            info!("Connected to the PostgreSQL database");

            let mut flipped_language_mappings = HashMap::new();

            for (key, value) in recipe_data.language_mappings() {
                let value = value.to_lowercase().replace(' ', "_");
                flipped_language_mappings.insert(value, key);
            }

            let state = Arc::new(AppState {
                client: Client::new(),
                bot_token: bot_token.into(),
                mpsc: queue_input.clone(),
                valid_recipes: recipe_data.valid_recipes.clone(),
                sqlx_pool: sqlx_pool.clone(),
                flipped_language_mappings: flipped_language_mappings.clone(),
                hackclub_api_key: hackclub_api_key.into(),
            });

            let mcrecipes_state = Arc::new(MCRecipesAppState {
                client: Client::new(),
                bot_token: mcrecipes_bot_token.into(),
                mpsc: queue_input,
                valid_recipes: recipe_data.valid_recipes.clone(),
                flipped_language_mappings,
            });

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
                            let processing_start = std::time::Instant::now();
                            let ctx = SlackMessageContext {
                                client: &client,
                                bot_token: &bot_token,
                                channel_id: &channel_id,
                                user_id: &user_id,
                                thread_ts: thread_ts.as_deref(),
                            };

                            match recipe_data
                                .process_recipe(
                                    item_name.as_str(),
                                    ctx,
                                    &mut client_jar_zip,
                                )
                                .await
                            {
                                Ok(..) => {
                                    counter("recipe.processed", 1)
                                        .attribute("result", "success")
                                        .capture();
                                    debug!(item_name = %item_name, user_id = %user_id, channel_id = %channel_id, "Recipe successfully processed");
                                }

                                Err(e) => {
                                    counter("recipe.processed", 1)
                                        .attribute("result", "error")
                                        .capture();
                                    capture_anyhow(&e);
                                    error!(
                                        error = ?e,
                                        item_name = %item_name,
                                        user_id = %user_id,
                                        "Failed to fulfil recipe task processing pipeline"
                                    );
                                    warn!(item_name = %item_name, user_id = %user_id, "Sending user-friendly error message to Slack");

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
                                                "text": format!(
                                                    "Uh oh, something went wrong! Please try again! If this persists, please contact @Akaalroop on Slack or email akaal@akaalroop.com. Error: {e}"
                                                )
                                            })
                                        };

                                        let mut response = client
                                            .post(&response_url)
                                            .json(&polite_msg)
                                            .send()
                                            .await;

                                        if response.is_err() {
                                            for _ in 0..=3 {
                                                error!(
                                                    error = ?response.err().unwrap(),
                                                    "The generic error message failed to send to the user"
                                                );

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
                                                "channel": channel_id,
                                                "thread_ts": thread_ts,
                                                "text": "Uh oh, that type of recipe isn't supported! This bot currently only supports crafting recipes. If that was supposed to work, please contact @Akaalroop or email akaal@akaalroop.com"
                                            })
                                        } else {
                                            json!({
                                                "channel": channel_id,
                                                "thread_ts": thread_ts,
                                                "text": format!(
                                                    "Uh oh, something went wrong! Please try again! If this persists, please contact @Akaalroop on Slack or email akaal@akaalroop.com. Error: {e}"
                                                )
                                            })
                                        };

                                        let mut response = legacy_send_message(
                                            &polite_msg,
                                            &client,
                                            &bot_token,
                                        )
                                            .await;

                                        if response.is_err() {
                                            for _ in 0..=3 {
                                                error!(
                                                    error = ?response.err().unwrap(),
                                                    "The generic error message failed to send to the user"
                                                );

                                                response = legacy_send_message(
                                                    &polite_msg,
                                                    &client,
                                                    &bot_token,
                                                )
                                                    .await;

                                                if response.is_ok() {
                                                    break;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                            distribution(
                                "recipe.processing.duration",
                                processing_start.elapsed().as_millis() as f64,
                            )
                            .unit(Unit::Millisecond)
                            .capture();
                        }

                        Subscriptions {
                            user_id,
                            trigger_id,
                            bot_token,
                        } => {
                            debug!(user_id = %user_id, "Processing Subscriptions task: fetching and building modal view");
                            let modal_view = match fetch_and_build_subs_modal_view(
                                &sqlx_pool,
                                0,
                                user_id,
                            )
                                .await
                            {
                                Ok(view) => {
                                    counter("subscriptions.modal", 1)
                                        .attribute("result", "opened")
                                        .capture();
                                    trace!("Subscriptions modal view built successfully");
                                    view
                                }

                                Err(e) => {
                                    counter("subscriptions.modal", 1)
                                        .attribute("result", "error")
                                        .capture();
                                    capture_anyhow(&e);
                                    error!(
                                        error = ?e,
                                        "An error occurred fetching and building the modal view"
                                    );

                                    continue;
                                }
                            };

                            info!("Opening subscriptions configuration modal");
                            let payload = json!({
                                "trigger_id": trigger_id,
                                "view": modal_view,
                            });

                            send_and_log_on_failure(
                                client
                                    .post("https://slack.com/api/views.open")
                                    .bearer_auth(bot_token)
                                    .json(&payload),
                                "Opening the initial configuration modal",
                            )
                                .await;
                        }
                    }
                }
            });

            let mcbot_router = axum::Router::new()
                .route("/slack/events", post(handle_event))
                .route("/slack/commands", post(handle_command))
                .route(
                    "/slack/interactions",
                    post(handle_interactions),
                )
                .route_layer(middleware::from_fn_with_state(
                    signing_secret,
                    verify_slack_signature,
                ))
                .with_state(state);

            let mcrecipes_router = axum::Router::new()
                .route(
                    "/slack/mcrecipes",
                    post(handle_mcrecipes),
                )
                .route_layer(middleware::from_fn_with_state(
                    mcrecipes_signing_secret,
                    verify_slack_signature,
                ))
                .with_state(mcrecipes_state);

            let uptime_router =
                axum::Router::new().route("/status/uptime", get(uptime));

            let router = axum::Router::new()
                .merge(mcbot_router)
                .merge(mcrecipes_router)
                .merge(uptime_router)
                .layer(
                    ServiceBuilder::new()
                        // Bind a Sentry Hub to each request so errors
                        // are correctly associated with that request.
                        .layer(
                            NewSentryLayer::<Request<Body>>::new_from_top(),
                        )
                        // Create a Sentry transaction for every HTTP request.
                        .layer(
                            SentryHttpLayer::new()
                                .enable_transaction(),
                        ),
                );

            let listener = TcpListener::bind("0.0.0.0:4598")
                .await
                .expect("Unable to bind the TcpListener");
            info!(addr = "0.0.0.0:4598", "MCBot HTTP server listening");

            axum::serve(listener, router)
                .await
                .expect("Unable to serve the axum server");

            Ok(())
        })
}

async fn handle_event(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SlackPayload>,
) -> Response<Body> {
    trace!("Received an event at /slack/events");
    match payload {
        SlackPayload::UrlVerification { challenge } => {
            counter("slack.event", 1)
                .attribute("type", "url_verification")
                .capture();
            info!("Url Verification challenge received");
            Json(json!({"challenge": challenge})).into_response()
        }

        SlackPayload::EventCallback { event } => {
            counter("slack.event", 1)
                .attribute("type", "event_callback")
                .attribute("subtype", format!("{:?}", event.event_type))
                .capture();
            trace!(event_type = ?event.event_type, ?event, "Received event");
            match event.event_type {
              SlackEvents::AppMention => send_message(&json!({"channel": event.channel, "text": "Hi! I'm MCBot, made by <@U08D22QNUVD>! :) \nUse /mcrecipe to get crafting recipes!", "thread_ts": event.ts}), &state.client, &state.bot_token).await,
                SlackEvents::Message => {
                    if let Some(bot_id) = event.bot_id {
                        if bot_id.to_uppercase().eq("B04SB2PQLS1") {
                            match event.username {
                                UsefulUsernames::Join => StatusCode::OK.into_response(),
                                UsefulUsernames::Leave => StatusCode::OK.into_response(),
                                UsefulUsernames::Nickname => {
                                    let text = event.text.split_ascii_whitespace().map(|part| part.to_string()).collect::<Vec<String>>();
                                    
                                    let old_nick = text.first().expect("This is a deterministic message. If this has changed then that is requires immediate attention.");
                                    let new_nick = text.last().expect("This is a deterministic message. If this has changed then that is requires immediate attention.");
                                    
                                    let row = match query!(
                                        "SELECT * FROM users WHERE $1 = ANY(mc_usernames)",
                                        old_nick
                                    )
                                        .fetch_one(&state.sqlx_pool)
                                        .await
                                    {
                                        Ok(row) => row,
                                        Err(error) => {
                                            error!(?error, timestamp=%event.ts, text=%event.text, ?old_nick, ?new_nick, "MANUAL INPUT REQUIRED. AN ERROR OCCURRED WHEN FETCHING THE ROW FROM THE DATABASE.");
                                            return StatusCode::OK.into_response();
                                        }
                                    };
                                    
                                    let mut mc_usernames = row.mc_usernames;
                                    mc_usernames.retain(|user| user != old_nick);
                                    mc_usernames.push(new_nick.clone());
                                    
                                    match query!("UPDATE users SET mc_usernames = $1 WHERE slack_id = $2", &mc_usernames, row.slack_id).execute(&state.sqlx_pool).await {
                                        Ok(..) => {
                                            info!(slack=%row.slack_id,"Successfully updated nick from {old_nick} to {new_nick}.");
                                            counter("nickname.update", 1)
                                                .capture();
                                        },
                                        Err(e) => error!(error=?e, timestamp=%event.ts, text=%event.text, %old_nick, %new_nick, slack=%row.slack_id, "MANUAL INPUT REQUIRED. AN ERROR OCCURRED WHEN UPDATING THE DATABASE IN THE FINAL STEP OF UPDATING A NICKNAME.")
                                    }
                                    
                                    StatusCode::OK.into_response()
                                    
                              /* Honestly this took me took long to make so in case I need it in the future I kept it
                              let response: MinecraftPlayerData = match state.client.get("https://api.mc.hackclub.com")
                                    .header("User-Agent", "MCBot")
                                    .bearer_auth(state.hackclub_api_key.clone())
                                    .send()
                                    .await {
                                        Ok(res) => match res.status() {
                                            StatusCode::OK => match res.json().await {
                                                Ok(mpd) => mpd,
                                                Err(e) => {
                                                    error!(error=?e, timestamp=%event.ts, text=%event.text, "MANUAL INPUT REQUIRED. AN ERROR OCCURRED WHEN CONVERTING THE RESPONSE FROM HACKCLUB API TO MINECRAFTPLAYERDATA.");
                                                    return StatusCode::OK.into_response()
                                                }
                                            }
                                            StatusCode::TOO_MANY_REQUESTS => {
                                                error!(timestamp=%event.ts, text=%event.text, response=?res, "MANUAL INPUT REQUIRED. YOU HIT THE RATELIMT FOR THE HACKCLUB API");
                                                return StatusCode::OK.into_response()
                                            }
                                            StatusCode::NOT_FOUND => {
                                                error!(timestamp=%event.ts, text=%event.text, response=?res, "MANUAL INPUT REQUIRED. THE API COULDN'T FIND THIS NICK EVEN THO IT SHOULD BE ABLE TO BE FOUND.");
                                                return StatusCode::OK.into_response()
                                            }
                                            _ => {
                                                error!(status=?res.status(), timestamp=%event.ts, text=%event.text, response=?res, "MANUAL INPUT REQUIRED. THE HACKCLUB API RETURNED AN ERROR STATUS CODE.");
                                                return StatusCode::OK.into_response()
                                            }
                                        }
                                    Err(e) => {
                                        error!(error=?e, timestamp=%event.ts, text=%event.text, "MANUAL INPUT REQUIRED. AN ERROR OCCURRED WHEN FETCHING THE SLACK ID FROM HACKCLUB API.");
                                        return StatusCode::OK.into_response()
                                    }
                                    }; */
                                },
                                UsefulUsernames::Irrelevant => StatusCode::OK.into_response()
                            }
                        } else {
                            StatusCode::OK.into_response()
                        }
                    }
                    else {
                        StatusCode::OK.into_response()
                    }
                }
            }
        }
    }
}

async fn handle_command(
    State(state): State<Arc<AppState>>,
    Form(payload): Form<SlackSlashCommand>,
) -> Response {
    trace!("Received command at /slack/commands");
    counter("slack.command", 1)
        .attribute("command", payload.command.as_str())
        .capture();
    match payload.command.as_str() {
        "/mcrecipe" => {
            trace!(
                "Received /mcrecipe command for {recipe}",
                recipe = &payload.text
            );
            if payload.text.is_empty() || payload.text.eq(" ") {
                counter("recipe.request", 1)
                    .attribute("result", "empty")
                    .capture();
                return Json(
                    json!({"response_type": "ephemeral", "text": "You didn't enter a recipe!"}),
                )
                .into_response();
            }
            let (is_recipe_valid, assumption_text, recipe) = validate_recipe(
                &payload.text,
                &state.valid_recipes,
                &state.flipped_language_mappings,
            );
            if is_recipe_valid {
                counter("recipe.request", 1)
                    .attribute("result", "valid")
                    .capture();
                match state.mpsc.try_send(Recipe {
                    item_name: recipe.clone(),
                    response_url: Some(payload.response_url),
                    channel_id: payload.channel_id,
                    user_id: payload.user_id.clone(),
                    thread_ts: None,
                    bot_token: state.bot_token.clone(),
                }) {
                    Ok(..) => {
                        info!(
                            "Started processing recipe for {} from {}",
                            recipe, payload.user_id
                        );
                        Json(
                            json!({"response_type": "ephemeral", "text": format!("Gathering images and sewing 'em up, hang on a second! {assumption_text}")}),
                        ).into_response()
                    }
                    Err(e) => {
                        counter("task.queue.full", 1)
                            .attribute("task", "recipe")
                            .capture();
                        error!("Error occurred sending task to generate image: {e}");
                        match e {
                            TrySendError::Full(..) => Json(
                                json!({"response_type": "ephemeral", "text": "Too many people have requested recipes at the moment. Please try again later."}),
                            ).into_response(),
                            _ => Json(
                                json!({"response_type": "ephemeral", "text": "I wasn't able to start generating your image. Please try again."}),
                            ).into_response(),
                        }
                    }
                }
            } else {
                counter("recipe.request", 1)
                    .attribute("result", "invalid")
                    .capture();
                warn!(
                    "User {} tried to get recipe {recipe} but it was invalid",
                    payload.user_id
                );
                Json(
                    json!({"response_type": "ephemeral", "text": format!("Sorry your recipe {recipe} was invalid.")}),
                ).into_response()
            }
        }
        "/mc-subs-config" => {
            match state.mpsc.try_send(Subscriptions {
                user_id: payload.user_id.clone(),
                trigger_id: payload.trigger_id,
                bot_token: state.bot_token.clone(),
            }) {
                Ok(..) => {
                    info!("Configuring updates for {}", payload.user_id);
                    StatusCode::OK.into_response()
                }
                Err(e) => {
                    counter("task.queue.full", 1)
                        .attribute("task", "subscriptions")
                        .capture();
                    error!("Error occurred sending task to generate image: {e}");
                    match e {
                        TrySendError::Full(..) => Json(
                            json!({"response_type": "ephemeral", "text": "Too many people are using MCBot at the moment. Please try again later."}),
                        ).into_response(),
                        _ => Json(
                            json!({"response_type": "ephemeral", "text": "I wasn't able to open the config menu. Please try again."}),
                        ).into_response(),
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
            ).into_response()
        } // only registered slash commands should even come, this shouldn't trigger anyway
    }
}

async fn handle_interactions(
    State(state): State<Arc<AppState>>,
    Form(payload): Form<SlackInteractionPayload>,
) -> Response {
    trace!("Received an interaction at /slack/interactions");
    let interaction: SlackInteraction = match serde_json::from_str(&payload.payload) {
        Ok(i) => i,
        Err(e) => {
            counter("slack.interaction", 1)
                .attribute("action", "parse_error")
                .capture();
            error!("Failed to parse interaction payload: {e}");
            return StatusCode::BAD_REQUEST.into_response();
        }
    };
    match interaction {
        SlackInteraction::BlockActions {
            user,
            mut view,
            actions,
            trigger_id,
            response_url,
        } => {
            trace!(user_id = %user.id, "Handling BlockActions interaction");
            let actions = &actions[0];
            counter("slack.interaction", 1)
                .attribute("action", actions.action_id.as_metric_name())
                .capture();
            let private_metadata: Option<SubsPageMetadata>;
            let mut page: i64 = 0;

            if let Some(view) = &view {
                #[allow(clippy::single_match)]
                match view.callback_id {
                    CallbackId::ConfigureSubsModal => {
                        private_metadata = if let Some(private_metadata) = &view.private_metadata {
                            let priv_metadata: Result<SubsPageMetadata, serde_json::error::Error> =
                                serde_json::from_str(private_metadata);
                            match priv_metadata {
                                Ok(priv_metadata) => Some(priv_metadata),
                                Err(e) => {
                                    warn!(error = ?e, "Couldn't convert private_metadata to array so just returning None");
                                    None
                                }
                            }
                        } else {
                            None
                        };
                        page = if let Some(pmd) = private_metadata {
                            pmd.page
                        } else {
                            warn!("Private metadata not found, defaulting page value to 0");
                            0
                        }
                    }
                    _ => (),
                };
            }
            match &actions.action_id {
                ActionId::RemoveSubscription { value } => {
                    debug!(user_id = %user.id, subscription_id = %value, "User requested subscription removal");
                    if let Some(view) = view {
                        let id = match value.parse::<i64>() {
                            Ok(id) => id,
                            Err(..) => {
                                error!("Failed to parse id as i64 (id = {value})");
                                return StatusCode::OK.into_response();
                            }
                        };
                        match query!(
                            "DELETE FROM subscriptions WHERE id = $1 and subscriber_id = $2",
                            id,
                            user.id
                        )
                        .execute(&state.sqlx_pool)
                        .await
                        {
                            Ok(..) => {
                                info!(user_id = %user.id, subscription_id = id, "Subscription removed from database");
                                trace!("Successfully deleted row from database");
                                let modal_view = match fetch_and_build_subs_modal_view(
                                    &state.sqlx_pool,
                                    page,
                                    user.id,
                                )
                                .await
                                {
                                    Ok(json) => json,
                                    Err(e) => {
                                        error!(error = ?e, "Unable to build and fetch subs");
                                        return StatusCode::OK.into_response();
                                    }
                                };
                                let json = json!({
                                    "hash": view.hash,
                                    "view": modal_view,
                                    "view_id": view.id
                                });
                                send_and_log_on_failure(
                                    state
                                        .client
                                        .post("https://slack.com/api/views.update")
                                        .bearer_auth(state.bot_token.clone())
                                        .json(&json),
                                    "Updating the view after a subscription was removed",
                                )
                                .await;
                                StatusCode::OK.into_response()
                            }
                            Err(e) => {
                                error!(
                                    "An error occurred when deleting a subscription from the database, error: {}",
                                    e
                                );
                                StatusCode::OK.into_response()
                            }
                        }
                    } else {
                        error!("View not found when required for removing a subscription");
                        StatusCode::BAD_REQUEST.into_response()
                    }
                }
                /*
                 TODO: Add to database on request
                 TODO: Send DM to subscriber that they accepted
                 TODO: Patrol #minecraft-bridge and send DM's (hammer the index remember)
                 TODO: Make the code better and use let Variant(x) = x else {} instead of if let Err(e) blah blah blah
                */
                ActionId::SubscribeNewPerson => {
                    debug!(user_id = %user.id, "User triggered SubscribeNewPerson action, pushing user selection modal");
                    let user_select_block = json!({
                        "type": "input",
                        "label": {
                            "type": "plain_text",
                            "text": "Select the user you wish to subscribe to:"
                        },
                        "element": {
                            "type": "users_select",
                            "placeholder": {
                                "type": "plain_text",
                                "text": "Select a user",
                                "emoji": true
                            },
                            "action_id": "users_select"
                        },
                        "dispatch_action": true,
                        "hint": {
                            "type": "plain_text",
                            "text": "How this works: After selecting and confirming the user, a DM will be sent which asks for approval from the user you selected. Their decision will be relayed back to you via a DM and if it's a yes, you will automatically start receiving DM updates when the join/leave the hackclub minecraft server."
                        },
                        "block_id": "users_select"
                    });

                    let json = json!({
                        "view": {
                            "type": "modal",
                            "callback_id": "input_new_sub_user",
                            "title": {
                                "type": "plain_text",
                                "text": "New Subscription",
                                "emoji": true
                            },
                            "submit": {
                                "type": "plain_text",
                                "text": "Confirm"
                            },
                            "blocks": [user_select_block]
                        },
                        "trigger_id": trigger_id
                    });

                    send_and_log_on_failure(
                        state
                            .client
                            .post("https://slack.com/api/views.push")
                            .bearer_auth(state.bot_token.clone())
                            .json(&json),
                        "Pushing an input view",
                    )
                    .await;

                    StatusCode::OK.into_response()
                }
                ActionId::SubsPageNext | ActionId::SubsPagePrev => {
                    if let Some(view) = view {
                        match actions.action_id {
                            ActionId::SubsPageNext => {
                                debug!(user_id = %user.id, current_page = page, new_page = page + 1, "User navigating to next subscription page");
                                page += 1;
                            }
                            ActionId::SubsPagePrev => {
                                debug!(user_id = %user.id, current_page = page, new_page = page - 1, "User navigating to previous subscription page");
                                page -= 1;
                            }
                            _ => unreachable!(),
                        }
                        let modal_view =
                            match fetch_and_build_subs_modal_view(&state.sqlx_pool, page, user.id)
                                .await
                            {
                                Ok(json) => json,
                                Err(e) => {
                                    error!(error = ?e, "Unable to build and fetch subs");
                                    return StatusCode::OK.into_response();
                                }
                            };
                        let json = json!({
                            "hash": view.hash,
                            "view": modal_view,
                            "view_id": view.id
                        });
                        send_and_log_on_failure(
                            state
                                .client
                                .post("https://slack.com/api/views.update")
                                .bearer_auth(state.bot_token.clone())
                                .json(&json),
                            "Updating the view after a page change",
                        )
                        .await;
                        StatusCode::OK.into_response()
                    } else {
                        error!("View not found when required for changing the page");
                        StatusCode::BAD_REQUEST.into_response()
                    }
                }
                ActionId::UserSelect { selected_user } => {
                    if let Some(view) = &mut view {
                        debug!(user_id = %user.id, selected_user = %selected_user, "User selected a target for subscription");
                        let existing_subscription = query!(
                    "SELECT 1 as exists FROM subscriptions WHERE subscriber_id = $1 AND target_id = $2",
                    user.id,
                    selected_user
                    )
                            .fetch_optional(&state.sqlx_pool)
                            .await;

                        let existing_subscription = match existing_subscription {
                            Ok(row) => row.is_some(),
                            Err(e) => {
                                error!("Failed to check for existing subscription: {e}");
                                return StatusCode::OK.into_response();
                            }
                        };

                        let non_player = match state
                            .client
                            .get(format!("https://api.mc.hackclub.com/player?slack={selected_user}"))
                            .bearer_auth(state.hackclub_api_key.clone())
                            .send()
                            .await
                        {
                            Ok(response) => response.status().eq(&StatusCode::NOT_FOUND),
                            Err(e) => {
                                error!(error=?e, "Failed to check for linked account on hackclub mc API.");
                                return StatusCode::OK.into_response();
                            }
                        };

                        let alert_text = if existing_subscription {
                            Some("You are already subscribed to this person".to_string())
                        } else if non_player {
                            Some(format!(
                                "This person doesn't play on the hackclub minecraft server!\nIf this is incorrect, please ask <@{selected_user}> to join the server and link their slack account."
                            ))
                        } else {
                            None
                        };

                        if let Some(alert_text) = alert_text {
                            let alert_block = json!({
                                "type": "alert",
                                "text": {
                                    "type": "mrkdwn",
                                    "text": format!("*Error*: {alert_text}"),
                                    "verbatim": false
                                },
                                "level": "error"
                            });

                            let already_error_block_present = view
                                .blocks
                                .iter()
                                .any(|v| v.get("type") == Some(&json!("alert")));

                            if !already_error_block_present {
                                view.blocks.insert(0, alert_block);
                            }
                        } else {
                            view.blocks
                                .retain(|v| v.get("type") != Some(&json!("alert")));
                        }

                        let json = json!({
                            "view": {
                                "type": "modal",
                                "callback_id": "input_new_sub_user",
                                "title": {
                                    "type": "plain_text",
                                    "text": "New Subscription",
                                    "emoji": true
                                },
                                "submit": {
                                    "type": "plain_text",
                                    "text": "Confirm"
                                },
                                "blocks": view.blocks
                            },
                            "hash": view.hash,
                            "view_id": view.id
                        });

                        send_and_log_on_failure(
                            state
                                .client
                                .post("https://slack.com/api/views.update")
                                .bearer_auth(state.bot_token.clone())
                                .json(&json),
                            "Updating the view after a user was selected",
                        )
                        .await;

                        StatusCode::OK.into_response()
                    } else {
                        error!("View not found when required for the user select block action");
                        StatusCode::BAD_REQUEST.into_response()
                    }
                }
                ActionId::DeclineSubscription { value }
                | ActionId::ApproveSubscription { value } => {
                    debug!(user_id = %user.id, subscriber_id = %value, action = ?actions.action_id, "Processing subscription approval/decline action");
                    let dm_text: String;
                    let completed_text: String;

                    match &actions.action_id {
                        ActionId::DeclineSubscription { value } => {
                            info!(target_id = %user.id, subscriber_id = %value, "User declined a subscription request");
                            dm_text = format!(
                                "Unfortunately <@{}> has declined your request to track their join/leave updates for the hackclub minecraft server",
                                user.id
                            );
                            completed_text = format!(
                                "Successfully declined request to track join/leave updates for the hackclub minecraft server from <@{value}>"
                            );

                            if let Err(e) = query!(
                        "DELETE FROM subscriptions WHERE target_id = $1 AND subscriber_id = $2",
                        user.id,
                        value
                    )
                                .execute(&state.sqlx_pool)
                                .await
                            {
                                error!(error=?e, "An error occurred when deleting a subscription row from the database where the target_id was {} and the subscriber_id was {value}", user.id);
                                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                            }
                        }
                        ActionId::ApproveSubscription { value } => {
                            info!(target_id = %user.id, subscriber_id = %value, "User approved a subscription request");
                            dm_text = format!(
                                "<@{}> has approved your request to track their join/leave updates on the hackclub minecraft server. You will begin receiving updates when they next join/leave the server.",
                                user.id
                            );
                            completed_text = format!(
                                "Successfully notified <@{value}> that you have approved their request!"
                            );

                            if let Err(e) = query!(
                        "UPDATE subscriptions SET active = true WHERE target_id = $1 AND subscriber_id = $2",
                        user.id,
                        value
                    )
                                .execute(&state.sqlx_pool)
                                .await
                            {
                                error!(error=?e, "An error occurred when setting a subscription to active from the database where the target_id was {} and the subscriber_id was {value}", user.id);
                                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                            }
                        }
                        _ => unreachable!(),
                    }

                    let json = json!({
                        "users": value
                    });

                    let Ok(response) = state
                        .client
                        .post("https://slack.com/api/conversations.open")
                        .bearer_auth(state.bot_token.clone())
                        .json(&json)
                        .send()
                        .await
                    else {
                        error!(
                            "An error occurred when sending the request to open a conversation with user {value}"
                        );
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    };

                    let Ok(response_bytes) = response.bytes().await else {
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    };

                    let Ok(json): serde_json::error::Result<OpenConversationResponse> =
                        serde_json::from_slice(&response_bytes)
                    else {
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    };
                    if !json.ok {
                        error!("Slack conversations.open API returned a non-OK response");
                    }

                    let dm_channel = json.channel.id;

                    let json = json!({
                        "text": dm_text,
                        "channel": dm_channel
                    });

                    if send_and_log_on_failure_with_return(
                        state
                            .client
                            .post("https://slack.com/api/chat.postMessage")
                            .bearer_auth(state.bot_token.clone())
                            .json(&json),
                        "Sending the DM to reply with the decision",
                    )
                    .await
                    {
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }

                    if let Some(response_url) = response_url {
                        send_and_log_on_failure(
                            state
                                .client
                                .post(response_url)
                                .bearer_auth(state.bot_token.clone())
                                .json(&json!({
                                    "replace_original": true,
                                    "text": completed_text
                                })),
                            "Replacing the request DM with the completed message",
                        )
                        .await;
                    } else {
                        error!(
                            "URGENT ERROR. SLACK HAS CHANGED THEIR API RESPONSE SHAPE AND HAS NOT GIVEN A RESPONSE URL FOR RESPONDING TO THE BUTTON CLICK IN A MESSAGE. THIS HAS ORIGINATED FROM THE DECLINE/APPROVE SUBSCRIPTION BRANCH IN THE BLOCK ACTIONS MATCH STATEMENT."
                        );
                        return StatusCode::INTERNAL_SERVER_ERROR.into_response();
                    }

                    StatusCode::OK.into_response()
                }
                ActionId::Other => {
                    warn!("Received a block action's event that is not handled");
                    debug!("Event: {:#?}", payload.payload);
                    StatusCode::OK.into_response()
                }
            }
        }
        SlackInteraction::ViewSubmission { user, view } => {
            counter("slack.interaction", 1)
                .attribute("action", "view_submission")
                .capture();
            debug!(user_id = %user.id, "Handling ViewSubmission interaction");
            match view.callback_id {
                CallbackId::ConfigureSubsModal => (),
                CallbackId::InputNewSubUser => {
                    let view_state = match view.state {
                        Some(view_state) => view_state,
                        None => {
                            warn!("No view state");
                            return build_inline_error_response(
                                "users_select",
                                "Internal error / Slack's fault: No state found in the view submission payload",
                            );
                        }
                    };

                    let users_select_state = match view_state.values.get("users_select") {
                        Some(user_select_state) => user_select_state,
                        None => {
                            warn!("No users_select state");
                            return build_inline_error_response(
                                "users_select",
                                "Internal error / Slack's fault: No users_select state found in the view submission payload",
                            );
                        }
                    };

                    let target_user_id =
                        match users_select_state.get("users_select").map(|s| match s {
                            // this is cuz the object is first its block id: users_select and then its action id, which I aptly named... users_select
                            StateElements::UserSelect { selected_user } => selected_user,
                        }) {
                            Some(tui) => tui,
                            None => {
                                // This clause should not trigger because slack validates input fields are not empty before submission
                                warn!("No target user selected");
                                return build_inline_error_response(
                                    "users_select",
                                    "Please enter a user!",
                                );
                            }
                        };

                    let existing_subscription = query!(
                    "SELECT 1 as exists FROM subscriptions WHERE subscriber_id = $1 AND target_id = $2",
                    user.id,
                    target_user_id
                    )
                        .fetch_optional(&state.sqlx_pool)
                        .await;

                    let existing_subscription = match existing_subscription {
                        Ok(row) => row.is_some(),
                        Err(e) => {
                            error!("Failed to check for existing subscription: {e}");
                            return build_inline_error_response(
                                "users_select",
                                "Internal error: failed to check for existing subscription.",
                            );
                        }
                    };

                    if existing_subscription {
                        return build_inline_error_response(
                            "users_select",
                            "You are already subscribed to this user!",
                        );
                    }

                    let response_from_hc_api = match state
                        .client
                        .get(format!(
                            "https://api.mc.hackclub.com/player?slack={target_user_id}"
                        ))
                        .bearer_auth(state.hackclub_api_key.clone())
                        .send()
                        .await
                    {
                        Ok(response) => response,
                        Err(e) => {
                            error!(error=?e, slack=%target_user_id, user_who_triggered=%user.id, "An error occurred when trying to get the information from the hackclub api.");
                            return build_inline_error_response(
                                "users_select",
                                "Internal: An error occurred when fetching information from the hackclub API about this player. This means I couldn't get the minecraft username which I need.",
                            );
                        }
                    };

                    if response_from_hc_api.status().eq(&StatusCode::NOT_FOUND) {
                        return build_inline_error_response(
                            "users_select",
                            "This player does not play on the hackclub minecraft server. If this is incorrect please ask them to join the server, go through the linking flow and then try again. If this still persists please contact <@U08D22QNUVD>.",
                        );
                    } else if !response_from_hc_api.status().is_success() {
                        error!(status=%response_from_hc_api.status(), target=%target_user_id, trigger_user=%user.id, "The hackclub API returned an non-404 error.");
                        return build_inline_error_response(
                            "users_select",
                            "Internal: The API returned an error when fetching information for this player.",
                        );
                    }

                    let minecraft_player_data: Vec<MinecraftPlayerData> = match response_from_hc_api
                        .json()
                        .await
                    {
                        Ok(mpd) => mpd,
                        Err(e) => {
                            error!(error=?e, "An error occurred when converting the response to MinecraftPlayerData");
                            return build_inline_error_response(
                                "users_select",
                                "Internal: I couldn't convert the response from the hackclub API to MinecraftPlayerData. (This is irrelevant to you lol, just know it didnt work)",
                            );
                        }
                    };

                    let mut mc_usernames = Vec::new();

                    for block in minecraft_player_data {
                        mc_usernames.push(block.nick.name)
                    }

                    if let Err(e) =
                        query!("INSERT INTO users (slack_id, mc_usernames) VALUES ($1, $2) ON CONFLICT (slack_id) DO NOTHING", target_user_id, &mc_usernames)
                            .execute(&state.sqlx_pool)
                            .await
                    {
                        error!("Failed to insert user into database: {e}");
                        return build_inline_error_response(
                            "users_select",
                            "Internal error: Failed to insert user into database.",
                        );
                    }

                    if let Err(e) = query!(
                        "INSERT INTO subscriptions (subscriber_id, target_id) VALUES ($1, $2)",
                        user.id,
                        target_user_id
                    )
                    .execute(&state.sqlx_pool)
                    .await
                    {
                        error!("Failed to insert new subscription: {e}");
                        return build_inline_error_response(
                            "users_select",
                            "Internal error: failed to create new subscription in database.",
                        );
                    }

                    info!(
                        "Added new subscription ({}) for {}",
                        target_user_id, user.id
                    );

                    let target_user_id = target_user_id.clone();

                    tokio::spawn(async move {
                        let mut last_err = None;
                        for attempt in 1..=5 {
                            match send_request_dm(
                                &state.client,
                                &state.bot_token,
                                &target_user_id,
                                &user,
                            )
                            .await
                            {
                                Ok(_) => {
                                    debug!("Request DM delivered on attempt {attempt}");
                                    return;
                                }
                                Err(e) => {
                                    warn!(attempt, error = ?e, "Request DM failed, retrying");
                                    last_err = Some(e);
                                    tokio::time::sleep(Duration::from_secs(attempt)).await;
                                }
                            }
                        }
                        // All retries exhausted — roll back the row so the user can retry
                        error!(
                            subscriber_id = %user.id,
                            target_id = %target_user_id,
                            error = ?last_err,
                            "Approval DM failed after 5 attempts; removing subscription row for retry"
                        );
                        if let Err(e) = query!(
                            "DELETE FROM subscriptions WHERE subscriber_id = $1 AND target_id = $2",
                            user.id,
                            target_user_id
                        )
                        .execute(&state.sqlx_pool)
                        .await
                        {
                            // Worst case: stuck row. Log everything needed to clean up manually.
                            error!(
                                subscriber_id = %user.id,
                                target_id = %target_user_id,
                                error = ?e,
                                "MANUAL CLEANUP NEEDED: failed to remove stuck subscription row"
                            );
                        }
                    });
                }
            }
            StatusCode::OK.into_response()
        }
    }
}

async fn handle_mcrecipes(
    State(state): State<Arc<MCRecipesAppState>>,
    Json(payload): Json<SlackPayload>,
) -> Response<Body> {
    trace!("Received an event at /slack/mcrecipes");
    match payload {
        SlackPayload::UrlVerification { challenge } => {
            info!("Url Verification challenge received for MCRecipes");
            Json(json!({"challenge": challenge})).into_response()
        }

        SlackPayload::EventCallback { event } => {
            let user_id = if let Some(user_id) = event.user {
                user_id.clone()
            } else if let Some(bot_id) = event.bot_id {
                bot_id.clone()
            } else {
                error!(event=?event, "No user/bot id was found in this event.");
                return StatusCode::OK.into_response();
            };
            let cleaned_text = match event.text.strip_prefix("<@U0A5X0FV9V4>") {
                Some(str) => str.to_string(),
                None => return StatusCode::OK.into_response(),
            };
            if cleaned_text.is_empty() || cleaned_text.eq(" ") {
                return Json(
                    json!({"response_type": "ephemeral", "text": "You didn't enter a recipe!"}),
                )
                .into_response();
            }
            let (is_recipe_valid, assumption_text, recipe) = validate_recipe(
                &cleaned_text,
                &state.valid_recipes,
                &state.flipped_language_mappings,
            );
            if is_recipe_valid {
                match state.mpsc.try_send(Recipe {
                    item_name: recipe.clone(),
                    response_url: None,
                    channel_id: event.channel.clone(),
                    user_id: user_id.clone(),
                    thread_ts: Some(event.ts.clone()),
                    bot_token: state.bot_token.clone(),
                }) {
                    Ok(..) => {
                        info!(
                            "Started processing recipe for {recipe} from {user_id}"
                        );
                        send_message(
                            &json!({"channel": event.channel, "thread_ts": event.ts, "text": format!("This bot now uses <@U0B8ER7U1S5>'s backend for responses, as it has been replaced by it. You can also use /mcrecipe to get the recipe!\nGathering images and sewing 'em up, hang on a second! {assumption_text}")}),
                            &state.client,
                            &state.bot_token
                        ).await
                    }
                    Err(e) => {
                        error!("Error occurred sending task to generate image: {e}");
                        match e {
                            TrySendError::Full(..) => {
                                send_message(
                                    &json!({"channel": event.channel, "thread_ts": event.ts, "text": "Too many people have requested recipes at the moment. Please try again later."}),
                                    &state.client,
                                    &state.bot_token
                                ).await
                            },
                            _ => {
                                send_message(
                                    &json!({"channel": event.channel, "thread_ts": event.ts, "text": "An error occurred when trying to send the task to generate your image. Please try again!"}),
                                    &state.client,
                                    &state.bot_token
                                ).await
                            }
                        }
                    }
                }
            } else {
                warn!(
                    "User {user_id} tried to get recipe {recipe} but it was invalid"
                );
                send_message(
                    &json!({"channel": event.channel, "thread_ts": event.ts, "text": "Sorry your recipe was invalid."}),
                    &state.client,
                    &state.bot_token
                ).await
            }
        }
    }
}

async fn uptime() -> Json<Value> {
    Json(json!({"ok": true}))
}

async fn verify_slack_signature(
    State(secret): State<Arc<String>>,
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
    let slack_signature = match parts.headers.get("x-slack-signature") {
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

    let basestring = format!("v0:{timestamp}:{request_string}");

    let mut my_signature = HmacSha256::new_from_slice(secret.as_bytes())
        .expect("Whats the point of this error is HMAC can take a key of any size");
    my_signature.update(basestring.as_bytes());

    let slack_signature = match slack_signature.strip_prefix("v0=") {
        Some(str) => match hex::decode(str) {
            Ok(hex) => hex,
            Err(..) => {
                error!("Slack request signature not valid hex");
                return Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body("Slack request signature not valid hex".into())
                    .unwrap();
            }
        },
        None => {
            error!("Slack request signature didn't begin with v0=");
            return Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body("Slack request signature incorrect".into())
                .unwrap();
        }
    };

    match my_signature.verify_slice(&slack_signature) {
        Ok(..) => {
            counter("slack.signature.verification", 1)
                .attribute("result", "success")
                .capture();
            trace!("Slack signature verification successful");
            next.run(Request::from_parts(parts, Body::from(request_bytes)))
                .await
        }
        Err(e) => {
            counter("slack.signature.verification", 1)
                .attribute("result", "failure")
                .capture();
            warn!("Slack signature verification failed: {e}");
            Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body("Slack signature verification failed".into())
                .unwrap()
        }
    }
}

async fn send_message(json: &Value, client: &Client, bot_token: &str) -> Response<Body> {
    match client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(bot_token)
        .json(json)
        .send()
        .await
    {
        Ok(..) => StatusCode::OK.into_response(),
        Err(e) => {
            error!("Error occurred sending message: {e}");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

async fn legacy_send_message(json: &Value, client: &Client, bot_token: &str) -> anyhow::Result<()> {
    client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(bot_token)
        .json(json)
        .send()
        .await?;
    Ok(())
}

async fn send_request_dm(
    client: &Client,
    bot_token: &str,
    target_user_id: &str,
    user: &SlackUser,
) -> anyhow::Result<()> {
    trace!(subscriber_id = %user.id, target_user_id = %target_user_id, "Sending subscription request DM");
    let json = json!({
    "users": target_user_id
    });

    debug!(target_user_id = %target_user_id, "Opening DM channel via conversations.open");
    let response = client
        .post("https://slack.com/api/conversations.open")
        .bearer_auth(bot_token)
        .json(&json)
        .send()
        .await
        .context(format!(
            "An error occurred when opening a conversation with user {}",
            user.id
        ))?;

    trace!("Opened conversation with user {target_user_id}");

    let response_bytes = response
        .bytes()
        .await
        .context("Failed to parse response from slack for conversations.open")?;

    let json: OpenConversationResponse =
        serde_json::from_slice(&response_bytes).context("Failed to convert the bytes to json")?;
    if !json.ok {
        error!(target_user_id = %target_user_id, "Slack conversations.open returned non-OK response for subscription request DM");
        return Err(anyhow!(
            "Slack conversations.open API returned a non-OK response"
        ));
    }

    let channel = json.channel.id;
    debug!(target_user_id = %target_user_id, channel_id = %channel, "DM channel opened successfully");

    let blocks = json!([
    {
        "type": "header",
        "text": {
            "type": "plain_text",
            "text": "Request for Approval",
            "emoji": true
        }
    },
    {
        "type": "section",
        "text": {
            "type": "mrkdwn",
            "text": format!("<@{}> wants to subscribe to your join/leave updates for the Hack Club Minecraft Server.", user.id)
        }
    },
    {
        "type": "actions",
        "block_id": "approval_actions",
        "elements": [
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Approve",
                    "emoji": true
                },
                "style": "primary",
                "action_id": "approve_subscription",
                "value": user.id
            },
            {
                "type": "button",
                "text": {
                    "type": "plain_text",
                    "text": "Decline",
                    "emoji": true
                },
                "style": "danger",
                "action_id": "decline_subscription",
                "value": user.id
            }
        ]
    },
    {
        "type": "context",
        "elements": [
            {
            "type": "mrkdwn",
            "text": "They will be notified of your decision."
            }
        ]
    }
    ]);

    let message = json!({
    "channel": channel,
    "blocks": blocks
    });

    let res = client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(bot_token)
        .json(&message)
        .send()
        .await
        .context(format!(
            "An error occurred sending a message to the newly opened DM with {target_user_id}"
        ))?;

    let res_bytes = res
        .bytes()
        .await
        .context("Failed to parse response from slack for chat.postMessage")?;

    let json: OpenConversationResponse =
        serde_json::from_slice(&res_bytes).context("Failed to convert the bytes to json")?;
    if !json.ok {
        error!(
            target_user_id = %target_user_id,
            response = %String::from_utf8_lossy(&res_bytes),
            "Slack chat.postMessage API returned a non-OK response for the subscription request DM"
        );
        return Err(anyhow!(
            "Slack chat.postMessage API returned a non-OK response"
        ));
    }
    info!(subscriber_id = %user.id, target_user_id = %target_user_id, "Subscription approval request DM successfully delivered");
    Ok(())
}

async fn fetch_and_build_subs_modal_view(
    sqlx_pool: &sqlx::PgPool,
    page: i64,
    user_id: String,
) -> anyhow::Result<Value> {
    trace!(user_id = %user_id, page = page, "Fetching subscriptions from database for modal view");
    let subs = match query_as!(
        Subscription,
        "SELECT s.id, s.active, s.target_id, u.mc_usernames
FROM subscriptions AS s
JOIN users AS u ON s.target_id = u.slack_id
WHERE s.subscriber_id = $1
ORDER BY s.created_at
LIMIT 6 OFFSET $2",
        user_id,
        page * 5
    )
    .fetch_all(sqlx_pool)
    .await
    {
        Ok(subs) => {
            debug!(user_id = %user_id, page = page, count = subs.len(), "Subscriptions fetched from database");
            subs
        }
        Err(e) => {
            error!(error = ?e, user_id = %user_id, page = page, "Failed to fetch subscriptions from database");
            return Err(anyhow!("Failed to fetch subscriptions. Error: {e}"));
        }
    };

    let metadata = SubsPageMetadata { page, page_size: 5 };

    let mut blocks: Vec<Value> = Vec::new();

    blocks.push(json!({"type": "section", "text": {"type": "mrkdwn", "text": "Configure your update subscriptions below"}})); // Title
    blocks.push(json!({"type": "divider"}));
    blocks.push(json!({
        "type": "section",
        "text": {
        "type": "mrkdwn",
        "text": ":heavy_plus_sign: *Subscribe to a new person*"
    },
        "accessory": {
        "type": "button",
        "text": {
            "type": "plain_text",
            "text": "Subscribe",
            "emoji": true
        },
        "style": "primary",
        "action_id": "subscribe_new_person",
        "value": "click_me_123"
    }
    }));
    blocks.push(json!({"type": "divider"}));
    blocks.push(json!({
        "type": "header",
        "text": {
        "type": "plain_text",
        "text": "Current Subscriptions",
        "emoji": true
    }
    }));

    for subscription in &subs[..subs.len().min(6)] {
        let len = subscription.mc_usernames.len();
        let title = if len > 1 {
            let mut mcusers = String::new();

            let i = 1;

            for mcuser in &subscription.mc_usernames {
                if i != len {
                    let mcuser = format!("{mcuser}, ");
                    mcusers.push_str(&mcuser)
                } else {
                    mcusers.push_str(mcuser)
                }
            }

            format!("<@{}> *({})*", subscription.target_id, mcusers)
        } else {
            format!(
                "<@{}> *({})*",
                subscription.target_id, subscription.mc_usernames[0]
            )
        };
        blocks.push(json!({
            "type": "section",
            "text": {
            "type": "mrkdwn",
            "text": title
        },
            "accessory": {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "Remove",
                "emoji": true
            },
            "style": "danger",
            "action_id": "remove_subscription",
            "value": subscription.id.to_string(),
            "confirm": {
                "title": {
                    "type": "plain_text",
                    "text": "Remove subscription?"
                },
                "text": {
                    "type": "mrkdwn",
                    "text": format!("You'll stop receiving updates for {title}. They will be asked for approval again should you wish to subscribe to them again.")
                },
                "confirm": {
                    "type": "plain_text",
                    "text": "Remove"
                },
                "deny": {
                    "type": "plain_text",
                    "text": "Cancel"
                },
                "style": "danger"
            }
        }
        }));
        if subscription.active {
            blocks.push(json!({
                "type": "context",
                "elements": [
                {
                    "type": "mrkdwn",
                    "text": ":large_green_circle: Active"
                }
                ]
            }))
        } else {
            blocks.push(json!({
                "type": "context",
                "elements": [
                {
                    "type": "mrkdwn",
                    "text": ":large_yellow_circle: Pending approval"
                }
                ]
            }))
        }
    }

    blocks.push(json!({
        "type": "divider"
    }));

    let mut pagination_buttons: Vec<Value> = Vec::new();

    if page > 0 && !subs.is_empty() {
        pagination_buttons.push(json!(
            {
                "type": "button",
                "text": {
                "type": "plain_text",
                "text": "◀ Prev",
                "emoji": true
            },
                "action_id": "subs_page_prev",
                "value": "prev"
            }
        ));
    }

    if subs.len() > 5 {
        pagination_buttons.push(json!(
            {
                "type": "button",
                "text": {
                "type": "plain_text",
                "text": "Next ▶",
                "emoji": true
            },
                "action_id": "subs_page_next",
                "value": "next"
            }
        ));
    }

    if !pagination_buttons.is_empty() {
        blocks.push(json!({
            "type": "actions",
            "block_id": "subs_pagination",
            "elements": pagination_buttons
        }));
        blocks.push(json!({
            "type": "divider"
        }));
    }

    blocks.push(json!({
                            "type": "section",
                            "text": {
                            "type": "mrkdwn",
                            "text": "*What is this?*\n This feature allows you to subscribe to DM updates when the player you choose joins/leaves the hackclub minecraft server."
                        }
                        }));

    Ok(json!(
                    {
	"type": "modal",
	"callback_id": "configure_subs_modal",
	"private_metadata": serde_json::to_string(&metadata).context("Unable to serialise private metadata to string")?,
                        "submit": {
                            "type": "plain_text",
                            "text": "Done",
                            "emoji": true
                        },
                        "close": {
                            "type": "plain_text",
                            "text": "Exit",
                            "emoji": true
                        },
                        "title": {
                            "type": "plain_text",
                            "text": "Configure Update Subs"
                        },
                        "blocks": blocks}))
}

fn build_inline_error_response(field: &str, message: &str) -> Response<Body> {
    let mut message = message.to_string();
    if message.to_lowercase().contains("internal") {
        message.push_str(" Please try again. If this persists, please contact the @Akaalroop on slack or email akaal@akaalroop.com");
    }
    Json(json!({
        "response_action": "errors",
        "errors": {
            field: message
        }
    }))
    .into_response()
}

async fn send_and_log_on_failure(request: reqwest::RequestBuilder, context: &str) {
    match request.send().await {
        Ok(response) => match response.json::<Value>().await {
            Ok(body) => {
                if body.get("ok") != Some(&json!(true)) {
                    error!(context, response = ?body, "Slack API call reported failure");
                }
            }
            Err(e) => error!(context, error = ?e, "Failed to parse Slack response as JSON"),
        },
        Err(e) => error!(context, error = ?e, "Request to Slack failed"),
    }
}

/// This does the exact same as [send_and_log_on_failure] but returns **true** if an error occurred which allows the caller to handle the error.
async fn send_and_log_on_failure_with_return(
    request: reqwest::RequestBuilder,
    context: &str,
) -> bool {
    // error: yes/no
    match request.send().await {
        Ok(response) => match response.json::<Value>().await {
            Ok(body) => {
                if body.get("ok") != Some(&json!(true)) {
                    error!(context, response = ?body, "Slack API call reported failure");
                    true
                } else {
                    false
                }
            }
            Err(e) => {
                error!(context, error = ?e, "Failed to parse Slack response as JSON");
                true
            }
        },
        Err(e) => {
            error!(context, error = ?e, "Request to Slack failed");
            true
        }
    }
}
