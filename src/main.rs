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
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::{query, query_as};
use std::{collections::HashMap, env, sync::Arc};
use tokio::{
    net::TcpListener,
    sync::{mpsc, mpsc::error::TrySendError},
};
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
    mc_username: Option<String>,
}

#[derive(Clone)]
struct AppState {
    client: Client,
    bot_token: Arc<str>,
    mpsc: mpsc::Sender<Task>,
    valid_recipes: HashMap<String, usize>,
    sqlx_pool: sqlx::PgPool,
}

#[derive(Clone)]
struct MCRecipesAppState {
    client: Client,
    bot_token: Arc<str>,
    mpsc: mpsc::Sender<Task>,
    valid_recipes: HashMap<String, usize>,
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
#[serde(tag = "type")]
enum SlackInteraction {
    #[serde(rename = "block_actions")]
    BlockActions {
        user: SlackUser,
        view: SlackView,
        actions: Vec<SlackActions>,
        trigger_id: String,
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
    #[serde(other)]
    Other,
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

    let signing_secret =
        Arc::new(env::var("SLACK_SIGNING_SECRET").expect("MCBot Signing Secret NOT FOUND"));
    let mcrecipes_signing_secret = Arc::new(
        env::var("SLACK_SIGNING_SECRET_MCRECIPES").expect("MCRecipes Bot Token NOT FOUND"),
    );

    let (queue_input, mut queue_output) = mpsc::channel::<Task>(128);

    let mut client_jar_zip = fetch_client_jar(&client).await;
    let mut recipe_data = RecipeData::default();
    info!("Now adding recipes, items & tags to memory");
    recipe_data
        .fetch_recipes_and_more(&mut client_jar_zip)
        .await
        .expect("Failed to fetch recipes");

    let sqlx_pool = sqlx::Pool::connect(&env::var("DATABASE_URL").expect("DATABASE_URL NOT FOUND"))
        .await
        .expect("Failed to connect to database");

    let state = Arc::new(AppState {
        client: Client::new(),
        bot_token: bot_token.into(),
        mpsc: queue_input.clone(),
        valid_recipes: recipe_data.valid_recipes.clone(),
        sqlx_pool: sqlx_pool.clone(),
    });

    let mcrecipes_state = Arc::new(MCRecipesAppState {
        client: Client::new(),
        bot_token: mcrecipes_bot_token.into(),
        mpsc: queue_input,
        valid_recipes: recipe_data.valid_recipes.clone(),
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
                Subscriptions {
                    user_id,
                    trigger_id,
                    bot_token,
                } => {
                    let modal_view = match fetch_and_build_subs_modal_view(&sqlx_pool, 0, user_id)
                        .await
                    {
                        Ok(view) => view,
                        Err(e) => {
                            error!(error = ?e, "An error occurred fetching and building the modal view");
                            continue;
                            // TODO: Tell the user somehow?
                        }
                    };

                    let payload = json!({
                        "trigger_id": trigger_id,
                        "view": modal_view
                    });
                    let _ = client
                        .post("https://slack.com/api/views.open")
                        .bearer_auth(bot_token)
                        .json(&payload)
                        .send()
                        .await; // TODO: Handle properly
                }
            }
        }
    });

    let mcbot_router = axum::Router::new()
        .route("/slack/events", post(handle_event))
        .route("/slack/commands", post(handle_command))
        .route("/slack/interactions", post(handle_interactions))
        .route_layer(middleware::from_fn_with_state(
            signing_secret,
            verify_slack_signature,
        ))
        .with_state(state);

    let mcrecipes_router = axum::Router::new()
        .route("/slack/mcrecipes", post(handle_mcrecipes))
        .route_layer(middleware::from_fn_with_state(
            mcrecipes_signing_secret,
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
) -> Json<Value> {
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
) -> Response {
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
                )
                .into_response();
            }
            let (is_recipe_valid, assumption_text, recipe) =
                validate_recipe(payload.text, &state.valid_recipes);
            if is_recipe_valid {
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
    let interaction: SlackInteraction = match serde_json::from_str(&payload.payload) {
        Ok(i) => i,
        Err(e) => {
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
        } => {
            let actions = &actions[0];
            let private_metadata: Option<SubsPageMetadata>;
            let mut page: i64 = 0;

            #[allow(clippy::single_match)]
            match view.callback_id {
                CallbackId::ConfigureSubsModal => {
                    private_metadata = if let Some(private_metadata) = view.private_metadata {
                        let priv_metadata: Result<SubsPageMetadata, serde_json::error::Error> =
                            serde_json::from_str(&private_metadata);
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
            match &actions.action_id {
                ActionId::RemoveSubscription { value } => {
                    let id = match value.parse::<i64>() {
                        Ok(id) => id,
                        Err(..) => {
                            error!("Failed to parse id as i64 (id = {})", value);
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
                            let _ = state
                                .client
                                .post("https://slack.com/api/views.update")
                                .bearer_auth(state.bot_token.clone())
                                .json(&json)
                                .send()
                                .await; //TODO: Handle properly
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
                }
                /* TODO: Do all the other TODO's
                 TODO: Add the entered user to the db - both subscriptions and users (obvs check it dont already exist for this user in subs)
                 TODO: Send the DM obviously
                 TODO: Verify the entered mc_username exists and is a valid one (Mojang API) and say only Java names supported at the moment
                 TODO: Add to database, accept/decline, and username if provided (might not be needed if in db already)
                 TODO: Send DM to subscriber that they accepted/declined
                 TODO: Patrol #minecraft-bridge and send DM's (hammer the index remember)
                */
                ActionId::SubscribeNewPerson => {
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

                    match state
                        .client
                        .post("https://slack.com/api/views.push")
                        .bearer_auth(state.bot_token.clone())
                        .json(&json)
                        .send()
                        .await
                    {
                        Ok(..) => (),
                        Err(e) => {
                            error!(error=?e, "An error occurred pushing an input view"); //TODO: Handle properly
                        }
                    }

                    StatusCode::OK.into_response()
                }
                ActionId::SubsPageNext | ActionId::SubsPagePrev => {
                    match actions.action_id {
                        ActionId::SubsPageNext => {
                            page += 1;
                        }
                        ActionId::SubsPagePrev => {
                            page -= 1;
                        }
                        _ => (), // This is literally impossible anyway
                    }
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
                    let _ = state
                        .client
                        .post("https://slack.com/api/views.update")
                        .bearer_auth(state.bot_token.clone())
                        .json(&json)
                        .send()
                        .await; //TODO: Handle properly
                    StatusCode::OK.into_response()
                }
                ActionId::UserSelect { selected_user } => {
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

                    let alert_block = json!({
                        "type": "alert",
                        "text": {
                            "type": "mrkdwn",
                            "text": "*Error*: You are already subscribed to this person",
                            "verbatim": false
                        },
                        "level": "error"
                    });

                    let already_error_block_present = view
                        .blocks
                        .iter()
                        .any(|v| v.get("type") == Some(&json!("alert")));

                    if existing_subscription && !already_error_block_present {
                        view.blocks.insert(0, alert_block);
                    } else if !existing_subscription && already_error_block_present {
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

                    let _ = state
                        .client
                        .post("https://slack.com/api/views.update")
                        .bearer_auth(state.bot_token.clone())
                        .json(&json)
                        .send()
                        .await; //TODO: Handle properly

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

                    if let Err(e) =
                        query!("INSERT INTO users (slack_id) VALUES ($1)", target_user_id)
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
                }
            }
            StatusCode::OK.into_response()
        }
    }
}

async fn handle_mcrecipes(
    State(state): State<Arc<MCRecipesAppState>>,
    Json(payload): Json<SlackPayload>,
) -> Json<Value> {
    trace!("Received an event at /slack/mcrecipes");
    match payload {
        SlackPayload::UrlVerification { challenge } => {
            info!("Url Verification challenge received for MCRecipes");
            Json(json!({"challenge": challenge}))
        }

        SlackPayload::EventCallback { event } => {
            let cleaned_text = event
                .text
                .strip_prefix("<@U0A5X0FV9V4>")
                .unwrap()
                .to_string();
            if cleaned_text.is_empty() || cleaned_text.eq(" ") {
                return Json(
                    json!({"response_type": "ephemeral", "text": "You didn't enter a recipe!"}),
                );
            }
            let (is_recipe_valid, assumption_text, recipe) =
                validate_recipe(cleaned_text, &state.valid_recipes);
            if is_recipe_valid {
                match state.mpsc.try_send(Recipe {
                    item_name: recipe.clone(),
                    response_url: None,
                    channel_id: event.channel.clone(),
                    user_id: event.user.clone(),
                    thread_ts: Some(event.ts.clone()),
                    bot_token: state.bot_token.clone(),
                }) {
                    Ok(..) => {
                        info!(
                            "Started processing recipe for {} from {}",
                            recipe, event.user
                        );
                        match send_message(
                            &json!({"channel": event.channel, "thread_ts": event.ts, "text": format!("This bot now uses <@U0B8ER7U1S5>'s backend for responses, as it has been replaced by it. You can also use /mcrecipe to get the recipe!\nGathering images and sewing 'em up, hang on a second! {assumption_text}")}),
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
                warn!(
                    "User {} tried to get recipe {recipe} but it was invalid",
                    event.user
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

async fn uptime() -> Json<Value> {
    Json(json!({"ok": true}))
}

//noinspection RsUnresolvedPath (RustRover seems to not be able to find the new_from_slice function in scope so supressed)
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

async fn send_message(json: &Value, client: &Client, bot_token: &str) -> anyhow::Result<()> {
    client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(bot_token)
        .json(json)
        .send()
        .await?;
    Ok(())
}

async fn fetch_and_build_subs_modal_view(
    sqlx_pool: &sqlx::PgPool,
    page: i64,
    user_id: String,
) -> anyhow::Result<Value> {
    let subs = match query_as!(
        Subscription,
        "SELECT s.id, s.active, s.target_id, u.mc_username
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
        Ok(subs) => subs,
        Err(e) => {
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
        let title = if let Some(mc_user) = &subscription.mc_username {
            format!("<@{}> *({})*", subscription.target_id, mc_user)
        } else {
            format!("<@{}>", subscription.target_id)
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
                    "text": ":large_yellow_circle: Pending acceptance"
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

    blocks.push(json!({
        "type": "actions",
        "block_id": "subs_pagination",
        "elements": pagination_buttons
    }));
    blocks.push(json!({
        "type": "divider"
    }));

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
