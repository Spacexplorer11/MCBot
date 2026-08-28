use super::AppState;
use crate::Task::{Recipe, Subscriptions};
use crate::handlers::recipes::validate_recipe;
use crate::capture_task_context;
use axum::{
    Form, Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use sentry::metrics::counter;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{error, info, trace, warn};

#[derive(Deserialize)]
pub struct SlackSlashCommand {
    pub command: String,
    pub text: String,
    pub channel_id: String,
    pub user_id: String,
    pub response_url: String,
    pub trigger_id: String,
}

pub async fn handle_command(
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
                let (hub, parent_span) = capture_task_context();
                match state.mpsc.try_send(Recipe {
                    item_name: recipe.clone(),
                    response_url: Some(payload.response_url),
                    channel_id: payload.channel_id,
                    user_id: payload.user_id.clone(),
                    thread_ts: None,
                    bot_token: state.bot_token.clone(),
                    queued_at: std::time::Instant::now(),
                    hub,
                    parent_span,
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
                    Err(error) => {
                        counter("task.queue.full", 1)
                            .attribute("task", "recipe")
                            .capture();
                        error!(?error, "Error occurred sending task to generate image");
                        match error {
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
                    user_id = %payload.user_id,
                    %recipe,
                    "User tried to get an invalid recipe"
                );
                Json(
                    json!({"response_type": "ephemeral", "text": format!("Sorry your recipe {recipe} was invalid.")}),
                ).into_response()
            }
        }
        "/mc-subs-config" => {
            let (hub, parent_span) = capture_task_context();
            match state.mpsc.try_send(Subscriptions {
                user_id: payload.user_id.clone(),
                trigger_id: payload.trigger_id,
                bot_token: state.bot_token.clone(),
                queued_at: std::time::Instant::now(),
                hub,
                parent_span,
            }) {
                Ok(..) => {
                    info!("Configuring updates for {}", payload.user_id);
                    StatusCode::OK.into_response()
                }
                Err(error) => {
                    counter("task.queue.full", 1)
                        .attribute("task", "subscriptions")
                        .capture();
                    error!(?error, "Error occurred sending task to generate image");
                    match error {
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