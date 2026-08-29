use super::MCRecipesAppState;
use crate::Task::Recipe;
use crate::capture_task_context;
use crate::handlers::events::SlackPayload;
use crate::handlers::recipes::validate_recipe;
use crate::helpers::messages::send_message;
use axum::{
    Json,
    body::Body,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use serde_json::json;
use std::sync::Arc;
use tokio::sync::mpsc::error::TrySendError;
use tracing::{error, info, trace, warn};

pub async fn handle_mcrecipes(
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
                let (hub, parent_span) = capture_task_context();
                match state.mpsc.try_send(Recipe {
                    item_name: recipe.clone(),
                    response_url: None,
                    channel_id: event.channel.clone(),
                    user_id: user_id.clone(),
                    thread_ts: Some(event.ts.clone()),
                    bot_token: state.bot_token.clone(),
                    queued_at: std::time::Instant::now(),
                    hub,
                    parent_span,
                }) {
                    Ok(..) => {
                        info!("Started processing recipe for {recipe} from {user_id}");
                        send_message(
                            &json!({"channel": event.channel, "thread_ts": event.ts, "text": format!("This bot now uses <@U0B8ER7U1S5>'s backend for responses, as it has been replaced by it. You can also use /mcrecipe to get the recipe!\nGathering images and sewing 'em up, hang on a second! {assumption_text}")}),
                            &state.client,
                            &state.bot_token
                        ).await
                    }
                    Err(error) => {
                        error!(?error, "Error occurred sending task to generate image");
                        match error {
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
                warn!(%user_id, %recipe, "User tried to get an invalid recipe");
                send_message(
                    &json!({"channel": event.channel, "thread_ts": event.ts, "text": "Sorry your recipe was invalid."}),
                    &state.client,
                    &state.bot_token
                ).await
            }
        }
    }
}
