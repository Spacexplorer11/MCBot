use super::{AppState, OpenConversationResponse, WhereIOnlyNeedTheIdField};
use crate::helpers::{
    errors::{
        build_inline_error_response, send_and_log_on_failure, send_and_log_on_failure_with_return,
    },
    logging,
    messages::send_request_dm,
    subs_modal_builder::{SubsPageMetadata, fetch_and_build_subs_modal_view},
};
use axum::{
    extract::{Form, State},
    http::StatusCode,
    response::{IntoResponse, Response},
};
use sentry::{integrations::anyhow::capture_anyhow, metrics::counter};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::query;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, trace, warn};

#[derive(Deserialize)]
pub struct MinecraftPlayerData {
    pub nick: MinecraftPlayerNick,
}

#[derive(Deserialize)]
pub struct MinecraftPlayerNick {
    pub name: String,
}

#[derive(Deserialize)]
pub enum CallbackId {
    #[serde(rename = "configure_subs_modal")]
    ConfigureSubsModal,
    #[serde(rename = "input_new_sub_user")]
    InputNewSubUser,
}

#[derive(Deserialize, Serialize)]
struct UsersSelectMetadata {
    hash: String,
    view_id: String,
    page: i64,
}

#[derive(Deserialize)]
pub struct SlackInteractionPayload {
    payload: String,
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
#[serde(tag = "type")]
enum SlackInteraction {
    #[serde(rename = "block_actions")]
    BlockActions {
        user: WhereIOnlyNeedTheIdField,
        view: Option<SlackView>,
        actions: Vec<SlackActions>,
        trigger_id: String,
        response_url: Option<String>,
    },
    #[serde(rename = "view_submission")]
    ViewSubmission {
        user: WhereIOnlyNeedTheIdField,
        view: SlackView,
    },
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

pub async fn handle_interactions(
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
                ActionId::SubscribeNewPerson => {
                    if let Some(view) = view {
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
                                "text": "How this works: After selecting and confirming the user, a DM will be sent which asks for approval from the user you selected. Their decision will be relayed back to you via a DM and if it's a yes, you will automatically start receiving DM updates when they join/leave the hackclub minecraft server."
                            },
                            "block_id": "users_select"
                        });

                        let metadata = UsersSelectMetadata {
                            hash: view.hash,
                            view_id: view.id,
                            page,
                        };

                        let metadata_as_str = serde_json::to_string(&metadata).unwrap_or_else(|error| {
                            warn!(?error, "An error occurred when converting the UsersSelectMetadata to string");
                            "".to_string()
                        });

                        let json = json!({
                            "view": {
                                "type": "modal",
                                "private_metadata": metadata_as_str,
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
                    } else {
                        error!(
                            "SLACK'S API HAS CLEARLY CHANGED AS A VIEW IS EXPECTED HERE. (WHEN INITIATING THE NEW PERSON SUBSCRIPTION FLOW)"
                        );
                        StatusCode::INTERNAL_SERVER_ERROR.into_response()
                    }
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
                        debug!(user_id = %user.id, %selected_user, "User selected a target for subscription");
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

                        let hackclub_start = std::time::Instant::now();
                        let non_player = match state
                            .client
                            .get(format!(
                                "https://api.mc.hackclub.com/player?slack={selected_user}"
                            ))
                            .bearer_auth(state.hackclub_api_key.clone())
                            .send()
                            .await
                        {
                            Ok(response) => {
                                logging::record_hackclub_api_metric(
                                    hackclub_start,
                                    &response.status().as_u16().to_string(),
                                );
                                response.status().eq(&StatusCode::NOT_FOUND)
                            }
                            Err(e) => {
                                logging::record_hackclub_api_metric(
                                    hackclub_start,
                                    "request_error",
                                );
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

                        view.blocks
                            .retain(|v| v.get("type") != Some(&json!("alert")));

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
                            view.blocks.insert(0, alert_block);
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
                                "private_metadata": view.private_metadata.clone().unwrap_or_default(),
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
                    debug!(%user.id, subscriber_id = %value, action = ?actions.action_id, "Processing subscription approval/decline action");
                    if query!(
                        "SELECT * FROM subscriptions WHERE target_id = $1 AND subscriber_id = $2",
                        user.id,
                        value
                    )
                    .fetch_optional(&state.sqlx_pool)
                    .await
                    .is_ok_and(|result| result.is_none())
                    {
                        return if let Some(response_url) = response_url {
                            send_and_log_on_failure(
                                state
                                    .client
                                    .post(response_url)
                                    .bearer_auth(state.bot_token.clone())
                                    .json(&json!({
                                    "replace_original": true,
                                    "text": format!("This request has expired. Please ask <@{}> to send a request again.", user.id)
                                })),
                                "Replacing the request DM with the completed message",
                            )
                                .await;
                            StatusCode::OK.into_response()
                        } else {
                            error!(
                                "URGENT ERROR. SLACK HAS CHANGED THEIR API RESPONSE SHAPE AND HAS NOT GIVEN A RESPONSE URL FOR RESPONDING TO THE BUTTON CLICK IN A MESSAGE. THIS HAS ORIGINATED FROM THE DECLINE/APPROVE SUBSCRIPTION BRANCH IN THE BLOCK ACTIONS MATCH STATEMENT."
                            );
                            StatusCode::INTERNAL_SERVER_ERROR.into_response()
                        };
                    }
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

                    let hackclub_start = std::time::Instant::now();
                    let response_from_hc_api = match state
                        .client
                        .get(format!(
                            "https://api.mc.hackclub.com/player?slack={target_user_id}"
                        ))
                        .bearer_auth(state.hackclub_api_key.clone())
                        .send()
                        .await
                    {
                        Ok(response) => {
                            logging::record_hackclub_api_metric(
                                hackclub_start,
                                &response.status().as_u16().to_string(),
                            );
                            response
                        }
                        Err(e) => {
                            logging::record_hackclub_api_metric(hackclub_start, "request_error");
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
                        error!(status=%response_from_hc_api.status(), %target_user_id, trigger_user=%user.id, "The hackclub API returned an non-404 error.");
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
                                    let mut metadata: Option<
                                        Result<UsersSelectMetadata, serde_json::Error>,
                                    > = None;
                                    if let Some(ref private_metadata) = view.private_metadata {
                                        metadata = Some(serde_json::from_str(private_metadata));
                                    } else {
                                        warn!(
                                            "The private metadata could not be found when submitting the input new user modal so therefore the underlying subscriptions view could not be updated"
                                        );
                                    }
                                    if let Some(Ok(metadata)) = metadata {
                                        let modal_view = match fetch_and_build_subs_modal_view(
                                            &state.sqlx_pool,
                                            metadata.page,
                                            user.id.clone(),
                                        )
                                        .await
                                        {
                                            Ok(view) => {
                                                counter("subscriptions.modal", 1)
                                                    .attribute("result", "opened")
                                                    .capture();
                                                trace!(
                                                    "Subscriptions modal view built successfully"
                                                );
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

                                        let json = json!({
                                            "hash": metadata.hash,
                                            "view_id": metadata.view_id,
                                            "view": modal_view
                                        });

                                        send_and_log_on_failure(state.client.post("https://slack.com/api/views.update")
                                                                    .bearer_auth(state.bot_token.clone())
                                                                    .json(&json), "Updating the subscriptions modal view after submission of the new subscription modal").await
                                    } else if let Some(Err(error)) = metadata {
                                        warn!(
                                            ?error,
                                            "The private metadata could not be parsed when submitting the input new user modal so therefore the underlying subscriptions view could not be updated"
                                        );
                                    }
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
                            %target_user_id,
                            ?last_err,
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
