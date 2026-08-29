use crate::{
    handlers::{OpenConversationResponse, WhereIOnlyNeedTheIdField},
    helpers::logging,
};
use anyhow::{Context, anyhow};
use axum::{
    body::Body,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use reqwest::Client;
use sentry::metrics::counter;
use serde_json::{Value, json};
use tracing::{debug, error, info, trace};

pub async fn send_message(json: &Value, client: &Client, bot_token: &str) -> Response<Body> {
    let start = std::time::Instant::now();
    match client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(bot_token)
        .json(json)
        .send()
        .await
    {
        Ok(..) => {
            logging::record_slack_api_metric("chat.postMessage", start, "ok");
            StatusCode::OK.into_response()
        }
        Err(error) => {
            logging::record_slack_api_metric("chat.postMessage", start, "request_error");
            error!(?error, "Error occurred sending message");
            StatusCode::INTERNAL_SERVER_ERROR.into_response()
        }
    }
}

pub async fn legacy_send_message(
    json: &Value,
    client: &Client,
    bot_token: &str,
) -> anyhow::Result<()> {
    let start = std::time::Instant::now();
    let result = client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(bot_token)
        .json(json)
        .send()
        .await;
    match result {
        Ok(..) => {
            logging::record_slack_api_metric("chat.postMessage", start, "ok");
            Ok(())
        }
        Err(e) => {
            logging::record_slack_api_metric("chat.postMessage", start, "request_error");
            Err(e.into())
        }
    }
}

pub async fn send_dm(
    client: &Client,
    bot_token: &str,
    target_user_id: &str,
    text: &str,
) -> anyhow::Result<()> {
    let result = send_dm_impl(client, bot_token, target_user_id, text).await;
    counter("dm.sent", 1)
        .attribute("kind", "update")
        .attribute("result", if result.is_ok() { "success" } else { "error" })
        .capture();
    result
}

#[tracing::instrument(name = "dm_sending_pipeline", skip(client, bot_token))]
pub async fn send_dm_impl(
    client: &Client,
    bot_token: &str,
    target_user_id: &str,
    text: &str,
) -> anyhow::Result<()> {
    trace!(%target_user_id, "Sending DM");
    let json = json!({
    "users": target_user_id
    });

    debug!(target_user_id = %target_user_id, "Opening DM channel via conversations.open");
    let open_start = std::time::Instant::now();
    let response = client
        .post("https://slack.com/api/conversations.open")
        .bearer_auth(bot_token)
        .json(&json)
        .send()
        .await
        .context(format!(
            "An error occurred when opening a conversation with user {target_user_id}",
        ));
    let response = match response {
        Ok(response) => {
            logging::record_slack_api_metric("conversations.open", open_start, "ok");
            response
        }
        Err(e) => {
            logging::record_slack_api_metric("conversations.open", open_start, "request_error");
            return Err(e);
        }
    };

    trace!("Opened conversation with user {target_user_id}");

    let json: OpenConversationResponse = response
        .json()
        .await
        .context("Failed to parse the conversations.open response to json")?;
    if !json.ok {
        error!(%target_user_id, "Slack conversations.open returned non-OK response for subscription request DM");
        return Err(anyhow!(
            "Slack conversations.open API returned a non-OK response"
        ));
    }

    let channel = json.channel.id;
    debug!(%target_user_id, channel_id = %channel, "DM channel opened successfully");

    let message = json!({
    "channel": channel,
    "text": text
    });

    let message_start = std::time::Instant::now();
    let res = client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(bot_token)
        .json(&message)
        .send()
        .await
        .context(format!(
            "An error occurred sending a message to the newly opened DM with {target_user_id}"
        ));
    let res = match res {
        Ok(res) => {
            logging::record_slack_api_metric("chat.postMessage", message_start, "ok");
            res
        }
        Err(e) => {
            logging::record_slack_api_metric("chat.postMessage", message_start, "request_error");
            return Err(e);
        }
    };

    let json: Value = res
        .json()
        .await
        .context("Failed to convert the chat.postMessage response to json")?;

    if json.get("ok") != Some(&json!(true)) {
        error!(
            %target_user_id,
            ?json,
            "Slack chat.postMessage API returned a non-OK response for the subscription request DM"
        );
        return Err(anyhow!(
            "Slack chat.postMessage API returned a non-OK response"
        ));
    }
    info!(%target_user_id, "DM successfully delivered");
    Ok(())
}

pub async fn send_request_dm(
    client: &Client,
    bot_token: &str,
    target_user_id: &str,
    user: &WhereIOnlyNeedTheIdField,
) -> anyhow::Result<()> {
    let result = send_request_dm_impl(client, bot_token, target_user_id, user).await;
    counter("dm.sent", 1)
        .attribute("kind", "approval_request")
        .attribute("result", if result.is_ok() { "success" } else { "error" })
        .capture();
    result
}

pub async fn send_request_dm_impl(
    client: &Client,
    bot_token: &str,
    target_user_id: &str,
    user: &WhereIOnlyNeedTheIdField,
) -> anyhow::Result<()> {
    trace!(subscriber_id = %user.id, %target_user_id, "Sending subscription request DM");
    let json = json!({
    "users": target_user_id
    });

    debug!(%target_user_id, "Opening DM channel via conversations.open");
    let open_start = std::time::Instant::now();
    let response = client
        .post("https://slack.com/api/conversations.open")
        .bearer_auth(bot_token)
        .json(&json)
        .send()
        .await
        .context(format!(
            "An error occurred when opening a conversation with user {}",
            user.id
        ));
    let response = match response {
        Ok(response) => {
            logging::record_slack_api_metric("conversations.open", open_start, "ok");
            response
        }
        Err(e) => {
            logging::record_slack_api_metric("conversations.open", open_start, "request_error");
            return Err(e);
        }
    };

    trace!("Opened conversation with user {target_user_id}");

    let json: OpenConversationResponse = response
        .json()
        .await
        .context("Failed to parse the conversations.open response to json")?;
    if !json.ok {
        error!(%target_user_id, "Slack conversations.open returned non-OK response for subscription request DM");
        return Err(anyhow!(
            "Slack conversations.open API returned a non-OK response"
        ));
    }

    let channel = json.channel.id;
    debug!(%target_user_id, channel_id = %channel, "DM channel opened successfully");

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

    let message_start = std::time::Instant::now();
    let res = client
        .post("https://slack.com/api/chat.postMessage")
        .bearer_auth(bot_token)
        .json(&message)
        .send()
        .await
        .context(format!(
            "An error occurred sending a message to the newly opened DM with {target_user_id}"
        ));
    let res = match res {
        Ok(res) => {
            logging::record_slack_api_metric("chat.postMessage", message_start, "ok");
            res
        }
        Err(e) => {
            logging::record_slack_api_metric("chat.postMessage", message_start, "request_error");
            return Err(e);
        }
    };

    let json: Value = res
        .json()
        .await
        .context("Failed to convert the chat.postMessage response to json")?;

    if json.get("ok") != Some(&json!(true)) {
        error!(
            %target_user_id,
            ?json,
            "Slack chat.postMessage API returned a non-OK response for the subscription request DM"
        );
        return Err(anyhow!(
            "Slack chat.postMessage API returned a non-OK response"
        ));
    }
    info!(subscriber_id = %user.id, %target_user_id, "Subscription approval request DM successfully delivered");
    Ok(())
}
