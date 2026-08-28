use crate::helpers::logging;
use axum::{
    Json,
    body::Body,
    response::{IntoResponse, Response},
};
use serde_json::{Value, json};
use tracing::error;

pub fn build_inline_error_response(field: &str, message: &str) -> Response<Body> {
    let mut message = message.to_string();
    if message.to_lowercase().contains("internal") {
        message.push_str(" Please try again. If this persists, please contact <@U08D22QNUVD> or email akaal@akaalroop.com");
    }
    Json(json!({
        "response_action": "errors",
        "errors": {
            field: message
        }
    }))
        .into_response()
}

pub async fn send_and_log_on_failure(request: reqwest::RequestBuilder, context: &str) {
    let (client, request) = request.build_split();
    let request = match request {
        Ok(request) => request,
        Err(e) => {
            error!(context, error = ?e, "Failed to build Slack request");
            return;
        }
    };
    let endpoint = request.url().path().trim_start_matches("/api/").to_string();
    let start = std::time::Instant::now();
    match client.execute(request).await {
        Ok(response) => {
            logging::record_slack_api_metric(
                &endpoint,
                start,
                &response.status().as_u16().to_string(),
            );
            match response.json::<Value>().await {
                Ok(body) => {
                    if body.get("ok") != Some(&json!(true)) {
                        error!(context, response = ?body, "Slack API call reported failure");
                    }
                }
                Err(e) => error!(context, error = ?e, "Failed to parse Slack response as JSON"),
            }
        }
        Err(e) => {
            logging::record_slack_api_metric(&endpoint, start, "request_error");
            error!(context, error = ?e, "Request to Slack failed");
        }
    }
}

/// This does the exact same as [send_and_log_on_failure] but returns **true** if an error occurred which allows the caller to handle the error.
pub async fn send_and_log_on_failure_with_return(
    request: reqwest::RequestBuilder,
    context: &str,
) -> bool {
    let (client, request) = request.build_split();
    let request = match request {
        Ok(request) => request,
        Err(e) => {
            error!(context, error = ?e, "Failed to build Slack request");
            return true;
        }
    };
    let endpoint = request.url().path().trim_start_matches("/api/").to_string();
    let start = std::time::Instant::now();
    // error: yes/no
    match client.execute(request).await {
        Ok(response) => {
            logging::record_slack_api_metric(
                &endpoint,
                start,
                &response.status().as_u16().to_string(),
            );
            match response.json::<Value>().await {
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
            }
        }
        Err(e) => {
            logging::record_slack_api_metric(&endpoint, start, "request_error");
            error!(context, error = ?e, "Request to Slack failed");
            true
        }
    }
}
