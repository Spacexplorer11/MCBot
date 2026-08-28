use crate::HmacSha256;
use axum::{
    body::Body,
    extract::{Request, State},
    http::StatusCode,
    middleware::Next,
    response::Response,
};
use chrono::Utc;
use hmac::{KeyInit, Mac};
use sentry::metrics::counter;
use std::sync::Arc;
use tracing::{error, trace, warn};

pub async fn verify_slack_signature(
    State(secret): State<Arc<String>>,
    request: Request,
    next: Next,
) -> Response {
    trace!("Received request to verify signature");
    let (parts, body) = request.into_parts();
    
    let request_bytes = match axum::body::to_bytes(body, 1024 * 16).await {
        Ok(bytes) => bytes,
        Err(error) => {
            counter("slack.signature.verification", 1)
                .attribute("result", "failure")
                .attribute("reason", "body_read_error")
                .capture();
            error!(?error, "Failed to read request body");
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
                    counter("slack.signature.verification", 1)
                        .attribute("result", "failure")
                        .attribute("reason", "invalid_timestamp")
                        .capture();
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
                    counter("slack.signature.verification", 1)
                        .attribute("result", "failure")
                        .attribute("reason", "invalid_timestamp")
                        .capture();
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
                counter("slack.signature.verification", 1)
                    .attribute("result", "failure")
                    .attribute("reason", "timestamp_expired")
                    .capture();
                error!("Slack request timestamp is too old");
                return Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body("Slack request timestamp is too old".into())
                    .unwrap();
            }
            ts.to_string()
        }
        None => {
            counter("slack.signature.verification", 1)
                .attribute("result", "failure")
                .attribute("reason", "missing_timestamp_header")
                .capture();
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
                counter("slack.signature.verification", 1)
                    .attribute("result", "failure")
                    .attribute("reason", "invalid_signature_format")
                    .capture();
                error!("Slack signature header not a string");
                return Response::builder()
                    .status(StatusCode::UNAUTHORIZED)
                    .body("Slack signature header not a string".into())
                    .unwrap();
            }
        },
        None => {
            counter("slack.signature.verification", 1)
                .attribute("result", "failure")
                .attribute("reason", "missing_signature_header")
                .capture();
            error!("Slack signature header not found");
            return Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .body("Slack signature header not found".into())
                .unwrap();
        }
    };
    
    let request_string = match str::from_utf8(request_bytes.as_ref()) {
        Ok(s) => s,
        Err(error) => {
            counter("slack.signature.verification", 1)
                .attribute("result", "failure")
                .attribute("reason", "invalid_body_encoding")
                .capture();
            error!(?error, "Slack request body not valid utf-8");
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
                counter("slack.signature.verification", 1)
                    .attribute("result", "failure")
                    .attribute("reason", "invalid_signature_format")
                    .capture();
                error!("Slack request signature not valid hex");
                return Response::builder()
                    .status(StatusCode::FORBIDDEN)
                    .body("Slack request signature not valid hex".into())
                    .unwrap();
            }
        },
        None => {
            counter("slack.signature.verification", 1)
                .attribute("result", "failure")
                .attribute("reason", "invalid_signature_format")
                .capture();
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
                .attribute("reason", "success")
                .capture();
            trace!("Slack signature verification successful");
            next.run(Request::from_parts(parts, Body::from(request_bytes)))
                .await
        }
        Err(error) => {
            counter("slack.signature.verification", 1)
                .attribute("result", "failure")
                .attribute("reason", "hmac_mismatch")
                .capture();
            warn!(?error, "Slack signature verification failed");
            Response::builder()
                .status(StatusCode::FORBIDDEN)
                .body("Slack signature verification failed".into())
                .unwrap()
        }
    }
}
