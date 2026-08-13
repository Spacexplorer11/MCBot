use axum::{extract::Request, middleware::Next, response::Response};
use sentry::integrations::tracing::EventFilter;
use sentry::metrics::{counter, distribution};
use sentry::protocol::Unit;
use std::time::Instant;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub fn initialise_logging() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,mcbot=debug"));

    // Copied straight from Sentry's docs
    let sentry_layer =
        sentry::integrations::tracing::layer().event_filter(|md| match *md.level() {
            // Capture error and warn level events as both logs and events in Sentry
            tracing::Level::ERROR | tracing::Level::WARN => EventFilter::Event | EventFilter::Log,
            // Ignore trace level events, as they're too verbose
            tracing::Level::TRACE => EventFilter::Ignore,
            // Capture everything else as both a breadcrumb and a log
            _ => EventFilter::Breadcrumb | EventFilter::Log,
        });

    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(filter)
        .with(sentry_layer)
        .init();
}

pub async fn metric_response(request: Request, next: Next) -> Response {
    let start = Instant::now();
    let response = next.run(request).await;
    counter("http.requests", 1)
        .attribute("status", response.status().as_u16().to_string())
        .capture();
    distribution("http.response.duration", start.elapsed().as_millis() as f64)
        .unit(Unit::Millisecond)
        .capture();
    response
}

pub fn record_slack_api_metric(endpoint: &str, start: Instant, result: &str) {
    counter("slack.api.request", 1)
        .attribute("endpoint", endpoint.to_string())
        .attribute("result", result.to_string())
        .capture();
    distribution("slack.api.duration", start.elapsed().as_millis() as f64)
        .unit(Unit::Millisecond)
        .capture();
}

pub fn record_hackclub_api_metric(start: Instant, result: &str) {
    counter("hackclub.api.request", 1)
        .attribute("result", result.to_string())
        .capture();
    distribution("hackclub.api.duration", start.elapsed().as_millis() as f64)
        .unit(Unit::Millisecond)
        .capture();
}
