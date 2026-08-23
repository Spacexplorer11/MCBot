use axum::{extract::Request, middleware::Next, response::Response};
use sentry::integrations::tracing::EventFilter;
use sentry::metrics::{counter, distribution};
use sentry::protocol::Unit;
use std::time::Instant;
use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

pub fn initialise_logging() {
    let filter =
        EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info,mcbot=debug"));

    // Adapted from Sentry's docs:
    // - Only ERROR auto-escalates to a Sentry Issue (Event). WARN is common for expected/
    //   recoverable conditions (missing .env, stale client.jar cache, etc.) and shouldn't page
    //   on every occurrence — it's still captured as a log/breadcrumb.
    // - Call sites that already report the full picture via `capture_anyhow` (exception chain +
    //   backtrace) tag their accompanying error!() with `already_reported = true` so this layer
    //   doesn't also create a second, lower-fidelity Sentry Event for the same failure.
    let sentry_layer = sentry::integrations::tracing::layer().event_filter(|md| {
        if md.fields().field("already_reported").is_some() {
            return EventFilter::Breadcrumb | EventFilter::Log;
        }
        match *md.level() {
            // Capture error level events as both logs and events in Sentry
            tracing::Level::ERROR => EventFilter::Event | EventFilter::Log,
            // Ignore trace level events, as they're too verbose
            tracing::Level::TRACE => EventFilter::Ignore,
            // Capture everything else as both a breadcrumb and a log
            _ => EventFilter::Breadcrumb | EventFilter::Log,
        }
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

pub fn record_db_query_metric(query: &str, start: Instant, result: &str) {
    counter("db.query", 1)
        .attribute("query", query.to_string())
        .attribute("result", result.to_string())
        .capture();
    distribution("db.query.duration", start.elapsed().as_millis() as f64)
        .unit(Unit::Millisecond)
        .capture();
}
