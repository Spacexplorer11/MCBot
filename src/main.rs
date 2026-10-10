#![allow(linker_messages)]

pub mod handlers;
pub mod helpers;

use crate::Task::{Recipe, Subscriptions, UpdateDMs};
use axum::{
    body::Body,
    extract::Request,
    middleware,
    routing::{get, post},
};
use dotenvy::dotenv;
use handlers::{
    SlackMessageContext, commands::handle_command, events::handle_event,
    interactions::handle_interactions, mcrecipes::handle_mcrecipes, recipes::RecipeData,
    uptime::uptime,
};
use helpers::{
    data::fetch_client_jar,
    errors::send_and_log_on_failure,
    logging::{self, initialise_logging},
    messages::{legacy_send_message, send_dm},
    slack_signature_verifier::verify_slack_signature,
    subs_modal_builder::fetch_and_build_subs_modal_view,
};
use reqwest::Client;
use sentry::{Hub, SentryFutureExt, TransactionContext, TransactionOrSpan};
use sentry::{
    integrations::{
        anyhow::capture_anyhow,
        tower::{NewSentryLayer, SentryHttpLayer},
    },
    metrics::{counter, distribution, gauge},
    protocol::Unit,
};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use sqlx::query;
use std::time::Duration;
use std::{collections::HashMap, env, io, sync::Arc};
use tokio::{net::TcpListener, sync::mpsc, task::JoinSet};
use tower::ServiceBuilder;
use tracing::{debug, error, info, trace, warn};

pub type HmacSha256 = hmac::Hmac<sha2::Sha256>;

pub enum Task {
    Recipe {
        item_name: Box<str>,
        response_url: Option<String>,
        channel_id: String,
        user_id: String,
        thread_ts: Option<String>,
        bot_token: Arc<str>,
        queued_at: std::time::Instant,
        hub: Arc<Hub>,
        parent_span: Option<TransactionOrSpan>,
    },
    Subscriptions {
        user_id: String,
        trigger_id: String,
        bot_token: Arc<str>,
        queued_at: std::time::Instant,
        hub: Arc<Hub>,
        parent_span: Option<TransactionOrSpan>,
    },
    UpdateDMs {
        people: Vec<String>,
        joining: bool,
        slack_and_mc: (String, String),
        bot_token: Arc<str>,
        queued_at: std::time::Instant,
        hub: Arc<Hub>,
        parent_span: Option<TransactionOrSpan>,
    },
}

/// Captures the enqueuing request's Hub (for breadcrumb continuity) and its active
/// span/transaction (so the task's later processing can continue the same Sentry trace
/// instead of showing up as a disconnected event).
fn capture_task_context() -> (Arc<Hub>, Option<TransactionOrSpan>) {
    let hub = Hub::current();
    let parent_span = hub.configure_scope(|scope| scope.get_span());
    (hub, parent_span)
}

/// Transactions aren't sent to Sentry until `finish()` is called, and there's no `Drop`
/// impl to do it automatically. This guarantees it happens on every exit path (including
/// early `return`s inside a task's processing) without having to call it manually at each one.
struct FinishTransactionOnDrop(Option<sentry::Transaction>);

impl Drop for FinishTransactionOnDrop {
    fn drop(&mut self) {
        if let Some(transaction) = self.0.take() {
            transaction.finish();
        }
    }
}

fn main() -> io::Result<()> {
    trace!("Loading .env");

    if dotenv().is_err() {
        warn!(".env file NOT LOADED");
    }

    let sentry_url = env::var("SENTRY_URL").expect("SENTRY URL NOT FOUND");

    let release = env::var("REVISION");

    // Sentry MUST be initialised before the Tokio runtime starts.
    let _guard = sentry::init(
        sentry::ClientOptions::new()
            .dsn(&sentry_url)
            .maybe_release({
                if release.is_err() {
                    sentry::release_name!()
                } else {
                    #[allow(clippy::unnecessary_unwrap)]
                    Some(release.unwrap().into())
                }
            })
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
            let metrics_queue = queue_input.clone();

            let mut client_jar_zip = fetch_client_jar(&client).await;
            let mut recipe_data = RecipeData::default();

            info!("Now adding recipes, items & tags to memory");

            match recipe_data
                .fetch_recipes_and_more(&mut client_jar_zip)
                .await
            {
                Ok(()) => info!("All startup assets loaded successfully"),
                Err(e) => {
                    // No capture_anyhow here: this is a fatal startup panic, and Sentry's panic
                    // hook already reports it (the `{e:?}` panic message carries the full anyhow
                    // chain), so an explicit capture would just double-report the same failure.
                    panic!("Failed to fetch recipes: {e:?}");
                }
            }

            let sqlx_pool = PgPoolOptions::new()
                .max_connections(5)
                .acquire_timeout(Duration::from_secs(10))
                .idle_timeout(Duration::from_secs(60))
                .max_lifetime(Duration::from_secs(10 * 60))
                .test_before_acquire(true)
                .connect(&env::var("DATABASE_URL").expect("DATABASE_URL NOT FOUND"))
                .await
                .expect("Failed to connect to database");
            info!("Connected to the PostgreSQL database");
            let metrics_pool = sqlx_pool.clone();

            let mut flipped_language_mappings = HashMap::new();

            for (key, value) in recipe_data.language_mappings() {
                let value = value.to_lowercase().replace(' ', "_");
                flipped_language_mappings.insert(value.into_boxed_str(), key.clone());
            }
            let flipped_language_mappings = Arc::new(flipped_language_mappings);

            let state = Arc::new(handlers::AppState::new(
                Client::new(),
                bot_token.into(),
                queue_input.clone(),
                recipe_data.valid_recipes.clone(),
                sqlx_pool.clone(),
                flipped_language_mappings.clone(),
                hackclub_api_key.into(),
                ));

            let mcrecipes_state = Arc::new(handlers::MCRecipesAppState::new(
                Client::new(),
                mcrecipes_bot_token.into(),
                queue_input,
                recipe_data.valid_recipes.clone(),
                flipped_language_mappings,
            ));

            tokio::spawn(async move {
                while let Some(task) = queue_output.recv().await {
                    // Fork this task's Hub from the *enqueuing request's* Hub (not the
                    // background thread's own), so it inherits that request's breadcrumbs
                    // (e.g. "signature verified", "command matched") instead of starting
                    // blank, and so its breadcrumbs/events don't bleed into whatever other
                    // tasks this long-lived background loop processes before or after it.
                    let task_hub = match &task {
                        Recipe { hub, .. } | Subscriptions { hub, .. } | UpdateDMs { hub, .. } => {
                            hub.clone()
                        }
                    };
                    async {
                    trace!("Received task in async thread");

                    match task {
                        Recipe {
                            item_name,
                            response_url,
                            channel_id,
                            user_id,
                            thread_ts,
                            bot_token,
                            queued_at,
                            hub: _,
                            parent_span,
                        } => {
                            // Continues the enqueuing request's trace as a new, linked
                            // transaction, so Sentry shows this async processing as connected
                            // to the HTTP request that triggered it instead of a bare event.
                            let transaction = sentry::start_transaction(
                                TransactionContext::continue_from_span(
                                    "task.recipe",
                                    "queue.process",
                                    parent_span,
                                ),
                            );
                            Hub::current().configure_scope(|scope| scope.set_span(Some(transaction.clone().into())));
                            let _txn_guard = FinishTransactionOnDrop(Some(transaction));

                            distribution("task.queue.delay", queued_at.elapsed().as_millis() as f64)
                                .unit(Unit::Millisecond)
                                .capture();
                            let processing_start = std::time::Instant::now();
                            let ctx = SlackMessageContext::new(&client, &bot_token, &channel_id, &user_id, response_url.as_deref());

                            match recipe_data
                                .process_recipe(
                                    item_name.as_ref(),
                                    ctx,
                                    &mut client_jar_zip,
                                )
                                .await
                            {
                                Ok(..) => {
                                    counter("recipe.processed", 1)
                                        .attribute("result", "success")
                                        .capture();
                                    debug!(%item_name, %user_id, %channel_id, "Recipe successfully processed");
                                }

                                Err(error) => {
                                    counter("recipe.processed", 1)
                                        .attribute("result", "error")
                                        .capture();
                                    let is_unsupported_recipe_type = error
                                        .to_string()
                                        .eq("Unsupported recipe");
                                    let is_rate_limited =
                                        error.to_string().starts_with("Slack rate-limited");
                                    if is_unsupported_recipe_type {
                                        // Expected: user asked for a non-crafting item. No capture_anyhow — this isn't a bug.
                                        warn!("Recipe could not be processed because it was not a crafting recipe");
                                    } else if is_rate_limited {
                                        // Expected under load: Slack itself rejected the send.
                                        // No capture_anyhow — this isn't a bug in our code.
                                        warn!(%item_name, %user_id, "Recipe image failed to send because Slack rate-limited the request");
                                    } else {
                                    capture_anyhow(&error);
                                    error!(
                                        already_reported = true,
                                        ?error,
                                        %item_name,
                                        %user_id,
                                        "Failed to fulfil recipe task processing pipeline"
                                    );
                                    }
                                    warn!(%item_name, %user_id, "Sending user-friendly error message to Slack");

                                    if let Some(response_url) = response_url {
                                        let polite_msg = if is_unsupported_recipe_type {
                                            json!({
                                                "response_type": "ephemeral",
                                                "text": "Uh oh, that type of recipe isn't supported! This bot currently only supports crafting recipes. If that was supposed to work, please contact <@U08D22QNUVD> or email akaal@akaalroop.com"
                                            })
                                        } else if is_rate_limited {
                                            json!({
                                                "response_type": "ephemeral",
                                                "text": "Whoa, lots of people are using MCBot right now! Please try again in a moment."
                                            })
                                        } else {
                                            json!({
                                                "response_type": "ephemeral",
                                                "text": format!(
                                                    "Uh oh, something went wrong! Please try again! If this persists, please contact <@U08D22QNUVD> or email akaal@akaalroop.com. Error: {error}"
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
                                        let polite_msg = if is_unsupported_recipe_type {
                                            json!({
                                                "channel": channel_id,
                                                "thread_ts": thread_ts,
                                                "text": "Uh oh, that type of recipe isn't supported! This bot currently only supports crafting recipes. If that was supposed to work, please contact <@U08D22QNUVD> or email akaal@akaalroop.com"
                                            })
                                        } else if is_rate_limited {
                                            json!({
                                                "channel": channel_id,
                                                "thread_ts": thread_ts,
                                                "text": "Whoa, lots of people are using MCBot right now! Please try again in a moment."
                                            })
                                        } else {
                                            json!({
                                                "channel": channel_id,
                                                "thread_ts": thread_ts,
                                                "text": format!(
                                                    "Uh oh, something went wrong! Please try again! If this persists, please contact <@U08D22QNUVD> or email akaal@akaalroop.com. Error: {error}"
                                                )
                                            })
                                        };

                                        let mut response = legacy_send_message(
                                            &polite_msg,
                                            &client,
                                            &bot_token,
                                        )
                                            .await;

                                        if let Err(error) = response {
                                            for _ in 0..=3 {
                                                error!(
                                                    ?error,
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
                            queued_at,
                            hub: _,
                            parent_span,
                        } => {
                            let transaction = sentry::start_transaction(
                                TransactionContext::continue_from_span(
                                    "task.subscriptions",
                                    "queue.process",
                                    parent_span,
                                ),
                            );
                            Hub::current().configure_scope(|scope| scope.set_span(Some(transaction.clone().into())));
                            let _txn_guard = FinishTransactionOnDrop(Some(transaction));

                            distribution("task.queue.delay", queued_at.elapsed().as_millis() as f64)
                                .unit(Unit::Millisecond)
                                .capture();
                            debug!(%user_id, "Processing Subscriptions task: fetching and building modal view");
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

                                Err(error) => {
                                    counter("subscriptions.modal", 1)
                                        .attribute("result", "error")
                                        .capture();
                                    capture_anyhow(&error);
                                    error!(
already_reported = true, ?error, "An error occurred fetching and building the modal view");
                                    return;
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

                        UpdateDMs {
                            people,
                            joining,
                            slack_and_mc,
                            bot_token,
                            queued_at,
                            hub: _,
                            parent_span,
                        } => {
                            let transaction = sentry::start_transaction(
                                TransactionContext::continue_from_span(
                                    "task.update_dms",
                                    "queue.process",
                                    parent_span,
                                ),
                            );
                            Hub::current().configure_scope(|scope| scope.set_span(Some(transaction.clone().into())));
                            let _txn_guard = FinishTransactionOnDrop(Some(transaction));

                            distribution("task.queue.delay", queued_at.elapsed().as_millis() as f64)
                                .unit(Unit::Millisecond)
                                .capture();
                            let text_to_send = if joining {
                                format!("Your friend <@{}> ({}) just joined the hackclub minecraft server!", slack_and_mc.0, slack_and_mc.1)
                            } else {
                                format!("Your friend <@{}> ({}) just left the hackclub minecraft server!", slack_and_mc.0, slack_and_mc.1)
                            };

                            let mut set = JoinSet::new();

                            if people.len() > 1 {
                                for person in people {
                                    let client = client.clone();
                                    let bot_token = bot_token.clone();
                                    let text_to_send = text_to_send.clone();
                                    set.spawn(
                                        async move {
                                            send_dm(&client, &bot_token, &person, &text_to_send).await
                                        }
                                        .bind_hub(Hub::current()),
                                    );
                                }

                                while let Some(result) = set.join_next().await {
                                    if let Ok(result) = result {
                                        if let Err(error) = result {
                                            capture_anyhow(&error);
                                            error!(already_reported = true, ?error, "MANUAL APOLOGY REQUIRED! AN ERROR OCCURRED WHEN SENDING THE UPDATE DM");
                                        } else {
                                            info!("Update DM successfully sent!");
                                        }
                                    }
                                }
                            } else if people.len() == 1 {
                                match send_dm(&client, &bot_token, &people[0], &text_to_send).await {
                                    Ok(..) => info!("Update DM successfully sent!"),
                                    Err(error) => {
                                        capture_anyhow(&error);
                                        error!(already_reported = true, ?error, "MANUAL APOLOGY REQUIRED! AN ERROR OCCURRED WHEN SENDING THE UPDATE DM");
                                    }
                                }
                            }
                        }
                    }
                    }
                    .bind_hub(Hub::new_from_top(&task_hub))
                    .await;
                }
            });

            tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(5 * 60));
                loop {
                    interval.tick().await;

                    let depth = task_queue_capacity - metrics_queue.capacity();
                    gauge("task.queue.depth", depth as f64).capture();

                    let query_start = std::time::Instant::now();
                    let active_count_result =
                        query!("SELECT count(*) AS count FROM subscriptions WHERE active = true")
                            .fetch_one(&metrics_pool)
                            .await;
                    logging::record_db_query_metric(
                        "subscriptions_active_count",
                        query_start,
                        if active_count_result.is_ok() { "ok" } else { "error" },
                    );
                    match active_count_result {
                        Ok(row) => {
                            gauge("subscriptions.count", row.count.unwrap_or(0) as f64)
                                .attribute("status", "active")
                                .capture();
                        }
                        Err(e) => warn!(
                            error = ?e,
                            "Failed to fetch active subscription count for gauge"
                        ),
                    }

                    let query_start = std::time::Instant::now();
                    let pending_count_result = query!(
                        "SELECT count(*) AS count FROM subscriptions WHERE active = false"
                    )
                    .fetch_one(&metrics_pool)
                    .await;
                    logging::record_db_query_metric(
                        "subscriptions_pending_count",
                        query_start,
                        if pending_count_result.is_ok() { "ok" } else { "error" },
                    );
                    match pending_count_result {
                        Ok(row) => {
                            gauge("subscriptions.count", row.count.unwrap_or(0) as f64)
                                .attribute("status", "pending")
                                .capture();
                        }
                        Err(e) => warn!(
                            error = ?e,
                            "Failed to fetch pending subscription count for gauge"
                        ),
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

            // Sentry Hub binding + transaction creation apply only to the Slack-facing routes.
            // /status/uptime is excluded so a health-check monitor polling it doesn't burn a
            // full Sentry transaction on every ping — it still gets the http.requests metric via
            // the metric_response middleware applied to the whole router below.
            let instrumented_router = axum::Router::new()
                .merge(mcbot_router)
                .merge(mcrecipes_router)
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

            let router = axum::Router::new()
                .merge(instrumented_router)
                .merge(uptime_router)
                .layer(axum::middleware::from_fn(logging::metric_response));

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
