use crate::Task;
use reqwest::Client;
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;

pub mod commands;
pub mod events;
pub mod interactions;
pub mod mcrecipes;
pub mod recipes;
pub mod uptime;

pub struct SlackMessageContext<'a> {
    client: &'a Client,
    bot_token: &'a str,
    channel_id: &'a str,
    user_id: &'a str,
    thread_ts: Option<&'a str>,
}

impl SlackMessageContext<'_> {
    pub fn new<'a>(
        client: &'a Client,
        bot_token: &'a str,
        channel_id: &'a str,
        user_id: &'a str,
        thread_ts: Option<&'a str>,
    ) -> SlackMessageContext<'a> {
        SlackMessageContext {
            client,
            bot_token,
            channel_id,
            user_id,
            thread_ts,
        }
    }
}

#[derive(Clone)]
pub struct AppState {
    client: Client,
    bot_token: Arc<str>,
    mpsc: mpsc::Sender<Task>,
    valid_recipes: HashMap<String, usize>,
    sqlx_pool: sqlx::PgPool,
    flipped_language_mappings: HashMap<String, String>,
    hackclub_api_key: Arc<str>,
}

impl AppState {
    pub fn new(
        client: Client,
        bot_token: Arc<str>,
        mpsc: mpsc::Sender<Task>,
        valid_recipes: HashMap<String, usize>,
        sqlx_pool: sqlx::PgPool,
        flipped_language_mappings: HashMap<String, String>,
        hackclub_api_key: Arc<str>,
    ) -> AppState {
        AppState {
            client,
            bot_token,
            mpsc,
            valid_recipes,
            sqlx_pool,
            flipped_language_mappings,
            hackclub_api_key,
        }
    }
}

#[derive(Clone)]
pub struct MCRecipesAppState {
    client: Client,
    bot_token: Arc<str>,
    mpsc: mpsc::Sender<Task>,
    valid_recipes: HashMap<String, usize>,
    flipped_language_mappings: HashMap<String, String>,
}

impl MCRecipesAppState {
    pub fn new(
        client: Client,
        bot_token: Arc<str>,
        mpsc: mpsc::Sender<Task>,
        valid_recipes: HashMap<String, usize>,
        flipped_language_mappings: HashMap<String, String>,
    ) -> MCRecipesAppState {
        MCRecipesAppState {
            client,
            bot_token,
            mpsc,
            valid_recipes,
            flipped_language_mappings,
        }
    }
}

#[derive(Deserialize)]
pub struct WhereIOnlyNeedTheIdField {
    pub id: String,
}

#[derive(Deserialize)]
pub struct OpenConversationResponse {
    pub ok: bool,
    pub channel: WhereIOnlyNeedTheIdField,
}
