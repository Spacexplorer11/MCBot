use super::AppState;
use crate::{Task::UpdateDMs, helpers::messages::send_message};
use axum::{
    Json,
    body::Body,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use sentry::metrics::counter;
use serde::Deserialize;
use serde_json::json;
use sqlx::query;
use std::sync::Arc;
use tracing::{debug, error, info, trace};

#[derive(Deserialize)]
#[serde(tag = "type")]
pub enum SlackPayload {
    #[serde(rename = "url_verification")]
    UrlVerification { challenge: String },

    #[serde(rename = "event_callback")]
    EventCallback { event: SlackEvent },
}

#[derive(Deserialize, Debug)]
pub struct SlackEvent {
    #[serde(rename = "type")]
    pub event_type: SlackEvents,
    pub channel: String,
    pub text: String,
    pub user: Option<String>,
    pub ts: String,
    pub bot_id: Option<String>,
    #[serde(default = "useless_username")]
    pub username: UsefulUsernames,
}

fn useless_username() -> UsefulUsernames {
    UsefulUsernames::Irrelevant
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub enum UsefulUsernames {
    Join,
    Leave,
    Nickname,
    #[serde(other)]
    Irrelevant,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "snake_case")]
pub enum SlackEvents {
    AppMention,
    Message,
}

pub async fn handle_event(
    State(state): State<Arc<AppState>>,
    Json(payload): Json<SlackPayload>,
) -> Response<Body> {
    trace!("Received an event at /slack/events");
    match payload {
        SlackPayload::UrlVerification { challenge } => {
            counter("slack.event", 1)
                .attribute("type", "url_verification")
                .capture();
            info!("Url Verification challenge received");
            Json(json!({"challenge": challenge})).into_response()
        }

        SlackPayload::EventCallback { event } => {
            counter("slack.event", 1)
                .attribute("type", "event_callback")
                .attribute("subtype", format!("{:?}", event.event_type))
                .capture();
            trace!(event_type = ?event.event_type, ?event, "Received event");
            match event.event_type {
                SlackEvents::AppMention => send_message(&json!({"channel": event.channel, "text": "Hi! I'm MCBot, made by <@U08D22QNUVD>! :) \nUse /mcrecipe to get crafting recipes!", "thread_ts": event.ts}), &state.client, &state.bot_token).await,
                SlackEvents::Message => {
                    if let Some(bot_id) = event.bot_id {
                        if bot_id.to_uppercase().eq("B04SB2PQLS1") {
                            match event.username {
                                UsefulUsernames::Join | UsefulUsernames::Leave => {
                                    let text = event.text.split_ascii_whitespace().map(|part| part.to_string()).collect::<Vec<String>>();

                                    let username = text.first().expect("This is a deterministic message. If this has changed then that is requires immediate attention.").replace('*', "");

                                    let slack_id = match query!(
                                        "SELECT * FROM users WHERE $1 = ANY(mc_usernames)",
                                        username
                                    )
                                        .fetch_optional(&state.sqlx_pool)
                                        .await
                                    {
                                        Ok(row) => {
                                            if let Some(row) = row {
                                                row.slack_id
                                            } else {
                                                debug!(timestamp=%event.ts, text=%event.text, ?username, "The user for join/leave row could not be found. This is likely expected because... not everyone is in the database.");
                                                return StatusCode::OK.into_response()
                                            }
                                        },
                                        Err(error) => {
                                            error!(?error, timestamp=%event.ts, text=%event.text, ?username, "MANUAL APOLOGY REQUIRED. AN ERROR OCCURRED WHEN FETCHING THE ROW FOR THE JOIN/LEAVE FROM THE DATABASE.");
                                            return StatusCode::OK.into_response();
                                        }
                                    };

                                    let subscribed_people = match query!(
                                        "SELECT *
                                        FROM subscriptions
                                        WHERE target_id = $1
                                          AND active = true;",
                                        slack_id
                                    )
                                        .fetch_all(&state.sqlx_pool)
                                        .await
                                    {
                                        Ok(rows) => rows.into_iter().map(|row| row.subscriber_id).collect::<Vec<String>>(),
                                        Err(error) => {
                                            error!(?error, timestamp=%event.ts, text=%event.text, ?slack_id, ?username, "MANUAL APOLOGY REQUIRED. AN ERROR OCCURRED WHEN FETCHING THE ROW FROM THE DATABASE.");
                                            return StatusCode::OK.into_response();
                                        }
                                    };

                                    let joining = event.text.contains("joined");

                                    match state.mpsc.try_send(
                                        UpdateDMs {
                                            people: subscribed_people,
                                            joining,
                                            slack_and_mc: (slack_id.clone(), username.to_string()),
                                            bot_token: state.bot_token.clone(),
                                            queued_at: std::time::Instant::now()
                                        }
                                    ) {
                                        Ok(..) => info!("Successfully sent the update DM's task to the mpsc queue"),
                                        Err(error) => error!(?error, timestamp=%event.ts, text=%event.text, ?slack_id, ?username, "MANUAL APOLOGY REQUIRED. AN ERROR OCCURRED WHEN SENDING THE UPDATE DMS TASK TO THE MPSC QUEUE")
                                    }

                                    StatusCode::OK.into_response()
                                },
                                UsefulUsernames::Nickname => {
                                    let text = event.text.split_ascii_whitespace().map(|part| part.to_string()).collect::<Vec<String>>();

                                    let old_nick = text.first().expect("This is a deterministic message. If this has changed then that is requires immediate attention.");
                                    let new_nick = text.last().expect("This is a deterministic message. If this has changed then that is requires immediate attention.");

                                    let row = match query!(
                                        "SELECT * FROM users WHERE $1 = ANY(mc_usernames)",
                                        old_nick
                                    )
                                        .fetch_optional(&state.sqlx_pool)
                                        .await
                                    {
                                        Ok(row) => {
                                            if let Some(row) = row {
                                                row
                                            } else {
                                                debug!(timestamp=%event.ts, text=%event.text, ?old_nick, ?new_nick, "The nickname row could not be found. This is likely expected because... not everyone is in the database.");
                                                return StatusCode::OK.into_response()
                                            }
                                        },
                                        Err(error) => {
                                            error!(?error, timestamp=%event.ts, text=%event.text, ?old_nick, ?new_nick, "MANUAL INPUT REQUIRED. AN ERROR OCCURRED WHEN FETCHING THE ROW FOR NICKNAME UPDATES FROM THE DATABASE.");
                                            return StatusCode::OK.into_response();
                                        }
                                    };

                                    let mut mc_usernames = row.mc_usernames;
                                    mc_usernames.retain(|user| user != old_nick);
                                    mc_usernames.push(new_nick.clone());

                                    match query!("UPDATE users SET mc_usernames = $1 WHERE slack_id = $2", &mc_usernames, row.slack_id).execute(&state.sqlx_pool).await {
                                        Ok(..) => {
                                            info!(slack=%row.slack_id,"Successfully updated nick from {old_nick} to {new_nick}.");
                                            counter("nickname.update", 1)
                                                .capture();
                                        },
                                        Err(e) => error!(error=?e, timestamp=%event.ts, text=%event.text, %old_nick, %new_nick, slack=%row.slack_id, "MANUAL INPUT REQUIRED. AN ERROR OCCURRED WHEN UPDATING THE DATABASE IN THE FINAL STEP OF UPDATING A NICKNAME.")
                                    }

                                    StatusCode::OK.into_response()

                                    /* Honestly this took me took long to make so in case I need it in the future I kept it
                                    let response: MinecraftPlayerData = match state.client.get("https://api.mc.hackclub.com")
                                          .header("User-Agent", "MCBot")
                                          .bearer_auth(state.hackclub_api_key.clone())
                                          .send()
                                          .await {
                                              Ok(res) => match res.status() {
                                                  StatusCode::OK => match res.json().await {
                                                      Ok(mpd) => mpd,
                                                      Err(e) => {
                                                          error!(error=?e, timestamp=%event.ts, text=%event.text, "MANUAL INPUT REQUIRED. AN ERROR OCCURRED WHEN CONVERTING THE RESPONSE FROM HACKCLUB API TO MINECRAFTPLAYERDATA.");
                                                          return StatusCode::OK.into_response()
                                                      }
                                                  }
                                                  StatusCode::TOO_MANY_REQUESTS => {
                                                      error!(timestamp=%event.ts, text=%event.text, response=?res, "MANUAL INPUT REQUIRED. YOU HIT THE RATELIMT FOR THE HACKCLUB API");
                                                      return StatusCode::OK.into_response()
                                                  }
                                                  StatusCode::NOT_FOUND => {
                                                      error!(timestamp=%event.ts, text=%event.text, response=?res, "MANUAL INPUT REQUIRED. THE API COULDN'T FIND THIS NICK EVEN THO IT SHOULD BE ABLE TO BE FOUND.");
                                                      return StatusCode::OK.into_response()
                                                  }
                                                  _ => {
                                                      error!(status=?res.status(), timestamp=%event.ts, text=%event.text, response=?res, "MANUAL INPUT REQUIRED. THE HACKCLUB API RETURNED AN ERROR STATUS CODE.");
                                                      return StatusCode::OK.into_response()
                                                  }
                                              }
                                          Err(e) => {
                                              error!(error=?e, timestamp=%event.ts, text=%event.text, "MANUAL INPUT REQUIRED. AN ERROR OCCURRED WHEN FETCHING THE SLACK ID FROM HACKCLUB API.");
                                              return StatusCode::OK.into_response()
                                          }
                                          }; */
                                },
                                UsefulUsernames::Irrelevant => StatusCode::OK.into_response()
                            }
                        } else {
                            StatusCode::OK.into_response()
                        }
                    }
                    else {
                        StatusCode::OK.into_response()
                    }
                }
            }
        }
    }
}
