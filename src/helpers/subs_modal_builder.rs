use anyhow::{Context, anyhow};
use crate::logging;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::query_as;
use tracing::{debug, error, trace};

struct Subscription {
    id: i64,
    target_id: String,
    active: bool,
    mc_usernames: Vec<String>,
}

#[derive(Deserialize, Serialize)]
pub struct SubsPageMetadata {
    pub page: i64,
    pub page_size: i64,
}

pub async fn fetch_and_build_subs_modal_view(
    sqlx_pool: &sqlx::PgPool,
    page: i64,
    user_id: String,
) -> anyhow::Result<Value> {
    trace!(user_id = %user_id, page = page, "Fetching subscriptions from database for modal view");
    let query_start = std::time::Instant::now();
    let subs_result = query_as!(
        Subscription,
        "SELECT s.id, s.active, s.target_id, u.mc_usernames
FROM subscriptions AS s
JOIN users AS u ON s.target_id = u.slack_id
WHERE s.subscriber_id = $1
ORDER BY s.created_at
LIMIT 6 OFFSET $2",
        user_id,
        page * 5
    )
        .fetch_all(sqlx_pool)
        .await;
    logging::record_db_query_metric(
        "subscriptions_fetch_page",
        query_start,
        if subs_result.is_ok() { "ok" } else { "error" },
    );
    let subs = match subs_result {
        Ok(subs) => {
            debug!(user_id = %user_id, page = page, count = subs.len(), "Subscriptions fetched from database");
            subs
        }
        Err(e) => {
            error!(error = ?e, user_id = %user_id, page = page, "Failed to fetch subscriptions from database");
            return Err(anyhow!("Failed to fetch subscriptions. Error: {e}"));
        }
    };
    
    let metadata = SubsPageMetadata { page, page_size: 5 };
    
    let mut blocks: Vec<Value> = Vec::new();
    
    blocks.push(json!({"type": "section", "text": {"type": "mrkdwn", "text": "Configure your update subscriptions below"}})); // Title
    blocks.push(json!({"type": "divider"}));
    blocks.push(json!({
        "type": "section",
        "text": {
        "type": "mrkdwn",
        "text": ":heavy_plus_sign: *Subscribe to a new person*"
    },
        "accessory": {
        "type": "button",
        "text": {
            "type": "plain_text",
            "text": "Subscribe",
            "emoji": true
        },
        "style": "primary",
        "action_id": "subscribe_new_person",
        "value": "click_me_123"
    }
    }));
    blocks.push(json!({"type": "divider"}));
    blocks.push(json!({
        "type": "header",
        "text": {
        "type": "plain_text",
        "text": "Current Subscriptions",
        "emoji": true
    }
    }));
    
    for subscription in &subs[..subs.len().min(6)] {
        let len = subscription.mc_usernames.len();
        let title = if len > 1 {
            let mut mcusers = String::new();
            
            let mut i = 1;
            
            for mcuser in &subscription.mc_usernames {
                if i != len {
                    let mcuser = format!("{mcuser}, ");
                    mcusers.push_str(&mcuser);
                    i += 1
                } else {
                    mcusers.push_str(mcuser)
                }
            }
            
            format!("<@{}> *({})*", subscription.target_id, mcusers)
        } else {
            format!(
                "<@{}> *({})*",
                subscription.target_id, subscription.mc_usernames[0]
            )
        };
        blocks.push(json!({
            "type": "section",
            "text": {
            "type": "mrkdwn",
            "text": title
        },
            "accessory": {
            "type": "button",
            "text": {
                "type": "plain_text",
                "text": "Remove",
                "emoji": true
            },
            "style": "danger",
            "action_id": "remove_subscription",
            "value": subscription.id.to_string(),
            "confirm": {
                "title": {
                    "type": "plain_text",
                    "text": "Remove subscription?"
                },
                "text": {
                    "type": "mrkdwn",
                    "text": format!("You'll stop receiving updates for {title}. They will be asked for approval again should you wish to subscribe to them again.")
                },
                "confirm": {
                    "type": "plain_text",
                    "text": "Remove"
                },
                "deny": {
                    "type": "plain_text",
                    "text": "Cancel"
                },
                "style": "danger"
            }
        }
        }));
        if subscription.active {
            blocks.push(json!({
                "type": "context",
                "elements": [
                {
                    "type": "mrkdwn",
                    "text": ":large_green_circle: Active"
                }
                ]
            }))
        } else {
            blocks.push(json!({
                "type": "context",
                "elements": [
                {
                    "type": "mrkdwn",
                    "text": ":large_yellow_circle: Pending approval"
                }
                ]
            }))
        }
    }
    
    blocks.push(json!({
        "type": "divider"
    }));
    
    let mut pagination_buttons: Vec<Value> = Vec::new();
    
    if page > 0 && !subs.is_empty() {
        pagination_buttons.push(json!(
            {
                "type": "button",
                "text": {
                "type": "plain_text",
                "text": "◀ Prev",
                "emoji": true
            },
                "action_id": "subs_page_prev",
                "value": "prev"
            }
        ));
    }
    
    if subs.len() > 5 {
        pagination_buttons.push(json!(
            {
                "type": "button",
                "text": {
                "type": "plain_text",
                "text": "Next ▶",
                "emoji": true
            },
                "action_id": "subs_page_next",
                "value": "next"
            }
        ));
    }
    
    if !pagination_buttons.is_empty() {
        blocks.push(json!({
            "type": "actions",
            "block_id": "subs_pagination",
            "elements": pagination_buttons
        }));
        blocks.push(json!({
            "type": "divider"
        }));
    }
    
    blocks.push(json!({
                            "type": "section",
                            "text": {
                            "type": "mrkdwn",
                            "text": "*What is this?*\n This feature allows you to subscribe to DM updates when the player you choose joins/leaves the hackclub minecraft server."
                        }
                        }));
    
    Ok(json!(
                    {
	"type": "modal",
	"callback_id": "configure_subs_modal",
	"private_metadata": serde_json::to_string(&metadata).context("Unable to serialise private metadata to string")?,
                        "submit": {
                            "type": "plain_text",
                            "text": "Done",
                            "emoji": true
                        },
                        "close": {
                            "type": "plain_text",
                            "text": "Exit",
                            "emoji": true
                        },
                        "title": {
                            "type": "plain_text",
                            "text": "Configure Update Subs"
                        },
                        "blocks": blocks}))
}
