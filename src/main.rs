use axum::{Form, Json, extract::State, routing::post};
use dotenvy::dotenv;
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::{env, io::Cursor};
use tokio::{net::TcpListener, sync::mpsc};

enum Task {
    Recipe {
        item_name: String,
        response_url: String,
    },
}

#[derive(Clone)]
struct AppState {
    client: Client,
    bot_token: String,
    mpsc: mpsc::Sender<Task>,
    valid_recipes: std::sync::Arc<HashSet<String>>,
}

#[derive(Deserialize)]
struct McRecipe {}

#[derive(Deserialize)]
#[serde(tag = "type")]
enum SlackPayload {
    #[serde(rename = "url_verification")]
    UrlVerification { challenge: String },

    #[serde(rename = "event_callback")]
    EventCallback { event: SlackEvent },
}

#[derive(Deserialize)]
struct SlackEvent {
    #[serde(rename = "type")]
    event_type: String,
    channel: String,
    text: String,
    user: String,
    ts: String,
}

#[derive(Deserialize)]
struct SlackSlashCommand {
    command: String,
    text: String,
    channel_id: String,
    user_id: String,
    response_url: String,
    trigger_id: String,
    team_id: String,
}

#[tokio::main]
async fn main() {
    if !dotenv().is_ok() {
        eprintln!(".env file NOT LOADED")
    }

    let bot_token = env::var("SLACK_BOT_TOKEN").expect("Bot Token NOT FOUND");
    let (queue_input, mut queue_output) = mpsc::channel::<Task>(2000);

    let mut valid_recipes = HashSet::new();
    let mut items: HashMap<String, Vec<u8>> = HashMap::new();

    {
        let client = Client::new();

        println!("Fetching recipes");
        let response = client
            .get("https://api.github.com/repos/misode/mcmeta/git/trees/data?recursive=1")
            .header("User-Agent", "mcbot") // GitHub API requires a User-Agent header
            .send()
            .await
            .expect("Failed to fetch recipe list from mcmeta");

        let tree: serde_json::Value = response
            .json()
            .await
            .expect("Invalid JSON from GitHub tree API");

        if let Some(entries) = tree["tree"].as_array() {
            for entry in entries {
                if let Some(name) = entry["path"].as_str() {
                    if let Some(name) = name.strip_prefix("data/minecraft/recipe/") {
                        if let Some(item_name) = name.strip_suffix(".json") {
                            valid_recipes.insert(item_name.to_owned());
                        }
                    }
                }
            }
        }

        println!("Recipes successfully fetched");
        println!("Initiated step 1 of fetching items (version manifest)");

        let response = client
            .get("https://launchermeta.mojang.com/mc/game/version_manifest_v2.json")
            .send()
            .await
            .expect(
                "An error occurred when fetching the version manifest (Step 1 of item fetching)",
            );
        let json_data = response
            .json::<serde_json::Value>()
            .await
            .expect("Version manifest returned incorrect json :pensive_face:");
        let latest_version = &json_data["latest"]["release"]
            .as_str()
            .expect("Could not find latest release version string");
        let versions = &json_data["versions"]
            .as_array()
            .expect("Versions part was not array?");

        let current_version_object = versions
            .iter()
            .find(|v| v["id"].as_str() == Some(latest_version))
            .expect("Could not find the latest version object in the versions array");

        let package_url = current_version_object["url"]
            .as_str()
            .expect("Could not get package URL as a string");

        println!(
            "Step 1 complete, version manifest successfully fetched! Fun fact, the latest version is {latest_version}"
        );

        let mut client_jar_version_path = env::current_exe()
            .expect("Failed to get current executable path")
            .parent()
            .expect("Failed to get executable directory")
            .to_path_buf();
        client_jar_version_path.push("assets");
        client_jar_version_path.push("version.txt");

        let mut version_valid = false;

        match tokio::fs::read_to_string(&client_jar_version_path).await {
            Ok(version) => {
                if version.as_bytes() == latest_version.as_bytes() {
                    println!(
                        "Skipping fetching client.jar as it already exists and is the latest version."
                    );
                    version_valid = true;
                } else {
                    println!("Initiated step 2 of fetching items (client.jar url)");
                }
            }
            Err(e) => {
                eprintln!(
                    "An error occurred when reading the version.txt for client.jar. *This error may be expected*. On the first run an error is expected as no version.txt exists. Error: {e}"
                )
            }
        }

        let mut client_jar_path = env::current_exe()
            .expect("Failed to get current executable path")
            .parent()
            .expect("Failed to get executable directory")
            .to_path_buf();

        client_jar_path.push("assets");
        client_jar_path.push("client.jar");

        let client_jar_bytes;

        if version_valid {
            client_jar_bytes = tokio::fs::read(&client_jar_path)
                .await
                .expect("Unable to convert client.jar to bytes or to read it??")
        } else {
            let response = client.get(package_url).send().await.expect(
                "An error occurred when fetching the package url (Step 2 of item fetching)",
            );
            let json_data = response
                .json::<serde_json::Value>()
                .await
                .expect("Package url returned incorrect json :pensive_face:");
            let client_jar_url = &json_data["downloads"]["client"]["url"]
                .as_str()
                .expect("Client jar url not a string???");

            println!("Step 2 complete, client.jar url successfully fetched");
            println!("Initiated step 3 of fetching items (client.jar itself)");

            let response = client
                .get(*client_jar_url)
                .send()
                .await
                .expect("An error occurred fetching the client jar (Step 3 of item fetching)");
            client_jar_bytes = Vec::from(
                response
                    .bytes()
                    .await
                    .expect("Failed to convert the client.jar to bytes :("),
            );
        }

        let client_jar_cursor = Cursor::new(&client_jar_bytes);

        if let Some(parent) = client_jar_path.parent() {
            match tokio::fs::create_dir_all(parent).await {
                Ok(..) => println!("Created parent directory"),
                Err(e) => {
                    eprintln!(
                        "An error occurred creating the assets directory. The items will now only be in memory and will need to be redownloaded on restart. Error: {e}"
                    )
                }
            }
        }

        match tokio::fs::write(&client_jar_path, &client_jar_bytes).await {
            Ok(..) => {
                println!("Saved client.jar to disk");
                match tokio::fs::write(client_jar_version_path, latest_version.as_bytes()).await {
                    Ok(..) => println!("Successfully saved client.jar's version in a txt file"),
                    Err(e) => eprintln!(
                        "An error occurred when saving the version file for client.jar. This will result in it being redownloaded on restart. Error: {e}"
                    ),
                }
            }
            Err(e) => {
                eprintln!(
                    "An error occurred saving the client.jar to the local disk. The items will now only be in memory and will need to be redownloaded on restart. Error: {e}"
                )
            }
        };

        let mut client_jar_zip =
            async_zip::tokio::read::seek::ZipFileReader::with_tokio(client_jar_cursor)
                .await
                .expect("Failed to read the cursor?? (Step 4 of item fetching / reading now)");
        let mut temp_items_map = HashMap::new();

        for (i, file) in client_jar_zip.file().entries().iter().enumerate() {
            let filename = file
                .filename()
                .as_str()
                .expect("Invalid UTF-8 Filename (Step 5 of item reading)");
            if filename.starts_with("assets/minecraft/textures/") && filename.ends_with(".png") {
                let item_name = filename
                    .strip_prefix("assets/minecraft/textures/")
                    .unwrap()
                    .strip_suffix(".png")
                    .unwrap()
                    .to_string();
                temp_items_map.insert(item_name, i);
            }
        }

        for item in temp_items_map {
            let mut item_png = client_jar_zip.reader_with_entry(item.1).await.unwrap();
            let mut item_png_bytes = Vec::new();

            match item_png.read_to_end_checked(&mut item_png_bytes).await {
                Ok(..) => (),
                Err(e) => panic!("Failed to convert image {}: {}", item.0, e),
            }
            items.insert(item.0, item_png_bytes);
        }
        println!("Saved items to the item map");
    }

    let shared_recipes = std::sync::Arc::new(valid_recipes);

    let state = AppState {
        client: Client::new(),
        bot_token,
        mpsc: queue_input,
        valid_recipes: shared_recipes,
    };

    tokio::spawn(async move {
        let client = Client::new();
        while let Some(task) = queue_output.recv().await {
            match task {
                Task::Recipe {
                    item_name,
                    response_url,
                } => {
                    let url = format!(
                        "https://raw.githubusercontent.com/misode/mcmeta/refs/heads/data-json/data/minecraft/recipe/{item_name}.json"
                    );
                    let response = match client.get(url).send().await {
                        Ok(response) => response,
                        Err(e) => {
                            panic!(
                                "An error occurred while fetching the recipe for {item_name}. Error: {e}"
                            );
                            // TODO: Tell user that it failed and not crash
                        }
                    };
                    let recipe_json: serde_json::Value = response
                        .json()
                        .await
                        .expect("Unable to convert... json file to json??");
                } // Add more later obvs
            }
        }
    });

    let router = axum::Router::new()
        .route("/slack/events", post(handle_event))
        .route("/slack/commands", post(handle_command))
        .with_state(state);
    let listener = TcpListener::bind("0.0.0.0:4598").await.unwrap();

    axum::serve(listener, router).await.expect("Uh oh");
}

async fn handle_event(
    State(state): State<AppState>,
    Json(payload): Json<SlackPayload>,
) -> Json<serde_json::Value> {
    match payload {
        SlackPayload::UrlVerification { challenge } => {
            println!("Url Verification challenge received");
            Json(json!({"challenge": challenge}))
        }

        SlackPayload::EventCallback { event } => {
            #[cfg(debug_assertions)]
            println!("Received event");
            match state.client.post("https://slack.com/api/chat.postMessage").bearer_auth(state.bot_token).json(&json!({"channel": format!("{}", event.channel), "text": "if this works, you deserve to proud of yourself :)"})).send().await {
                Ok(response) => {
                    #[cfg(debug_assertions)]
                    println!("{:#?}", response)
                },
                Err(e) => eprintln!("Something went wrong with sending a message, {e}")
            };
            Json(json!({"ok":"true"}))
        }
    }
}

async fn handle_command(
    State(state): State<AppState>,
    Form(payload): Form<SlackSlashCommand>,
) -> Json<serde_json::Value> {
    match payload.command.as_str() {
        "/recipe" => {
            if state.valid_recipes.contains(&payload.text) {
                Json(
                    json!({"response_type": "ephemeral", "text": "Gathering images and sewing 'em up, hang on a second!"}),
                )
            } else {
                Json(json!({"response_type": "ephemeral", "text": "Your recipe was invalid."}))
            }
        }
        _ => Json(
            json!({"response_type": "ephemeral", "text": "Sorry that command isn't supported as of right now. (If you got this, let @<U08D22QNUVD> know)"}),
        ), // only registered slash commands should even come, this shouldn't trigger anyway
    }
}
