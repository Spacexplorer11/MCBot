use async_zip::tokio::read::seek::ZipFileReader;
use reqwest::Client;
use std::env;
use tokio::fs::File;
use tokio::io::BufReader;
use tokio_util::compat::TokioAsyncWriteCompatExt;
use tracing::{info, warn};

#[tracing::instrument(name = "client_jar_fetching_pipeline", skip(client))]
pub async fn fetch_client_jar(client: &Client) -> ZipFileReader<BufReader<File>> {
    info!("Initiated step 1 of fetching client.jar - (version manifest)");

    let response = client
        .get("https://launchermeta.mojang.com/mc/game/version_manifest_v2.json")
        .send()
        .await
        .expect("An error occurred when fetching the version manifest (Step 1 of item fetching)");
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

    info!(
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

    let mut client_jar_path = env::current_exe()
        .expect("Failed to get current executable path")
        .parent()
        .expect("Failed to get executable directory")
        .to_path_buf();

    client_jar_path.push("assets");
    client_jar_path.push("client.jar");

    match tokio::fs::read_to_string(&client_jar_version_path).await {
        Ok(version) => {
            if version.trim() == *latest_version
                && tokio::fs::metadata(&client_jar_path).await.is_ok()
            {
                info!(
                    "Skipping fetching client.jar as it already exists and is the latest version."
                );
                version_valid = true;
            } else {
                info!("Initiated step 2 of fetching items (client.jar url)");
            }
        }
        Err(e) => {
            warn!(
                "An error occurred when reading the version.txt for client.jar. *This error may be expected*. On the first run an error is expected as no version.txt exists. Error: {e}"
            )
        }
    }

    let client_jar_bytes;

    if !version_valid {
        let response =
            client.get(package_url).send().await.expect(
                "An error occurred when fetching the package url (Step 2 of item fetching)",
            );
        let json_data = response
            .json::<serde_json::Value>()
            .await
            .expect("Package url returned incorrect json :pensive_face:");
        let client_jar_url = &json_data["downloads"]["client"]["url"]
            .as_str()
            .expect("Client jar url not a string???");

        info!("Step 2 complete, client.jar url successfully fetched");
        info!("Initiated step 3 of fetching items (client.jar itself)");

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
        if let Some(parent) = client_jar_path.parent() {
            match tokio::fs::create_dir_all(parent).await {
                Ok(..) => info!("Created parent directory"),
                Err(e) => {
                    warn!(
                        "An error occurred creating the assets directory. The items will now only be in memory and will need to be redownloaded on restart. Error: {e}"
                    )
                }
            }
        }

        match tokio::fs::write(&client_jar_path, &client_jar_bytes).await {
            Ok(..) => {
                info!("Saved client.jar to disk");
                match tokio::fs::write(client_jar_version_path, latest_version.as_bytes()).await {
                    Ok(..) => info!("Successfully saved client.jar's version in a txt file"),
                    Err(e) => warn!(
                        "An error occurred when saving the version file for client.jar. This will result in it being redownloaded on restart. Error: {e}"
                    ),
                }
            }
            Err(e) => {
                warn!(
                    "An error occurred saving the client.jar to the local disk. The items will now only be in memory and will need to be redownloaded on restart. Error: {e}"
                )
            }
        };
    }

    let client_jar_bufreader = BufReader::new(
        File::open(client_jar_path)
            .await
            .expect("Unable to read client.jar"),
    )
    .compat_write();

    ZipFileReader::new(client_jar_bufreader)
        .await
        .expect("Failed to read the bufreader?? (Step 4 of item fetching / reading now)")
}
