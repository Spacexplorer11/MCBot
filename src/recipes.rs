use crate::font::MinecraftFont;
use anyhow::{Context, Result, anyhow};
use async_zip::tokio::read::seek::ZipFileReader;
use image::{DynamicImage, ImageFormat, imageops};
use regex::Regex;
use reqwest::{Client, StatusCode};
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::HashSet;
use std::sync::Arc;
use std::{collections::HashMap, io::Cursor};
use strsim::levenshtein;
use tokio::{fs::File, io::BufReader, task::JoinSet};
use tracing::{info, trace};

#[derive(Deserialize)]
#[serde(tag = "type")]
enum MCRecipe {
    #[serde(rename = "minecraft:crafting_shaped")]
    Shaped {
        key: HashMap<String, RecipeIngredient>,
        pattern: Vec<String>,
        result: RecipeResult,
    },
    #[serde(rename = "minecraft:crafting_shapeless")]
    Shapeless {
        ingredients: Vec<String>,
        result: RecipeResult,
    },
    #[serde(rename = "minecraft:crafting_transmute")]
    Transmute {
        input: String,
        material: String,
        result: RecipeResult,
    },
}

#[derive(Deserialize)]
struct RecipeTag {
    values: Vec<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RecipeIngredient {
    Single(String),
    Multiple(Vec<String>),
}

#[derive(Deserialize)]
struct RecipeResult {
    #[serde(default)]
    count: u8,
    id: String,
}

impl RecipeResult {
    fn get_item(&self) -> &str {
        self.id
            .strip_prefix("minecraft:")
            .expect("Result doesn't start with 'minecraft:'")
    }

    fn get_pretty_item(&self) -> String {
        self.id
            .strip_prefix("minecraft:")
            .expect("Result doesn't start with 'minecraft:'")
            .replace("_", " ")
    }
}

#[derive(Default)]
pub struct RecipeData {
    items: HashMap<String, Vec<u8>>,
    tags: HashMap<String, Vec<String>>,
    language_mappings: HashMap<String, String>,
    pub valid_recipes: HashMap<String, usize>,
    font: MinecraftFont,
    recipe_links: HashMap<String, String>,
}

impl RecipeData {
    #[tracing::instrument(name = "fetching_on_startup_pipeline", skip(self, client_jar_zip))]
    pub async fn fetch_recipes_and_more(
        &mut self,
        client_jar_zip: &mut ZipFileReader<BufReader<File>>,
    ) -> Result<()> {
        let mut temp_items_map = HashMap::new();
        let mut temp_recipe_tags = HashMap::new();
        let mut language_map_index: Option<usize> = None;
        let mut font_indexes: Vec<Option<usize>> = Vec::new();

        // Oh no I'm not a pro dev :( I added comments. Soz but with 4 different if statements with such similar branch code i gotta do it

        for (i, file) in client_jar_zip.file().entries().iter().enumerate() {
            let filename = file
                .filename()
                .as_str()
                .context("Invalid UTF-8 Filename (Step 5 of item reading)")?;
            if filename.starts_with("data/minecraft/recipe") && filename.ends_with(".json") {
                // Fetch recipes from the jar
                let mut filename_parts = filename.split('/');
                let recipe_name = filename_parts
                    .next_back()
                    .unwrap()
                    .strip_suffix(".json")
                    .unwrap()
                    .to_string();

                self.valid_recipes.insert(recipe_name, i);
            } else if filename.eq("assets/minecraft/textures/gui/container/crafting_table.png") {
                // Get the crafting table GUI image from the jar
                let item_name = filename
                    .strip_prefix("assets/minecraft/textures/")
                    .unwrap()
                    .strip_suffix(".png")
                    .unwrap()
                    .to_string();
                temp_items_map.insert(item_name, i);
            } else if filename.eq("assets/minecraft/lang/en_us.json") {
                language_map_index = Some(i); // Get the language mapping file's index
            } else if filename.eq("assets/minecraft/font/include/default.json") {
                font_indexes.insert(0, Some(i)); // Get the font json's index
            } else if filename.eq("assets/minecraft/textures/font/ascii.png") {
                font_indexes.push(Some(i)); // Get the font image's index
            } else if filename.starts_with("assets/minecraft/textures/item") // Get the item images from the jar
                && filename.ends_with(".png")
            {
                let mut filename_parts = filename.split('/');
                let item_name = filename_parts
                    .next_back()
                    .unwrap()
                    .strip_suffix(".png")
                    .unwrap()
                    .to_string();

                temp_items_map.insert(item_name, i);
            } else if filename.starts_with("data/minecraft/tags/item")
                && filename.ends_with(".json")
            // Get tags json files
            {
                let mut filename_parts = filename.split('/');
                let tag_name = filename_parts
                    .next_back()
                    .unwrap()
                    .strip_suffix(".json")
                    .unwrap()
                    .to_string();

                temp_recipe_tags.insert(tag_name, i);
            }
        }
        info!("Saved recipes to recipe map");
        trace!("Saved the relevant things to their temporary maps");

        for (item, index) in temp_items_map {
            let mut item_png = client_jar_zip.reader_with_entry(index).await?;
            let mut item_png_bytes = Vec::new();

            item_png
                .read_to_end_checked(&mut item_png_bytes)
                .await
                .context(format!("Failed to convert image {item}"))?;
            self.items.insert(item, item_png_bytes);
        }
        info!("Saved items to the item map");

        for (tag, index) in temp_recipe_tags {
            let mut tag_reader = client_jar_zip.reader_with_entry(index).await?;
            let mut tag_value_string = String::new();

            tag_reader
                .read_to_string_checked(&mut tag_value_string)
                .await
                .context(format!("Failed to read tag {tag}"))?;

            let tag_values: RecipeTag =
                serde_json::from_str(&tag_value_string).context("Unable to convert tag to json")?;

            self.tags.insert(tag, tag_values.values);
        }
        info!("Saved tags to tags map");

        let mut language_map_reader = client_jar_zip
            .reader_with_entry(
                language_map_index.context("Failed to find language mappings index")?,
            )
            .await
            .context("Failed to read language mappings")?;
        let mut language_map_string = String::new();

        match language_map_reader
            .read_to_string_checked(&mut language_map_string)
            .await
        {
            Ok(..) => drop(language_map_reader),
            Err(e) => panic!("Failed to read en-us.json {e}"),
        }

        let raw_lang: HashMap<String, String> =
            serde_json::from_str(&language_map_string).context("Unable to parse en-us.json")?;

        for (key, value) in raw_lang {
            if key.starts_with("item.minecraft.") {
                let item_id = key.strip_prefix("item.minecraft.").unwrap().to_string();
                self.language_mappings.insert(item_id, value);
            } else if key.starts_with("block.minecraft.") {
                let block_id = key.strip_prefix("block.minecraft.").unwrap().to_string();
                self.language_mappings.insert(block_id, value);
            }
        }
        info!("Saved language mappings to language mappings map");

        self.font = match MinecraftFont::initialise(font_indexes, client_jar_zip).await {
            Ok(mcfont) => {
                info!("Successfully initialised font (fetched the bitmap + image)");
                mcfont
            }
            Err(e) => panic!("Failed to initialise font {e}"),
        };

        Ok(())
    }

    #[tracing::instrument(
        name = "recipe_generation_pipeline",
        skip(self, client, bot_token, client_jar_zip)
    )]
    pub async fn process_recipe(
        &mut self,
        item_name: &str,
        client: &Client,
        bot_token: &str,
        channel_id: &str,
        user_id: &str,
        client_jar_zip: &mut ZipFileReader<BufReader<File>>,
    ) -> Result<()> {
        let recipe_index = self
            .valid_recipes
            .get(item_name)
            .context("Somehow the recipe doesn't exist in the valid recipes map even tho it was previously validated")?;
        let mut recipe = client_jar_zip
            .reader_with_entry(*recipe_index)
            .await
            .context("Unable to find recipe")?;
        let mut recipe_string = String::new();
        recipe
            .read_to_string_checked(&mut recipe_string)
            .await
            .context("Unable to read the recipe file")?;

        let recipe_json: MCRecipe = serde_json::from_str(&recipe_string)
            .context("Unable to convert the json to MCRecipe type")?;

        drop(recipe);
        drop(recipe_string);

        match recipe_json {
            MCRecipe::Shaped {
                key,
                mut pattern,
                result,
            } => {
                let mut items_placement = Vec::new();
                if pattern.len() == 1 {
                    pattern.insert(0, "   ".to_string());
                    pattern.push("   ".to_string());
                } else if pattern.len() == 2 {
                    pattern.push("   ".to_string());
                }
                for mut part in pattern {
                    if part.chars().count() == 1 {
                        part.insert(0, ' ');
                        part.push(' ');
                    } else if part.chars().count() == 2 {
                        part.push(' ');
                    }

                    for char in part.chars() {
                        let item;
                        if !char.is_whitespace() {
                            let item_or_tag: &String = match key
                                .get(char.to_string().as_str())
                                .context("Character key missing from recipe definition")?
                            {
                                RecipeIngredient::Single(key) => key,
                                RecipeIngredient::Multiple(key) => &key[0],
                            };

                            if item_or_tag.starts_with("#minecraft:") {
                                let tag = item_or_tag.strip_prefix("#minecraft:").unwrap();

                                let mut tag_possible_items =
                                    self.tags.get(tag).context("Unable to find tag in tags")?;

                                while !tag_possible_items[0].starts_with("minecraft:") {
                                    let tag = tag_possible_items[0]
                                        .strip_prefix("#minecraft:")
                                        .context("The tag... didn't start with a #")?;
                                    tag_possible_items = self
                                        .tags
                                        .get(tag)
                                        .context("Unable to find nested tag in tags")?;
                                }
                                item = tag_possible_items[0]
                                    .as_str()
                                    .strip_prefix("minecraft:")
                                    .context("The loop failed somehow or the item doesn't begin with 'minecraft:'")?;
                            } else {
                                item = item_or_tag.strip_prefix("minecraft:").unwrap_or(" ");
                            }
                        } else {
                            item = " ";
                        }
                        items_placement.push(item.to_string());
                    }
                }

                Self::make_and_send_image_to_slack(
                    self,
                    client,
                    bot_token,
                    channel_id,
                    user_id,
                    &result,
                    items_placement,
                )
                .await?
            }
            MCRecipe::Shapeless {
                ingredients,
                result,
            } => {
                let mut items_to_place = Vec::new();
                for ingredient in ingredients {
                    let item: &str;
                    if ingredient.starts_with("#minecraft:") {
                        let tag = ingredient.strip_prefix("#minecraft:").unwrap();

                        let mut tag_possible_items =
                            self.tags.get(tag).context("Unable to find tag in tags")?;

                        while !tag_possible_items[0].starts_with("minecraft:") {
                            let tag = tag_possible_items[0]
                                .strip_prefix("#minecraft:")
                                .context("The tag... didn't start with a #")?;
                            tag_possible_items = self
                                .tags
                                .get(tag)
                                .context("Unable to find nested tag in tags")?;
                        }
                        item = tag_possible_items[0]
                            .as_str()
                            .strip_prefix("minecraft:")
                            .context("The loop failed somehow or the item doesn't begin with 'minecraft:'")?;
                    } else {
                        item = ingredient.strip_prefix("minecraft:").unwrap_or(" ");
                    }
                    items_to_place.push(item.to_string());
                }

                Self::make_and_send_image_to_slack(
                    self,
                    client,
                    bot_token,
                    channel_id,
                    user_id,
                    &result,
                    items_to_place,
                )
                .await?
            }
            MCRecipe::Transmute {
                input,
                material,
                result,
            } => {
                let mut items_to_place = Vec::new();
                let mut item: &str;
                if input.starts_with("#minecraft:") {
                    let tag = input.strip_prefix("#minecraft:").unwrap();

                    let mut tag_possible_items =
                        self.tags.get(tag).context("Unable to find tag in tags")?;

                    while !tag_possible_items[0].starts_with("minecraft:") {
                        let tag = tag_possible_items[0]
                            .strip_prefix("#minecraft:")
                            .context("The tag... didn't start with a #")?;
                        tag_possible_items = self
                            .tags
                            .get(tag)
                            .context("Unable to find nested tag in tags")?;
                    }
                    item = tag_possible_items[0]
                        .as_str()
                        .strip_prefix("minecraft:")
                        .context("The item doesn't begin with 'minecraft:'")?;
                } else {
                    item = input.strip_prefix("minecraft:").unwrap_or(" ");
                }
                items_to_place.push(item.to_string());

                if material.starts_with("#minecraft:") {
                    let tag = material.strip_prefix("#minecraft:").unwrap();

                    let mut tag_possible_items =
                        self.tags.get(tag).context("Unable to find tag in tags")?;

                    while !tag_possible_items[0].starts_with("minecraft:") {
                        let tag = tag_possible_items[0]
                            .strip_prefix("#minecraft:")
                            .context("The tag... didn't start with a #")?;
                        tag_possible_items = self
                            .tags
                            .get(tag)
                            .context("Unable to find nested tag in tags")?;
                    }
                    item = tag_possible_items[0]
                        .as_str()
                        .strip_prefix("minecraft:")
                        .context("The item doesn't begin with 'minecraft:'")?;
                } else {
                    item = material.strip_prefix("minecraft:").unwrap_or(" ");
                }
                items_to_place.push(item.to_string());

                Self::make_and_send_image_to_slack(
                    self,
                    client,
                    bot_token,
                    channel_id,
                    user_id,
                    &result,
                    items_to_place,
                )
                .await?
            }
        }

        Ok(())
    }

    async fn make_and_send_image_to_slack(
        &mut self,
        client: &Client,
        bot_token: &str,
        channel_id: &str,
        user_id: &str,
        result: &RecipeResult,
        recipe_ingredients: Vec<String>,
    ) -> Result<()> {
        let recipe_link = self.recipe_links.get(result.get_item());
        if let Some(recipe_link) = recipe_link {
            let response = client.post("https://slack.com/api/chat.postMessage")
                .bearer_auth(bot_token)
                .json(&json!({"channel": channel_id, "text": format!("<@{}> Here's your {} recipe!\n {}", user_id, result.get_pretty_item(), recipe_link), "unfurl_links": true, "unfurl_media": true}))
                .send()
                .await?;
            trace!("Successfully sent the file link, saving precious compute time!");

            let response_json: Value = response.json().await?;
            let message_ts = response_json
                .get("ts")
                .context("Missing ts in postMessage response")?
                .as_str()
                .context("Unable to convert ts to string")?
                .to_string();

            let is_valid = client
                .head(recipe_link)
                .send()
                .await?
                .status()
                .eq(&StatusCode::OK);

            if !is_valid {
                trace!("Oh no invalid link detected!");
                self.recipe_links.remove(result.get_item());

                client
                    .post("https://slack.com/api/chat.update")
                    .bearer_auth(bot_token)
                    .json(&json!({
            "channel": channel_id,
            "ts": message_ts,
            "text": "Whoops, looks like that link is invalid! Please run the command again to get a fresh image!"
        }))
                    .send()
                    .await?;
            }

            return Ok(());
        }

        let crafting_table_gui_bytes = self
            .items
            .get("gui/container/crafting_table")
            .context("Unable to find crafting table grid in items vector")?;
        let crafting_table_gui = image::load_from_memory(crafting_table_gui_bytes)
            .context("Unable to make an image from the crafting table bytes")?;

        let crafting_table_gui = crafting_table_gui.crop_imm(0, 0, 170, 80);
        let mut crafting_table_gui =
            imageops::resize(&crafting_table_gui, 340, 160, imageops::FilterType::Nearest);

        let grid_origin_x = 60;
        let grid_origin_y = 33;
        let cell_size = 36; // +2 for the border

        let mut missing_items = HashSet::new();
        for item in &recipe_ingredients {
            if self.items.get(item).is_none() && !item.eq(" ") {
                missing_items.insert(item.to_string());
            }
        }
        if self.items.get(result.get_item()).is_none() {
            missing_items.insert(result.get_item().to_string());
        }

        let language_mappings = Arc::new(self.language_mappings.clone());

        let mut set = JoinSet::new();
        for item in missing_items {
            let lang_mappings = language_mappings.clone();
            let client = client.clone();
            set.spawn(async move {
                let lang_mapped_item = lang_mappings
                    .get(item.as_str())
                    .unwrap_or(&item)
                    .replace(' ', "_");
                fallback_fetch_from_wiki(client, item.clone(), lang_mapped_item.clone()).await
            });
        }

        while let Some(result) = set.join_next().await {
            let item_result = result?;
            let (item, bytes) = item_result?;
            self.items.insert(item, bytes);
        }

        let mut i = 0;
        for row in 0..3 {
            for col in 0..3 {
                let cell_x = grid_origin_x + (col * cell_size);
                let cell_y = grid_origin_y + (row * cell_size);

                if recipe_ingredients.get(i).is_some()
                    && !recipe_ingredients.get(i).unwrap().eq(" ")
                {
                    let item_bytes = match self.items.get(&recipe_ingredients[i]) {
                        Some(bytes) => bytes,
                        None => {
                            return Err(anyhow!(
                                "Couldn't find item {} in items somehow?",
                                recipe_ingredients[i]
                            ));
                        }
                    };
                    let item_texture_img =
                        image::load_from_memory_with_format(item_bytes, ImageFormat::Png)
                            .context("Unable to make an image from an item's bytes")?
                            .to_rgba8();

                    let item_texture_img =
                        imageops::resize(&item_texture_img, 32, 32, imageops::FilterType::Nearest);

                    imageops::overlay(&mut crafting_table_gui, &item_texture_img, cell_x, cell_y);
                }

                i += 1;

                if i == 9 {
                    let result_x = cell_x + 107; // magic number obtained through trial and error
                    let result_y = 62;
                    let item_bytes = match self.items.get(result.get_item()) {
                        Some(bytes) => bytes,
                        None => {
                            return Err(anyhow!(
                                "Couldn't find item {} in items somehow?",
                                result.get_item()
                            ));
                        }
                    };
                    let item_texture_img = image::load_from_memory(item_bytes)
                        .context("Unable to make an image from an item's bytes")?
                        .to_rgba8();

                    let mut item_texture_img =
                        imageops::resize(&item_texture_img, 48, 48, imageops::FilterType::Nearest);

                    let mut count_images: Vec<Option<DynamicImage>> = Vec::new();
                    if result.count > 1 {
                        trace!("Result count is greater than 1, adding count to image");
                        if result.count > 9 {
                            let result_count_as_string = result.count.to_string();
                            for char in result_count_as_string.chars() {
                                for (i, line) in self.font.bitmap.iter().enumerate() {
                                    if line.contains(char.to_string().as_str()) {
                                        let count_image = self
                                            .font
                                            .get_character_image(i, char.to_string())
                                            .await?;
                                        let count_image = imageops::resize(
                                            &count_image,
                                            18,
                                            18,
                                            imageops::FilterType::Nearest,
                                        );
                                        count_images.push(Some(count_image.into()));
                                    }
                                }
                            }
                        } else {
                            for (i, line) in self.font.bitmap.iter().enumerate() {
                                if line.contains(result.count.to_string().as_str()) {
                                    let count_image = self
                                        .font
                                        .get_character_image(i, result.count.to_string())
                                        .await?;
                                    let count_image = imageops::resize(
                                        &count_image,
                                        18,
                                        18,
                                        imageops::FilterType::Nearest,
                                    );
                                    count_images.push(Some(count_image.into()));
                                }
                            }
                        }
                    }

                    let mut i = 0;
                    for count_image in count_images.iter().rev() {
                        if count_image.is_some() {
                            let x = 34_i64.checked_sub((i * 14) as i64).context("How the hell did this happen? The x position for the count image exceeded the bounds of I64. HOW?!?")?;
                            let y = 33;

                            imageops::overlay(
                                &mut item_texture_img,
                                count_image.as_ref().unwrap(),
                                x,
                                y,
                            );
                            i += 1;
                        }
                    }

                    imageops::overlay(
                        &mut crafting_table_gui,
                        &item_texture_img,
                        result_x,
                        result_y,
                    );
                }
            }
        }

        let mut bytes_to_send_to_slack = Vec::new(); // lovely name I know thank you
        crafting_table_gui
            .write_to(
                &mut Cursor::new(&mut bytes_to_send_to_slack),
                ImageFormat::WebP,
            )
            .context("Failed to convert the image back into bytes")?;

        trace!("Fetching upload URL from Slack (Step 1 of file upload)");
        let upload_url_response = client
            .post("https://slack.com/api/files.getUploadURLExternal")
            .bearer_auth(bot_token)
            .form(&[
                ("filename", format!("{}_recipe.webp", result.get_item())),
                ("length", bytes_to_send_to_slack.len().to_string()),
            ])
            .send()
            .await
            .context("Failed to ask for crafting recipe file upload url from slack")?
            .error_for_status()
            .context("Slack returned an error when asking for the upload url (Step 1)")?;

        let upload_data: Value = upload_url_response
            .json()
            .await
            .context("Unable to convert the upload url response into json")?;
        let upload_url = upload_data["upload_url"]
            .as_str()
            .context("Couldn't find the upload url")?;
        let file_id = upload_data["file_id"]
            .as_str()
            .context("Couldn't find the file id")?;

        trace!("Uploading crafting recipe file bytes to Slack (Step 2 of file upload)");
        client
            .post(upload_url)
            .body(bytes_to_send_to_slack)
            .send()
            .await
            .context("Failed to upload crafting recipe file bytes to slack")?
            .error_for_status()
            .context("Slack returned an error when uploading the file (Step 2)")?;

        trace!("Completing the file upload (Step 3 of file upload)");
        let complete_upload_response = client
            .post("https://slack.com/api/files.completeUploadExternal")
            .bearer_auth(bot_token)
            .json(&json!({
                        "files": [{ "id": file_id, "title": format!("{} recipe", result.get_pretty_item()) }],
                        "channel_id": channel_id,
                        "initial_comment": format!("<@{}> Here's your {} recipe!", user_id, result.get_pretty_item())
                    }))
            .send()
            .await
            .context("Unable to send the completion request for the file")?
            .error_for_status().context("Slack returned an error when completing the upload (Step 3)")?;

        trace!("Converting the upload completion response to bytes then JSON");
        let complete_upload_response_bytes = complete_upload_response
            .bytes()
            .await
            .context("Unable to convert the response to json")?;
        let complete_upload_response_json: Value =
            serde_json::from_slice(&complete_upload_response_bytes)
                .context("Unable to convert the response to json")?;

        trace!("Getting the permalink");
        let files_array = complete_upload_response_json
            .get("files")
            .context("Unable to find the 'files' key in the response")?;
        let permalink = files_array[0]
            .get("permalink")
            .context("Unable to find the 'permalink_public' key in the response")?
            .as_str()
            .context("Unable to convert the 'permalink' key to a string")?
            .to_string();

        self.recipe_links
            .insert(result.get_item().to_string(), permalink);
        trace!("Added the permalink to the array of recipe links");

        Ok(())
    }
}

pub fn fix_recipe_typo(
    valid_recipes: &HashMap<String, usize>,
    recipe_to_fix: &str,
) -> Option<String> {
    let mut lowest_distance = usize::MAX;
    let mut closest_recipe: Option<String> = None;
    for recipe in valid_recipes.keys() {
        let distance = levenshtein(recipe, recipe_to_fix);

        if distance < lowest_distance && distance <= 3 {
            // Max edits
            closest_recipe = Some(recipe.clone());
            lowest_distance = distance;
        }
    }
    closest_recipe
}

pub fn fix_recipe(recipe: &str) -> String {
    // Matches any whitespace (\s), dashes (\-), forward slashes (/), or backslashes (\\)
    let re = Regex::new(r"[\s\-/\\]+").unwrap();

    re.replace_all(recipe.to_lowercase().as_str(), "_")
        .into_owned()
}

async fn fallback_fetch_from_wiki(
    client: Client,
    item: String,
    lang_mapped_item: String,
) -> Result<(String, Vec<u8>)> {
    let response = client
        .get(format!(
            "https://minecraft.wiki/images/Invicon_{}.png",
            lang_mapped_item
        ))
        .header("User-Agent", "MCBot")
        .send()
        .await
        .context("Unable to get image from wiki")?;
    if !response.status().eq(&StatusCode::OK) {
        return Err(anyhow!(
            "Failed to get image from wiki: {}",
            response.status().as_u16()
        ));
    }
    let item_bytes = response
        .bytes()
        .await
        .context("Unable to convert the wiki's response to bytes")?
        .to_vec();
    Ok((item, item_bytes))
}
