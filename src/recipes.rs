use anyhow::{Context, Result};
use async_zip::tokio::read::seek::ZipFileReader;
use image::{ImageFormat, imageops};
use reqwest::Client;
use serde::Deserialize;
use serde_json::json;
use std::collections::HashMap;
use std::io::Cursor;
use tokio::fs::File;
use tokio::io::BufReader;
use tracing::info;

#[derive(Deserialize)]
#[serde(tag = "type")]
enum MCRecipe {
    #[serde(rename = "minecraft:crafting_shaped")]
    Shaped {
        key: HashMap<String, String>,
        pattern: Vec<String>,
        result: RecipeResult,
    },
    #[serde(rename = "minecraft:crafting_shapeless")]
    Shapeless {
        ingredients: Vec<String>,
        result: RecipeResult,
    },
}

#[derive(Deserialize)]
struct RecipeTag {
    values: Vec<String>,
}

#[derive(Deserialize)]
struct RecipeResult {
    #[serde(default)]
    count: u8,
    id: String,
}

pub struct RecipeData {
    items: HashMap<String, Vec<u8>>,
    tags: HashMap<String, Vec<String>>,
    language_mappings: HashMap<String, String>,
    pub valid_recipes: HashMap<String, usize>,
}

impl Default for RecipeData {
    fn default() -> Self {
        Self::new()
    }
}

impl RecipeData {
    fn new() -> RecipeData {
        RecipeData {
            items: HashMap::new(),
            tags: HashMap::new(),
            language_mappings: HashMap::new(),
            valid_recipes: HashMap::new(),
        }
    }

    pub async fn fetch_recipes_and_more(
        &mut self,
        client_jar_zip: &mut ZipFileReader<BufReader<File>>,
    ) -> Result<()> {
        let mut temp_items_map = HashMap::new();
        let mut temp_recipe_tags = HashMap::new();
        let mut language_map_index = 0;

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
                language_map_index = i; // Get the language mapping file's index
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

        for item in temp_items_map {
            let mut item_png = client_jar_zip.reader_with_entry(item.1).await?;
            let mut item_png_bytes = Vec::new();

            item_png
                .read_to_end_checked(&mut item_png_bytes)
                .await
                .context(format!("Failed to convert image {}", item.0))?;
            self.items.insert(item.0, item_png_bytes);
        }

        for tag in temp_recipe_tags {
            let mut tag_reader = client_jar_zip.reader_with_entry(tag.1).await?;
            let mut tag_value_string = String::new();

            tag_reader
                .read_to_string_checked(&mut tag_value_string)
                .await
                .context(format!("Failed to read tag {}", tag.0))?;

            let tag_values: RecipeTag =
                serde_json::from_str(&tag_value_string).context("Unable to convert tag to json")?;

            self.tags.insert(tag.0, tag_values.values);
        }

        let mut language_map_reader = client_jar_zip.reader_with_entry(language_map_index).await?;
        let mut language_map_string = String::new();

        match language_map_reader
            .read_to_string_checked(&mut language_map_string)
            .await
        {
            Ok(..) => (),
            Err(e) => panic!("Failed to read en-us.json {}", e),
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
        info!("Saved recipes to recipe map");
        info!("Saved items to the item map");
        info!("Saved tags to tags map");

        Ok(())
    }

    #[tracing::instrument(
        name = "recipe_generation_pipeline",
        skip(self, client, bot_token, client_jar_zip),
        fields(item = %item_name, channel = %channel_id)
    )]
    pub async fn make_and_send_recipe_image(
        &mut self,
        item_name: String,
        client: &Client,
        bot_token: &str,
        channel_id: String,
        user_id: String,
        client_jar_zip: &mut ZipFileReader<BufReader<File>>,
    ) -> Result<()> {
        let recipe_index = self
            .valid_recipes
            .get(&item_name)
            .context("How on earth did this happen (1)")?;
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

        let mut items_placement = Vec::new();
        if let MCRecipe::Shaped {
            key,
            mut pattern,
            result,
        } = recipe_json
        {
            if pattern.len() < 3 {
                while pattern.len() != 3 {
                    pattern.push("   ".to_string());
                }
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
                        let item_or_tag = key
                            .get(char.to_string().as_str())
                            .context("Character key missing from recipe definition")?;

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
                    items_placement.push(item);
                }
            }

            let crafting_table_gui_bytes = self
                .items
                .get("gui/container/crafting_table")
                .context("Unable to find crafting table grid in items vector")?;
            let crafting_table_gui = image::load_from_memory(crafting_table_gui_bytes)
                .context("Unable to make an image from the crafting table bytes")?;

            let crafting_table_gui = crafting_table_gui.crop_imm(0, 0, 170, 80);
            let mut crafting_table_gui =
                imageops::resize(&crafting_table_gui, 340, 160, imageops::Nearest);

            let grid_origin_x = 60;
            let grid_origin_y = 33;
            let cell_size = 36; // +2 for the border

            let mut i = 0;
            for row in 0..3 {
                for col in 0..3 {
                    let cell_x = grid_origin_x + (col * cell_size);
                    let cell_y = grid_origin_y + (row * cell_size);

                    if items_placement[i] != " " {
                        let item_bytes = match self.items.get(items_placement[i]) {
                            Some(bytes) => bytes.clone(),
                            None => {
                                //DEBUG: info!("https://minecraft.wiki/images/Invicon_{}.png", capitalise_words( items_placement[i].to_string() ));
                                let response = client
                                    .get(format!(
                                        "https://minecraft.wiki/images/Invicon_{}.png",
                                        self.language_mappings
                                            .get(items_placement[i])
                                            .context("Unable to find item/block language mapping")?
                                            .replace(' ', "_")
                                    ))
                                    .header("User-Agent", "MCBot")
                                    .send()
                                    .await
                                    .context("Unable to get image from wiki")?;
                                response
                                    .bytes()
                                    .await
                                    .context("Unable to convert the wiki's response to bytes")?
                                    .to_vec()
                            }
                        };
                        let item_texture_img =
                            image::load_from_memory_with_format(&item_bytes, ImageFormat::Png)
                                .context("Unable to make an image from an item's bytes")?
                                .to_rgba8();

                        let item_texture_img = imageops::resize(
                            &item_texture_img,
                            32,
                            32,
                            imageops::FilterType::Nearest,
                        );

                        imageops::overlay(
                            &mut crafting_table_gui,
                            &item_texture_img,
                            cell_x,
                            cell_y,
                        );
                    }

                    i += 1;

                    if i == 9 {
                        let result_x = cell_x + 107; // magic number obtained through trial and error
                        let result_y = 62;
                        let item_bytes = match self.items.get(&item_name) {
                            Some(bytes) => bytes,
                            None => {
                                //DEBUG: info!("https://minecraft.wiki/images/Invicon_{}.png", capitalise_words( items_placement[i].to_string() ));
                                let response = client
                                    .get(format!(
                                        "https://minecraft.wiki/images/Invicon_{}.png",
                                        self.language_mappings
                                            .get(&item_name)
                                            .context("Unable to find item/block language mapping")?
                                            .replace(' ', "_")
                                    ))
                                    .header("User-Agent", "MCBot")
                                    .send()
                                    .await
                                    .context("Unable to get image from wiki")?;
                                &response
                                    .bytes()
                                    .await
                                    .context("Unable to convert the wiki's response to bytes")?
                                    .to_vec()
                            }
                        };
                        let item_texture_img = image::load_from_memory(item_bytes)
                            .context("Unable to make an image from an item's bytes")?
                            .to_rgba8();

                        let item_texture_img = imageops::resize(
                            &item_texture_img,
                            48,
                            48,
                            imageops::FilterType::Nearest,
                        );

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
                    ImageFormat::Png,
                )
                .context("Failed to convert the image back into bytes")?;

            let upload_url_response = client
                .post("https://slack.com/api/files.getUploadURLExternal")
                .bearer_auth(bot_token)
                .form(&[
                    ("filename", format!("{item_name}_recipe.png")),
                    ("length", bytes_to_send_to_slack.len().to_string()),
                ])
                .send()
                .await
                .context("Failed to ask for crafting recipe file upload url from slack")?;

            let upload_data: serde_json::Value = upload_url_response
                .json()
                .await
                .context("Unable to convert the upload url response into json")?;
            let upload_url = upload_data["upload_url"]
                .as_str()
                .context("Couldn't find the upload url")?;
            let file_id = upload_data["file_id"]
                .as_str()
                .context("Couldn't find the file id")?;

            client
                .post(upload_url)
                .body(bytes_to_send_to_slack)
                .send()
                .await
                .context("Failed to upload crafting recipe file bytes to slack")?;

            client
                .post("https://slack.com/api/files.completeUploadExternal")
                .bearer_auth(bot_token)
                .json(&json!({
                    "files": [{ "id": file_id, "title": "Recipe" }],
                    "channel_id": channel_id,
                    "initial_comment": format!("<@{}> Here's your {} recipe!", user_id, item_name.clone().replace('_', " "))
                }))
                .send()
                .await
                .context("Unable to send the completion request for the file")?;
        }

        Ok(())
    } // Add more later obvs
}
