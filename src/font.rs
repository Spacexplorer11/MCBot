use anyhow::{Context, Result};
use async_zip::tokio::read::seek::ZipFileReader;
use image::{DynamicImage, load_from_memory};
use serde_json::Value;
use tokio::{fs::File, io::BufReader};

#[derive(Default)]
pub struct MinecraftFont {
    pub bitmap: Vec<String>,
    image: DynamicImage,
}

impl MinecraftFont {
    #[tracing::instrument(name = "fetching_font_pipeline", skip(font_indexes, file))]
    pub async fn initialise(
        font_indexes: Vec<Option<usize>>,
        file: &mut ZipFileReader<BufReader<File>>,
    ) -> Result<MinecraftFont> {
        let mut font_bitmap_reader = file
            .reader_with_entry(font_indexes[0].context("Unable to find font bitmap index")?)
            .await
            .context("Unable to get the font bitmap reader")?;
        let mut font_bitmap_string = String::new();

        font_bitmap_reader
            .read_to_string_checked(&mut font_bitmap_string)
            .await
            .context("Failed to read font/default.json.")?;

        let font_bitmap_json: Value = serde_json::from_str(&font_bitmap_string)
            .context("Unable to parse font/default.json")?;
        drop(font_bitmap_reader);
        drop(font_bitmap_string);
        let font_bitmap: Vec<String> = font_bitmap_json["providers"][2]["chars"]
            .as_array()
            .context("Bitmap is not an array?")?
            .iter()
            .map(|bitmap| {
                Value::as_str(bitmap)
                    .expect("Couldn't convert a bit in the map to string")
                    .to_string()
            })
            .collect();
        drop(font_bitmap_json);
        let mut font_image_reader = file
            .reader_with_entry(font_indexes[1].context("Unable to find font image's index")?)
            .await
            .context("Unable to get the font image reader")?;
        let mut font_image_bytes = Vec::new();
        font_image_reader
            .read_to_end_checked(&mut font_image_bytes)
            .await
            .context("Failed to read font/ascii.png.")?;
        drop(font_image_reader);
        let font_image =
            load_from_memory(&font_image_bytes).context("Unable to load font/ascii.png")?;

        Ok(MinecraftFont {
            bitmap: font_bitmap,
            image: font_image,
        })
    }

    #[tracing::instrument(
        name = "fetching_font_character_pipeline",
    skip(self),
    fields(index = %index, character = %character)
    )]
    pub fn get_character_image(&self, index: usize, character: String) -> Result<DynamicImage> {
        let line = &self.bitmap[index];
        for (i, bit) in line.chars().enumerate() {
            if bit.to_string().eq(&character) {
                let x = i * 8;
                let y = index * 8; // The images/letters/characters idk what are 8 x 8px
                return Ok(self.image.crop_imm(x as u32, y as u32, 8, 8));
            }
        }
        Err(anyhow::anyhow!("Character not found in font"))
    }
}
