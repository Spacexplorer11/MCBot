# MCBot
![Hackatime badge](https://hackatime.hackclub.com/api/v1/badge/U08D22QNUVD/Spacexplorer11/MCBot)  
This is a minecraft general-purpose bot that I have made for [Macondo!](https://macondo.hackclub.com/projects/9422)  

## Where can I test this?
You can test this in [#akaalroop-bot-testing](https://hackclub.enterprise.slack.com/archives/C09461MK0KZ)

## What commands/features are available:
- `/mcrecipe [recipe]` - you can use this command to get the crafting recipe for any minecraft item, even super new ones!
- `/mc-subs-config` - this opens a modal so you can configure subscriptions! You can add users and then receive instant DM's when they leave or join the hackclub minecraft server!

## Anything else?
This bot is now the backend for [@MCRecipes](https://github.com/spacexplorer11/mcrecipes) so if you do @MCRecipes \[item\] it will give you the recipe as well!

### Self-hosting
This project ain’t designed for self hosting just yet however if you edit the code or have an AI do it (on your fork obviously) to like disable the update subscriptions & MCRecipes features then you can just clone, run `cargo run` and provided that you’ve put env variables of SLACK_BOT_TOKEN and SLACK_SIGNING_SECRET then you can run it and if you’ve setup the commands then you can use /mcrecipe command and get recipes!

### Dependencies
Look at cargo.toml?  
I don’t remember them all but they’re mainly Axum, Tokio, Tracing, SQLx, Serde, Serde_json.