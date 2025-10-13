use crate::{ConfigOption, SiteType, Theme, ThemeInfo, ThemeTemplates};
use ratatui::style::Style;
use std::collections::HashMap;

pub struct ObsidianTheme;

impl ObsidianTheme {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Theme for ObsidianTheme {
    fn info(&self) -> ThemeInfo {
        let mut schema = HashMap::new();

        schema.insert(
            "obsidian_css".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "static/obsidian.css".to_string(),
                description: "Path to Obsidian CSS (served from /static/)".to_string(),
            },
        );

        schema.insert(
            "color_mode".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "auto".to_string(),
                description: "Dark/light mode handling (auto | dark | light)".to_string(),
            },
        );

        ThemeInfo {
            name: "Obsidian".to_string(),
            version: "1.0.0".to_string(),
            author: "Blogr Team".to_string(),
            description: "Adopts Obsidian community themes to style Blogr content".to_string(),
            config_schema: schema,
            site_type: SiteType::Blog,
        }
    }

    fn templates(&self) -> ThemeTemplates {
        ThemeTemplates::new("base.html", include_str!("templates/base.html"))
            .with_template("index.html", include_str!("templates/index.html"))
            .with_template("post.html", include_str!("templates/post.html"))
            .with_template("archive.html", include_str!("templates/archive.html"))
            .with_template("tag.html", include_str!("templates/tag.html"))
            .with_template("tags.html", include_str!("templates/tags.html"))
    }

    fn assets(&self) -> HashMap<String, Vec<u8>> {
        let mut assets = HashMap::new();

        // Bundle a default Obsidian-compatible CSS
        assets.insert(
            "css/obsidian-default.css".to_string(),
            include_bytes!("assets/obsidian-default.css").to_vec(),
        );

        assets
    }

    fn preview_tui_style(&self) -> Style {
        use ratatui::style::Color;
        Style::default()
            .fg(Color::Rgb(167, 139, 250)) // Obsidian purple accent
            .bg(Color::Rgb(32, 32, 32)) // Dark background similar to Obsidian
    }
}

impl Default for ObsidianTheme {
    fn default() -> Self {
        Self::new()
    }
}
