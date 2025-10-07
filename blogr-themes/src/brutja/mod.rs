use crate::{ConfigOption, Theme, ThemeInfo, ThemeTemplates};
use ratatui::style::Style;
use std::collections::HashMap;

pub struct BrutjaTheme;

impl BrutjaTheme {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Theme for BrutjaTheme {
    fn info(&self) -> ThemeInfo {
        let mut schema = HashMap::new();
        schema.insert(
            "css".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "static/styles.css".to_string(),
                description: "Path to user CSS (served from /static/)".to_string(),
            },
        );

        schema.insert(
            "hero_title".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "Welcome".to_string(),
                description: "Homepage hero title".to_string(),
            },
        );

        schema.insert(
            "hero_subtitle".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "Customize your theme".to_string(),
                description: "Homepage hero subtitle".to_string(),
            },
        );

        schema.insert(
            "github_username".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: String::new(),
                description: "Your github username".to_string(),
            },
        );

        schema.insert(
            "linkedin_username".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: String::new(),
                description:
                    "The last segment of your linkedin profile URL. Do not include slashes."
                        .to_string(),
            },
        );

        ThemeInfo {
            name: "Brutja".to_string(),
            version: "1.0.0".to_string(),
            author: "Benjamin Corey".to_string(),
            description: "Brutalist, minimal theme with pops of color.".to_string(),
            config_schema: schema,
        }
    }

    fn templates(&self) -> ThemeTemplates {
        ThemeTemplates::new("base.html", include_str!("templates/base.html"))
            .with_template("post_card.html", include_str!("templates/post_card.html"))
            .with_template("index.html", include_str!("templates/index.html"))
            .with_template("post.html", include_str!("templates/post.html"))
            .with_template("archive.html", include_str!("templates/archive.html"))
            .with_template("tag.html", include_str!("templates/tag.html"))
            .with_template("tags.html", include_str!("templates/tags.html"))
    }

    fn assets(&self) -> HashMap<String, Vec<u8>> {
        let mut assets = HashMap::new();

        // Bundle the brutja theme defaults
        assets.insert(
            "css/brutja-default.css".to_string(),
            include_bytes!("assets/brutja-default.css").to_vec(),
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

impl Default for BrutjaTheme {
    fn default() -> Self {
        Self::new()
    }
}
