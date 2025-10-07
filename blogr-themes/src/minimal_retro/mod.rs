use crate::{ConfigOption, Theme, ThemeInfo, ThemeTemplates};
use ratatui::style::{Color, Style};
use std::collections::HashMap;

pub struct MinimalRetroTheme;

impl MinimalRetroTheme {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Theme for MinimalRetroTheme {
    fn info(&self) -> ThemeInfo {
        let mut config_schema = HashMap::new();

        config_schema.insert(
            "primary_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#FF6B35".to_string(),
                description: "Primary accent color (retro orange)".to_string(),
            },
        );

        config_schema.insert(
            "secondary_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#F7931E".to_string(),
                description: "Secondary accent color (warm amber)".to_string(),
            },
        );

        config_schema.insert(
            "background_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#2D1B0F".to_string(),
                description: "Background color (dark brown)".to_string(),
            },
        );

        config_schema.insert(
            "font_family".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "'Crimson Text', 'Playfair Display', Georgia, serif".to_string(),
                description: "Artistic serif font family".to_string(),
            },
        );

        config_schema.insert(
            "accent_font".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "'Space Mono', 'Courier Prime', monospace".to_string(),
                description: "Monospace accent font for tags and metadata".to_string(),
            },
        );

        config_schema.insert(
            "show_reading_time".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Display estimated reading time".to_string(),
            },
        );

        config_schema.insert(
            "show_author".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Display post author".to_string(),
            },
        );

        config_schema.insert(
            "expandable_posts".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Enable expandable post previews on homepage".to_string(),
            },
        );

        ThemeInfo {
            name: "Minimal Retro".to_string(),
            version: "2.0.0".to_string(),
            author: "Blogr Team".to_string(),
            description: "An artistic, minimal theme focused on content with expandable posts and beautiful typography".to_string(),
            config_schema,
            site_type: "blog".to_string(),
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

        // Main CSS file
        assets.insert(
            "css/style.css".to_string(),
            include_bytes!("assets/style.css").to_vec(),
        );

        // Optional: Add a retro favicon
        // assets.insert(
        //     "favicon.ico".to_string(),
        //     include_bytes!("assets/favicon.ico").to_vec()
        // );

        assets
    }

    fn preview_tui_style(&self) -> Style {
        Style::default()
            .fg(Color::Rgb(255, 107, 53)) // Retro orange
            .bg(Color::Rgb(45, 27, 15)) // Dark brown background
    }
}

impl Default for MinimalRetroTheme {
    fn default() -> Self {
        Self::new()
    }
}
