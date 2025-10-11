use crate::{ConfigOption, Theme, ThemeInfo, ThemeTemplates};
use ratatui::style::{Color, Style};
use std::collections::HashMap;

pub struct TerminalCandyTheme;

impl TerminalCandyTheme {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Theme for TerminalCandyTheme {
    fn info(&self) -> ThemeInfo {
        let mut config_schema = HashMap::new();

        config_schema.insert(
            "primary_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#FFB3D9".to_string(),
                description: "Primary accent color (pastel pink)".to_string(),
            },
        );

        config_schema.insert(
            "secondary_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#B4F8C8".to_string(),
                description: "Secondary accent color (mint green)".to_string(),
            },
        );

        config_schema.insert(
            "tertiary_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#C7CEEA".to_string(),
                description: "Tertiary accent color (lavender)".to_string(),
            },
        );

        config_schema.insert(
            "background_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#0D1117".to_string(),
                description: "Background color (dark terminal)".to_string(),
            },
        );

        config_schema.insert(
            "text_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#E6EDF3".to_string(),
                description: "Main text color (light)".to_string(),
            },
        );

        config_schema.insert(
            "font_family".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "'JetBrains Mono', 'Fira Code', 'Consolas', monospace".to_string(),
                description: "Monospace font family".to_string(),
            },
        );

        config_schema.insert(
            "enable_glitch".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Enable glitch effects on hover".to_string(),
            },
        );

        config_schema.insert(
            "enable_typewriter".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Enable typewriter animation for bio/description".to_string(),
            },
        );

        config_schema.insert(
            "show_ascii_art".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Show ASCII art decorations".to_string(),
            },
        );

        ThemeInfo {
            name: "Terminal Candy".to_string(),
            version: "1.0.0".to_string(),
            author: "Blogr Team".to_string(),
            description: "A quirky terminal-inspired theme with pastel colors, glitch effects, and playful animations. Perfect for personal websites.".to_string(),
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

        assets.insert(
            "css/style.css".to_string(),
            include_bytes!("assets/style.css").to_vec(),
        );

        assets
    }

    fn preview_tui_style(&self) -> Style {
        Style::default()
            .fg(Color::Rgb(255, 179, 217)) // Pastel pink
            .bg(Color::Rgb(13, 17, 23)) // Dark terminal background
    }
}

impl Default for TerminalCandyTheme {
    fn default() -> Self {
        Self::new()
    }
}
