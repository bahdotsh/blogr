use crate::{ConfigOption, SiteType, Theme, ThemeInfo, ThemeTemplates};
use ratatui::style::{Color, Style};
use std::collections::HashMap;

pub struct SlatePortfolioTheme;

impl SlatePortfolioTheme {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Theme for SlatePortfolioTheme {
    fn info(&self) -> ThemeInfo {
        let mut config_schema = HashMap::new();

        config_schema.insert(
            "accent_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#10b981".to_string(),
                description: "Accent color (emerald)".to_string(),
            },
        );

        config_schema.insert(
            "background_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#0f172a".to_string(),
                description: "Background color (slate 900)".to_string(),
            },
        );

        config_schema.insert(
            "card_background".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#1e293b".to_string(),
                description: "Card background color (slate 800)".to_string(),
            },
        );

        config_schema.insert(
            "text_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#f1f5f9".to_string(),
                description: "Main text color (slate 100)".to_string(),
            },
        );

        config_schema.insert(
            "secondary_text_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#94a3b8".to_string(),
                description: "Secondary text color (slate 400)".to_string(),
            },
        );

        config_schema.insert(
            "font_family".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default:
                    "'IBM Plex Sans', -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif"
                        .to_string(),
                description: "Font family".to_string(),
            },
        );

        config_schema.insert(
            "show_avatar".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "false".to_string(),
                description: "Show avatar image in About section".to_string(),
            },
        );

        config_schema.insert(
            "avatar_url".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "".to_string(),
                description: "URL to avatar image".to_string(),
            },
        );

        config_schema.insert(
            "cta_text".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "View My Work".to_string(),
                description: "Call-to-action button text".to_string(),
            },
        );

        ThemeInfo {
            name: "Slate Portfolio".to_string(),
            version: "1.0.0".to_string(),
            author: "Blogr Team".to_string(),
            description: "A sleek, modern dark portfolio theme with polished aesthetics and smooth interactions.".to_string(),
            config_schema,
            site_type: SiteType::Personal,
        }
    }

    fn templates(&self) -> ThemeTemplates {
        ThemeTemplates::new("base.html", include_str!("templates/base.html"))
            .with_template("index.html", include_str!("templates/index.html"))
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
            .fg(Color::Rgb(16, 185, 129)) // Emerald
            .bg(Color::Rgb(15, 23, 42)) // Slate 900
    }
}

impl SlatePortfolioTheme {
    /// Get example content.md for this theme
    pub fn example_content(author: &str) -> String {
        format!(
            r##"---
title: "{}"
description: "Creative developer & design engineer"
author: "{}"
theme: "slate-portfolio"
theme_config:
  accent_color: "#10b981"
  show_avatar: false
  cta_text: "View My Work"
sections:
  about:
    title: "About"
    content: |
      <p>I'm a creative developer and design engineer who builds thoughtful,
      user-centered digital experiences. I specialize in transforming complex
      ideas into elegant, accessible interfaces.</p>

      <p>With a focus on clean code and beautiful design, I craft solutions
      that are both functional and delightful to use.</p>

  projects:
    title: "Selected Work"
    items:
      - title: "Quantum Dashboard"
        description: "A real-time analytics platform with beautiful data visualizations and seamless UX. Built for scale and performance."
        link: "https://github.com/yourusername/quantum"

      - title: "Neural Studio"
        description: "AI-powered creative tools that help designers and developers work faster and smarter."
        link: "https://github.com/yourusername/neural-studio"

      - title: "Flux Design System"
        description: "A comprehensive component library and design system powering modern web applications."
        link: "https://github.com/yourusername/flux"

      - title: "Prism Editor"
        description: "Next-generation code editor with AI assistance and collaborative features."
        link: "https://github.com/yourusername/prism"

  contact:
    title: "Let's Connect"
    text: "I'm currently available for freelance projects and full-time opportunities. Let's build something amazing together."
    email: "{}@example.com"
    social:
      github: "https://github.com/yourusername"
      twitter: "https://twitter.com/yourusername"
      linkedin: "https://linkedin.com/in/yourusername"
      blog: "https://yourblog.com"
---
"##,
            author,
            author,
            author.to_lowercase().replace(' ', "")
        )
    }
}

impl Default for SlatePortfolioTheme {
    fn default() -> Self {
        Self::new()
    }
}
