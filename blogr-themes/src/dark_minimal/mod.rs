use crate::{ConfigOption, Theme, ThemeInfo, ThemeTemplates};
use ratatui::style::{Color, Style};
use std::collections::HashMap;

pub struct DarkMinimalTheme;

impl DarkMinimalTheme {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Theme for DarkMinimalTheme {
    fn info(&self) -> ThemeInfo {
        let mut config_schema = HashMap::new();

        config_schema.insert(
            "primary_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#00ff88".to_string(),
                description: "Primary accent color (neon green)".to_string(),
            },
        );

        config_schema.insert(
            "background_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#0a0a0a".to_string(),
                description: "Background color (pure dark)".to_string(),
            },
        );

        config_schema.insert(
            "text_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#e0e0e0".to_string(),
                description: "Main text color (soft white)".to_string(),
            },
        );

        config_schema.insert(
            "secondary_text_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#888888".to_string(),
                description: "Secondary text color (gray)".to_string(),
            },
        );

        config_schema.insert(
            "font_family".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default:
                    "'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif"
                        .to_string(),
                description: "Font family".to_string(),
            },
        );

        config_schema.insert(
            "enable_animations".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Enable smooth animations".to_string(),
            },
        );

        config_schema.insert(
            "show_social_icons".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Show social media icons".to_string(),
            },
        );

        config_schema.insert(
            "show_status_bar".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Show availability status bar".to_string(),
            },
        );

        config_schema.insert(
            "status_text".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "Available for opportunities".to_string(),
                description: "Custom text for status bar".to_string(),
            },
        );

        config_schema.insert(
            "status_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#00ff88".to_string(),
                description: "Status dot color (hex code)".to_string(),
            },
        );

        ThemeInfo {
            name: "Dark Minimal".to_string(),
            version: "1.0.0".to_string(),
            author: "Blogr Team".to_string(),
            description: "A dark, minimal theme for personal websites with quirky interactions and clean aesthetics.".to_string(),
            config_schema,
            site_type: "personal".to_string(),
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
            .fg(Color::Rgb(0, 255, 136)) // Neon green
            .bg(Color::Rgb(10, 10, 10)) // Pure dark
    }
}

impl DarkMinimalTheme {
    /// Get example content.md for this theme
    pub fn example_content(author: &str) -> String {
        format!(
            r##"---
title: "{}"
description: "Creative technologist & digital craftsperson"
author: "{}"
theme: "dark-minimal"
theme_config:
  enable_animations: true
  show_social_icons: true
  show_status_bar: true
  status_text: "Available for opportunities"
  status_color: "#00ff88"
sections:
  about:
    title: "About Me"
    content: |
      <p>I'm {}, a creative technologist passionate about building
      elegant solutions to complex problems. I love experimenting with new
      technologies and bringing ideas to life.</p>

      <p>My work sits at the intersection of design and engineering, where beautiful
      interfaces meet robust, scalable systems.</p>
    principles:
      - "Craft"
      - "Innovation"
      - "Impact"

  skills:
    title: "Skills & Expertise"
    items:
      - title: "Development"
        description: "Building scalable applications with modern tech stacks"
        tags:
          - "JavaScript"
          - "TypeScript"
          - "Python"
          - "Rust"

      - title: "Design"
        description: "Creating beautiful, user-friendly interfaces"
        tags:
          - "UI/UX"
          - "Design Systems"
          - "Figma"
          - "CSS"

      - title: "Innovation"
        description: "Pushing boundaries and exploring new possibilities"
        tags:
          - "AI/ML"
          - "Web3"
          - "DevOps"
          - "Cloud"

  projects:
    title: "Featured Work"
    items:
      - title: "Project Alpha"
        status: "Live"
        description: "A modern web application that solves real problems with elegant design and powerful features."
        tech:
          - "React"
          - "Node.js"
          - "PostgreSQL"
        link: "https://github.com/yourusername/project-alpha"

      - title: "Project Beta"
        status: "In Development"
        description: "An experimental tool pushing the boundaries of what's possible on the web."
        tech:
          - "Rust"
          - "WebAssembly"
          - "Three.js"

  contact:
    title: "Get In Touch"
    text: "I'm always open to new opportunities and collaborations. Feel free to reach out if you'd like to work together!"
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
            author,
            author.to_lowercase().replace(' ', "")
        )
    }
}

impl Default for DarkMinimalTheme {
    fn default() -> Self {
        Self::new()
    }
}
