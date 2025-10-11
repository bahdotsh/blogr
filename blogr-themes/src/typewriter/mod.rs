use crate::{ConfigOption, Theme, ThemeInfo, ThemeTemplates};
use ratatui::style::{Color, Style};
use std::collections::HashMap;

pub struct TypewriterTheme;

impl TypewriterTheme {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Theme for TypewriterTheme {
    fn info(&self) -> ThemeInfo {
        let mut config_schema = HashMap::new();

        config_schema.insert(
            "paper_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#f4f1e8".to_string(),
                description: "Paper background color (vintage cream)".to_string(),
            },
        );

        config_schema.insert(
            "ink_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#2b2b2b".to_string(),
                description: "Text/ink color (dark charcoal)".to_string(),
            },
        );

        config_schema.insert(
            "accent_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#8b4513".to_string(),
                description: "Accent color (vintage brown)".to_string(),
            },
        );

        config_schema.insert(
            "font_family".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "'Courier Prime', 'Courier New', monospace".to_string(),
                description: "Typewriter-style font family".to_string(),
            },
        );

        config_schema.insert(
            "show_paper_texture".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Show subtle paper texture overlay".to_string(),
            },
        );

        config_schema.insert(
            "typing_animation".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Enable typewriter typing animation for title".to_string(),
            },
        );

        config_schema.insert(
            "show_date_stamp".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Show vintage date stamp in header".to_string(),
            },
        );

        config_schema.insert(
            "cursor_blink".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Show blinking cursor effect".to_string(),
            },
        );

        ThemeInfo {
            name: "Typewriter".to_string(),
            version: "1.0.0".to_string(),
            author: "Blogr Team".to_string(),
            description: "A vintage typewriter-inspired theme with nostalgic aesthetics and mechanical charm.".to_string(),
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
            .fg(Color::Rgb(43, 43, 43)) // Dark charcoal
            .bg(Color::Rgb(244, 241, 232)) // Vintage cream
    }
}

impl TypewriterTheme {
    /// Get example content.md for this theme
    pub fn example_content(author: &str) -> String {
        format!(
            r##"---
title: "{}"
description: "Writer, thinker, creator"
author: "{}"
theme: "typewriter"
theme_config:
  show_paper_texture: true
  typing_animation: true
  show_date_stamp: true
  cursor_blink: true
sections:
  about:
    title: "About Me"
    content: |
      <p>Hello, I'm {}. I craft words and ideas with the same care
      a typewriter demands—deliberate, thoughtful, and permanent.</p>

      <p>My work explores the intersection of technology and humanity,
      seeking to understand how we can build a better future while
      honoring the wisdom of the past.</p>
    tagline: "One keystroke at a time"

  writing:
    title: "Writing & Work"
    items:
      - title: "Essays & Articles"
        description: "Long-form explorations of technology, philosophy, and the human condition"
        note: "Published in various outlets"

      - title: "Technical Documentation"
        description: "Clear, concise guides for complex systems"
        note: "Open source contributions"

      - title: "Personal Reflections"
        description: "Thoughts on craft, creativity, and the daily practice of making"
        note: "Updated regularly"

  skills:
    title: "Tools & Techniques"
    items:
      - "Writing & Editing"
      - "Technical Communication"
      - "Research & Analysis"
      - "Content Strategy"
      - "Documentation"
      - "Storytelling"

  projects:
    title: "Selected Works"
    items:
      - title: "The Art of Code"
        year: "2024"
        description: "A collection of essays on programming as a creative practice"
        link: "https://github.com/yourusername/art-of-code"

      - title: "Digital Minimalism Guide"
        year: "2023"
        description: "A practical handbook for intentional technology use"

      - title: "Open Source Contributions"
        year: "Ongoing"
        description: "Documentation and community building for various projects"

  contact:
    title: "Get In Touch"
    text: "I'm always interested in meaningful conversations and collaborations. Drop me a line if you'd like to connect."
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

impl Default for TypewriterTheme {
    fn default() -> Self {
        Self::new()
    }
}
