use crate::{ConfigOption, SiteType, Theme, ThemeInfo, ThemeTemplates};
use ratatui::style::{Color, Style};
use std::collections::HashMap;

pub struct MusashiTheme;

impl MusashiTheme {
    #[must_use]
    pub fn new() -> Self {
        Self
    }
}

impl Theme for MusashiTheme {
    fn info(&self) -> ThemeInfo {
        let mut config_schema = HashMap::new();

        config_schema.insert(
            "primary_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#1a1a1a".to_string(),
                description: "Primary color (ink black)".to_string(),
            },
        );

        config_schema.insert(
            "background_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#faf9f7".to_string(),
                description: "Background color (warm paper)".to_string(),
            },
        );

        config_schema.insert(
            "text_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#2d2d2d".to_string(),
                description: "Main text color (charcoal)".to_string(),
            },
        );

        config_schema.insert(
            "secondary_text_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#6b6b6b".to_string(),
                description: "Secondary text color (warm gray)".to_string(),
            },
        );

        config_schema.insert(
            "accent_color".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "#4a4a4a".to_string(),
                description: "Accent color (slate)".to_string(),
            },
        );

        config_schema.insert(
            "font_family".to_string(),
            ConfigOption {
                option_type: "string".to_string(),
                default: "'Noto Serif JP', 'Georgia', serif".to_string(),
                description: "Font family with Japanese serif style".to_string(),
            },
        );

        config_schema.insert(
            "enable_animations".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Enable subtle zen-like animations".to_string(),
            },
        );

        config_schema.insert(
            "show_brush_strokes".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "true".to_string(),
                description: "Show ink brush stroke decorative elements".to_string(),
            },
        );

        config_schema.insert(
            "zen_mode".to_string(),
            ConfigOption {
                option_type: "boolean".to_string(),
                default: "false".to_string(),
                description: "Ultra-minimalist mode with maximum whitespace".to_string(),
            },
        );

        ThemeInfo {
            name: "Musashi".to_string(),
            version: "1.0.0".to_string(),
            author: "Blogr Team".to_string(),
            description: "An elegant monochrome theme inspired by sumi-e ink wash painting. Soft whites, warm grays, and ink blacks. Peaceful, refined, embodying the warrior's disciplined way. Fully customizable from content.md.".to_string(),
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
            .fg(Color::Rgb(26, 26, 26)) // Ink black
            .bg(Color::Rgb(250, 249, 247)) // Warm paper
    }
}

impl MusashiTheme {
    /// Get example content.md for this theme
    pub fn example_content(author: &str) -> String {
        format!(
            r##"---
title: "{}"
description: "Developer, designer, and lifelong learner on the path of mastery"
author: "{}"
theme: "musashi"
theme_config:
  enable_animations: true
  show_brush_strokes: true
  zen_mode: false
sections:
  about:
    title: "The Way"
    quote:
      text: "The way is in training. Become acquainted with every art."
      author: "Book of Five Rings"
    content: |
      <p>I walk the path of continuous improvement, where every day presents an opportunity
      to refine my craft. Like ink on paper, each experience leaves its mark, shaping who
      I am and what I create.</p>

      <p>My journey is guided by curiosity, discipline, and the pursuit of mastery.
      I believe in the power of focused work, thoughtful design, and code that speaks
      for itself.</p>
    principles:
      - "Discipline"
      - "Mastery"
      - "Simplicity"

  skills:
    title: "Expertise"
    items:
      - title: "Full-Stack Development"
        description: "Building robust applications with modern technologies and best practices"
        tags:
          - "Rust"
          - "TypeScript"
          - "React"
          - "Node.js"

      - title: "System Design"
        description: "Architecting scalable, maintainable systems that stand the test of time"
        tags:
          - "Microservices"
          - "APIs"
          - "Databases"
          - "Cloud"

      - title: "Product & Design"
        description: "Creating intuitive experiences that balance beauty with functionality"
        tags:
          - "UI/UX"
          - "Product Strategy"
          - "Design Systems"

  projects:
    title: "Selected Work"
    items:
      - title: "Zen Task Manager"
        status: "Live"
        description: "A minimalist task management system built with focus and flow in mind. Features a clean interface and powerful keyboard shortcuts."
        tech:
          - "React"
          - "TypeScript"
          - "Tailwind CSS"
        link: "https://github.com/yourusername/zen-tasks"

      - title: "Ink & Paper"
        status: "In Progress"
        description: "A note-taking application inspired by traditional pen and paper, bringing digital convenience to analog simplicity."
        tech:
          - "Rust"
          - "Tauri"
          - "SQLite"

      - title: "Haiku Compiler"
        status: "Complete"
        description: "An experimental programming language where every program reads like poetry. A meditation on code as art."
        tech:
          - "Rust"
          - "LLVM"
          - "Parser Combinators"

  contact:
    title: "Connect"
    text: "Interested in collaborating or just want to chat? I'm always open to interesting conversations and new opportunities."
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

impl Default for MusashiTheme {
    fn default() -> Self {
        Self::new()
    }
}
