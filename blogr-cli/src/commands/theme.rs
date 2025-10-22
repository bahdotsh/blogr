use crate::project::Project;
use crate::utils::Console;
use anyhow::{anyhow, Result};
use blogr_themes::{get_all_themes, get_theme};
use std::collections::hash_map::Entry;

pub async fn handle_list() -> Result<()> {
    Console::info("Available themes:");

    // Load all available themes from blogr-themes crate
    let all_themes = get_all_themes();

    // Get current theme if we're in a project
    let current_theme = if let Ok(Some(project)) = Project::find_project() {
        match project.load_config() {
            Ok(config) => Some(config.theme.name),
            Err(_) => None,
        }
    } else {
        None
    };

    println!("📋 Available themes:");

    if all_themes.is_empty() {
        println!("  📦 No themes available");
    } else {
        for theme in all_themes {
            let info = theme.info();
            let is_active = current_theme.as_ref() == Some(&info.name);
            let status_icon = if is_active { "✅" } else { "📦" };
            let status_text = if is_active { " (active)" } else { "" };

            println!(
                "  {} {}{} - {}",
                status_icon, info.name, status_text, info.description
            );
            println!(
                "      👤 Author: {} | 📦 Version: {}",
                info.author, info.version
            );
        }
    }

    println!();
    println!("💡 Use 'blogr theme info <name>' for detailed information");

    Ok(())
}

pub async fn handle_info(name: String) -> Result<()> {
    Console::info(&format!("Theme information: {}", name));

    // Load theme by name
    if let Some(theme) = get_theme(&name) {
        let info = theme.info();

        println!("🎨 Theme: {}", info.name);
        println!("📝 Description: {}", info.description);
        println!("👤 Author: {}", info.author);
        println!("📦 Version: {}", info.version);
        println!();

        if !info.config_schema.is_empty() {
            println!("⚙️ Configuration options:");
            for (option_name, config_option) in info.config_schema {
                println!(
                    "  - {}: {} ({})",
                    option_name, config_option.value, config_option.description
                );
            }
        } else {
            println!("⚙️ No configuration options available");
        }

        // Check if theme is currently active
        if let Ok(Some(project)) = Project::find_project() {
            if let Ok(config) = project.load_config() {
                if config.theme.name == name {
                    println!();
                    println!("✅ This theme is currently active");
                } else {
                    println!();
                    println!("💡 Use 'blogr theme set {}' to activate this theme", name);
                }
            }
        }
    } else {
        Console::warn(&format!("Theme '{}' not found", name));
        println!("💡 Run 'blogr theme list' to see available themes");
    }

    Ok(())
}

pub async fn handle_set(name: String) -> Result<()> {
    Console::info(&format!("Setting theme: {}", name));

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Validate theme exists
    let theme = get_theme(&name).ok_or_else(|| {
        anyhow!(
            "Theme '{}' not found. Run 'blogr theme list' to see available themes.",
            name
        )
    })?;

    // Load current configuration
    let mut config = project.load_config()?;

    // Update theme name
    config.theme.name = name.clone();

    // Load theme configuration schema and update config with defaults
    let theme_info = theme.info();
    for (option_name, config_option) in theme_info.config_schema.clone() {
        // Only set default if the option doesn't exist in current config
        if let Entry::Vacant(e) = config.theme.config.entry(option_name) {
            e.insert(config_option.value);
        }
    }

    // Save updated configuration
    let config_path = project.root.join("blogr.toml");
    config.save_to_file(&config_path)?;

    Console::success(&format!("Theme set to: {}", name));
    println!("🎨 Theme changed successfully");
    println!("📝 Configuration updated in blogr.toml");
    println!("🔄 Run 'blogr build' or 'blogr serve' to see changes");

    if !theme_info.config_schema.is_empty() {
        println!();
        println!(
            "💡 Use 'blogr theme info {}' to see available configuration options",
            name
        );
    }

    Ok(())
}

pub async fn handle_preview(name: String) -> Result<()> {
    Console::info(&format!("Previewing theme: {}", name));

    // Load theme by name
    let theme = get_theme(&name).ok_or_else(|| {
        anyhow!(
            "Theme '{}' not found. Run 'blogr theme list' to see available themes.",
            name
        )
    })?;

    let theme_info = theme.info();

    // For now, show a text-based preview with theme information
    println!();
    println!("🎨 Theme Preview: {}", theme_info.name);
    println!("{}", "─".repeat(50));
    println!("📝 Description: {}", theme_info.description);
    println!("👤 Author: {}", theme_info.author);
    println!("📦 Version: {}", theme_info.version);
    println!();

    if !theme_info.config_schema.is_empty() {
        println!("⚙️ Available Configuration Options:");
        for (option_name, config_option) in &theme_info.config_schema {
            println!(
                "  • {} ({}): {}",
                option_name,
                config_option.value.type_str(),
                config_option.description
            );
            println!("    Default: {}", config_option.value);
        }
    } else {
        println!("⚙️ No configuration options available");
    }

    println!();
    println!("🎨 Sample Content Preview:");
    println!("{}", "─".repeat(50));
    println!("# Welcome to My Blog");
    println!("*Published on January 1, 2024 by John Doe*");
    println!();
    println!("This is a sample blog post showing how your content would");
    println!(
        "look with the **{}** theme. This theme features:",
        theme_info.name
    );
    println!();

    for (option_name, config_option) in theme_info.config_schema {
        if option_name.contains("color") {
            println!(
                "  • {}: {}",
                option_name.replace('_', " "),
                config_option.value
            );
        }
    }

    println!();
    println!("```rust");
    println!("fn main() {{");
    println!("    println!(\"Hello, world!\");");
    println!("}}");
    println!("```");
    println!();
    println!("{}", "─".repeat(50));

    // Check if we're in a project and offer to set the theme
    if let Ok(Some(project)) = Project::find_project() {
        if let Ok(config) = project.load_config() {
            if config.theme.name != name {
                println!();
                println!(
                    "💡 Like this theme? Use 'blogr theme set {}' to activate it",
                    name
                );
            } else {
                println!();
                println!("✅ This theme is currently active in your project");
            }
        }
    } else {
        println!();
        println!("💡 Create a new project with 'blogr init' to use this theme");
    }

    Ok(())
}
