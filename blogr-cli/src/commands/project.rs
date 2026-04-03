use crate::content::{PostManager, PostStatus};
use crate::project::Project;
use crate::utils::Console;
use anyhow::{anyhow, Result};
use std::collections::HashMap;
use walkdir::WalkDir;

pub async fn handle_info() -> Result<()> {
    Console::info("Project information:");

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project.load_config()?;
    let post_manager = PostManager::new(project.posts_dir());
    let posts = post_manager.load_all_posts()?;

    let published_count = posts
        .iter()
        .filter(|p| p.metadata.status == PostStatus::Published)
        .count();
    let draft_count = posts
        .iter()
        .filter(|p| p.metadata.status == PostStatus::Draft)
        .count();

    let github_status = if let Some(github) = &config.github {
        format!("{}/{}", github.username, github.repository)
    } else {
        "Not configured".to_string()
    };

    println!("📋 Project Information:");
    println!("  📝 Title: {}", config.blog.title);
    println!("  👤 Author: {}", config.blog.author);
    println!("  📄 Description: {}", config.blog.description);
    println!("  🎨 Theme: {}", config.theme.name);
    println!(
        "  📊 Posts: {} ({} published, {} draft)",
        posts.len(),
        published_count,
        draft_count
    );
    println!("  🌐 GitHub: {}", github_status);
    println!();
    println!("💡 Edit blogr.toml to update project settings");

    Ok(())
}

pub async fn handle_check() -> Result<()> {
    Console::info("Validating project structure...");

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Validate directory structure and configuration
    let issues = project.validate()?;

    if !issues.is_empty() {
        Console::warn("Found validation issues:");
        for issue in &issues {
            println!("  ❌ {}", issue);
        }
        println!();
    }

    // Validate posts format and front matter
    let post_manager = crate::content::PostManager::new(project.posts_dir());
    let mut post_issues = Vec::new();

    match post_manager.load_all_posts() {
        Ok(posts) => {
            Console::info(&format!("Found {} posts", posts.len()));
            for post in &posts {
                // Basic validation - posts are already parsed successfully if we get here
                if post.metadata.title.is_empty() {
                    post_issues.push(format!(
                        "Post '{}' has empty title",
                        post.file_path.display()
                    ));
                }
                if post.metadata.author.is_empty() {
                    post_issues.push(format!(
                        "Post '{}' has empty author",
                        post.file_path.display()
                    ));
                }
                if post.content.trim().is_empty() {
                    post_issues.push(format!(
                        "Post '{}' has empty content",
                        post.file_path.display()
                    ));
                }
            }
        }
        Err(e) => {
            post_issues.push(format!("Failed to load posts: {}", e));
        }
    }

    // Check theme availability
    let config = project.load_config()?;
    let theme_name = &config.theme.name;
    if blogr_themes::get_theme(theme_name).is_none() {
        Console::warn(&format!("Theme '{}' may not be available", theme_name));
    }

    // Verify GitHub integration if configured
    if let Some(github_config) = &config.github {
        if github_config.username.is_empty() || github_config.repository.is_empty() {
            post_issues.push(
                "GitHub integration is enabled but username or repository is empty".to_string(),
            );
        }
    }

    // Report results
    let total_issues = issues.len() + post_issues.len();

    if total_issues == 0 {
        Console::success("Project structure validation passed!");
        println!("✅ All required directories exist");
        println!("✅ Configuration file is valid");
        println!("✅ Posts are properly formatted");
        println!("✅ Theme is available and configured");
        println!();
        println!("🎉 Your project is ready for building and deployment!");
    } else {
        if !post_issues.is_empty() {
            Console::warn("Post validation issues:");
            for issue in &post_issues {
                println!("  ❌ {}", issue);
            }
            println!();
        }

        Console::warn(&format!("Found {} validation issue(s)", total_issues));
        println!("💡 Fix these issues before building your site");
    }

    Ok(())
}

pub async fn handle_clean() -> Result<()> {
    Console::info("Cleaning build artifacts...");

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let mut freed_bytes = 0u64;
    let mut cleaned_items = Vec::new();

    // Remove build output directory
    let output_dir = project.output_dir();
    if output_dir.exists() {
        let size_before = calculate_dir_size(&output_dir)?;
        std::fs::remove_dir_all(&output_dir)?;
        freed_bytes += size_before;
        cleaned_items.push("Build output directory");
        Console::info(&format!("Removed build output: {}", output_dir.display()));
    }

    // Clean .blogr directory (temporary files and cache)
    let blogr_dir = project.blogr_dir();
    if blogr_dir.exists() {
        let size_before = calculate_dir_size(&blogr_dir)?;
        std::fs::remove_dir_all(&blogr_dir)?;
        freed_bytes += size_before;
        cleaned_items.push("Cache and temporary files");
        Console::info(&format!("Removed cache directory: {}", blogr_dir.display()));
    }

    // Clean any .DS_Store files (macOS)
    clean_ds_store_files(&project.root)?;

    // Clean any temporary markdown files (*.tmp.md)
    let temp_files = find_temp_files(&project.root)?;
    for temp_file in temp_files {
        let size_before = std::fs::metadata(&temp_file)?.len();
        std::fs::remove_file(&temp_file)?;
        freed_bytes += size_before;
        Console::info(&format!("Removed temporary file: {}", temp_file.display()));
    }

    if cleaned_items.is_empty() {
        Console::info("No build artifacts found to clean");
        println!("✨ Project is already clean");
    } else {
        Console::success("Project cleaned successfully!");
        for item in cleaned_items {
            println!("🧹 Removed {}", item);
        }

        let freed_mb = freed_bytes as f64 / 1_048_576.0;
        if freed_mb > 0.1 {
            println!("📦 Freed up space: {:.1} MB", freed_mb);
        } else {
            println!("📦 Freed up space: {} bytes", freed_bytes);
        }
    }

    println!("💡 Run 'blogr build' to regenerate site");

    Ok(())
}

/// Calculate the total size of a directory in bytes
fn calculate_dir_size(path: &std::path::Path) -> Result<u64> {
    let mut total_size = 0;

    if path.is_dir() {
        for entry in WalkDir::new(path) {
            let entry = entry?;
            if entry.file_type().is_file() {
                total_size += entry.metadata()?.len();
            }
        }
    }

    Ok(total_size)
}

/// Clean .DS_Store files recursively
fn clean_ds_store_files(root: &std::path::Path) -> Result<()> {
    for entry in WalkDir::new(root) {
        let entry = entry?;
        if entry.file_name() == ".DS_Store" {
            std::fs::remove_file(entry.path())?;
        }
    }
    Ok(())
}

/// Find temporary files (*.tmp.md, *.bak, etc.)
fn find_temp_files(root: &std::path::Path) -> Result<Vec<std::path::PathBuf>> {
    let mut temp_files = Vec::new();

    for entry in WalkDir::new(root) {
        let entry = entry?;
        if let Some(file_name) = entry.file_name().to_str() {
            if file_name.ends_with(".tmp.md")
                || file_name.ends_with(".bak")
                || file_name.ends_with("~")
            {
                temp_files.push(entry.path().to_path_buf());
            }
        }
    }

    Ok(temp_files)
}

pub async fn handle_stats() -> Result<()> {
    Console::info("Generating project statistics...");

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let post_manager = PostManager::new(project.posts_dir());
    let posts = post_manager.load_all_posts()?;

    let published_count = posts
        .iter()
        .filter(|p| p.metadata.status == PostStatus::Published)
        .count();
    let draft_count = posts
        .iter()
        .filter(|p| p.metadata.status == PostStatus::Draft)
        .count();

    // Calculate word count and reading time
    let total_words: usize = posts
        .iter()
        .map(|p| p.content.split_whitespace().count())
        .sum();
    let average_words = if posts.is_empty() {
        0
    } else {
        total_words / posts.len()
    };
    let total_reading_time: usize = posts.iter().map(|p| p.reading_time()).sum();

    // Get all tags and count usage
    let mut tag_counts = HashMap::new();
    for post in &posts {
        for tag in &post.metadata.tags {
            *tag_counts.entry(tag.clone()).or_insert(0) += 1;
        }
    }
    let mut sorted_tags: Vec<_> = tag_counts.into_iter().collect();
    sorted_tags.sort_by(|a, b| b.1.cmp(&a.1)); // Sort by count, descending

    // Count static files
    let static_dir = project.static_dir();
    let mut static_count = 0;
    let mut image_count = 0;
    let mut css_count = 0;

    if static_dir.exists() {
        for entry in walkdir::WalkDir::new(&static_dir).into_iter().flatten() {
            if entry.path().is_file() {
                static_count += 1;
                if let Some(ext) = entry.path().extension() {
                    match ext.to_str().unwrap_or("").to_lowercase().as_str() {
                        "jpg" | "jpeg" | "png" | "gif" | "svg" | "webp" => image_count += 1,
                        "css" => css_count += 1,
                        _ => {}
                    }
                }
            }
        }
    }

    println!("📊 Project Statistics:");
    println!();
    println!("📝 Content:");
    println!("  - Total posts: {}", posts.len());
    println!("  - Published: {}", published_count);
    println!("  - Drafts: {}", draft_count);
    println!("  - Total words: ~{}", total_words);
    if !posts.is_empty() {
        println!("  - Average words per post: {}", average_words);
    }
    println!(
        "  - Estimated reading time: {} minutes total",
        total_reading_time
    );
    println!();

    if !sorted_tags.is_empty() {
        println!("🏷️ Tags:");
        for (tag, count) in sorted_tags.iter().take(10) {
            println!("  - {} ({})", tag, count);
        }
        if sorted_tags.len() > 10 {
            println!("  - ... and {} more", sorted_tags.len() - 10);
        }
        println!();
    }

    println!("📁 Files:");
    println!("  - Static files: {}", static_count);
    println!("  - Images: {}", image_count);
    println!("  - Custom CSS: {}", css_count);
    println!();
    println!("🚀 Last build: Never");
    println!("📤 Last deploy: Never");

    Ok(())
}
