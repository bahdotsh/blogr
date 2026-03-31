use crate::content::{PostManager, PostStatus};
use crate::project::Project;
use crate::utils::Console;
use anyhow::{anyhow, Result};
use chrono::{DateTime, Local};

pub async fn handle_list(
    drafts_only: bool,
    published_only: bool,
    tag_filter: Option<String>,
    sort_order: String,
) -> Result<()> {
    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let post_manager = PostManager::new(project.posts_dir());
    let mut posts = post_manager.load_all_posts()?;

    // Apply filters
    if drafts_only {
        posts.retain(|p| p.metadata.status == PostStatus::Draft);
    } else if published_only {
        posts.retain(|p| p.metadata.status == PostStatus::Published);
    }

    if let Some(tag) = &tag_filter {
        posts.retain(|p| p.has_tag(tag));
    }

    // Apply sorting
    match sort_order.as_str() {
        "title" => posts.sort_by(|a, b| a.metadata.title.cmp(&b.metadata.title)),
        "slug" => posts.sort_by(|a, b| a.metadata.slug.cmp(&b.metadata.slug)),
        "date" => posts.sort_by(|a, b| b.metadata.date.cmp(&a.metadata.date)), // Newest first
        _ => {
            Console::warn(&format!(
                "Unknown sort order '{}', using 'date'",
                sort_order
            ));
            posts.sort_by(|a, b| b.metadata.date.cmp(&a.metadata.date));
        }
    }

    if posts.is_empty() {
        let filter_msg = if let Some(tag) = tag_filter {
            format!(" with tag '{}'", tag)
        } else if drafts_only {
            " (drafts only)".to_string()
        } else if published_only {
            " (published only)".to_string()
        } else {
            String::new()
        };

        Console::info(&format!("No posts found{}", filter_msg));
        println!("💡 Create your first post with: blogr new \"My First Post\"");
        return Ok(());
    }

    // Display header
    let filter_info = if let Some(tag) = tag_filter {
        format!(" (tagged '{}')", tag)
    } else if drafts_only {
        " (drafts)".to_string()
    } else if published_only {
        " (published)".to_string()
    } else {
        String::new()
    };

    Console::info(&format!("Found {} post(s){}", posts.len(), filter_info));
    println!();

    // Display posts
    for (i, post) in posts.iter().enumerate() {
        let status_icon = match post.metadata.status {
            PostStatus::Published => "✅",
            PostStatus::Draft => "📝",
        };

        let featured_icon = if post.metadata.featured {
            "⭐"
        } else if post.is_external() {
            "🔗"
        } else {
            "  "
        };

        let local_date: DateTime<Local> = post.metadata.date.into();
        let date_str = local_date.format("%Y-%m-%d %H:%M").to_string();

        println!(
            "{}{} {:2}. {:20} \"{}\"",
            status_icon,
            featured_icon,
            i + 1,
            post.metadata.slug,
            post.metadata.title
        );

        println!(
            "      📅 {} | 👤 {} | ⏱️  {} min read",
            date_str,
            post.metadata.author,
            post.reading_time()
        );

        if !post.metadata.tags.is_empty() {
            println!("      🏷️  {}", post.metadata.tags.join(", "));
        }

        if !post.metadata.description.is_empty() {
            let desc = if post.metadata.description.len() > 80 {
                format!("{}...", &post.metadata.description[..77])
            } else {
                post.metadata.description.clone()
            };
            println!("      📄 {}", desc);
        }

        println!();
    }

    println!("💡 Commands:");
    println!("  • Edit a post: blogr edit <slug>");
    println!("  • Delete a post: blogr delete <slug>");
    println!("  • Create new post: blogr new \"Title\"");
    if posts.iter().any(|p| p.metadata.status == PostStatus::Draft) {
        println!("  • List only drafts: blogr list --drafts");
    }
    if posts
        .iter()
        .any(|p| p.metadata.status == PostStatus::Published)
    {
        println!("  • List only published: blogr list --published");
    }

    Ok(())
}
