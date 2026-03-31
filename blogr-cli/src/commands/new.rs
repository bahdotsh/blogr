use crate::content::{Post, PostManager, PostStatus};
use crate::project::Project;
use crate::utils::Console;
use anyhow::{anyhow, Result};

pub async fn handle_new(
    title: String,
    _template: String,
    draft: bool,
    slug: Option<String>,
    tags: Option<String>,
    use_tui: bool,
    external_url: Option<String>,
) -> Result<()> {
    Console::info(&format!("Creating new post: '{}'", title));

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project.load_config()?;

    // Parse tags
    let tags = tags
        .map(|t| t.split(',').map(|tag| tag.trim().to_string()).collect())
        .unwrap_or_default();

    // Set post status
    let status = if draft {
        PostStatus::Draft
    } else {
        PostStatus::Published
    };

    // Create new post
    let is_external = external_url.is_some();
    let post = Post::new(
        title.clone(),
        config.blog.author.clone(),
        None, // Will use default description
        tags,
        slug,
        status,
        external_url,
    )?;

    // Save the post
    let post_manager = PostManager::new(project.posts_dir());
    let file_path = post_manager.save_post(&post)?;

    Console::success(&format!("Created new post: '{}'", title));
    println!("📝 Post saved to: {}", file_path.display());
    println!("🏷️  Slug: {}", post.metadata.slug);
    println!("📊 Status: {:?}", post.metadata.status);

    if !post.metadata.tags.is_empty() {
        println!("🏷️  Tags: {}", post.metadata.tags.join(", "));
    }

    if let Some(ref url) = post.metadata.external_url {
        println!("🔗 External URL: {}", url);
    }

    // Launch TUI editor if requested (skip for external posts)
    if use_tui && !is_external {
        println!();
        println!("🚀 Launching TUI editor...");

        use crate::tui_launcher;

        let edited_post = tui_launcher::launch_editor(post, &project).await?;

        // Save the edited post
        let final_file_path = post_manager.save_post(&edited_post)?;

        Console::success("Post edited and saved!");
        println!("📝 Final post saved to: {}", final_file_path.display());
    } else if !is_external {
        println!();
        println!("💡 Next steps:");
        println!("  • Edit the post: blogr edit {} --tui", post.metadata.slug);
        println!("  • Edit externally: blogr edit {}", post.metadata.slug);
        println!("  • Start dev server: blogr serve");
        if post.metadata.status == PostStatus::Draft {
            println!("  • Publish when ready: change status to 'published' in frontmatter");
        }
    }

    Ok(())
}
