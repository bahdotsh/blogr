use crate::generator::SiteBuilder;
use crate::project::Project;
use crate::utils::Console;
use anyhow::{anyhow, Result};
use axum::Router;
use tower_http::services::ServeDir;

pub async fn handle_serve(port: u16, host: String, drafts: bool, open: bool) -> Result<()> {
    Console::info(&format!("Starting development server on {}:{}", host, port));

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Ensure templates generate root-relative URLs in dev (so assets load locally)
    std::env::set_var("BLOGR_DEV", "1");

    // Build site initially
    Console::info("Building site...");

    // Load config to get the correct output directory
    let config = project.load_config()?;
    let output_dir = config
        .build
        .output_dir
        .as_ref()
        .map(|p| project.root.join(p))
        .unwrap_or_else(|| project.root.join("_site"));

    let site_builder = SiteBuilder::new(project.clone(), Some(output_dir.clone()), drafts, false)?;
    site_builder.build()?;

    // Create router
    let app = Router::new().fallback_service(ServeDir::new(output_dir.clone()));

    Console::success(&format!(
        "Development server running at http://{}:{}",
        host, port
    ));
    println!("📝 Site built and ready");
    println!("🌐 Server serving from: {}", output_dir.display());
    if drafts {
        println!("📝 Including draft posts");
    }
    println!("Press Ctrl+C to stop");

    // Open browser if requested
    if open {
        let url = format!("http://{}:{}", host, port);
        Console::info(&format!("Opening browser to {}", url));
        if let Err(e) = ::open::that(&url) {
            Console::warn(&format!("Failed to open browser: {}", e));
        }
    }

    // Start server
    let listener = tokio::net::TcpListener::bind(format!("{}:{}", host, port)).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
