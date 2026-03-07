//! Newsletter command handlers

use anyhow::{Context, Result};
use std::io::{self, Write};

use crate::newsletter::{
    ApiConfig, MigrationConfig, MigrationManager, MigrationSource, ModernApprovalApp,
    NewsletterApiServer, NewsletterManager, PluginManager, SubscriberStatus,
};
use crate::project::Project;
use crate::tui;

/// Handle the fetch-subscribers command
pub async fn handle_fetch_subscribers(interactive: bool) -> Result<()> {
    // Find the current project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load project configuration
    let config = project
        .load_config()
        .context("Failed to load project configuration")?;

    // Create newsletter manager
    let newsletter_manager = NewsletterManager::new(config, &project.root)
        .context("Failed to initialize newsletter manager")?;

    // Check if newsletter is enabled
    if !newsletter_manager.is_enabled() {
        println!("Newsletter functionality is not enabled.");
        println!("To enable it, add the following to your blogr.toml:");
        println!();
        println!("[newsletter]");
        println!("enabled = true");
        println!("subscribe_email = \"subscribe@yourdomain.com\"");
        println!("sender_name = \"Your Blog Name\"");
        println!();
        println!("You'll also need to configure IMAP settings:");
        println!();
        println!("[newsletter.imap]");
        println!("server = \"imap.gmail.com\"");
        println!("port = 993");
        println!("username = \"subscribe@yourdomain.com\"");
        println!();
        println!("And set the NEWSLETTER_IMAP_PASSWORD environment variable.");
        return Ok(());
    }

    // Print current configuration status
    newsletter_manager
        .print_status()
        .context("Failed to print newsletter status")?;

    println!();

    // If interactive mode, offer to set up configuration
    if interactive {
        match newsletter_manager.get_imap_config()? {
            Some(_) => {
                println!("IMAP configuration found. Proceeding with email fetch...");
            }
            None => {
                println!("No IMAP configuration found.");
                if prompt_yes_no("Would you like to set up IMAP configuration now?")? {
                    let imap_config = crate::newsletter::config::setup_imap_config()?;
                    println!("IMAP configuration created. Please add it to your blogr.toml:");
                    println!();
                    println!("[newsletter.imap]");
                    println!("server = \"{}\"", imap_config.server);
                    println!("port = {}", imap_config.port);
                    println!("username = \"{}\"", imap_config.username);
                    println!();
                    println!("Also set the NEWSLETTER_IMAP_PASSWORD environment variable and run the command again.");
                    return Ok(());
                }
            }
        }
    }

    // Attempt to fetch subscribers
    match newsletter_manager.fetch_subscribers(interactive) {
        Ok(()) => {
            println!("✓ Newsletter subscriber fetch completed successfully.");

            // Show updated statistics
            println!("\nUpdated subscriber statistics:");
            let total = newsletter_manager.database().get_subscriber_count(None)?;
            let pending = newsletter_manager.database().get_subscriber_count(Some(
                crate::newsletter::database::SubscriberStatus::Pending,
            ))?;
            let approved = newsletter_manager.database().get_subscriber_count(Some(
                crate::newsletter::database::SubscriberStatus::Approved,
            ))?;

            println!("  Total: {}", total);
            println!("  Pending: {}", pending);
            println!("  Approved: {}", approved);

            if pending > 0 {
                println!("\nNext steps:");
                println!("  1. Run 'blogr newsletter approve' to review pending subscribers");
                println!("  2. Run 'blogr newsletter list' to see all subscribers");
            }
        }
        Err(e) => {
            eprintln!("Failed to fetch subscribers: {}", e);

            // Provide helpful troubleshooting information
            println!("\nTroubleshooting tips:");
            println!("1. Verify your IMAP credentials are correct");
            println!("2. Check that NEWSLETTER_IMAP_PASSWORD environment variable is set");
            println!("3. Ensure your email provider allows IMAP access");
            println!("4. For Gmail, you may need to use an App Password instead of your regular password");
            println!("5. Check your firewall and network connectivity");

            return Err(e);
        }
    }

    Ok(())
}

/// Handle the approve command - launches the modern TUI approval interface
pub fn handle_approve() -> Result<()> {
    // Find the current project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load project configuration
    let config = project
        .load_config()
        .context("Failed to load project configuration")?;

    // Create newsletter manager
    let newsletter_manager = NewsletterManager::new(config, &project.root)
        .context("Failed to initialize newsletter manager")?;

    // Check if newsletter is enabled
    if !newsletter_manager.is_enabled() {
        println!("Newsletter functionality is not enabled.");
        println!("To enable it, add the following to your blogr.toml:");
        println!();
        println!("[newsletter]");
        println!("enabled = true");
        println!("subscribe_email = \"subscribe@yourdomain.com\"");
        println!("sender_name = \"Your Blog Name\"");
        return Ok(());
    }

    // Get database
    let database = newsletter_manager.take_database();

    // Check if there are any subscribers
    let total_count = database.get_subscriber_count(None)?;
    if total_count == 0 {
        println!("No subscribers found.");
        println!(
            "Run 'blogr newsletter fetch-subscribers' first to import subscribers from email."
        );
        return Ok(());
    }

    // Initialize TUI with optimized settings for approval interface
    let mut tui = tui::init_optimized()?; // Use optimized TUI with faster event handling
    tui.init()?;

    // Create modern approval app
    let mut app = ModernApprovalApp::new(database)?;

    // Main event loop - use the TUI's built-in event handler for optimal performance
    loop {
        // Update animations and state
        let needs_update = app.update();

        // Only redraw when actually needed to maximize performance
        if app.needs_redraw() || needs_update {
            tui.draw_approval(&mut app)?;
        }

        // Handle events using TUI's optimized event handler
        match tui.events.next()? {
            crate::tui::Event::Tick => {
                // Minimal tick processing for animations only
                if app.has_active_animations() {
                    // Animation update already handled above
                }
            }
            crate::tui::Event::Key(key_event) => {
                use crate::newsletter::ApprovalResult;
                match app.handle_key_event(key_event)? {
                    ApprovalResult::Quit => break,
                    ApprovalResult::Continue => {}
                    ApprovalResult::Error(err) => {
                        eprintln!("Error: {}", err);
                        break;
                    }
                }
            }
            crate::tui::Event::Mouse(_) => {
                // Mouse events - no processing needed for keyboard-driven interface
            }
            crate::tui::Event::Resize(_width, _height) => {
                // Terminal resize - let ratatui handle this automatically
                app.mark_for_redraw();
            }
            crate::tui::Event::Redraw => {
                // Force redraw requested
                app.mark_for_redraw();
            }
        }

        if !app.running {
            break;
        }
    }

    tui.exit()?;
    println!("✨ Newsletter approval session completed!");
    Ok(())
}

/// Handle the list command - show all subscribers in a table format
pub fn handle_list(status_filter: Option<String>) -> Result<()> {
    // Find the current project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load project configuration
    let config = project
        .load_config()
        .context("Failed to load project configuration")?;

    // Create newsletter manager
    let newsletter_manager = NewsletterManager::new(config, &project.root)
        .context("Failed to initialize newsletter manager")?;

    // Check if newsletter is enabled
    if !newsletter_manager.is_enabled() {
        println!("Newsletter functionality is not enabled.");
        return Ok(());
    }

    let database = newsletter_manager.database();

    // Parse status filter
    let status = if let Some(ref filter) = status_filter {
        Some(filter.parse::<SubscriberStatus>()?)
    } else {
        None
    };

    // Get subscribers
    let subscribers = database.get_subscribers(status)?;

    if subscribers.is_empty() {
        println!("No subscribers found.");
        if status_filter.is_some() {
            println!("Try running without a status filter to see all subscribers.");
        } else {
            println!("Run 'blogr newsletter fetch-subscribers' to import subscribers from email.");
        }
        return Ok(());
    }

    // Print header
    println!();
    println!(
        "{:<5} {:<30} {:<10} {:<20} {:<15}",
        "ID", "Email", "Status", "Subscribed", "Approved"
    );
    println!("{}", "-".repeat(85));

    // Print subscribers
    for subscriber in &subscribers {
        let approved_str = subscriber
            .approved_at
            .map(|dt| dt.format("%Y-%m-%d %H:%M").to_string())
            .unwrap_or_else(|| "-".to_string());

        println!(
            "{:<5} {:<30} {:<10} {:<20} {:<15}",
            subscriber.id.unwrap_or(0),
            subscriber.email,
            subscriber.status,
            subscriber.subscribed_at.format("%Y-%m-%d %H:%M"),
            approved_str
        );
    }

    println!();
    println!("Total: {} subscribers", subscribers.len());

    // Show statistics
    let total = database.get_subscriber_count(None)?;
    let pending = database.get_subscriber_count(Some(SubscriberStatus::Pending))?;
    let approved = database.get_subscriber_count(Some(SubscriberStatus::Approved))?;
    let declined = database.get_subscriber_count(Some(SubscriberStatus::Declined))?;

    println!();
    println!("Statistics:");
    println!("  Total: {}", total);
    println!("  Pending: {}", pending);
    println!("  Approved: {}", approved);
    println!("  Declined: {}", declined);

    Ok(())
}

/// Handle the remove command - remove a subscriber by email
pub fn handle_remove(email: &str, force: bool) -> Result<()> {
    // Find the current project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load project configuration
    let config = project
        .load_config()
        .context("Failed to load project configuration")?;

    // Create newsletter manager
    let newsletter_manager = NewsletterManager::new(config, &project.root)
        .context("Failed to initialize newsletter manager")?;

    // Check if newsletter is enabled
    if !newsletter_manager.is_enabled() {
        println!("Newsletter functionality is not enabled.");
        return Ok(());
    }

    let database = newsletter_manager.take_database();

    // Check if subscriber exists
    let subscriber = database.get_subscriber_by_email(email)?;
    match subscriber {
        Some(sub) => {
            println!("Found subscriber:");
            println!("  Email: {}", sub.email);
            println!("  Status: {}", sub.status);
            println!(
                "  Subscribed: {}",
                sub.subscribed_at.format("%Y-%m-%d %H:%M")
            );

            if !force && !prompt_yes_no(&format!("Remove subscriber '{}'?", email))? {
                println!("Operation cancelled.");
                return Ok(());
            }

            if database.remove_subscriber(email)? {
                println!("✓ Subscriber '{}' has been removed.", email);
            } else {
                println!("Failed to remove subscriber '{}'.", email);
            }
        }
        None => {
            println!("Subscriber '{}' not found.", email);
        }
    }

    Ok(())
}

/// Handle the export command - export subscribers to CSV or JSON
pub fn handle_export(
    format: &str,
    output_file: Option<&str>,
    status_filter: Option<String>,
) -> Result<()> {
    // Find the current project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load project configuration
    let config = project
        .load_config()
        .context("Failed to load project configuration")?;

    // Create newsletter manager
    let newsletter_manager = NewsletterManager::new(config, &project.root)
        .context("Failed to initialize newsletter manager")?;

    // Check if newsletter is enabled
    if !newsletter_manager.is_enabled() {
        println!("Newsletter functionality is not enabled.");
        return Ok(());
    }

    let database = newsletter_manager.database();

    // Parse status filter
    let status = if let Some(ref filter) = status_filter {
        Some(filter.parse::<SubscriberStatus>()?)
    } else {
        None
    };

    // Get subscribers
    let subscribers = database.get_subscribers(status)?;

    if subscribers.is_empty() {
        println!("No subscribers found to export.");
        return Ok(());
    }

    // Generate output
    let output = match format.to_lowercase().as_str() {
        "csv" => export_csv(&subscribers)?,
        "json" => export_json(&subscribers)?,
        _ => {
            return Err(anyhow::anyhow!(
                "Unsupported format: {}. Use 'csv' or 'json'.",
                format
            ))
        }
    };

    // Write to file or stdout
    match output_file {
        Some(file_path) => {
            std::fs::write(file_path, output)?;
            println!(
                "✓ Exported {} subscribers to '{}'",
                subscribers.len(),
                file_path
            );
        }
        None => {
            println!("{}", output);
        }
    }

    Ok(())
}

/// Escape a field for CSV output (RFC 4180)
fn csv_escape(field: &str) -> String {
    if field.contains(',') || field.contains('"') || field.contains('\n') || field.contains('\r') {
        format!("\"{}\"", field.replace('"', "\"\""))
    } else {
        field.to_string()
    }
}

/// Export subscribers to CSV format
fn export_csv(subscribers: &[crate::newsletter::Subscriber]) -> Result<String> {
    let mut output = String::new();

    // Header
    output.push_str("id,email,status,subscribed_at,approved_at,source_email_id,notes\n");

    // Data
    for subscriber in subscribers {
        let approved_at = subscriber
            .approved_at
            .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
            .unwrap_or_default();

        let source_email_id = subscriber.source_email_id.as_deref().unwrap_or("");
        let notes = subscriber.notes.as_deref().unwrap_or("");

        output.push_str(&format!(
            "{},{},{},{},{},{},{}\n",
            subscriber.id.unwrap_or(0),
            csv_escape(&subscriber.email),
            subscriber.status,
            subscriber.subscribed_at.format("%Y-%m-%d %H:%M:%S"),
            csv_escape(&approved_at),
            csv_escape(source_email_id),
            csv_escape(notes),
        ));
    }

    Ok(output)
}

/// Export subscribers to JSON format
fn export_json(subscribers: &[crate::newsletter::Subscriber]) -> Result<String> {
    let json = serde_json::to_string_pretty(subscribers)?;
    Ok(json)
}

/// Handle the send latest post command
pub async fn handle_send_latest(interactive: bool, tag: Option<&str>) -> Result<()> {
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project
        .load_config()
        .context("Failed to load project configuration")?;
    let newsletter_manager = NewsletterManager::new(config.clone(), &project.root)?;

    if !newsletter_manager.is_enabled() {
        println!("❌ Newsletter functionality is not enabled.");
        return Ok(());
    }

    // Load posts
    let post_manager = crate::content::PostManager::new(project.posts_dir());
    let mut posts = post_manager.load_all_posts()?;
    posts.retain(|p| p.metadata.status == crate::content::PostStatus::Published);
    posts.sort_by(|a, b| b.metadata.date.cmp(&a.metadata.date));

    if posts.is_empty() {
        println!("❌ No published posts found");
        return Ok(());
    }

    // Load theme
    let theme = blogr_themes::get_theme(&config.theme.name)
        .ok_or_else(|| anyhow::anyhow!("Theme '{}' not found", config.theme.name))?;

    // Compose newsletter from latest post
    println!(
        "📝 Composing newsletter from latest post: '{}'",
        posts[0].metadata.title
    );
    let newsletter = newsletter_manager.compose_from_latest_post(theme, &posts)?;

    // Preview
    let composer =
        newsletter_manager.create_composer(blogr_themes::get_theme(&config.theme.name).unwrap())?;
    composer.preview_in_terminal(&newsletter)?;

    // Confirm sending
    if interactive && !prompt_yes_no("Send this newsletter to all approved subscribers?")? {
        println!("Newsletter sending cancelled.");
        return Ok(());
    }

    // Send newsletter
    println!("📤 Sending newsletter...");
    let report = newsletter_manager
        .send_newsletter_with_tag(&newsletter, interactive, tag)
        .await?;

    println!("✅ Newsletter sending completed!");
    println!("📊 Success rate: {:.1}%", report.success_rate() * 100.0);

    Ok(())
}

/// Handle the send custom newsletter command
pub async fn handle_send_custom(
    subject: String,
    content: String,
    interactive: bool,
    tag: Option<&str>,
) -> Result<()> {
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project
        .load_config()
        .context("Failed to load project configuration")?;
    let newsletter_manager = NewsletterManager::new(config.clone(), &project.root)?;

    if !newsletter_manager.is_enabled() {
        println!("❌ Newsletter functionality is not enabled.");
        return Ok(());
    }

    // Load theme and create composer
    let theme = blogr_themes::get_theme(&config.theme.name)
        .ok_or_else(|| anyhow::anyhow!("Theme '{}' not found", config.theme.name))?;
    let composer = newsletter_manager.create_composer(theme)?;

    // Compose custom newsletter
    println!("📝 Composing custom newsletter: '{}'", subject);
    let newsletter = composer.compose_custom(subject, content)?;

    // Preview
    composer.preview_in_terminal(&newsletter)?;

    // Confirm sending
    if interactive && !prompt_yes_no("Send this newsletter to all approved subscribers?")? {
        println!("Newsletter sending cancelled.");
        return Ok(());
    }

    // Send newsletter
    println!("📤 Sending newsletter...");
    let report = newsletter_manager
        .send_newsletter_with_tag(&newsletter, interactive, tag)
        .await?;

    println!("✅ Newsletter sending completed!");
    println!("📊 Success rate: {:.1}%", report.success_rate() * 100.0);

    Ok(())
}

/// Handle the draft newsletter command (preview only)
pub async fn handle_draft_latest() -> Result<()> {
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project
        .load_config()
        .context("Failed to load project configuration")?;
    let newsletter_manager = NewsletterManager::new(config.clone(), &project.root)?;

    if !newsletter_manager.is_enabled() {
        println!("❌ Newsletter functionality is not enabled.");
        return Ok(());
    }

    // Load posts
    let post_manager = crate::content::PostManager::new(project.posts_dir());
    let mut posts = post_manager.load_all_posts()?;
    posts.retain(|p| p.metadata.status == crate::content::PostStatus::Published);
    posts.sort_by(|a, b| b.metadata.date.cmp(&a.metadata.date));

    if posts.is_empty() {
        println!("❌ No published posts found");
        return Ok(());
    }

    // Load theme
    let theme = blogr_themes::get_theme(&config.theme.name)
        .ok_or_else(|| anyhow::anyhow!("Theme '{}' not found", config.theme.name))?;

    // Compose and preview newsletter
    let newsletter = newsletter_manager.compose_from_latest_post(theme, &posts)?;
    let composer =
        newsletter_manager.create_composer(blogr_themes::get_theme(&config.theme.name).unwrap())?;
    composer.preview_in_terminal(&newsletter)?;

    Ok(())
}

/// Handle the draft custom newsletter command
pub async fn handle_draft_custom(subject: String, content: String) -> Result<()> {
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project
        .load_config()
        .context("Failed to load project configuration")?;
    let newsletter_manager = NewsletterManager::new(config.clone(), &project.root)?;

    if !newsletter_manager.is_enabled() {
        println!("❌ Newsletter functionality is not enabled.");
        return Ok(());
    }

    // Load theme and create composer
    let theme = blogr_themes::get_theme(&config.theme.name)
        .ok_or_else(|| anyhow::anyhow!("Theme '{}' not found", config.theme.name))?;
    let composer = newsletter_manager.create_composer(theme)?;

    // Compose and preview custom newsletter
    let newsletter = composer.compose_custom(subject, content)?;
    composer.preview_in_terminal(&newsletter)?;

    Ok(())
}

/// Handle the test email command
pub async fn handle_test_email(test_email: String, interactive: bool) -> Result<()> {
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project
        .load_config()
        .context("Failed to load project configuration")?;
    let newsletter_manager = NewsletterManager::new(config.clone(), &project.root)?;

    if !newsletter_manager.is_enabled() {
        println!("❌ Newsletter functionality is not enabled.");
        return Ok(());
    }

    // Load posts for test content
    let post_manager = crate::content::PostManager::new(project.posts_dir());
    let mut posts = post_manager.load_all_posts()?;
    posts.retain(|p| p.metadata.status == crate::content::PostStatus::Published);
    posts.sort_by(|a, b| b.metadata.date.cmp(&a.metadata.date));

    if posts.is_empty() {
        println!("❌ No published posts found for test email");
        return Ok(());
    }

    // Load theme and compose test newsletter
    let theme = blogr_themes::get_theme(&config.theme.name)
        .ok_or_else(|| anyhow::anyhow!("Theme '{}' not found", config.theme.name))?;

    let newsletter = newsletter_manager.compose_from_latest_post(theme, &posts)?;

    // Send test email
    newsletter_manager.send_test_newsletter(&newsletter, &test_email, interactive)?;

    Ok(())
}

/// Prompt user for yes/no input
fn prompt_yes_no(question: &str) -> Result<bool> {
    loop {
        print!("{} (y/n): ", question);
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim().to_lowercase();

        match input.as_str() {
            "y" | "yes" => return Ok(true),
            "n" | "no" => return Ok(false),
            _ => println!("Please enter 'y' or 'n'"),
        }
    }
}

/// Handle the import command
pub async fn handle_import(
    source: &str,
    file: &str,
    preview: bool,
    preview_limit: usize,
    email_column: Option<&str>,
    name_column: Option<&str>,
    status_column: Option<&str>,
) -> Result<()> {
    // Find the current project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load project configuration
    let config = project
        .load_config()
        .context("Failed to load project configuration")?;

    // Create newsletter manager
    let newsletter_manager = NewsletterManager::new(config, &project.root)
        .context("Failed to initialize newsletter manager")?;

    // Check if newsletter is enabled
    if !newsletter_manager.is_enabled() {
        println!("Newsletter functionality is not enabled.");
        println!("Enable it first with newsletter configuration in blogr.toml");
        return Ok(());
    }

    // Parse migration source
    let migration_source: MigrationSource = source
        .parse()
        .with_context(|| format!("Unsupported migration source: {}", source))?;

    // Create migration configuration
    let mut migration_config = MigrationConfig::for_source(migration_source, file.to_string());

    // Override column mappings if provided
    if let Some(email_col) = email_column {
        migration_config.email_column = Some(email_col.to_string());
    }
    if let Some(name_col) = name_column {
        migration_config.name_column = Some(name_col.to_string());
    }
    if let Some(status_col) = status_column {
        migration_config.status_column = Some(status_col.to_string());
    }

    // Create migration manager
    let database = newsletter_manager.take_database();
    let mut migration_manager = MigrationManager::new(database);

    if preview {
        // Preview mode
        println!("Previewing import from {} source...", source);
        println!("File: {}", file);
        println!("Preview limit: {}", preview_limit);
        println!();

        match migration_manager.preview_migration(&migration_config, Some(preview_limit)) {
            Ok(preview_data) => {
                println!("Preview of {} subscribers:", preview_data.len());
                println!("{:-<80}", "");

                for (i, subscriber) in preview_data.iter().enumerate() {
                    println!("{}. Email: {}", i + 1, subscriber.email);
                    if let Some(ref name) = subscriber.name {
                        println!("   Name: {}", name);
                    }
                    if let Some(ref status) = subscriber.status {
                        println!("   Status: {}", status);
                    }
                    if let Some(date) = subscriber.subscribed_at {
                        println!("   Date: {}", date.format("%Y-%m-%d %H:%M:%S"));
                    }
                    if !subscriber.tags.is_empty() {
                        println!("   Tags: {}", subscriber.tags.join(", "));
                    }
                    println!();
                }

                println!("{:-<80}", "");
                println!("To import these subscribers, run the same command without --preview");
            }
            Err(e) => {
                eprintln!("Failed to preview migration: {}", e);
                return Err(e);
            }
        }
    } else {
        // Import mode
        print!(
            "Are you sure you want to import subscribers from {}? (y/N): ",
            file
        );
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        if !input.trim().to_lowercase().starts_with('y') {
            println!("Import cancelled.");
            return Ok(());
        }

        match migration_manager.import_from_file(&migration_config) {
            Ok(result) => {
                println!("\nImport completed successfully!");
                println!("Total processed: {}", result.total_processed);
                println!("Successfully imported: {}", result.successfully_imported);
                println!("Skipped duplicates: {}", result.skipped_duplicates);

                if !result.errors.is_empty() {
                    println!("Errors encountered: {}", result.errors.len());
                    for error in &result.errors {
                        eprintln!("  - {}", error);
                    }
                }

                if result.successfully_imported > 0 {
                    println!("\nNext steps:");
                    println!(
                        "1. Run 'blogr newsletter approve' to review and approve new subscribers"
                    );
                    println!("2. Use 'blogr newsletter list --status pending' to see pending subscribers");
                }
            }
            Err(e) => {
                eprintln!("Import failed: {}", e);
                return Err(e);
            }
        }
    }

    Ok(())
}

/// Handle the plugin list command
pub async fn handle_plugin_list() -> Result<()> {
    // Find the current project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load project configuration
    let _config = project
        .load_config()
        .context("Failed to load project configuration")?;

    // Create plugin manager
    let plugin_manager = PluginManager::new(project.root.clone());

    let plugins = plugin_manager.list_plugins();

    if plugins.is_empty() {
        println!("No plugins are currently loaded.");
        println!("\nTo add plugins, implement the NewsletterPlugin trait and register them in your code.");
        return Ok(());
    }

    println!("Loaded Newsletter Plugins:");
    println!("{:-<80}", "");

    for plugin in plugins {
        println!("Name: {}", plugin.name);
        println!("Version: {}", plugin.version);
        println!("Author: {}", plugin.author);
        println!("Description: {}", plugin.description);

        if let Some(ref homepage) = plugin.homepage {
            println!("Homepage: {}", homepage);
        }

        if !plugin.keywords.is_empty() {
            println!("Keywords: {}", plugin.keywords.join(", "));
        }

        println!();
    }

    Ok(())
}

/// Handle the plugin info command
pub async fn handle_plugin_info(name: &str) -> Result<()> {
    // Find the current project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Create plugin manager
    let plugin_manager = PluginManager::new(project.root.clone());

    if let Some(plugin) = plugin_manager.get_plugin(name) {
        let metadata = plugin.metadata();

        println!("Plugin Information:");
        println!("{:-<80}", "");
        println!("Name: {}", metadata.name);
        println!("Version: {}", metadata.version);
        println!("Author: {}", metadata.author);
        println!("Description: {}", metadata.description);

        if let Some(ref homepage) = metadata.homepage {
            println!("Homepage: {}", homepage);
        }

        if let Some(ref repository) = metadata.repository {
            println!("Repository: {}", repository);
        }

        if let Some(ref license) = metadata.license {
            println!("License: {}", license);
        }

        if !metadata.keywords.is_empty() {
            println!("Keywords: {}", metadata.keywords.join(", "));
        }

        if !metadata.dependencies.is_empty() {
            println!("Dependencies: {}", metadata.dependencies.join(", "));
        }

        if let Some(ref min_version) = metadata.min_blogr_version {
            println!("Minimum Blogr Version: {}", min_version);
        }

        let custom_commands = plugin.custom_commands();
        if !custom_commands.is_empty() {
            println!("\nCustom Commands:");
            for command in custom_commands {
                println!("  - {}", command);
            }
        }

        let custom_templates = plugin.custom_templates();
        if !custom_templates.is_empty() {
            println!("\nCustom Templates:");
            for template in custom_templates {
                println!("  - {}", template);
            }
        }
    } else {
        println!("Plugin '{}' not found.", name);
        println!("Use 'blogr newsletter plugin list' to see available plugins.");
    }

    Ok(())
}

/// Handle the plugin enable command
pub async fn handle_plugin_enable(name: &str) -> Result<()> {
    println!("Plugin enable/disable functionality requires configuration management.");
    println!(
        "This feature is not yet implemented - plugins are currently managed through blogr.toml"
    );
    println!("\nTo enable a plugin, add it to your blogr.toml:");
    println!();
    println!("[newsletter.plugins.{}]", name);
    println!("enabled = true");
    println!("# plugin-specific configuration here");

    Ok(())
}

/// Handle the plugin disable command
pub async fn handle_plugin_disable(name: &str) -> Result<()> {
    println!("Plugin enable/disable functionality requires configuration management.");
    println!(
        "This feature is not yet implemented - plugins are currently managed through blogr.toml"
    );
    println!("\nTo disable a plugin, set enabled = false in your blogr.toml:");
    println!();
    println!("[newsletter.plugins.{}]", name);
    println!("enabled = false");

    Ok(())
}

/// Handle the plugin run command
pub async fn handle_plugin_run(command: &str, args: &[String]) -> Result<()> {
    // Find the current project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load project configuration
    let config = project
        .load_config()
        .context("Failed to load project configuration")?;

    // Create newsletter manager
    let newsletter_manager = NewsletterManager::new(config.clone(), &project.root)
        .context("Failed to initialize newsletter manager")?;

    // Create plugin manager
    let plugin_manager = PluginManager::new(project.root.clone());

    // Create plugin context
    use crate::newsletter::{create_plugin_context, PluginHook};
    use std::collections::HashMap;
    use std::sync::Arc;

    let context = create_plugin_context(
        Arc::new(config),
        Arc::new(newsletter_manager.take_database()),
        project.root.clone(),
        PluginHook::CustomCommand,
        HashMap::new(),
    );

    match plugin_manager.execute_custom_command(command, args, &context) {
        Ok(result) => {
            if result.success {
                if let Some(message) = result.message {
                    println!("{}", message);
                }
                println!("Plugin command '{}' executed successfully.", command);
            } else {
                if let Some(message) = result.message {
                    eprintln!("Error: {}", message);
                }
                eprintln!("Plugin command '{}' failed.", command);
            }
        }
        Err(e) => {
            eprintln!("Failed to execute plugin command '{}': {}", command, e);
            return Err(e);
        }
    }

    Ok(())
}

/// Handle the cleanup command
pub async fn handle_cleanup(days: u32) -> Result<()> {
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project
        .load_config()
        .context("Failed to load project configuration")?;
    let newsletter_manager = NewsletterManager::new(config, &project.root)?;

    if !newsletter_manager.is_enabled() {
        println!("Newsletter functionality is not enabled.");
        return Ok(());
    }

    let db = newsletter_manager.database();

    let recipients_deleted = db.cleanup_old_send_recipients(days)?;
    println!(
        "Cleaned up {} send recipient records older than {} days",
        recipients_deleted, days
    );

    let queue_deleted = db.cleanup_completed_send_queue()?;
    println!("Cleaned up {} completed send queue items", queue_deleted);

    Ok(())
}

/// Handle the tag command
pub async fn handle_tag(email: &str, add: Option<&str>, remove: Option<&str>) -> Result<()> {
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project
        .load_config()
        .context("Failed to load project configuration")?;
    let newsletter_manager = NewsletterManager::new(config, &project.root)?;

    if !newsletter_manager.is_enabled() {
        println!("Newsletter functionality is not enabled.");
        return Ok(());
    }

    let db = newsletter_manager.database();

    let subscriber = db
        .get_subscriber_by_email(email)?
        .ok_or_else(|| anyhow::anyhow!("Subscriber '{}' not found", email))?;

    let id = subscriber.id.unwrap();

    if let Some(tag) = add {
        db.add_tag(id, tag)?;
        println!("Added tag '{}' to {}", tag, email);
    }

    if let Some(tag) = remove {
        if db.remove_tag(id, tag)? {
            println!("Removed tag '{}' from {}", tag, email);
        } else {
            println!("Tag '{}' not found on {}", tag, email);
        }
    }

    // Show current tags
    let tags = db.get_tags(id)?;
    if tags.is_empty() {
        println!("No tags for {}", email);
    } else {
        println!("Current tags for {}: {}", email, tags.join(", "));
    }

    Ok(())
}

/// Handle the tags list command
pub async fn handle_tags() -> Result<()> {
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project
        .load_config()
        .context("Failed to load project configuration")?;
    let newsletter_manager = NewsletterManager::new(config, &project.root)?;

    if !newsletter_manager.is_enabled() {
        println!("Newsletter functionality is not enabled.");
        return Ok(());
    }

    let tags = newsletter_manager.database().get_all_tags()?;

    if tags.is_empty() {
        println!("No tags found.");
        println!("Use 'blogr newsletter tag <email> --add <tag>' to add tags.");
        return Ok(());
    }

    println!("{:<30} {:<10}", "Tag", "Count");
    println!("{}", "-".repeat(40));
    for (tag, count) in &tags {
        println!("{:<30} {:<10}", tag, count);
    }
    println!("\nTotal: {} tags", tags.len());

    Ok(())
}

/// Handle the API server command
pub async fn handle_api_server(
    host: &str,
    port: u16,
    api_key: Option<&str>,
    cors_enabled: bool,
) -> Result<()> {
    // Find the current project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load project configuration
    let config = project
        .load_config()
        .context("Failed to load project configuration")?;

    // Create newsletter manager
    let newsletter_manager = NewsletterManager::new(config.clone(), &project.root)
        .context("Failed to initialize newsletter manager")?;

    // Check if newsletter is enabled
    if !newsletter_manager.is_enabled() {
        println!("Newsletter functionality is not enabled.");
        println!("Enable it first with newsletter configuration in blogr.toml");
        return Ok(());
    }

    // Create API configuration
    let api_config = ApiConfig {
        host: host.to_string(),
        port,
        api_key: api_key.map(|s| s.to_string()),
        cors_enabled,
        rate_limit: Some(100), // 100 requests per minute
    };

    // Create and start the API server
    let api_server = NewsletterApiServer::new(newsletter_manager, config, api_config);

    println!("Newsletter API Documentation:");
    println!("  GET  /health                - Health check");
    println!("  GET  /subscribers           - List subscribers");
    println!("  POST /subscribers           - Create subscriber");
    println!("  GET  /subscribers/{{email}}   - Get subscriber");
    println!("  PUT  /subscribers/{{email}}   - Update subscriber");
    println!("  DEL  /subscribers/{{email}}   - Delete subscriber");
    println!("  GET  /stats                 - Get statistics");
    println!("  GET  /export                - Export subscribers");
    println!();

    if let Some(key) = api_key {
        println!("API Key authentication is enabled.");
        println!(
            "Include 'Authorization: Bearer {}' header in requests.",
            key
        );
        println!();
    }

    api_server.start().await
}
