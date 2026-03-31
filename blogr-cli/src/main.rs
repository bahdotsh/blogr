use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

mod commands;
mod config;
mod content;
mod generator;
mod newsletter;
mod project;
mod tui;
mod tui_launcher;
mod utils;

use commands::*;

#[derive(Parser)]
#[command(name = "blogr")]
#[command(about = "A CLI static site generator for blogs")]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Initialize a new blog project
    Init {
        /// Project name
        name: Option<String>,
        /// Project directory (defaults to current directory)
        #[arg(short, long)]
        path: Option<PathBuf>,
        /// GitHub username for repository creation
        #[arg(long)]
        github_username: Option<String>,
        /// GitHub repository name (defaults to project name)
        #[arg(long)]
        github_repo: Option<String>,
        /// Skip GitHub repository creation
        #[arg(long)]
        no_github: bool,
        /// Initialize as a personal website (no blog posts)
        #[arg(long)]
        personal: bool,
    },
    /// Create a new blog post
    New {
        /// Post title
        title: String,
        /// Post template to use
        #[arg(short, long, default_value = "post")]
        template: String,
        /// Set post as draft
        #[arg(short, long)]
        draft: bool,
        /// Custom slug for the post URL
        #[arg(short, long)]
        slug: Option<String>,
        /// Post tags (comma-separated)
        #[arg(long)]
        tags: Option<String>,
        /// Open in TUI editor instead of external editor
        #[arg(long)]
        tui: bool,
        /// URL for an external post (appears in listings but links externally)
        #[arg(long)]
        external_url: Option<String>,
    },
    /// List all blog posts
    List {
        /// Show only draft posts
        #[arg(long)]
        drafts: bool,
        /// Show only published posts
        #[arg(long)]
        published: bool,
        /// Filter by tag
        #[arg(short, long)]
        tag: Option<String>,
        /// Sort order (date, title, slug)
        #[arg(short, long, default_value = "date")]
        sort: String,
    },
    /// Edit an existing blog post
    Edit {
        /// Post slug to edit
        slug: String,
        /// Open in TUI editor instead of external editor
        #[arg(long)]
        tui: bool,
    },
    /// Delete a blog post
    Delete {
        /// Post slug to delete
        slug: String,
        /// Skip confirmation prompt
        #[arg(short, long)]
        force: bool,
    },
    /// Build the static site
    Build {
        /// Output directory
        #[arg(short, long)]
        output: Option<PathBuf>,
        /// Include draft posts
        #[arg(long)]
        drafts: bool,
        /// Include future-dated posts
        #[arg(long)]
        future: bool,
    },
    /// Start development server with live reload
    Serve {
        /// Port to serve on
        #[arg(short, long, default_value = "3000")]
        port: u16,
        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        /// Include draft posts
        #[arg(long)]
        drafts: bool,
        /// Open browser automatically
        #[arg(long)]
        open: bool,
    },
    /// Deploy the site to GitHub Pages
    Deploy {
        /// Deploy branch (default: gh-pages)
        #[arg(short, long, default_value = "gh-pages")]
        branch: String,
        /// Deployment message
        #[arg(short, long)]
        message: Option<String>,
    },
    /// Theme management commands
    Theme {
        #[command(subcommand)]
        action: ThemeAction,
    },
    /// Project management commands
    Project {
        #[command(subcommand)]
        action: ProjectAction,
    },
    /// Configuration management commands
    Config {
        #[command(subcommand)]
        action: ConfigAction,
    },
    /// Newsletter management commands
    Newsletter {
        #[command(subcommand)]
        action: NewsletterAction,
    },
}

#[derive(Subcommand)]
enum ThemeAction {
    /// List available themes
    List,
    /// Show theme details
    Info {
        /// Theme name
        name: String,
    },
    /// Set active theme
    Set {
        /// Theme name
        name: String,
    },
    /// Preview theme in TUI
    Preview {
        /// Theme name
        name: String,
    },
}

#[derive(Subcommand)]
enum ProjectAction {
    /// Show project information
    Info,
    /// Validate project structure
    Check,
    /// Clean build artifacts
    Clean,
    /// Show project statistics
    Stats,
}

#[derive(Subcommand)]
enum ConfigAction {
    /// Open interactive configuration editor (TUI)
    Edit,
    /// Get configuration value
    Get {
        /// Configuration key (e.g., blog.title, domains.primary)
        key: String,
    },
    /// Set configuration value
    Set {
        /// Configuration key
        key: String,
        /// Configuration value
        value: String,
    },
    /// Domain configuration commands
    Domain {
        #[command(subcommand)]
        action: DomainAction,
    },
}

#[derive(Subcommand)]
enum DomainAction {
    /// Set primary domain or subdomain
    Set {
        /// Domain name (e.g., example.com or blog.example.com)
        domain: Option<String>,
        /// Subdomain prefix (for subdomain configuration)
        #[arg(long)]
        subdomain: Option<String>,
        /// Enforce HTTPS
        #[arg(long, default_value = "true")]
        enforce_https: bool,
        /// Create CNAME file for GitHub Pages
        #[arg(long)]
        github_pages: bool,
    },
    /// List all configured domains
    List,
    /// Clear all domain configuration
    Clear,
    /// Add domain alias
    AddAlias {
        /// Alias domain name
        alias: String,
    },
    /// Remove domain alias
    RemoveAlias {
        /// Alias domain name to remove
        alias: String,
    },
}

#[derive(Subcommand)]
enum NewsletterAction {
    /// Fetch subscribers from email inbox
    FetchSubscribers {
        /// Interactive IMAP server configuration
        #[arg(long)]
        interactive: bool,
    },
    /// Launch approval UI for managing subscriber requests
    Approve,
    /// List all subscribers
    List {
        /// Filter by status (pending, approved, declined)
        #[arg(long)]
        status: Option<String>,
    },
    /// Remove a subscriber by email address
    Remove {
        /// Email address to remove
        email: String,
        /// Skip confirmation prompt
        #[arg(short, long)]
        force: bool,
    },
    /// Export subscribers to CSV or JSON
    Export {
        /// Output format (csv, json)
        #[arg(short, long, default_value = "csv")]
        format: String,
        /// Output file path (prints to stdout if not specified)
        #[arg(short, long)]
        output: Option<String>,
        /// Filter by status (pending, approved, declined)
        #[arg(long)]
        status: Option<String>,
    },
    /// Send newsletter with latest blog post
    SendLatest {
        /// Interactive confirmation before sending
        #[arg(long)]
        interactive: bool,
        /// Only send to subscribers with this tag
        #[arg(long)]
        tags: Option<String>,
    },
    /// Send custom newsletter
    SendCustom {
        /// Newsletter subject
        subject: String,
        /// Newsletter content (markdown)
        content: String,
        /// Interactive confirmation before sending
        #[arg(long)]
        interactive: bool,
        /// Only send to subscribers with this tag
        #[arg(long)]
        tags: Option<String>,
    },
    /// Preview newsletter without sending (latest post)
    DraftLatest,
    /// Preview custom newsletter without sending
    DraftCustom {
        /// Newsletter subject
        subject: String,
        /// Newsletter content (markdown)
        content: String,
    },
    /// Send test email to specific address
    Test {
        /// Test email address
        email: String,
        /// Interactive SMTP configuration
        #[arg(long)]
        interactive: bool,
    },
    /// Import subscribers from external services
    Import {
        /// Import source (mailchimp, convertkit, substack, beehiiv, generic, json)
        #[arg(short, long)]
        source: String,
        /// Path to import file (CSV or JSON)
        file: String,
        /// Preview only, don't import
        #[arg(long)]
        preview: bool,
        /// Number of records to preview (default: 10)
        #[arg(long, default_value = "10")]
        preview_limit: usize,
        /// Email column name (for custom CSV)
        #[arg(long)]
        email_column: Option<String>,
        /// Name column name (for custom CSV)
        #[arg(long)]
        name_column: Option<String>,
        /// Status column name (for custom CSV)
        #[arg(long)]
        status_column: Option<String>,
    },
    /// Plugin management commands
    Plugin {
        #[command(subcommand)]
        action: PluginAction,
    },
    /// Clean up old send data
    Cleanup {
        /// Delete send recipients older than this many days
        #[arg(long, default_value = "90")]
        days: u32,
    },
    /// Tag a subscriber
    Tag {
        /// Subscriber email
        email: String,
        /// Add a tag
        #[arg(long)]
        add: Option<String>,
        /// Remove a tag
        #[arg(long)]
        remove: Option<String>,
    },
    /// List all tags with subscriber counts
    Tags,
    /// Start API server for external integrations
    ApiServer {
        /// Port to serve on
        #[arg(short, long, default_value = "3001")]
        port: u16,
        /// Host to bind to
        #[arg(long, default_value = "127.0.0.1")]
        host: String,
        /// API key for authentication (optional)
        #[arg(long)]
        api_key: Option<String>,
        /// Disable CORS
        #[arg(long)]
        no_cors: bool,
    },
}

#[derive(Subcommand)]
enum PluginAction {
    /// List all available plugins
    List,
    /// Show plugin information
    Info {
        /// Plugin name
        name: String,
    },
    /// Enable a plugin
    Enable {
        /// Plugin name
        name: String,
    },
    /// Disable a plugin
    Disable {
        /// Plugin name
        name: String,
    },
    /// Execute a custom plugin command
    Run {
        /// Plugin command name
        command: String,
        /// Command arguments
        #[arg(last = true)]
        args: Vec<String>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok(); // Load .env file if it exists

    let cli = Cli::parse();

    match cli.command {
        Commands::Init {
            name,
            path,
            github_username,
            github_repo,
            no_github,
            personal,
        } => {
            init::handle_init(
                name,
                path,
                github_username,
                github_repo,
                no_github,
                personal,
            )
            .await
        }
        Commands::New {
            title,
            template,
            draft,
            slug,
            tags,
            tui,
            external_url,
        } => new::handle_new(title, template, draft, slug, tags, tui, external_url).await,
        Commands::List {
            drafts,
            published,
            tag,
            sort,
        } => list::handle_list(drafts, published, tag, sort).await,
        Commands::Edit { slug, tui } => edit::handle_edit(slug, tui).await,
        Commands::Delete { slug, force } => delete::handle_delete(slug, force).await,
        Commands::Build {
            output,
            drafts,
            future,
        } => build::handle_build(output, drafts, future).await,
        Commands::Serve {
            port,
            host,
            drafts,
            open,
        } => serve::handle_serve(port, host, drafts, open).await,
        Commands::Deploy { branch, message } => deploy::handle_deploy(branch, message).await,
        Commands::Theme { action } => match action {
            ThemeAction::List => theme::handle_list().await,
            ThemeAction::Info { name } => theme::handle_info(name).await,
            ThemeAction::Set { name } => theme::handle_set(name).await,
            ThemeAction::Preview { name } => theme::handle_preview(name).await,
        },
        Commands::Project { action } => match action {
            ProjectAction::Info => project_cmd::handle_info().await,
            ProjectAction::Check => project_cmd::handle_check().await,
            ProjectAction::Clean => project_cmd::handle_clean().await,
            ProjectAction::Stats => project_cmd::handle_stats().await,
        },
        Commands::Config { action } => match action {
            ConfigAction::Edit => {
                use crate::project::Project;
                use crate::tui_launcher;

                let project = Project::find_project()?.ok_or_else(|| {
                    anyhow::anyhow!("Not in a blogr project. Run 'blogr init' first.")
                })?;

                tui_launcher::launch_config_editor(&project).await
            }
            ConfigAction::Get { key } => commands::config::handle_get(key).await,
            ConfigAction::Set { key, value } => commands::config::handle_set(key, value).await,
            ConfigAction::Domain { action } => {
                let domain_action = match action {
                    DomainAction::Set {
                        domain,
                        subdomain,
                        enforce_https,
                        github_pages,
                    } => commands::config::DomainAction::Set {
                        domain,
                        subdomain,
                        enforce_https,
                        github_pages,
                    },
                    DomainAction::List => commands::config::DomainAction::List,
                    DomainAction::Clear => commands::config::DomainAction::Clear,
                    DomainAction::AddAlias { alias } => {
                        commands::config::DomainAction::AddAlias { alias }
                    }
                    DomainAction::RemoveAlias { alias } => {
                        commands::config::DomainAction::RemoveAlias { alias }
                    }
                };
                commands::config::handle_domain(domain_action).await
            }
        },
        Commands::Newsletter { action } => match action {
            NewsletterAction::FetchSubscribers { interactive } => {
                commands::newsletter::handle_fetch_subscribers(interactive).await
            }
            NewsletterAction::Approve => commands::newsletter::handle_approve(),
            NewsletterAction::List { status } => commands::newsletter::handle_list(status),
            NewsletterAction::Remove { email, force } => {
                commands::newsletter::handle_remove(&email, force)
            }
            NewsletterAction::Export {
                format,
                output,
                status,
            } => commands::newsletter::handle_export(&format, output.as_deref(), status),
            NewsletterAction::SendLatest { interactive, tags } => {
                commands::newsletter::handle_send_latest(interactive, tags.as_deref()).await
            }
            NewsletterAction::SendCustom {
                subject,
                content,
                interactive,
                tags,
            } => {
                commands::newsletter::handle_send_custom(
                    subject,
                    content,
                    interactive,
                    tags.as_deref(),
                )
                .await
            }
            NewsletterAction::DraftLatest => commands::newsletter::handle_draft_latest().await,
            NewsletterAction::DraftCustom { subject, content } => {
                commands::newsletter::handle_draft_custom(subject, content).await
            }
            NewsletterAction::Test { email, interactive } => {
                commands::newsletter::handle_test_email(email, interactive).await
            }
            NewsletterAction::Import {
                source,
                file,
                preview,
                preview_limit,
                email_column,
                name_column,
                status_column,
            } => {
                commands::newsletter::handle_import(
                    &source,
                    &file,
                    preview,
                    preview_limit,
                    email_column.as_deref(),
                    name_column.as_deref(),
                    status_column.as_deref(),
                )
                .await
            }
            NewsletterAction::Cleanup { days } => commands::newsletter::handle_cleanup(days).await,
            NewsletterAction::Tag { email, add, remove } => {
                commands::newsletter::handle_tag(&email, add.as_deref(), remove.as_deref()).await
            }
            NewsletterAction::Tags => commands::newsletter::handle_tags().await,
            NewsletterAction::Plugin { action } => match action {
                PluginAction::List => commands::newsletter::handle_plugin_list().await,
                PluginAction::Info { name } => {
                    commands::newsletter::handle_plugin_info(&name).await
                }
                PluginAction::Enable { name } => {
                    commands::newsletter::handle_plugin_enable(&name).await
                }
                PluginAction::Disable { name } => {
                    commands::newsletter::handle_plugin_disable(&name).await
                }
                PluginAction::Run { command, args } => {
                    commands::newsletter::handle_plugin_run(&command, &args).await
                }
            },
            NewsletterAction::ApiServer {
                port,
                host,
                api_key,
                no_cors,
            } => {
                commands::newsletter::handle_api_server(&host, port, api_key.as_deref(), !no_cors)
                    .await
            }
        },
    }
}
