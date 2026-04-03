use crate::project::Project;
use crate::utils::Console;
use anyhow::{anyhow, Result};
use std::io::{self, Write};

/// Handle domain configuration commands
pub async fn handle_domain(action: DomainAction) -> Result<()> {
    match action {
        DomainAction::Set {
            domain,
            subdomain,
            enforce_https,
            github_pages,
        } => handle_domain_set(domain, subdomain, enforce_https, github_pages).await,
        DomainAction::List => handle_domain_list().await,
        DomainAction::Clear => handle_domain_clear().await,
        DomainAction::AddAlias { alias } => handle_domain_add_alias(alias).await,
        DomainAction::RemoveAlias { alias } => handle_domain_remove_alias(alias).await,
    }
}

#[derive(Debug, Clone)]
pub enum DomainAction {
    Set {
        domain: Option<String>,
        subdomain: Option<String>,
        enforce_https: bool,
        github_pages: bool,
    },
    List,
    Clear,
    AddAlias {
        alias: String,
    },
    RemoveAlias {
        alias: String,
    },
}

async fn handle_domain_set(
    domain: Option<String>,
    subdomain: Option<String>,
    enforce_https: bool,
    github_pages: bool,
) -> Result<()> {
    Console::info("Configuring domain settings...");

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load current configuration
    let mut config = project.load_config()?;

    // Interactive mode if no domain provided
    let domain_input = if let Some(d) = domain {
        d
    } else {
        print!("Enter domain (e.g., example.com or blog.example.com): ");
        io::stdout().flush()?;
        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();
        if input.is_empty() {
            anyhow::bail!("Domain cannot be empty");
        }
        input.to_string()
    };

    // Determine if it's a subdomain or primary domain
    if let Some(subdomain_prefix) = subdomain {
        // User explicitly specified subdomain configuration
        let parts: Vec<&str> = domain_input.split('.').collect();
        if parts.len() < 2 {
            anyhow::bail!("Invalid domain format for subdomain configuration");
        }

        let base_domain = parts[1..].join(".");
        config.set_subdomain(subdomain_prefix, base_domain, enforce_https);

        Console::success(&format!("Subdomain configured: {}", domain_input));
    } else {
        // Check if the domain looks like a subdomain (has more than 2 parts)
        let parts: Vec<&str> = domain_input.split('.').collect();
        if parts.len() > 2 {
            // Likely a subdomain, ask user for confirmation
            print!("This looks like a subdomain. Configure as subdomain? [Y/n]: ");
            io::stdout().flush()?;
            let mut input = String::new();
            io::stdin().read_line(&mut input)?;
            let input = input.trim().to_lowercase();

            if input.is_empty() || input == "y" || input == "yes" {
                let prefix = parts[0].to_string();
                let base_domain = parts[1..].join(".");
                config.set_subdomain(prefix, base_domain, enforce_https);
                Console::success(&format!("Subdomain configured: {}", domain_input));
            } else {
                config.set_primary_domain(domain_input.clone(), enforce_https);
                Console::success(&format!("Primary domain configured: {}", domain_input));
            }
        } else {
            // Primary domain
            config.set_primary_domain(domain_input.clone(), enforce_https);
            Console::success(&format!("Primary domain configured: {}", domain_input));
        }
    }

    // Save configuration
    let config_path = project.root.join("blogr.toml");
    config.save_to_file(&config_path)?;

    // Create CNAME file for GitHub Pages if requested
    if github_pages {
        create_cname_file(&project, &config)?;
    }

    // Display configuration summary
    display_domain_summary(&config)?;

    Ok(())
}

async fn handle_domain_list() -> Result<()> {
    Console::info("Domain Configuration");

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project.load_config()?;

    println!();
    println!("🌐 Current Domain Configuration:");
    println!("{}", "=".repeat(50));

    if let Some(domains) = &config.blog.domains {
        if let Some(primary) = &domains.primary {
            println!("🏠 Primary Domain: {}", primary);
        }

        if let Some(subdomain) = &domains.subdomain {
            println!(
                "📡 Subdomain: {}.{}",
                subdomain.prefix, subdomain.base_domain
            );
        }

        if !domains.aliases.is_empty() {
            println!("🔗 Domain Aliases:");
            for alias in &domains.aliases {
                println!("  • {}", alias);
            }
        }

        println!(
            "🔒 HTTPS Enforced: {}",
            if domains.enforce_https { "Yes" } else { "No" }
        );

        if let Some(github_domain) = &domains.github_pages_domain {
            println!("🐙 GitHub Pages Domain: {}", github_domain);
        }

        println!();
        println!("📍 Effective Base URL: {}", config.get_effective_base_url());
    } else {
        println!("No custom domain configuration found.");
        println!("📍 Using default Base URL: {}", config.blog.base_url);
    }

    println!();

    Ok(())
}

async fn handle_domain_clear() -> Result<()> {
    Console::info("Clearing domain configuration...");

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Confirmation prompt
    print!("Are you sure you want to clear all domain configuration? [y/N]: ");
    io::stdout().flush()?;
    let mut input = String::new();
    io::stdin().read_line(&mut input)?;
    let input = input.trim().to_lowercase();

    if input != "y" && input != "yes" {
        Console::info("Domain configuration not changed.");
        return Ok(());
    }

    // Load and update configuration
    let mut config = project.load_config()?;
    config.clear_domains();

    // Save configuration
    let config_path = project.root.join("blogr.toml");
    config.save_to_file(&config_path)?;

    // Remove CNAME file if it exists
    let cname_path = project.root.join("CNAME");
    if cname_path.exists() {
        std::fs::remove_file(&cname_path)?;
        Console::info("Removed CNAME file");
    }

    Console::success("Domain configuration cleared");
    Ok(())
}

async fn handle_domain_add_alias(alias: String) -> Result<()> {
    Console::info(&format!("Adding domain alias: {}", alias));

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load and update configuration
    let mut config = project.load_config()?;
    config.add_domain_alias(alias.clone());

    // Save configuration
    let config_path = project.root.join("blogr.toml");
    config.save_to_file(&config_path)?;

    Console::success(&format!("Added domain alias: {}", alias));
    display_domain_summary(&config)?;

    Ok(())
}

async fn handle_domain_remove_alias(alias: String) -> Result<()> {
    Console::info(&format!("Removing domain alias: {}", alias));

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    // Load and update configuration
    let mut config = project.load_config()?;
    config.remove_domain_alias(&alias);

    // Save configuration
    let config_path = project.root.join("blogr.toml");
    config.save_to_file(&config_path)?;

    Console::success(&format!("Removed domain alias: {}", alias));
    display_domain_summary(&config)?;

    Ok(())
}

fn create_cname_file(project: &Project, config: &crate::config::Config) -> Result<()> {
    if let Some(domains) = &config.blog.domains {
        if let Some(github_domain) = &domains.github_pages_domain {
            let cname_path = project.root.join("CNAME");
            std::fs::write(&cname_path, format!("{}\n", github_domain))?;
            Console::success(&format!("Created CNAME file: {}", github_domain));
        }
    }
    Ok(())
}

fn display_domain_summary(config: &crate::config::Config) -> Result<()> {
    println!();
    println!("📊 Domain Configuration Summary:");
    println!("{}", "-".repeat(40));

    if let Some(domains) = &config.blog.domains {
        let all_domains = config.get_all_domains();
        if !all_domains.is_empty() {
            println!("🌐 Configured domains:");
            for domain in all_domains {
                println!("  • {}", domain);
            }
        }

        println!(
            "🔒 HTTPS: {}",
            if domains.enforce_https {
                "Enforced"
            } else {
                "Optional"
            }
        );
        println!("📍 Effective URL: {}", config.get_effective_base_url());
    }

    println!();
    Ok(())
}

/// Handle general configuration commands
pub async fn handle_get(key: String) -> Result<()> {
    Console::info(&format!("Getting configuration value: {}", key));

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let config = project.load_config()?;

    // Parse the key path (e.g., "blog.title", "theme.name", "domains.primary")
    let parts: Vec<&str> = key.split('.').collect();

    match parts.as_slice() {
        ["blog", "title"] => println!("{}", config.blog.title),
        ["blog", "author"] => println!("{}", config.blog.author),
        ["blog", "description"] => println!("{}", config.blog.description),
        ["blog", "base_url"] => println!("{}", config.blog.base_url),
        ["blog", "language"] => {
            println!("{}", config.blog.language.as_deref().unwrap_or("Not set"))
        }
        ["blog", "timezone"] => {
            println!("{}", config.blog.timezone.as_deref().unwrap_or("Not set"))
        }
        ["theme", "name"] => println!("{}", config.theme.name),
        ["domains", "primary"] => {
            if let Some(domains) = &config.blog.domains {
                println!("{}", domains.primary.as_deref().unwrap_or("Not set"));
            } else {
                println!("Not set");
            }
        }
        ["domains", "enforce_https"] => {
            if let Some(domains) = &config.blog.domains {
                println!("{}", domains.enforce_https);
            } else {
                println!("true (default)");
            }
        }
        _ => {
            anyhow::bail!("Unknown configuration key: {}", key);
        }
    }

    Ok(())
}

pub async fn handle_set(key: String, value: String) -> Result<()> {
    Console::info(&format!("Setting configuration: {} = {}", key, value));

    // Check if we're in a blogr project
    let project = Project::find_project()?
        .ok_or_else(|| anyhow!("Not in a blogr project. Run 'blogr init' first."))?;

    let mut config = project.load_config()?;

    // Parse the key path and set the value
    let parts: Vec<&str> = key.split('.').collect();

    match parts.as_slice() {
        ["blog", "title"] => config.blog.title = value.clone(),
        ["blog", "author"] => config.blog.author = value.clone(),
        ["blog", "description"] => config.blog.description = value.clone(),
        ["blog", "base_url"] => config.blog.base_url = value.clone(),
        ["blog", "language"] => config.blog.language = Some(value.clone()),
        ["blog", "timezone"] => config.blog.timezone = Some(value.clone()),
        ["theme", "name"] => {
            let theme = blogr_themes::get_theme(&value)
                .ok_or_else(|| anyhow::anyhow!("Theme '{}' not found", value))?;
            config.theme.name = theme.info().slug();
        }
        _ => {
            anyhow::bail!("Unknown or unsupported configuration key: {}", key);
        }
    }

    // Validate the configuration
    config.validate()?;

    // Save configuration
    let config_path = project.root.join("blogr.toml");
    config.save_to_file(&config_path)?;

    Console::success(&format!("Configuration updated: {} = {}", key, value));
    Ok(())
}
