use anyhow::{Context, Result};
use blogr_themes::SiteType;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

/// Deployment type detection for different hosting scenarios
#[derive(Debug, Clone, PartialEq)]
pub enum DeploymentType {
    /// Custom domain (e.g., blog.example.com)
    CustomDomain,
    /// GitHub Pages at root (e.g., username.github.io)
    GitHubPagesRoot,
    /// GitHub Pages with subpath (e.g., username.github.io/repo)
    GitHubPagesSubpath,
    /// Unable to determine deployment type
    Unknown,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    pub blog: BlogConfig,
    pub theme: ThemeConfig,
    pub github: Option<GitHubConfig>,
    pub build: BuildConfig,
    #[serde(default)]
    pub dev: DevConfig,
    #[serde(default)]
    pub search: SearchConfig,
    #[serde(default)]
    pub newsletter: NewsletterConfig,
    #[serde(default)]
    pub site: SiteConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct SiteConfig {
    /// Type of site: "blog" or "personal"
    #[serde(default)]
    pub site_type: SiteType,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlogConfig {
    pub title: String,
    pub author: String,
    pub description: String,
    pub base_url: String,
    pub language: Option<String>,
    pub timezone: Option<String>,
    pub domains: Option<DomainConfig>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DomainConfig {
    /// Primary domain for the blog (e.g., "example.com")
    pub primary: Option<String>,
    /// List of additional domains that redirect to primary
    #[serde(default)]
    pub aliases: Vec<String>,
    /// Subdomain configuration
    pub subdomain: Option<SubdomainConfig>,
    /// Whether to enforce HTTPS
    #[serde(default = "default_enforce_https")]
    pub enforce_https: bool,
    /// Custom domain for GitHub Pages (CNAME file content)
    pub github_pages_domain: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubdomainConfig {
    /// Subdomain prefix (e.g., "blog" for blog.example.com)
    pub prefix: String,
    /// Base domain (e.g., "example.com")
    pub base_domain: String,
}

fn default_enforce_https() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThemeConfig {
    pub name: String,
    #[serde(default)]
    pub config: HashMap<String, toml::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitHubConfig {
    pub username: String,
    pub repository: String,
    pub branch: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BuildConfig {
    pub output_dir: Option<String>,
    #[serde(default)]
    pub drafts: bool,
    #[serde(default)]
    pub future_posts: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DevConfig {
    #[serde(default = "default_port")]
    pub port: u16,
    #[serde(default)]
    pub auto_reload: bool,
}

fn default_port() -> u16 {
    3000
}

impl Default for DevConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
            auto_reload: true,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SearchConfig {
    /// Whether search is enabled
    #[serde(default = "default_search_enabled")]
    pub enabled: bool,
    /// Fields to include in search
    #[serde(default = "default_search_fields")]
    pub fields: Vec<String>,
    /// Paths to exclude from search
    #[serde(default = "default_search_exclude")]
    pub exclude: Vec<String>,
    /// Maximum content length in characters
    #[serde(default = "default_max_content_chars")]
    pub max_content_chars: usize,
    /// Excerpt length in words
    #[serde(default = "default_excerpt_words")]
    pub excerpt_words: usize,
    /// Whether to minify the search index JSON
    #[serde(default = "default_minify")]
    pub minify: bool,
    /// Whether to lazy load search assets
    #[serde(default = "default_lazy_load")]
    pub lazy_load: bool,
    /// Whether to remove common stopwords from search
    #[serde(default = "default_remove_stopwords")]
    pub remove_stopwords: bool,
    /// Field boost weights for search scoring
    #[serde(default = "default_field_boosts")]
    pub field_boosts: std::collections::HashMap<String, f32>,
}

fn default_search_enabled() -> bool {
    true
}

fn default_search_fields() -> Vec<String> {
    vec![
        "title".to_string(),
        "tags".to_string(),
        "content".to_string(),
    ]
}

fn default_search_exclude() -> Vec<String> {
    vec!["drafts/".to_string()]
}

fn default_max_content_chars() -> usize {
    2000
}

fn default_excerpt_words() -> usize {
    30
}

fn default_minify() -> bool {
    true
}

fn default_lazy_load() -> bool {
    true
}

fn default_remove_stopwords() -> bool {
    false
}

fn default_field_boosts() -> std::collections::HashMap<String, f32> {
    let mut boosts = std::collections::HashMap::new();
    boosts.insert("title".to_string(), 5.0);
    boosts.insert("tags".to_string(), 3.0);
    boosts.insert("content".to_string(), 1.0);
    boosts
}

impl Default for SearchConfig {
    fn default() -> Self {
        Self {
            enabled: default_search_enabled(),
            fields: default_search_fields(),
            exclude: default_search_exclude(),
            max_content_chars: default_max_content_chars(),
            excerpt_words: default_excerpt_words(),
            minify: default_minify(),
            lazy_load: default_lazy_load(),
            remove_stopwords: default_remove_stopwords(),
            field_boosts: default_field_boosts(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NewsletterConfig {
    /// Whether newsletter functionality is enabled
    #[serde(default = "default_newsletter_enabled")]
    pub enabled: bool,
    /// Email address for newsletter subscriptions
    pub subscribe_email: Option<String>,
    /// Name to display in newsletter emails
    pub sender_name: Option<String>,
    /// Subject line for confirmation emails
    pub confirmation_subject: Option<String>,
    /// Optional IMAP configuration (can be set via CLI)
    pub imap: Option<ImapConfig>,
    /// Optional SMTP configuration (can be set via CLI)
    pub smtp: Option<SmtpConfig>,
    /// Plugin configurations
    #[serde(default)]
    pub plugins: Option<std::collections::HashMap<String, crate::newsletter::PluginConfig>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImapConfig {
    pub server: String,
    pub port: u16,
    pub username: String,
    /// Password is not stored in config - set via environment variable NEWSLETTER_IMAP_PASSWORD
    pub use_tls: Option<bool>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SmtpConfig {
    pub server: String,
    pub port: u16,
    pub username: String,
    /// Password is not stored in config - set via environment variable NEWSLETTER_SMTP_PASSWORD
    pub use_tls: Option<bool>,
}

fn default_newsletter_enabled() -> bool {
    false
}

impl Default for NewsletterConfig {
    fn default() -> Self {
        Self {
            enabled: default_newsletter_enabled(),
            subscribe_email: None,
            sender_name: None,
            confirmation_subject: None,
            imap: None,
            smtp: None,
            plugins: None,
        }
    }
}

impl Default for Config {
    fn default() -> Self {
        Self {
            blog: BlogConfig {
                title: "My Blog".to_string(),
                author: "Anonymous".to_string(),
                description: "A blog powered by Blogr".to_string(),
                base_url: "https://username.github.io/repository".to_string(),
                language: Some("en".to_string()),
                timezone: Some("UTC".to_string()),
                domains: None,
            },
            theme: ThemeConfig {
                name: "minimal-retro".to_string(),
                config: HashMap::new(),
            },
            github: None,
            build: BuildConfig {
                output_dir: Some("dist".to_string()),
                drafts: false,
                future_posts: false,
            },
            dev: DevConfig::default(),
            search: SearchConfig::default(),
            newsletter: NewsletterConfig::default(),
            site: SiteConfig::default(),
        }
    }
}

impl Config {
    /// Load configuration from blogr.toml file
    #[allow(dead_code)]
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let content = fs::read_to_string(&path)
            .with_context(|| format!("Failed to read config file: {}", path.as_ref().display()))?;

        let config: Self =
            toml::from_str(&content).with_context(|| "Failed to parse configuration file")?;

        Ok(config)
    }

    /// Save configuration to blogr.toml file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let content =
            toml::to_string_pretty(self).with_context(|| "Failed to serialize configuration")?;

        fs::write(&path, content)
            .with_context(|| format!("Failed to write config file: {}", path.as_ref().display()))?;

        Ok(())
    }

    /// Create default configuration with custom values
    pub fn new_with_defaults(
        title: String,
        author: String,
        description: String,
        github_username: Option<String>,
        github_repo: Option<String>,
    ) -> Self {
        let mut config = Self::default();

        config.blog.title = title.clone();
        config.blog.author = author;
        config.blog.description = description;

        if let (Some(username), Some(repo)) = (github_username, github_repo) {
            config.blog.base_url = format!("https://{}.github.io/{}", username, repo);
            config.github = Some(GitHubConfig {
                username,
                repository: repo,
                branch: Some("main".to_string()),
            });
        }

        config
    }

    /// Create personal website configuration
    pub fn new_personal(
        title: String,
        author: String,
        description: String,
        github_username: Option<String>,
        github_repo: Option<String>,
    ) -> Self {
        let mut config =
            Self::new_with_defaults(title, author, description, github_username, github_repo);
        config.site.site_type = SiteType::Personal;
        config.theme.name = "dark-minimal".to_string();
        config
    }

    /// Get the project root directory (where blogr.toml is located)
    pub fn find_project_root() -> Result<Option<PathBuf>> {
        let mut current =
            std::env::current_dir().with_context(|| "Failed to get current directory")?;

        loop {
            let config_path = current.join("blogr.toml");
            if config_path.exists() {
                return Ok(Some(current));
            }

            if !current.pop() {
                break;
            }
        }

        Ok(None)
    }

    /// Load configuration from the project root
    #[allow(dead_code)]
    pub fn load_from_project() -> Result<(Self, PathBuf)> {
        let project_root = Self::find_project_root()
            .with_context(|| "Failed to find project root")?
            .ok_or_else(|| {
                anyhow::anyhow!("No blogr project found. Run 'blogr init' to create a new project.")
            })?;

        let config_path = project_root.join("blogr.toml");
        let config = Self::load_from_file(&config_path)
            .with_context(|| "Failed to load project configuration")?;

        Ok((config, project_root))
    }

    /// Get the posts directory path
    pub fn posts_dir(&self, project_root: &Path) -> PathBuf {
        project_root.join("posts")
    }

    /// Get the themes directory path
    #[allow(dead_code)]
    pub fn themes_dir(&self, project_root: &Path) -> PathBuf {
        project_root.join("themes")
    }

    /// Get the output directory path
    #[allow(dead_code)]
    pub fn output_dir(&self, project_root: &Path) -> PathBuf {
        let output_dir = self.build.output_dir.as_deref().unwrap_or("dist");
        project_root.join(output_dir)
    }

    /// Validate configuration
    pub fn validate(&self) -> Result<()> {
        if self.blog.title.trim().is_empty() {
            anyhow::bail!("Blog title cannot be empty");
        }

        if self.blog.author.trim().is_empty() {
            anyhow::bail!("Blog author cannot be empty");
        }

        if self.blog.description.trim().is_empty() {
            anyhow::bail!("Blog description cannot be empty");
        }

        if !self.blog.base_url.starts_with("http://") && !self.blog.base_url.starts_with("https://")
        {
            anyhow::bail!("Base URL must start with http:// or https://");
        }

        if self.dev.port == 0 {
            anyhow::bail!("Development server port must be greater than 0");
        }

        // Validate newsletter configuration
        if self.newsletter.enabled {
            if self.newsletter.subscribe_email.is_none() {
                anyhow::bail!("Newsletter is enabled but subscribe_email is not configured");
            }

            if let Some(email) = &self.newsletter.subscribe_email {
                if email.trim().is_empty() || !email.contains('@') {
                    anyhow::bail!("Invalid newsletter subscribe_email format");
                }
            }
        }

        Ok(())
    }

    /// Update theme configuration
    #[allow(dead_code)]
    pub fn set_theme(&mut self, theme_name: String, theme_config: HashMap<String, toml::Value>) {
        self.theme.name = theme_name;
        self.theme.config = theme_config;
    }

    /// Get theme configuration value
    #[allow(dead_code)]
    pub fn get_theme_config(&self, key: &str) -> Option<&toml::Value> {
        self.theme.config.get(key)
    }

    /// Set theme configuration value
    #[allow(dead_code)]
    pub fn set_theme_config(&mut self, key: String, value: toml::Value) {
        self.theme.config.insert(key, value);
    }

    /// Get the effective base URL, considering domain configuration
    /// This is the single source of truth for the site's URL
    pub fn get_effective_base_url(&self) -> String {
        // Priority 1: Domain configuration (most specific)
        if let Some(domains) = &self.blog.domains {
            if let Some(primary) = &domains.primary {
                let protocol = if domains.enforce_https {
                    "https"
                } else {
                    "http"
                };
                return format!("{}://{}", protocol, primary)
                    .trim_end_matches('/')
                    .to_string();
            } else if let Some(subdomain) = &domains.subdomain {
                let protocol = if domains.enforce_https {
                    "https"
                } else {
                    "http"
                };
                return format!(
                    "{}://{}.{}",
                    protocol, subdomain.prefix, subdomain.base_domain
                )
                .trim_end_matches('/')
                .to_string();
            }
        }

        // Priority 2: base_url (fallback)
        self.blog.base_url.trim_end_matches('/').to_string()
    }

    /// Detect the deployment type based on the effective base URL
    pub fn get_deployment_type(&self) -> DeploymentType {
        let base_url = self.get_effective_base_url();

        if let Ok(url) = url::Url::parse(&base_url) {
            if let Some(host) = url.host_str() {
                if host.ends_with(".github.io") && url.path() != "/" && !url.path().is_empty() {
                    return DeploymentType::GitHubPagesSubpath;
                } else if host.ends_with(".github.io") {
                    return DeploymentType::GitHubPagesRoot;
                } else if host.contains("github.io") {
                    return DeploymentType::GitHubPagesSubpath;
                } else {
                    return DeploymentType::CustomDomain;
                }
            }
        }

        DeploymentType::Unknown
    }

    /// Sync base_url with domain configuration to ensure consistency
    pub fn sync_base_url_with_domains(&mut self) {
        let effective_url = self.get_effective_base_url();
        self.blog.base_url = effective_url;
    }

    /// Set primary domain
    pub fn set_primary_domain(&mut self, domain: String, enforce_https: bool) {
        if self.blog.domains.is_none() {
            self.blog.domains = Some(DomainConfig {
                primary: Some(domain.clone()),
                aliases: Vec::new(),
                subdomain: None,
                enforce_https,
                github_pages_domain: Some(domain.clone()),
            });
        } else if let Some(domains) = &mut self.blog.domains {
            domains.primary = Some(domain.clone());
            domains.enforce_https = enforce_https;
            domains.github_pages_domain = Some(domain);
        }
    }

    /// Set subdomain configuration
    pub fn set_subdomain(&mut self, prefix: String, base_domain: String, enforce_https: bool) {
        let full_domain = format!("{}.{}", prefix, base_domain);

        if self.blog.domains.is_none() {
            self.blog.domains = Some(DomainConfig {
                primary: None,
                aliases: Vec::new(),
                subdomain: Some(SubdomainConfig {
                    prefix: prefix.clone(),
                    base_domain: base_domain.clone(),
                }),
                enforce_https,
                github_pages_domain: Some(full_domain),
            });
        } else if let Some(domains) = &mut self.blog.domains {
            domains.subdomain = Some(SubdomainConfig {
                prefix: prefix.clone(),
                base_domain: base_domain.clone(),
            });
            domains.enforce_https = enforce_https;
            domains.github_pages_domain = Some(full_domain);
        }
    }

    /// Add domain alias
    pub fn add_domain_alias(&mut self, alias: String) {
        if self.blog.domains.is_none() {
            self.blog.domains = Some(DomainConfig {
                primary: None,
                aliases: vec![alias],
                subdomain: None,
                enforce_https: true,
                github_pages_domain: None,
            });
        } else if let Some(domains) = &mut self.blog.domains {
            if !domains.aliases.contains(&alias) {
                domains.aliases.push(alias);
            }
        }
    }

    /// Remove domain alias
    pub fn remove_domain_alias(&mut self, alias: &str) {
        if let Some(domains) = &mut self.blog.domains {
            domains.aliases.retain(|a| a != alias);
        }
    }

    /// Clear all domain configuration
    pub fn clear_domains(&mut self) {
        self.blog.domains = None;
    }

    /// Get all configured domains (primary + aliases + subdomain)
    pub fn get_all_domains(&self) -> Vec<String> {
        let mut domains = Vec::new();

        if let Some(domain_config) = &self.blog.domains {
            if let Some(primary) = &domain_config.primary {
                domains.push(primary.clone());
            }

            if let Some(subdomain) = &domain_config.subdomain {
                domains.push(format!("{}.{}", subdomain.prefix, subdomain.base_domain));
            }

            domains.extend(domain_config.aliases.clone());
        }

        domains
    }
}

/// Environment variable utilities for configuration
pub struct EnvConfig;

impl EnvConfig {
    /// Get GitHub token from environment
    pub fn github_token() -> Option<String> {
        std::env::var("GITHUB_TOKEN")
            .ok()
            .or_else(|| std::env::var("GH_TOKEN").ok())
    }

    /// Get author name from git config or environment
    pub fn git_author_name() -> Option<String> {
        std::env::var("GIT_AUTHOR_NAME").ok().or_else(|| {
            std::process::Command::new("git")
                .args(["config", "user.name"])
                .output()
                .ok()
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|s| s.trim().to_string())
        })
    }

    /// Get author email from git config or environment
    pub fn git_author_email() -> Option<String> {
        std::env::var("GIT_AUTHOR_EMAIL").ok().or_else(|| {
            std::process::Command::new("git")
                .args(["config", "user.email"])
                .output()
                .ok()
                .and_then(|output| String::from_utf8(output.stdout).ok())
                .map(|s| s.trim().to_string())
        })
    }

    /// Get GitHub username from environment or git config
    pub fn github_username() -> Option<String> {
        std::env::var("GITHUB_USERNAME")
            .ok()
            .or_else(|| std::env::var("GITHUB_USER").ok())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let toml_str = toml::to_string(&config).unwrap();
        let deserialized: Config = toml::from_str(&toml_str).unwrap();

        assert_eq!(config.blog.title, deserialized.blog.title);
        assert_eq!(config.theme.name, deserialized.theme.name);
    }

    #[test]
    fn test_config_file_operations() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("blogr.toml");

        let config = Config::default();
        config.save_to_file(&config_path).unwrap();

        let loaded_config = Config::load_from_file(&config_path).unwrap();
        assert_eq!(config.blog.title, loaded_config.blog.title);
    }

    #[test]
    fn test_config_validation() {
        let mut config = Config::default();
        assert!(config.validate().is_ok());

        config.blog.title = "".to_string();
        assert!(config.validate().is_err());
    }

    #[test]
    fn test_backward_compatibility() {
        // Test that old config files without [search], [dev], and [newsletter] sections can still be parsed
        let old_config_toml = r#"
[blog]
title = "My Blog"
author = "Test Author"
description = "Test Description"
base_url = "https://example.com"

[theme]
name = "minimal-retro"

[build]
output_dir = "dist"
"#;

        let config: Config = toml::from_str(old_config_toml).unwrap();

        // Should use default search config
        assert!(config.search.enabled);
        assert_eq!(config.search.fields, vec!["title", "tags", "content"]);
        assert!(config.search.minify);
        assert!(config.search.lazy_load);
        assert!(!config.search.remove_stopwords);

        // Should use default dev config
        assert_eq!(config.dev.port, 3000);
        assert!(config.dev.auto_reload);

        // Should use default newsletter config
        assert!(!config.newsletter.enabled);
        assert!(config.newsletter.subscribe_email.is_none());
        assert!(config.newsletter.sender_name.is_none());
    }

    #[test]
    fn test_newsletter_validation() {
        let mut config = Config::default();

        // Default newsletter config should be valid
        assert!(config.validate().is_ok());

        // Enabled newsletter without subscribe_email should fail
        config.newsletter.enabled = true;
        assert!(config.validate().is_err());

        // Invalid email format should fail
        config.newsletter.subscribe_email = Some("invalid-email".to_string());
        assert!(config.validate().is_err());

        // Valid email should pass
        config.newsletter.subscribe_email = Some("newsletter@example.com".to_string());
        assert!(config.validate().is_ok());
    }
}
