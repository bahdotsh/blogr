use anyhow::{Context, Result};
use blogr_themes::SiteType;
use std::fs;
use std::path::{Path, PathBuf};

use crate::config::Config;

/// Project structure and utilities
#[derive(Debug, Clone)]
pub struct Project {
    pub root: PathBuf,
    pub config: Config,
}

impl Project {
    /// Create a new project instance
    pub fn new(root: PathBuf, config: Config) -> Self {
        Self { root, config }
    }

    /// Find and load existing project
    #[allow(dead_code)]
    pub fn find_and_load() -> Result<Self> {
        let (config, root) = Config::load_from_project()?;
        Ok(Self::new(root, config))
    }

    /// Check if current directory is inside a Blogr project
    pub fn is_in_project() -> bool {
        Config::find_project_root().unwrap_or(None).is_some()
    }

    /// Find a project starting from current directory
    pub fn find_project() -> Result<Option<Self>> {
        if let Some(root) = Config::find_project_root()? {
            let config_path = root.join("blogr.toml");
            let config = Config::load_from_file(&config_path)?;
            Ok(Some(Self::new(root, config)))
        } else {
            Ok(None)
        }
    }

    /// Load configuration from the project root
    pub fn load_config(&self) -> Result<Config> {
        let config_path = self.root.join("blogr.toml");
        Config::load_from_file(&config_path)
    }

    /// Initialize a new project in the given directory
    pub fn init<P: AsRef<Path>>(
        path: P,
        name: String,
        author: String,
        description: String,
        github_username: Option<String>,
        github_repo: Option<String>,
    ) -> Result<Self> {
        Self::init_with_type(
            path,
            name,
            author,
            description,
            github_username,
            github_repo,
            false,
        )
    }

    /// Initialize a new personal website in the given directory
    pub fn init_personal<P: AsRef<Path>>(
        path: P,
        name: String,
        author: String,
        description: String,
        github_username: Option<String>,
        github_repo: Option<String>,
    ) -> Result<Self> {
        Self::init_with_type(
            path,
            name,
            author,
            description,
            github_username,
            github_repo,
            true,
        )
    }

    /// Internal initialization function
    fn init_with_type<P: AsRef<Path>>(
        path: P,
        name: String,
        author: String,
        description: String,
        github_username: Option<String>,
        github_repo: Option<String>,
        is_personal: bool,
    ) -> Result<Self> {
        let project_path = path.as_ref().to_path_buf().join(&name);

        // Create project directory if it doesn't exist
        if !project_path.exists() {
            fs::create_dir_all(&project_path).with_context(|| {
                format!(
                    "Failed to create project directory: {}",
                    project_path.display()
                )
            })?;
        }

        // Check if directory is empty or contains only hidden files
        let entries: Vec<_> = fs::read_dir(&project_path)?
            .filter_map(|entry| entry.ok())
            .filter(|entry| !entry.file_name().to_string_lossy().starts_with('.'))
            .collect();

        if !entries.is_empty() {
            anyhow::bail!("Directory is not empty. Please choose an empty directory or remove existing files.");
        }

        // Create project structure
        Self::create_directory_structure(&project_path, is_personal)?;

        // Create configuration
        let config = if is_personal {
            Config::new_personal(name, author, description, github_username, github_repo)
        } else {
            Config::new_with_defaults(name, author, description, github_username, github_repo)
        };

        // Validate configuration
        config.validate()?;

        // Save configuration
        let config_path = project_path.join("blogr.toml");
        config.save_to_file(&config_path)?;

        // Create sample files
        if is_personal {
            Self::create_personal_files(&project_path, &config)?;
        } else {
            Self::create_sample_files(&project_path, &config)?;
        }

        // Create GitHub Actions workflow if GitHub integration is enabled
        if config.github.is_some() {
            Self::create_github_workflow(&project_path, &config)?;
        }

        Ok(Self::new(project_path, config))
    }

    /// Create the basic directory structure for a new project
    fn create_directory_structure(project_path: &Path, is_personal: bool) -> Result<()> {
        let base_dirs = [
            "themes",
            "static",
            "static/images",
            "static/css",
            "static/js",
            ".blogr",
        ];

        for dir in base_dirs {
            let dir_path = project_path.join(dir);
            fs::create_dir_all(&dir_path)
                .with_context(|| format!("Failed to create directory: {}", dir_path.display()))?;
        }

        // Only create posts directory for blog mode
        if !is_personal {
            let posts_dir = project_path.join("posts");
            fs::create_dir_all(&posts_dir)
                .with_context(|| format!("Failed to create directory: {}", posts_dir.display()))?;
        }

        Ok(())
    }

    /// Create sample files for a new project
    fn create_sample_files(project_path: &Path, config: &Config) -> Result<()> {
        // Create .gitignore
        let gitignore_content = include_str!("../templates/gitignore.template");
        fs::write(project_path.join(".gitignore"), gitignore_content)
            .with_context(|| "Failed to create .gitignore file")?;

        // Create README.md
        let readme_template = include_str!("../templates/readme.template");
        let readme_content = readme_template
            .replace("{title}", &config.blog.title)
            .replace("{description}", &config.blog.description)
            .replace("{author}", &config.blog.author);
        fs::write(project_path.join("README.md"), readme_content)
            .with_context(|| "Failed to create README.md file")?;

        // Create sample post
        let sample_post_template = include_str!("../templates/sample_post.template");
        let sample_post = sample_post_template
            .replace("{title}", "Welcome to My Blog")
            .replace("{author}", &config.blog.author)
            .replace("{date}", &chrono::Utc::now().format("%Y-%m-%d").to_string())
            .replace("{blog_title}", &config.blog.title);
        fs::write(project_path.join("posts/welcome.md"), sample_post)
            .with_context(|| "Failed to create sample post")?;

        // Create about page
        let about_template = include_str!("../templates/about.template");
        let about_content = about_template
            .replace("{author}", &config.blog.author)
            .replace("{blog_title}", &config.blog.title)
            .replace("{date}", &chrono::Utc::now().format("%Y-%m-%d").to_string());
        fs::write(project_path.join("posts/about.md"), about_content)
            .with_context(|| "Failed to create about page")?;

        Ok(())
    }

    /// Create sample files for a personal website
    fn create_personal_files(project_path: &Path, config: &Config) -> Result<()> {
        // Create .gitignore
        let gitignore_content = include_str!("../templates/gitignore.template");
        fs::write(project_path.join(".gitignore"), gitignore_content)
            .with_context(|| "Failed to create .gitignore file")?;

        // Create README.md
        let readme_template = include_str!("../templates/readme.template");
        let readme_content = readme_template
            .replace("{title}", &config.blog.title)
            .replace("{description}", &config.blog.description)
            .replace("{author}", &config.blog.author);
        fs::write(project_path.join("README.md"), readme_content)
            .with_context(|| "Failed to create README.md file")?;

        // Create content.md for personal info - theme-specific
        let content_md = match config.theme.name.as_str() {
            "musashi" => blogr_themes::MusashiTheme::example_content(&config.blog.author),
            "dark-minimal" => blogr_themes::DarkMinimalTheme::example_content(&config.blog.author),
            _ => {
                // Generic fallback for other themes
                format!(
                    r#"# {}

Welcome to my personal website!

## About Me

I'm {}, {}

## What I Do

- 💻 Developer
- 🎨 Designer
- 🚀 Creator

## Get In Touch

Feel free to reach out if you'd like to collaborate or just say hello!

- Email: hello@example.com
- GitHub: https://github.com/username
- Twitter: @username
"#,
                    config.blog.title, config.blog.author, config.blog.description
                )
            }
        };

        fs::write(project_path.join("content.md"), content_md)
            .with_context(|| "Failed to create content.md file")?;

        Ok(())
    }

    /// Create GitHub Actions workflow for automated deployment
    fn create_github_workflow(project_path: &Path, config: &Config) -> Result<()> {
        if let Some(github_config) = &config.github {
            // Create .github/workflows directory
            let workflows_dir = project_path.join(".github/workflows");
            fs::create_dir_all(&workflows_dir)
                .with_context(|| "Failed to create .github/workflows directory")?;

            // Create workflow file
            let workflow_template = include_str!("../templates/github_workflow.template");
            let workflow_content = workflow_template
                .replace("your-username", &github_config.username)
                .replace("your-repo", &github_config.repository);

            fs::write(workflows_dir.join("deploy.yml"), workflow_content)
                .with_context(|| "Failed to create GitHub Actions workflow")?;
        }
        Ok(())
    }

    /// Get posts directory
    pub fn posts_dir(&self) -> PathBuf {
        self.config.posts_dir(&self.root)
    }

    /// Get themes directory
    #[allow(dead_code)]
    pub fn themes_dir(&self) -> PathBuf {
        self.config.themes_dir(&self.root)
    }

    /// Get output directory
    #[allow(dead_code)]
    pub fn output_dir(&self) -> PathBuf {
        self.config.output_dir(&self.root)
    }

    /// Get static files directory
    pub fn static_dir(&self) -> PathBuf {
        self.root.join("static")
    }

    /// Get blogr internal directory
    #[allow(dead_code)]
    pub fn blogr_dir(&self) -> PathBuf {
        self.root.join(".blogr")
    }

    /// Validate project structure
    pub fn validate(&self) -> Result<Vec<String>> {
        let mut issues = Vec::new();

        // Check required directories based on site type
        let is_personal = self.config.site.site_type == SiteType::Personal;

        if !is_personal {
            // For blogs, require posts directory
            let posts_dir = self.posts_dir();
            if !posts_dir.exists() {
                issues.push("Missing required directory: posts".to_string());
            } else if !posts_dir.is_dir() {
                issues.push("posts exists but is not a directory".to_string());
            }
        }

        // Static directory is required for all site types
        let static_dir = self.static_dir();
        if !static_dir.exists() {
            issues.push("Missing required directory: static".to_string());
        } else if !static_dir.is_dir() {
            issues.push("static exists but is not a directory".to_string());
        }

        // Check config file
        let config_path = self.root.join("blogr.toml");
        if !config_path.exists() {
            issues.push("Missing blogr.toml configuration file".to_string());
        }

        // Validate configuration
        if let Err(e) = self.config.validate() {
            issues.push(format!("Configuration validation error: {}", e));
        }

        Ok(issues)
    }

    /// Clean build artifacts and temporary files
    #[allow(dead_code)]
    pub fn clean(&self) -> Result<()> {
        let output_dir = self.output_dir();
        if output_dir.exists() {
            fs::remove_dir_all(&output_dir).with_context(|| {
                format!(
                    "Failed to remove output directory: {}",
                    output_dir.display()
                )
            })?;
        }

        let blogr_dir = self.blogr_dir();
        if blogr_dir.exists() {
            // Remove cache files but keep the directory
            for entry in fs::read_dir(&blogr_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.is_file() && path.extension().is_some_and(|ext| ext == "cache") {
                    fs::remove_file(&path).with_context(|| {
                        format!("Failed to remove cache file: {}", path.display())
                    })?;
                }
            }
        }

        Ok(())
    }

    /// Get project statistics
    #[allow(dead_code)]
    pub fn stats(&self) -> Result<ProjectStats> {
        let posts_dir = self.posts_dir();
        let mut stats = ProjectStats::default();

        if posts_dir.exists() {
            for entry in walkdir::WalkDir::new(&posts_dir) {
                let entry = entry?;
                let path = entry.path();

                if path.is_file() {
                    if let Some(ext) = path.extension() {
                        if ext == "md" || ext == "markdown" {
                            stats.total_posts += 1;

                            // Read file to get more details
                            if let Ok(content) = fs::read_to_string(path) {
                                if content.contains("status: draft")
                                    || content.contains("draft: true")
                                {
                                    stats.draft_posts += 1;
                                } else {
                                    stats.published_posts += 1;
                                }

                                // Estimate word count
                                let word_count = content.split_whitespace().count();
                                stats.total_words += word_count;
                            }
                        }
                    }
                }
            }
        }

        let static_dir = self.static_dir();
        if static_dir.exists() {
            for entry in walkdir::WalkDir::new(&static_dir) {
                let entry = entry?;
                if entry.path().is_file() {
                    stats.static_files += 1;
                }
            }
        }

        Ok(stats)
    }

    /// Reload configuration from file
    #[allow(dead_code)]
    pub fn reload_config(&mut self) -> Result<()> {
        let config_path = self.root.join("blogr.toml");
        self.config = Config::load_from_file(&config_path)?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone)]
#[allow(dead_code)]
pub struct ProjectStats {
    pub total_posts: usize,
    pub published_posts: usize,
    pub draft_posts: usize,
    pub total_words: usize,
    pub static_files: usize,
}

impl ProjectStats {
    #[allow(dead_code)]
    pub fn average_words_per_post(&self) -> f64 {
        if self.total_posts == 0 {
            0.0
        } else {
            self.total_words as f64 / self.total_posts as f64
        }
    }
}

/// Auto-initialization helper
#[allow(dead_code)]
pub struct AutoInit;

impl AutoInit {
    /// Check if command should trigger auto-initialization prompt
    #[allow(dead_code)]
    pub fn should_prompt(command_name: &str) -> bool {
        matches!(
            command_name,
            "new" | "build" | "serve" | "deploy" | "theme" | "project"
        )
    }

    /// Prompt user for auto-initialization
    #[allow(dead_code)]
    pub fn prompt_user(command_name: &str) -> Result<bool> {
        use std::io::{self, Write};

        println!("❌ No Blogr project found in the current directory or its parents.");
        println!("The '{}' command requires a Blogr project.", command_name);
        println!();
        print!("Would you like to initialize a new project here? [y/N]: ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;

        let input = input.trim().to_lowercase();
        Ok(input == "y" || input == "yes")
    }

    /// Interactive project initialization
    #[allow(dead_code)]
    pub fn interactive_init() -> Result<Project> {
        use std::io::{self, Write};

        println!("🚀 Let's set up your new blog!");
        println!();

        // Get project name
        print!("Project title: ");
        io::stdout().flush()?;
        let mut title = String::new();
        io::stdin().read_line(&mut title)?;
        let title = title.trim().to_string();

        // Get author name
        let default_author =
            crate::config::EnvConfig::git_author_name().unwrap_or_else(|| "Anonymous".to_string());
        print!("Author name [{}]: ", default_author);
        io::stdout().flush()?;
        let mut author = String::new();
        io::stdin().read_line(&mut author)?;
        let author = if author.trim().is_empty() {
            default_author
        } else {
            author.trim().to_string()
        };

        // Get description
        print!("Blog description: ");
        io::stdout().flush()?;
        let mut description = String::new();
        io::stdin().read_line(&mut description)?;
        let description = description.trim().to_string();

        // GitHub integration
        print!("GitHub username (optional): ");
        io::stdout().flush()?;
        let mut github_username = String::new();
        io::stdin().read_line(&mut github_username)?;
        let github_username = if github_username.trim().is_empty() {
            None
        } else {
            Some(github_username.trim().to_string())
        };

        let github_repo = if github_username.is_some() {
            print!("GitHub repository name [{}]: ", title);
            io::stdout().flush()?;
            let mut repo = String::new();
            io::stdin().read_line(&mut repo)?;
            let repo = if repo.trim().is_empty() {
                title.clone()
            } else {
                repo.trim().to_string()
            };
            Some(repo)
        } else {
            None
        };

        let current_dir = std::env::current_dir()?;
        Project::init(
            current_dir,
            title,
            author,
            description,
            github_username,
            github_repo,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_project_initialization() {
        let temp_dir = TempDir::new().unwrap();
        let project = Project::init(
            temp_dir.path(),
            "Test Blog".to_string(),
            "Test Author".to_string(),
            "A test blog".to_string(),
            None,
            None,
        )
        .unwrap();

        assert!(project.root.join("blogr.toml").exists());
        assert!(project.root.join("posts").exists());
        assert!(project.root.join("static").exists());
        assert!(project.root.join("README.md").exists());
    }

    #[test]
    fn test_project_validation() {
        let temp_dir = TempDir::new().unwrap();
        let project = Project::init(
            temp_dir.path(),
            "Test Blog".to_string(),
            "Test Author".to_string(),
            "A test blog".to_string(),
            None,
            None,
        )
        .unwrap();

        let issues = project.validate().unwrap();
        assert!(issues.is_empty());
    }
}
