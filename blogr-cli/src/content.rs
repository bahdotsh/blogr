use anyhow::{anyhow, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostMetadata {
    pub title: String,
    #[serde(deserialize_with = "deserialize_date")]
    pub date: DateTime<Utc>,
    pub author: String,
    pub description: String,
    pub tags: Vec<String>,
    pub status: PostStatus,
    pub slug: String,
    #[serde(default)]
    pub featured: bool,
    #[serde(default)]
    pub external_url: Option<String>,
}

fn deserialize_date<'de, D>(deserializer: D) -> Result<DateTime<Utc>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    use serde::Deserialize;
    let s = String::deserialize(deserializer)?;

    // Try parsing as RFC3339 first
    if let Ok(dt) = DateTime::parse_from_rfc3339(&s) {
        return Ok(dt.with_timezone(&Utc));
    }

    // Try parsing as date only (YYYY-MM-DD)
    if let Ok(naive_date) = chrono::NaiveDate::parse_from_str(&s, "%Y-%m-%d") {
        let naive_datetime = naive_date.and_hms_opt(0, 0, 0).unwrap();
        return Ok(DateTime::from_naive_utc_and_offset(naive_datetime, Utc));
    }

    // Try parsing as datetime without timezone
    if let Ok(naive_datetime) = chrono::NaiveDateTime::parse_from_str(&s, "%Y-%m-%d %H:%M:%S") {
        return Ok(DateTime::from_naive_utc_and_offset(naive_datetime, Utc));
    }

    Err(serde::de::Error::custom(format!(
        "Unable to parse date: {}",
        s
    )))
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PostStatus {
    #[default]
    Draft,
    Published,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Post {
    pub metadata: PostMetadata,
    pub content: String,
    #[allow(dead_code)]
    pub file_path: PathBuf,
}

impl Post {
    /// Create a new post with the given metadata
    pub fn new(
        title: String,
        author: String,
        description: Option<String>,
        tags: Vec<String>,
        slug: Option<String>,
        status: PostStatus,
        external_url: Option<String>,
    ) -> Result<Self> {
        // Validate external URL if provided
        if let Some(ref url) = external_url {
            if !url.starts_with("http://") && !url.starts_with("https://") {
                return Err(anyhow!(
                    "external_url must start with http:// or https://, got: {}",
                    url
                ));
            }
            // Ensure the URL has a host (not just a bare scheme)
            let after_scheme = url.split_once("://").map(|(_, rest)| rest).unwrap_or("");
            if after_scheme.is_empty() || after_scheme == "/" {
                return Err(anyhow!("external_url must include a host, got: {}", url));
            }
        }

        let slug = slug.unwrap_or_else(|| Self::generate_slug(&title));
        let description = description.unwrap_or_else(|| format!("A post about {}", title));

        let metadata = PostMetadata {
            title: title.clone(),
            date: Utc::now(),
            author,
            description,
            tags,
            status,
            slug: slug.clone(),
            featured: false,
            external_url: external_url.clone(),
        };

        let content = if external_url.is_some() {
            String::new()
        } else {
            format!("# {}\n\nYour content goes here...", title)
        };

        Ok(Self {
            metadata,
            content,
            file_path: PathBuf::from(format!("{}.md", slug)),
        })
    }

    /// Check if this is an external post (links to external URL)
    pub fn is_external(&self) -> bool {
        self.metadata.external_url.is_some()
    }

    /// Get the URL for this post. Returns an absolute URL for external posts,
    /// or a relative path (e.g. "posts/slug.html") for local posts. Callers
    /// rendering into templates should apply the `url()` helper to local paths
    /// but use external URLs as-is.
    pub fn post_url(&self) -> String {
        match &self.metadata.external_url {
            Some(url) => url.clone(),
            None => format!("posts/{}.html", self.metadata.slug),
        }
    }

    /// Parse a post from a markdown file
    pub fn from_file<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let content = fs::read_to_string(path)?;

        // Split frontmatter and content
        let (frontmatter, body) = Self::parse_frontmatter(&content)?;

        // Parse metadata from frontmatter
        let metadata: PostMetadata = serde_yaml::from_str(&frontmatter)
            .map_err(|e| anyhow!("Failed to parse frontmatter: {}", e))?;

        Ok(Self {
            metadata,
            content: body,
            file_path: path.to_path_buf(),
        })
    }

    /// Save the post to a file
    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let path = path.as_ref();

        // Create a serializable version with date as string
        #[derive(Serialize)]
        struct SerializableMetadata {
            title: String,
            date: String,
            author: String,
            description: String,
            tags: Vec<String>,
            status: PostStatus,
            slug: String,
            featured: bool,
            #[serde(skip_serializing_if = "Option::is_none")]
            external_url: Option<String>,
        }

        let serializable = SerializableMetadata {
            title: self.metadata.title.clone(),
            date: self.metadata.date.format("%Y-%m-%d").to_string(),
            author: self.metadata.author.clone(),
            description: self.metadata.description.clone(),
            tags: self.metadata.tags.clone(),
            status: self.metadata.status.clone(),
            slug: self.metadata.slug.clone(),
            featured: self.metadata.featured,
            external_url: self.metadata.external_url.clone(),
        };

        // Create frontmatter
        let frontmatter = serde_yaml::to_string(&serializable)
            .map_err(|e| anyhow!("Failed to serialize frontmatter: {}", e))?;

        // Combine frontmatter and content
        let full_content = format!("---\n{}---\n\n{}", frontmatter, self.content);

        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }

        fs::write(path, full_content)?;
        Ok(())
    }

    /// Generate a URL-friendly slug from a title
    pub fn generate_slug(title: &str) -> String {
        title
            .to_lowercase()
            .chars()
            .map(|c| if c.is_alphanumeric() { c } else { '-' })
            .collect::<String>()
            .split('-')
            .filter(|s| !s.is_empty())
            .collect::<Vec<_>>()
            .join("-")
    }

    /// Parse frontmatter and content from markdown
    fn parse_frontmatter(content: &str) -> Result<(String, String)> {
        if !content.starts_with("---\n") {
            return Err(anyhow!("Post must start with YAML frontmatter"));
        }

        let content = &content[4..]; // Skip initial "---\n"

        if let Some(end_pos) = content.find("\n---\n") {
            let frontmatter = &content[..end_pos];
            let body = &content[end_pos + 5..]; // Skip "\n---\n"
            Ok((frontmatter.to_string(), body.to_string()))
        } else {
            Err(anyhow!("Frontmatter not properly closed with '---'"))
        }
    }

    /// Get the post's filename based on its slug
    pub fn filename(&self) -> String {
        format!("{}.md", self.metadata.slug)
    }

    /// Check if post matches the given tag
    pub fn has_tag(&self, tag: &str) -> bool {
        self.metadata
            .tags
            .iter()
            .any(|t| t.eq_ignore_ascii_case(tag))
    }

    /// Get estimated reading time in minutes
    pub fn reading_time(&self) -> usize {
        if self.is_external() {
            return 0;
        }
        const WORDS_PER_MINUTE: usize = 200;
        let word_count = self.content.split_whitespace().count();
        (word_count / WORDS_PER_MINUTE).max(1)
    }
}

/// Manager for blog posts
pub struct PostManager {
    posts_dir: PathBuf,
}

impl PostManager {
    pub fn new<P: AsRef<Path>>(posts_dir: P) -> Self {
        Self {
            posts_dir: posts_dir.as_ref().to_path_buf(),
        }
    }

    /// Load all posts from the posts directory
    pub fn load_all_posts(&self) -> Result<Vec<Post>> {
        let mut posts = Vec::new();

        if !self.posts_dir.exists() {
            return Ok(posts);
        }

        for entry in WalkDir::new(&self.posts_dir)
            .follow_links(true)
            .into_iter()
            .filter_map(|e| e.ok())
        {
            let path = entry.path();
            if path.is_file() && path.extension().is_some_and(|ext| ext == "md") {
                match Post::from_file(path) {
                    Ok(post) => posts.push(post),
                    Err(e) => eprintln!("Warning: Failed to load post {}: {}", path.display(), e),
                }
            }
        }

        // Sort by date (newest first)
        posts.sort_by(|a, b| b.metadata.date.cmp(&a.metadata.date));

        Ok(posts)
    }

    /// Find a post by slug
    pub fn find_by_slug(&self, slug: &str) -> Result<Option<Post>> {
        let posts = self.load_all_posts()?;
        Ok(posts.into_iter().find(|p| p.metadata.slug == slug))
    }

    /// Find all posts with a specific tag
    #[allow(dead_code)]
    pub fn find_by_tag(&self, tag: &str) -> Result<Vec<Post>> {
        let posts = self.load_all_posts()?;
        Ok(posts.into_iter().filter(|p| p.has_tag(tag)).collect())
    }

    /// Get posts with a specific status
    #[allow(dead_code)]
    pub fn find_by_status(&self, status: PostStatus) -> Result<Vec<Post>> {
        let posts = self.load_all_posts()?;
        Ok(posts
            .into_iter()
            .filter(|p| p.metadata.status == status)
            .collect())
    }

    /// Save a post to the posts directory
    pub fn save_post(&self, post: &Post) -> Result<PathBuf> {
        let file_path = self.posts_dir.join(post.filename());
        post.save_to_file(&file_path)?;
        Ok(file_path)
    }

    /// Delete a post by slug
    pub fn delete_post(&self, slug: &str) -> Result<bool> {
        if let Some(post) = self.find_by_slug(slug)? {
            let file_path = self.posts_dir.join(post.filename());
            fs::remove_file(file_path)?;
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Get all unique tags from all posts
    #[allow(dead_code)]
    pub fn get_all_tags(&self) -> Result<Vec<String>> {
        let posts = self.load_all_posts()?;
        let mut tags: Vec<String> = posts
            .iter()
            .flat_map(|p| &p.metadata.tags)
            .cloned()
            .collect();

        tags.sort();
        tags.dedup();
        Ok(tags)
    }

    /// Get post statistics
    #[allow(dead_code)]
    pub fn get_stats(&self) -> Result<PostStats> {
        let posts = self.load_all_posts()?;
        let published_count = posts
            .iter()
            .filter(|p| p.metadata.status == PostStatus::Published)
            .count();
        let draft_count = posts
            .iter()
            .filter(|p| p.metadata.status == PostStatus::Draft)
            .count();
        let featured_count = posts.iter().filter(|p| p.metadata.featured).count();
        let tags = self.get_all_tags()?;

        Ok(PostStats {
            total_posts: posts.len(),
            published_posts: published_count,
            draft_posts: draft_count,
            featured_posts: featured_count,
            total_tags: tags.len(),
            unique_tags: tags,
        })
    }
}

#[derive(Debug)]
#[allow(dead_code)]
pub struct PostStats {
    pub total_posts: usize,
    pub published_posts: usize,
    pub draft_posts: usize,
    pub featured_posts: usize,
    pub total_tags: usize,
    pub unique_tags: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_slug_generation() {
        assert_eq!(Post::generate_slug("Hello World"), "hello-world");
        assert_eq!(Post::generate_slug("Rust & JavaScript"), "rust-javascript");
        assert_eq!(
            Post::generate_slug("2023 Year in Review"),
            "2023-year-in-review"
        );
        assert_eq!(Post::generate_slug("Multiple---Dashes"), "multiple-dashes");
    }

    #[test]
    fn test_frontmatter_parsing() {
        let content = r#"---
title: "Test Post"
date: "2023-01-01T00:00:00Z"
author: "Test Author"
description: "A test post"
tags: ["test"]
status: "draft"
slug: "test-post"
featured: false
---

# Test Content

This is the body of the post."#;

        let (frontmatter, body) = Post::parse_frontmatter(content).unwrap();
        assert!(frontmatter.contains("title: \"Test Post\""));
        assert!(body.trim_start().starts_with("# Test Content"));
    }

    #[test]
    fn test_post_creation_and_save() {
        let temp_dir = TempDir::new().unwrap();
        let post = Post::new(
            "Test Post".to_string(),
            "Test Author".to_string(),
            Some("A test description".to_string()),
            vec!["test".to_string(), "example".to_string()],
            None,
            PostStatus::Draft,
            None,
        )
        .unwrap();

        let file_path = temp_dir.path().join("test-post.md");
        post.save_to_file(&file_path).unwrap();

        let loaded_post = Post::from_file(&file_path).unwrap();
        assert_eq!(loaded_post.metadata.title, "Test Post");
        assert_eq!(loaded_post.metadata.slug, "test-post");
        assert_eq!(loaded_post.metadata.tags, vec!["test", "example"]);
        assert!(!loaded_post.is_external());
        assert_eq!(loaded_post.metadata.external_url, None);
    }

    #[test]
    fn test_external_post_creation_and_roundtrip() {
        let temp_dir = TempDir::new().unwrap();
        let url = "https://example.com/my-article".to_string();
        let post = Post::new(
            "External Article".to_string(),
            "Test Author".to_string(),
            Some("An article from elsewhere".to_string()),
            vec!["external".to_string()],
            None,
            PostStatus::Published,
            Some(url.clone()),
        )
        .unwrap();

        assert!(post.is_external());
        assert_eq!(post.post_url(), url);
        assert_eq!(post.reading_time(), 0);
        assert!(post.content.is_empty());

        let file_path = temp_dir.path().join("external-article.md");
        post.save_to_file(&file_path).unwrap();

        let loaded = Post::from_file(&file_path).unwrap();
        assert!(loaded.is_external());
        assert_eq!(loaded.metadata.external_url, Some(url));
        assert_eq!(loaded.metadata.title, "External Article");
    }

    #[test]
    fn test_backward_compat_no_external_url() {
        let content = r#"---
title: "Old Post"
date: "2023-06-15"
author: "Author"
description: "A post without external_url field"
tags: ["test"]
status: "published"
slug: "old-post"
---

Some content."#;

        let temp_dir = TempDir::new().unwrap();
        let file_path = temp_dir.path().join("old-post.md");
        std::fs::write(&file_path, content).unwrap();

        let post = Post::from_file(&file_path).unwrap();
        assert!(!post.is_external());
        assert_eq!(post.metadata.external_url, None);
        assert_eq!(post.post_url(), "posts/old-post.html");
    }

    #[test]
    fn test_external_url_validation_rejects_invalid() {
        let result = Post::new(
            "Bad Link".to_string(),
            "Author".to_string(),
            None,
            vec![],
            None,
            PostStatus::Published,
            Some("not-a-url".to_string()),
        );
        assert!(result.is_err());

        let result = Post::new(
            "JS Injection".to_string(),
            "Author".to_string(),
            None,
            vec![],
            None,
            PostStatus::Published,
            Some("javascript:alert(1)".to_string()),
        );
        assert!(result.is_err());

        // Bare scheme with no host
        let result = Post::new(
            "Bare Scheme".to_string(),
            "Author".to_string(),
            None,
            vec![],
            None,
            PostStatus::Published,
            Some("https://".to_string()),
        );
        assert!(result.is_err());

        // Bare scheme with only a trailing slash
        let result = Post::new(
            "Bare Scheme Slash".to_string(),
            "Author".to_string(),
            None,
            vec![],
            None,
            PostStatus::Published,
            Some("https:///".to_string()),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_external_url_validation_accepts_valid() {
        assert!(Post::new(
            "HTTP".to_string(),
            "Author".to_string(),
            None,
            vec![],
            None,
            PostStatus::Published,
            Some("http://example.com".to_string()),
        )
        .is_ok());

        assert!(Post::new(
            "HTTPS".to_string(),
            "Author".to_string(),
            None,
            vec![],
            None,
            PostStatus::Published,
            Some("https://example.com/article".to_string()),
        )
        .is_ok());
    }

    #[test]
    fn test_post_url_local_vs_external() {
        let local = Post::new(
            "Local Post".to_string(),
            "Author".to_string(),
            None,
            vec![],
            Some("my-local-post".to_string()),
            PostStatus::Published,
            None,
        )
        .unwrap();
        assert_eq!(local.post_url(), "posts/my-local-post.html");
        assert!(!local.is_external());

        let external = Post::new(
            "External Post".to_string(),
            "Author".to_string(),
            None,
            vec![],
            None,
            PostStatus::Published,
            Some("https://example.com/article?a=1&b=2".to_string()),
        )
        .unwrap();
        assert_eq!(external.post_url(), "https://example.com/article?a=1&b=2");
        assert!(external.is_external());
    }
}
