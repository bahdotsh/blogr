use crate::config::Config;
use crate::content::{Post, PostManager, PostStatus};
use crate::project::Project;
use anyhow::{anyhow, Result};
use blogr_themes::{get_theme_by_name, SiteType, Theme};
use chrono::{Datelike, Utc};
use serde_json::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use tera::{Context, Tera};

// Embed search assets so they are always available in builds and deployments
// Paths are relative to this file: blogr-cli/src/generator/site.rs → ../../static/...
const EMBEDDED_SEARCH_JS: &str = include_str!("../../static/js/search.js");
const EMBEDDED_MINISEARCH_JS: &str = include_str!("../../static/js/vendor/minisearch.min.js");

/// Static site generator
pub struct SiteBuilder {
    /// Project reference
    project: Project,
    /// Build configuration
    config: Config,
    /// Template engine
    tera: Tera,
    /// Theme instance
    theme: Box<dyn Theme>,
    /// Output directory
    output_dir: PathBuf,
    /// Include drafts in build
    include_drafts: bool,
    /// Include future posts in build
    include_future: bool,
    /// Pre-loaded content.md (used during deploy to preserve uncommitted changes)
    content_md: Option<String>,
}

impl SiteBuilder {
    /// Generate newsletter subscription form HTML
    fn generate_newsletter_form(&self) -> String {
        if !self.config.newsletter.enabled {
            return String::new();
        }

        let subscribe_email = match &self.config.newsletter.subscribe_email {
            Some(email) => email,
            None => return String::new(),
        };

        let confirmation_subject = self
            .config
            .newsletter
            .confirmation_subject
            .as_deref()
            .unwrap_or("Newsletter Subscription Request");

        // Create the email body with user's email
        let email_body = "Please add my email address to your newsletter list.";
        let mailto_url = format!(
            "mailto:{}?subject={}&body={}",
            subscribe_email,
            urlencoding::encode(confirmation_subject),
            urlencoding::encode(email_body)
        );

        let form_html = format!(
            r#"<div class="newsletter-subscription">
  <div class="newsletter-header">
    <h3>Stay Updated</h3>
    <p>Join our newsletter for the latest posts and updates</p>
  </div>
  <form class="newsletter-form" onsubmit="handleNewsletterSubmit(event, '{}')" method="get">
    <div class="newsletter-input-group">
      <input
        type="email"
        id="newsletter-email"
        name="email"
        placeholder="Enter your email address"
        required
        class="newsletter-email-input"
        aria-label="Email address for newsletter subscription">
      <button type="submit" class="newsletter-submit-btn">
        <span class="btn-text">Subscribe</span>
        <svg class="btn-icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
          <path d="M22 2L11 13"/>
          <path d="M22 2L15 22L11 13L2 9L22 2Z"/>
        </svg>
      </button>
    </div>
    <p class="newsletter-privacy">
      <small>We respect your privacy. Unsubscribe at any time.</small>
    </p>
  </form>
  <noscript>
    <p class="newsletter-fallback">
      <a href="{}">Subscribe via email</a>
    </p>
  </noscript>
</div>

<script>
function handleNewsletterSubmit(event, subscribeEmail) {{
  event.preventDefault();

  const emailInput = document.getElementById('newsletter-email');
  const userEmail = emailInput.value;

  if (!userEmail) {{
    alert('Please enter your email address');
    return;
  }}

  const subject = encodeURIComponent('{}');
  const body = encodeURIComponent(`Hello,

I would like to subscribe to your newsletter.

My email address is: ${{userEmail}}

Thank you!`);

  const mailtoUrl = `mailto:${{subscribeEmail}}?subject=${{subject}}&body=${{body}}`;

  // Try to open the email client
  window.location.href = mailtoUrl;

  // Show confirmation message
  const form = event.target;
  const originalContent = form.innerHTML;

  form.innerHTML = `
    <div style="text-align: center; padding: 20px;">
      <div style="font-size: 24px; margin-bottom: 10px;">✅</div>
      <h4 style="margin: 0 0 10px 0; color: var(--color-primary, #007acc);">Email Client Opened!</h4>
      <p style="margin: 0; color: var(--color-text-muted, #666);">Please send the email to complete your subscription.</p>
      <button onclick="location.reload()" style="margin-top: 15px; padding: 8px 16px; background: var(--color-primary, #007acc); color: white; border: none; border-radius: 6px; cursor: pointer;">Try Again</button>
    </div>
  `;

  // Reset form after 10 seconds
  setTimeout(() => {{
    form.innerHTML = originalContent;
  }}, 10000);
}}
</script>"#,
            subscribe_email, mailto_url, confirmation_subject
        );

        form_html
    }

    /// Create a new site builder
    pub fn new(
        project: Project,
        output_dir: Option<PathBuf>,
        include_drafts: bool,
        include_future: bool,
    ) -> Result<Self> {
        let config = project.load_config()?;
        Self::new_with_config(project, config, output_dir, include_drafts, include_future)
    }

    /// Create a new site builder with pre-loaded config
    pub fn new_with_config(
        project: Project,
        config: Config,
        output_dir: Option<PathBuf>,
        include_drafts: bool,
        include_future: bool,
    ) -> Result<Self> {
        // Get theme
        let theme_name = &config.theme.name;
        let theme = get_theme_by_name(theme_name)
            .ok_or_else(|| anyhow!("Theme '{}' not found", theme_name))?;

        // Set up template engine (create empty Tera instance)
        let mut tera = Tera::default();

        // Register theme templates
        for (name, template) in theme.templates() {
            tera.add_raw_template(name, template)
                .map_err(|e| anyhow!("Failed to register template '{}': {}", name, e))?;
        }

        // Register template functions for URL generation
        Self::register_template_functions(&mut tera, &config)?;

        let output_dir = output_dir.unwrap_or_else(|| {
            config
                .build
                .output_dir
                .as_ref()
                .map(|p| project.root.join(p))
                .unwrap_or_else(|| project.root.join("_site"))
        });

        Ok(Self {
            project,
            config,
            tera,
            theme,
            output_dir,
            include_drafts,
            include_future,
            content_md: None,
        })
    }

    /// Create a new site builder with pre-loaded config and content.md
    /// Used during deployment to preserve uncommitted changes
    pub fn new_with_config_and_content(
        project: Project,
        config: Config,
        content_md: Option<String>,
        output_dir: Option<PathBuf>,
        include_drafts: bool,
        include_future: bool,
    ) -> Result<Self> {
        let mut builder =
            Self::new_with_config(project, config, output_dir, include_drafts, include_future)?;
        builder.content_md = content_md;
        Ok(builder)
    }

    /// Build the entire site
    pub fn build(&self) -> Result<()> {
        let theme_info = get_theme_by_name(&self.config.theme.name)
            .ok_or(anyhow!(
                "Theme {} not found in this version of Blogr.",
                self.config.theme.name
            ))
            .map(|theme| theme.info())?;

        println!("🚀 Building site with theme '{}'", self.config.theme.name);

        // Clean output directory
        self.clean_output_dir()?;

        match theme_info.site_type {
            SiteType::Blog => self.generate_blog()?,
            SiteType::Personal => self.generate_personal_index()?,
        }
        // Copy theme assets (both blog and personal)
        self.copy_theme_assets()?;

        // Copy project static assets (both blog and personal)
        self.copy_static_assets()?;

        // Generate CNAME file if domain configuration exists
        self.generate_cname_file()?;

        println!(
            "✅ Site built successfully to: {}",
            self.output_dir.display()
        );
        Ok(())
    }

    fn generate_blog(&self) -> Result<()> {
        // Blog mode - generate all blog pages
        // Load all posts
        let post_manager = PostManager::new(self.project.posts_dir());
        let mut all_posts = post_manager.load_all_posts()?;

        // Filter posts based on build options
        all_posts.retain(|post| self.should_include_post(post));

        // Sort posts by date (newest first)
        all_posts.sort_by(|a, b| b.metadata.date.cmp(&a.metadata.date));

        println!("📝 Processing {} posts", all_posts.len());

        // Generate individual post pages
        self.generate_post_pages(&all_posts)?;

        // Generate index page
        self.generate_index_page(&all_posts)?;

        // Generate archive pages
        self.generate_archive_pages(&all_posts)?;

        // Generate tag pages
        self.generate_tag_pages(&all_posts)?;

        // Generate RSS feed
        self.generate_rss_feed(&all_posts)?;

        // Generate static JSON files for pagination
        self.generate_posts_json(&all_posts)?;

        // Generate search index
        self.generate_search_index(&all_posts)?;

        // Copy built-in search assets
        self.copy_search_assets()?;
        Ok(())
    }

    /// Check if a post should be included in the build
    fn should_include_post(&self, post: &Post) -> bool {
        // Check draft status
        if post.metadata.status == PostStatus::Draft && !self.include_drafts {
            return false;
        }

        // Check future posts
        if !self.include_future {
            let now = Utc::now();
            if post.metadata.date > now {
                return false;
            }
        }

        true
    }

    /// Clean the output directory
    fn clean_output_dir(&self) -> Result<()> {
        if self.output_dir.exists() {
            fs::remove_dir_all(&self.output_dir)
                .map_err(|e| anyhow!("Failed to clean output directory: {}", e))?;
        }
        fs::create_dir_all(&self.output_dir)
            .map_err(|e| anyhow!("Failed to create output directory: {}", e))?;
        Ok(())
    }

    /// Generate individual post pages
    fn generate_post_pages(&self, posts: &[Post]) -> Result<()> {
        for post in posts {
            let mut context = Context::new();

            // Add site config
            context.insert("site", &self.config);

            // Add newsletter config and generated form
            context.insert("newsletter", &self.config.newsletter);
            context.insert("newsletter_form", &self.generate_newsletter_form());

            // Add post data
            context.insert("post", post);

            // Convert markdown to HTML
            let html_content = crate::generator::markdown::render_markdown(&post.content)?;
            context.insert("content", &html_content);

            // Calculate reading time (average 200 words per minute)
            let word_count = post.content.split_whitespace().count();
            let reading_time = (word_count / 200).max(1);
            context.insert("reading_time", &reading_time);

            // Render template
            let html = self.tera.render("post.html", &context).map_err(|e| {
                eprintln!("Full Tera error: {:?}", e);
                anyhow!("Failed to render post template: {}", e)
            })?;

            // Write to file
            let post_dir = self.output_dir.join("posts");
            fs::create_dir_all(&post_dir)?;

            let post_file = post_dir.join(format!("{}.html", post.metadata.slug));
            fs::write(&post_file, html).map_err(|e| anyhow!("Failed to write post file: {}", e))?;
        }
        Ok(())
    }

    /// Generate personal website index page
    fn generate_personal_index(&self) -> Result<()> {
        let mut context = Context::new();

        // Add site config with all necessary fields
        context.insert("blog_title", &self.config.blog.title);
        context.insert("blog_description", &self.config.blog.description);
        context.insert("author", &self.config.blog.author);
        context.insert("base_url", &self.config.get_effective_base_url());
        context.insert(
            "language",
            &self.config.blog.language.as_deref().unwrap_or("en"),
        );
        context.insert("site", &self.config);
        context.insert("theme_config", &self.config.theme.config);

        // Add current year
        context.insert("current_year", &Utc::now().year());

        // Read and parse content.md for sections data
        // Use pre-loaded content if available (for deployment with uncommitted changes),
        // otherwise read from disk
        let content_md = if let Some(preloaded) = &self.content_md {
            Some(preloaded.clone())
        } else {
            let content_md_path = self.project.root.join("content.md");
            if content_md_path.exists() {
                Some(
                    fs::read_to_string(&content_md_path)
                        .map_err(|e| anyhow!("Failed to read content.md: {}", e))?,
                )
            } else {
                None
            }
        };

        if let Some(content_md) = content_md {
            // Parse frontmatter to get sections
            if let Ok((frontmatter, _)) = self.parse_frontmatter(&content_md) {
                if let Ok(frontmatter_data) =
                    serde_yaml::from_str::<serde_yaml::Value>(&frontmatter)
                {
                    // Override description with content.md description if it exists
                    if let Some(description) = frontmatter_data.get("description") {
                        if let Some(desc_str) = description.as_str() {
                            context.insert("blog_description", desc_str);
                        }
                    }

                    // Override title with content.md title if it exists
                    if let Some(title) = frontmatter_data.get("title") {
                        if let Some(title_str) = title.as_str() {
                            context.insert("blog_title", title_str);
                        }
                    }

                    // Extract sections if they exist
                    if let Some(sections) = frontmatter_data.get("sections") {
                        context.insert("sections", sections);
                    }

                    // Override theme_config with content.md theme_config if it exists
                    if let Some(content_theme_config) = frontmatter_data.get("theme_config") {
                        context.insert("theme_config", content_theme_config);
                    }
                }
            }
        }

        // Render template
        let html = self
            .tera
            .render("index.html", &context)
            .map_err(|e| anyhow!("Failed to render personal index template: {}", e))?;

        // Write to file
        let index_file = self.output_dir.join("index.html");
        fs::write(&index_file, html).map_err(|e| anyhow!("Failed to write index file: {}", e))?;

        Ok(())
    }

    /// Parse frontmatter from markdown content
    fn parse_frontmatter(&self, content: &str) -> Result<(String, String)> {
        if !content.starts_with("---\n") && !content.starts_with("---\r\n") {
            return Err(anyhow!("Content must start with YAML frontmatter"));
        }

        let content = content
            .strip_prefix("---\r\n")
            .or_else(|| content.strip_prefix("---\n"))
            .ok_or_else(|| anyhow!("Content must start with YAML frontmatter"))?;

        // Find the closing ---
        if let Some(end_pos) = content
            .find("\n---\n")
            .or_else(|| content.find("\r\n---\r\n"))
        {
            let frontmatter = &content[..end_pos];
            let body = content[end_pos..]
                .strip_prefix("\r\n---\r\n")
                .or_else(|| content[end_pos..].strip_prefix("\n---\n"))
                .ok_or_else(|| anyhow!("Could not find closing --- for frontmatter"))?;
            Ok((frontmatter.to_string(), body.to_string()))
        } else {
            Err(anyhow!("Could not find closing --- for frontmatter"))
        }
    }

    /// Generate index page
    fn generate_index_page(&self, posts: &[Post]) -> Result<()> {
        let mut context = Context::new();

        // Add site config
        context.insert("site", &self.config);

        // Add newsletter config and generated form
        context.insert("newsletter", &self.config.newsletter);
        context.insert("newsletter_form", &self.generate_newsletter_form());

        // Prepare posts with rendered content for index
        // Load first batch of posts for initial page load
        let initial_posts: Vec<_> = posts.iter().take(10).collect();
        let mut posts_with_content = Vec::new();

        for post in &initial_posts {
            // Convert markdown to HTML for each post
            let html_content = crate::generator::markdown::render_markdown(&post.content)?;

            // Calculate reading time (average 200 words per minute)
            let word_count = post.content.split_whitespace().count();
            let reading_time = (word_count / 200).max(1);

            // Create a struct that includes both post data and rendered content
            let post_data = serde_json::json!({
                "metadata": post.metadata,
                "content": html_content,
                "reading_time": reading_time
            });

            posts_with_content.push(post_data);
        }

        context.insert("posts", &posts_with_content);

        // Add pagination info
        context.insert("has_more", &(posts.len() > 10));
        context.insert("total_posts", &posts.len());

        // Render template
        let html = self
            .tera
            .render("index.html", &context)
            .map_err(|e| anyhow!("Failed to render index template: {}", e))?;

        // Write to file
        let index_file = self.output_dir.join("index.html");
        fs::write(&index_file, html).map_err(|e| anyhow!("Failed to write index file: {}", e))?;

        Ok(())
    }

    /// Generate archive pages
    fn generate_archive_pages(&self, posts: &[Post]) -> Result<()> {
        let mut context = Context::new();

        // Add site config
        context.insert("site", &self.config);

        // Add newsletter config and generated form
        context.insert("newsletter", &self.config.newsletter);
        context.insert("newsletter_form", &self.generate_newsletter_form());

        // Prepare posts with rendered content
        let mut posts_with_content = Vec::new();
        for post in posts {
            // Convert markdown to HTML for each post
            let html_content = crate::generator::markdown::render_markdown(&post.content)?;

            // Calculate reading time (average 200 words per minute)
            let word_count = post.content.split_whitespace().count();
            let reading_time = (word_count / 200).max(1);

            let post_data = serde_json::json!({
                "metadata": post.metadata,
                "content": html_content,
                "reading_time": reading_time
            });

            posts_with_content.push(post_data);
        }

        context.insert("posts", &posts_with_content);

        // Group posts by year
        let mut posts_by_year: HashMap<i32, Vec<serde_json::Value>> = HashMap::new();
        for post_data in &posts_with_content {
            let year = post_data["metadata"]["date"]
                .as_str()
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                .map(|dt| dt.year())
                .unwrap_or(2024); // fallback year
            posts_by_year
                .entry(year)
                .or_default()
                .push(post_data.clone());
        }
        context.insert("posts_by_year", &posts_by_year);

        // Render template
        let html = self
            .tera
            .render("archive.html", &context)
            .map_err(|e| anyhow!("Failed to render archive template: {}", e))?;

        // Write to file
        let archive_file = self.output_dir.join("archive.html");
        fs::write(&archive_file, html)
            .map_err(|e| anyhow!("Failed to write archive file: {}", e))?;

        Ok(())
    }

    /// Generate tag pages
    fn generate_tag_pages(&self, posts: &[Post]) -> Result<()> {
        // Group posts by tag
        let mut posts_by_tag: HashMap<String, Vec<&Post>> = HashMap::new();
        for post in posts {
            for tag in &post.metadata.tags {
                posts_by_tag.entry(tag.clone()).or_default().push(post);
            }
        }

        // Create tags directory
        let tags_dir = self.output_dir.join("tags");
        fs::create_dir_all(&tags_dir)?;

        // Generate individual tag pages
        for (tag, tag_posts) in &posts_by_tag {
            let mut context = Context::new();

            // Add site config
            context.insert("site", &self.config);

            // Add newsletter config and generated form
            context.insert("newsletter", &self.config.newsletter);
            context.insert("newsletter_form", &self.generate_newsletter_form());

            // Add tag info
            context.insert("tag", tag);

            // Prepare posts with rendered content for this tag
            let mut posts_with_content = Vec::new();
            for post in tag_posts {
                // Convert markdown to HTML for each post
                let html_content = crate::generator::markdown::render_markdown(&post.content)?;

                // Calculate reading time (average 200 words per minute)
                let word_count = post.content.split_whitespace().count();
                let reading_time = (word_count / 200).max(1);

                let post_data = serde_json::json!({
                    "metadata": post.metadata,
                    "content": html_content,
                    "reading_time": reading_time
                });

                posts_with_content.push(post_data);
            }

            context.insert("posts", &posts_with_content);

            // Render template
            let html = self
                .tera
                .render("tag.html", &context)
                .map_err(|e| anyhow!("Failed to render tag template for '{}': {}", tag, e))?;

            // Write to file
            let tag_file = tags_dir.join(format!("{}.html", tag));
            fs::write(&tag_file, html)
                .map_err(|e| anyhow!("Failed to write tag file for '{}': {}", tag, e))?;
        }

        // Generate tags index
        let mut context = Context::new();
        context.insert("site", &self.config);

        // Add newsletter config and generated form
        context.insert("newsletter", &self.config.newsletter);
        context.insert("newsletter_form", &self.generate_newsletter_form());

        // Create tag info with post counts
        let tag_info: Vec<(String, usize)> = posts_by_tag
            .iter()
            .map(|(tag, posts)| (tag.clone(), posts.len()))
            .collect();
        context.insert("tags", &tag_info);

        let html = self
            .tera
            .render("tags.html", &context)
            .map_err(|e| anyhow!("Failed to render tags index template: {}", e))?;

        let tags_index = self.output_dir.join("tags").join("index.html");
        fs::write(&tags_index, html).map_err(|e| anyhow!("Failed to write tags index: {}", e))?;

        Ok(())
    }

    /// Generate RSS feed
    fn generate_rss_feed(&self, posts: &[Post]) -> Result<()> {
        // Get effective base URL for all feed URLs
        let effective_base_url = self.config.get_effective_base_url();

        // Take only the most recent 20 posts for the RSS feed
        let recent_posts: Vec<&Post> = posts.iter().take(20).collect();

        let mut rss_items = Vec::new();

        for post in recent_posts {
            // Convert markdown to HTML for RSS content
            let html_content = crate::generator::markdown::render_markdown(&post.content)?;

            // Create RSS item
            let post_url = format!(
                "{}/posts/{}.html",
                effective_base_url.trim_end_matches('/'),
                post.metadata.slug
            );

            let rss_item = format!(
                r#"    <item>
      <title><![CDATA[{}]]></title>
      <link>{}</link>
      <guid>{}</guid>
      <description><![CDATA[{}]]></description>
      <pubDate>{}</pubDate>
      <author>{}</author>
    </item>"#,
                post.metadata.title,
                post_url,
                post_url,
                html_content,
                post.metadata.date.format("%a, %d %b %Y %H:%M:%S %z"),
                self.config.blog.author
            );

            rss_items.push(rss_item);
        }

        // Generate RSS XML
        let rss_content = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0" xmlns:atom="http://www.w3.org/2005/Atom">
  <channel>
    <title><![CDATA[{}]]></title>
    <link>{}</link>
    <atom:link href="{}/rss.xml" rel="self" type="application/rss+xml" />
    <description><![CDATA[{}]]></description>
    <language>{}</language>
    <lastBuildDate>{}</lastBuildDate>
    <generator>Blogr Static Site Generator</generator>
{}
  </channel>
</rss>"#,
            self.config.blog.title,
            effective_base_url,
            effective_base_url.trim_end_matches('/'),
            self.config.blog.description,
            self.config.blog.language.as_deref().unwrap_or("en"),
            Utc::now().format("%a, %d %b %Y %H:%M:%S %z"),
            rss_items.join("\n")
        );

        // Write RSS feed
        let rss_file = self.output_dir.join("rss.xml");
        fs::write(&rss_file, rss_content)
            .map_err(|e| anyhow!("Failed to write RSS feed: {}", e))?;

        // Also generate Atom feed
        self.generate_atom_feed(posts)?;

        Ok(())
    }

    /// Generate Atom feed
    fn generate_atom_feed(&self, posts: &[Post]) -> Result<()> {
        // Get effective base URL for all feed URLs
        let effective_base_url = self.config.get_effective_base_url();

        // Take only the most recent 20 posts for the Atom feed
        let recent_posts: Vec<&Post> = posts.iter().take(20).collect();

        let mut atom_entries = Vec::new();

        for post in recent_posts {
            // Convert markdown to HTML for Atom content
            let html_content = crate::generator::markdown::render_markdown(&post.content)?;

            // Create Atom entry
            let post_url = format!(
                "{}/posts/{}.html",
                effective_base_url.trim_end_matches('/'),
                post.metadata.slug
            );

            let atom_entry = format!(
                r#"  <entry>
    <title><![CDATA[{}]]></title>
    <link href="{}"/>
    <id>{}</id>
    <updated>{}</updated>
    <summary><![CDATA[{}]]></summary>
    <content type="html"><![CDATA[{}]]></content>
    <author>
      <name>{}</name>
    </author>
  </entry>"#,
                post.metadata.title,
                post_url,
                post_url,
                post.metadata.date.format("%Y-%m-%dT%H:%M:%S%z"),
                &post.metadata.description,
                html_content,
                self.config.blog.author
            );

            atom_entries.push(atom_entry);
        }

        // Generate Atom XML
        let atom_content = format!(
            r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title><![CDATA[{}]]></title>
  <link href="{}"/>
  <link href="{}/atom.xml" rel="self"/>
  <id>{}</id>
  <updated>{}</updated>
  <subtitle><![CDATA[{}]]></subtitle>
  <generator>Blogr Static Site Generator</generator>
{}
</feed>"#,
            self.config.blog.title,
            effective_base_url,
            effective_base_url.trim_end_matches('/'),
            effective_base_url,
            Utc::now().format("%Y-%m-%dT%H:%M:%S%z"),
            self.config.blog.description,
            atom_entries.join("\n")
        );

        // Write Atom feed
        let atom_file = self.output_dir.join("atom.xml");
        fs::write(&atom_file, atom_content)
            .map_err(|e| anyhow!("Failed to write Atom feed: {}", e))?;

        Ok(())
    }

    /// Copy theme assets
    fn copy_theme_assets(&self) -> Result<()> {
        let assets = self.theme.assets();
        for (path, content) in &assets {
            // Place assets directly in output directory (e.g., css/style.css -> /css/style.css)
            let asset_path = self.output_dir.join(path);

            // Create parent directories if needed
            if let Some(parent) = asset_path.parent() {
                fs::create_dir_all(parent)?;
            }

            fs::write(&asset_path, content)
                .map_err(|e| anyhow!("Failed to write asset '{}': {}", path, e))?;
        }

        Ok(())
    }

    /// Copy project static assets
    fn copy_static_assets(&self) -> Result<()> {
        let static_dir = self.project.root.join("static");
        if !static_dir.exists() {
            return Ok(());
        }

        let output_static = self.output_dir.join("static");
        crate::generator::assets::copy_dir_recursive(&static_dir, &output_static)
            .map_err(|e| anyhow!("Failed to copy static assets: {}", e))?;

        Ok(())
    }

    /// Copy built-in search assets
    fn copy_search_assets(&self) -> Result<()> {
        // Always emit embedded assets first for reliability (works in release binaries)
        let js_dir = self.output_dir.join("js");
        fs::create_dir_all(&js_dir)?;

        // Inject configuration into search.js
        let field_boosts_json = serde_json::to_string(&self.config.search.field_boosts)
            .unwrap_or_else(|_| r#"{"title": 5, "tags": 3, "content": 1}"#.to_string());

        let search_js_content = EMBEDDED_SEARCH_JS
            .replace(
                "lazyLoad: true,",
                &format!("lazyLoad: {},", self.config.search.lazy_load),
            )
            .replace(
                r#"boost: { title: 5, tags: 3, content: 1 }"#,
                &format!("boost: {}", field_boosts_json),
            );

        let search_js_dst = js_dir.join("search.js");
        fs::write(&search_js_dst, search_js_content)
            .map_err(|e| anyhow!("Failed to write embedded search.js: {}", e))?;

        let vendor_dir_dst = js_dir.join("vendor");
        fs::create_dir_all(&vendor_dir_dst)?;
        let minisearch_dst = vendor_dir_dst.join("minisearch.min.js");
        fs::write(&minisearch_dst, EMBEDDED_MINISEARCH_JS)
            .map_err(|e| anyhow!("Failed to write embedded minisearch.min.js: {}", e))?;

        // Optionally override with local files if present (useful during development)
        let dev_static = std::env::current_dir()?.join("blogr-cli/static");
        if dev_static.exists() {
            let search_js_src = dev_static.join("js/search.js");
            if search_js_src.exists() {
                let _ = fs::copy(&search_js_src, &search_js_dst);
            }
            let vendor_src = dev_static.join("js/vendor");
            if vendor_src.exists() {
                let _ = crate::generator::assets::copy_dir_recursive(&vendor_src, &vendor_dir_dst);
            }
        }

        Ok(())
    }

    // (Removed unused copy_search_assets_from to avoid dead_code warnings)

    /// Get the output directory
    pub fn output_dir(&self) -> &Path {
        &self.output_dir
    }

    /// Generate CNAME file for custom domains (GitHub Pages)
    fn generate_cname_file(&self) -> Result<()> {
        if let Some(domains) = &self.config.blog.domains {
            if let Some(github_domain) = &domains.github_pages_domain {
                let cname_file = self.output_dir.join("CNAME");
                fs::write(&cname_file, format!("{}\n", github_domain))
                    .map_err(|e| anyhow!("Failed to write CNAME file: {}", e))?;

                println!("📝 Generated CNAME file for: {}", github_domain);
            }
        }
        Ok(())
    }

    /// Register template functions for URL generation
    fn register_template_functions(tera: &mut Tera, config: &Config) -> Result<()> {
        let base_url = config.get_effective_base_url();

        // Use relative paths when running the local dev server; otherwise use base_url-prefixed URLs
        let is_dev = std::env::var("BLOGR_DEV").is_ok();
        let use_relative_paths = is_dev;

        // Clone base_url for use in closures
        let base_url_for_asset = base_url.clone();
        let base_url_for_url = base_url.clone();

        // Register asset_url function
        let use_relative_for_asset = use_relative_paths;
        tera.register_function(
            "asset_url",
            move |args: &HashMap<String, Value>| -> tera::Result<Value> {
                let path = args
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| tera::Error::msg("asset_url requires a 'path' argument"))?;

                // If absolute URL, return as-is
                if path.starts_with("http://") || path.starts_with("https://") {
                    return Ok(Value::String(path.to_string()));
                }

                let url = if use_relative_for_asset {
                    // Local development: root-relative
                    format!("/{}", path.trim_start_matches('/'))
                } else {
                    // Production: prefix with base_url
                    format!(
                        "{}/{}",
                        base_url_for_asset.trim_end_matches('/'),
                        path.trim_start_matches('/')
                    )
                };
                Ok(Value::String(url))
            },
        );

        // Register url function for internal links
        let use_relative_for_url = use_relative_paths;
        tera.register_function(
            "url",
            move |args: &HashMap<String, Value>| -> tera::Result<Value> {
                let path = args
                    .get("path")
                    .and_then(|v| v.as_str())
                    .ok_or_else(|| tera::Error::msg("url requires a 'path' argument"))?;

                // If absolute URL, return as-is
                if path.starts_with("http://") || path.starts_with("https://") {
                    return Ok(Value::String(path.to_string()));
                }

                let url = if use_relative_for_url {
                    // Local development: root-relative
                    if path.is_empty() {
                        "/".to_string()
                    } else {
                        format!("/{}", path.trim_start_matches('/'))
                    }
                } else {
                    // Production: prefix with base_url
                    format!(
                        "{}/{}",
                        base_url_for_url.trim_end_matches('/'),
                        path.trim_start_matches('/')
                    )
                };
                Ok(Value::String(url))
            },
        );

        Ok(())
    }

    /// Generate static JSON files for post pagination
    fn generate_posts_json(&self, posts: &[Post]) -> Result<()> {
        const POSTS_PER_PAGE: usize = 10;

        // Create api directory
        let api_dir = self.output_dir.join("api");
        fs::create_dir_all(&api_dir)?;

        // Generate paginated JSON files
        let total_posts = posts.len();
        let total_pages = total_posts.div_ceil(POSTS_PER_PAGE);

        for page in 1..=total_pages {
            let start_index = (page - 1) * POSTS_PER_PAGE;
            let end_index = (start_index + POSTS_PER_PAGE).min(total_posts);
            let page_posts = &posts[start_index..end_index];

            // Prepare posts with rendered content
            let mut posts_with_content = Vec::new();
            for post in page_posts {
                // Convert markdown to HTML for each post
                let html_content = crate::generator::markdown::render_markdown(&post.content)?;

                // Calculate reading time (average 200 words per minute)
                let word_count = post.content.split_whitespace().count();
                let reading_time = (word_count / 200).max(1);

                // Create a struct that includes both post data and rendered content
                let post_data = serde_json::json!({
                    "metadata": post.metadata,
                    "content": html_content,
                    "reading_time": reading_time
                });

                posts_with_content.push(post_data);
            }

            // Create response structure matching the API format
            let response = serde_json::json!({
                "posts": posts_with_content,
                "has_more": page < total_pages,
                "total": total_posts,
                "page": page,
                "limit": POSTS_PER_PAGE
            });

            // Write JSON file for this page
            let json_file = api_dir.join(format!("posts-page-{}.json", page));
            let json_content = serde_json::to_string_pretty(&response)
                .map_err(|e| anyhow!("Failed to serialize posts JSON for page {}: {}", page, e))?;

            fs::write(&json_file, json_content)
                .map_err(|e| anyhow!("Failed to write posts JSON file for page {}: {}", page, e))?;
        }

        println!("📄 Generated {} paginated JSON files", total_pages);
        Ok(())
    }

    /// Generate search index
    fn generate_search_index(&self, posts: &[Post]) -> Result<()> {
        use crate::generator::SearchIndexer;

        let indexer = SearchIndexer::new(self.config.search.clone());
        indexer.generate_index(posts, &self.output_dir)?;
        Ok(())
    }
}
