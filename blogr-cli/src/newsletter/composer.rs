//! Email composition for newsletters
//!
//! This module handles converting blog posts to email format,
//! creating custom newsletters, and managing email templates.

use anyhow::{Context, Result};
use blogr_themes::Theme;
use chrono::{DateTime, Utc};
use html2text::from_read;
use serde::{Deserialize, Serialize};
use tera::{Context as TeraContext, Tera};

use crate::config::Config;
use crate::content::Post;
use crate::generator::markdown;

/// Site context for email templates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SiteContext {
    pub title: String,
    pub author: String,
    pub description: String,
    pub base_url: String,
    pub url: String, // Alias for base_url for template compatibility
    pub email: String,
    pub language: Option<String>,
    pub timezone: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Newsletter {
    pub subject: String,
    pub html_content: String,
    pub text_content: String,
    pub created_at: DateTime<Utc>,
    pub unsubscribe_token: Option<String>,
}

impl Newsletter {
    pub fn new(subject: String, html_content: String, text_content: String) -> Self {
        Self {
            subject,
            html_content,
            text_content,
            created_at: Utc::now(),
            unsubscribe_token: None,
        }
    }

    #[allow(dead_code)]
    pub fn with_unsubscribe_token(mut self, token: String) -> Self {
        self.unsubscribe_token = Some(token);
        self
    }
}

pub struct NewsletterComposer {
    #[allow(dead_code)]
    theme: Box<dyn Theme>,
    config: Config,
    tera: Tera,
}

impl NewsletterComposer {
    /// Create a new newsletter composer
    pub fn new(theme: Box<dyn Theme>, config: Config) -> Result<Self> {
        let mut tera = Tera::default();

        // Register email templates
        let email_base_template = include_str!("../templates/email/base.html");
        let email_post_template = include_str!("../templates/email/post.html");
        let email_custom_template = include_str!("../templates/email/custom.html");

        tera.add_raw_template("email/base.html", email_base_template)?;
        tera.add_raw_template("email/post.html", email_post_template)?;
        tera.add_raw_template("email/custom.html", email_custom_template)?;

        Ok(Self {
            theme,
            config,
            tera,
        })
    }

    /// Create site context for email templates
    fn create_site_context(&self) -> SiteContext {
        let email = self
            .config
            .newsletter
            .subscribe_email
            .as_deref()
            .unwrap_or(&self.config.blog.author) // Fallback to author name if no email set
            .to_string();

        SiteContext {
            title: self.config.blog.title.clone(),
            author: self.config.blog.author.clone(),
            description: self.config.blog.description.clone(),
            base_url: self.config.blog.base_url.clone(),
            url: self.config.blog.base_url.clone(), // Alias for template compatibility
            email,
            language: self.config.blog.language.clone(),
            timezone: self.config.blog.timezone.clone(),
        }
    }

    /// Compose newsletter from a blog post
    pub fn compose_from_post(&self, post: &Post) -> Result<Newsletter> {
        let mut context = TeraContext::new();

        // Add site config with email support
        let site_context = self.create_site_context();
        context.insert("site", &site_context);

        // Add post data
        context.insert("post", post);

        // Convert markdown to HTML
        let html_content = markdown::render_markdown(&post.content, self.config.math.enabled)?;
        context.insert("content", &html_content);

        // Calculate reading time
        let word_count = post.content.split_whitespace().count();
        let reading_time = (word_count / 200).max(1);
        context.insert("reading_time", &reading_time);

        // Add newsletter metadata
        context.insert("newsletter_title", &self.get_newsletter_title());
        context.insert(
            "unsubscribe_url",
            &self.generate_unsubscribe_url("{{unsubscribe_token}}"),
        );

        // Render HTML email
        let html_email = self
            .tera
            .render("email/post.html", &context)
            .context("Failed to render email template")?;

        // Inline CSS for better email client compatibility
        let inlined_html = css_inline::inline(&html_email)
            .map_err(|e| anyhow::anyhow!("Failed to inline CSS: {}", e))?;

        // Generate plain text version
        let text_content = self.html_to_text(&inlined_html)?;

        // Generate subject line
        let subject = format!("{}: {}", self.get_newsletter_title(), post.metadata.title);

        Ok(Newsletter::new(subject, inlined_html, text_content))
    }

    /// Compose custom newsletter
    pub fn compose_custom(&self, subject: String, content: String) -> Result<Newsletter> {
        let mut context = TeraContext::new();

        // Add site config with email support
        let site_context = self.create_site_context();
        context.insert("site", &site_context);

        // Convert markdown content to HTML
        let html_content = markdown::render_markdown(&content, self.config.math.enabled)?;
        context.insert("content", &html_content);

        // Add newsletter metadata
        context.insert("newsletter_title", &self.get_newsletter_title());
        context.insert(
            "unsubscribe_url",
            &self.generate_unsubscribe_url("{{unsubscribe_token}}"),
        );
        context.insert("subject", &subject);

        // Render HTML email
        let html_email = self
            .tera
            .render("email/custom.html", &context)
            .context("Failed to render custom email template")?;

        // Inline CSS for better email client compatibility
        let inlined_html = css_inline::inline(&html_email)
            .map_err(|e| anyhow::anyhow!("Failed to inline CSS: {}", e))?;

        // Generate plain text version
        let text_content = self.html_to_text(&inlined_html)?;

        Ok(Newsletter::new(subject, inlined_html, text_content))
    }

    /// Preview newsletter in terminal
    pub fn preview_in_terminal(&self, newsletter: &Newsletter) -> Result<()> {
        println!("📧 Newsletter Preview");
        println!("══════════════════════════════════════");
        println!("Subject: {}", newsletter.subject);
        println!(
            "Created: {}",
            newsletter.created_at.format("%Y-%m-%d %H:%M:%S UTC")
        );
        println!("══════════════════════════════════════");
        println!();

        println!("📄 Plain Text Version:");
        println!("──────────────────────");
        println!("{}", newsletter.text_content);
        println!();

        println!("🌐 HTML Version (truncated):");
        println!("─────────────────────────────");
        let html_preview = if newsletter.html_content.len() > 500 {
            format!(
                "{}...\n\n[HTML content truncated - {} total characters]",
                &newsletter.html_content[..500],
                newsletter.html_content.len()
            )
        } else {
            newsletter.html_content.clone()
        };
        println!("{}", html_preview);

        Ok(())
    }

    /// Convert HTML to plain text
    fn html_to_text(&self, html: &str) -> Result<String> {
        let text = from_read(html.as_bytes(), 80);
        Ok(text)
    }

    /// Get newsletter title from config
    fn get_newsletter_title(&self) -> String {
        self.config
            .newsletter
            .sender_name
            .as_deref()
            .unwrap_or(&self.config.blog.title)
            .to_string()
    }

    /// Generate unsubscribe URL
    fn generate_unsubscribe_url(&self, token: &str) -> String {
        format!("mailto:{}?subject=Unsubscribe&body=Please unsubscribe me from the newsletter. Token: {}", 
            self.config.newsletter.subscribe_email.as_deref().unwrap_or("unsubscribe@example.com"),
            token
        )
    }

    /// Get theme-specific CSS for email
    #[allow(dead_code)]
    fn get_email_css(&self) -> String {
        // Basic email-safe CSS that works across email clients
        r#"
        <style>
        body { 
            font-family: Arial, sans-serif; 
            line-height: 1.6; 
            color: #333; 
            max-width: 600px; 
            margin: 0 auto; 
            padding: 20px; 
        }
        .header { 
            border-bottom: 2px solid #eee; 
            padding-bottom: 20px; 
            margin-bottom: 30px; 
        }
        .header h1 { 
            color: #2c3e50; 
            margin: 0; 
        }
        .content { 
            margin-bottom: 30px; 
        }
        .content h1, .content h2, .content h3 { 
            color: #2c3e50; 
        }
        .content img { 
            max-width: 100%; 
            height: auto; 
        }
        .footer { 
            border-top: 1px solid #eee; 
            padding-top: 20px; 
            margin-top: 30px; 
            font-size: 14px; 
            color: #666; 
        }
        .unsubscribe { 
            font-size: 12px; 
            color: #999; 
        }
        .unsubscribe a { 
            color: #999; 
        }
        blockquote {
            border-left: 4px solid #ddd;
            margin: 0;
            padding-left: 20px;
            color: #666;
        }
        code {
            background-color: #f4f4f4;
            padding: 2px 4px;
            border-radius: 3px;
            font-family: monospace;
        }
        pre {
            background-color: #f4f4f4;
            padding: 15px;
            border-radius: 5px;
            overflow-x: auto;
        }
        </style>
        "#
        .to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::content::{PostMetadata, PostStatus};
    use chrono::Utc;
    use std::path::PathBuf;

    #[allow(dead_code)]
    fn create_test_post() -> Post {
        Post {
            metadata: PostMetadata {
                title: "Test Post".to_string(),
                date: Utc::now(),
                author: "Test Author".to_string(),
                description: "Test description".to_string(),
                tags: vec!["test".to_string()],
                status: PostStatus::Published,
                slug: "test-post".to_string(),
                featured: false,
            },
            content: "# Test Content\n\nThis is a test post with some **bold** text.".to_string(),
            file_path: PathBuf::from("test.md"),
        }
    }

    #[test]
    fn test_newsletter_creation() {
        let newsletter = Newsletter::new(
            "Test Subject".to_string(),
            "<html><body>Test</body></html>".to_string(),
            "Test".to_string(),
        );

        assert_eq!(newsletter.subject, "Test Subject");
        assert!(newsletter.html_content.contains("Test"));
        assert_eq!(newsletter.text_content, "Test");
    }
}
