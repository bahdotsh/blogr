//! Newsletter module for handling email-based subscriptions
//!
//! This module provides functionality for:
//! - Fetching subscription emails via IMAP
//! - Parsing and extracting subscriber information
//! - Managing subscriber database
//! - Email composition and sending
//! - Plugin system for third-party extensions

pub mod api;
pub mod composer;
pub mod config;
pub mod database;
pub mod fetcher;
pub mod migration;
pub mod plugin;
pub mod sender;
pub mod ui;
pub mod webhooks;

pub use api::{ApiConfig, NewsletterApiServer};
pub use composer::Newsletter;
pub use config::NewsletterManager;
pub use database::{NewsletterDatabase, Subscriber, SubscriberStatus};
pub use migration::{MigrationConfig, MigrationManager, MigrationSource};
pub use plugin::{create_plugin_context, PluginConfig, PluginHook, PluginManager};
pub use ui::{ApprovalResult, ModernApprovalApp};

/// Email format validation.
/// Checks structure per RFC 5321 basics: local@domain, reasonable lengths,
/// no consecutive dots, no leading/trailing dots, no whitespace, valid characters.
pub fn is_valid_email(email: &str) -> bool {
    let parts: Vec<&str> = email.split('@').collect();
    if parts.len() != 2 {
        return false;
    }
    let local = parts[0];
    let domain = parts[1];

    // Overall length (RFC 5321: 254 max for addr, local max 64)
    if email.len() < 5 || email.len() > 254 || local.len() > 64 {
        return false;
    }

    // Local part checks
    if local.is_empty()
        || local.starts_with('.')
        || local.ends_with('.')
        || local.contains("..")
        || email.chars().any(|c| c.is_whitespace())
    {
        return false;
    }

    // Local part: only allow RFC 5321 permitted characters
    // (alphanumeric, dot, and common special chars)
    if !local.chars().all(|c| {
        c.is_ascii_alphanumeric()
            || matches!(
                c,
                '.' | '+'
                    | '-'
                    | '_'
                    | '!'
                    | '#'
                    | '$'
                    | '%'
                    | '&'
                    | '\''
                    | '*'
                    | '/'
                    | '='
                    | '?'
                    | '^'
                    | '`'
                    | '{'
                    | '|'
                    | '}'
                    | '~'
            )
    }) {
        return false;
    }

    // Domain checks
    if !domain.contains('.')
        || domain.starts_with('.')
        || domain.ends_with('.')
        || domain.contains("..")
        || domain.len() < 3
    {
        return false;
    }

    // Validate each domain label
    for label in domain.split('.') {
        if label.is_empty() || label.len() > 63 {
            return false;
        }
        if label.starts_with('-') || label.ends_with('-') {
            return false;
        }
        if !label.chars().all(|c| c.is_ascii_alphanumeric() || c == '-') {
            return false;
        }
    }

    true
}

/// Maximum allowed length for subscriber tags.
pub const MAX_TAG_LENGTH: usize = 100;

/// Maximum number of tags per subscriber.
pub const MAX_TAGS_PER_SUBSCRIBER: usize = 50;

/// Validate a tag: non-empty, within length limit, no control characters.
pub fn is_valid_tag(tag: &str) -> bool {
    !tag.is_empty()
        && tag.len() <= MAX_TAG_LENGTH
        && !tag.chars().any(|c| c.is_control())
        && tag.trim().len() == tag.len()
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Email validation tests ---

    #[test]
    fn test_valid_emails() {
        assert!(is_valid_email("user@example.com"));
        assert!(is_valid_email("user+tag@example.com"));
        assert!(is_valid_email("first.last@example.com"));
        assert!(is_valid_email("user@sub.domain.com"));
        assert!(is_valid_email("user@example.co.uk"));
        assert!(is_valid_email("a_b-c@example.com"));
    }

    #[test]
    fn test_invalid_emails_structure() {
        assert!(!is_valid_email(""));
        assert!(!is_valid_email("no-at-sign"));
        assert!(!is_valid_email("@no-local.com"));
        assert!(!is_valid_email("no-domain@"));
        assert!(!is_valid_email("two@@signs.com"));
        assert!(!is_valid_email("user@.com"));
        assert!(!is_valid_email("user@com."));
        assert!(!is_valid_email("user@."));
    }

    #[test]
    fn test_invalid_emails_local_part() {
        assert!(!is_valid_email(".leading@example.com"));
        assert!(!is_valid_email("trailing.@example.com"));
        assert!(!is_valid_email("double..dot@example.com"));
        assert!(!is_valid_email("spa ce@example.com"));
        assert!(!is_valid_email("tab\t@example.com"));
        assert!(!is_valid_email("paren(@example.com"));
        assert!(!is_valid_email("comma,user@example.com"));
    }

    #[test]
    fn test_invalid_emails_domain() {
        assert!(!is_valid_email("user@-leading-hyphen.com"));
        assert!(!is_valid_email("user@trailing-hyphen-.com"));
        assert!(!is_valid_email("user@double..dot.com"));
        assert!(!is_valid_email("user@no-dot"));
        assert!(!is_valid_email("user@dot_underscore.com"));
    }

    #[test]
    fn test_email_length_limits() {
        // Exactly at max local length (64 chars)
        let long_local = "a".repeat(64);
        assert!(is_valid_email(&format!("{}@example.com", long_local)));
        // Over max local length
        let too_long_local = "a".repeat(65);
        assert!(!is_valid_email(&format!("{}@example.com", too_long_local)));
    }

    // --- Tag validation tests ---

    #[test]
    fn test_valid_tags() {
        assert!(is_valid_tag("vip"));
        assert!(is_valid_tag("early-adopter"));
        assert!(is_valid_tag("Tag With Spaces"));
        assert!(is_valid_tag("a")); // minimum length
    }

    #[test]
    fn test_invalid_tags() {
        assert!(!is_valid_tag("")); // empty
        assert!(!is_valid_tag(" leading")); // leading space
        assert!(!is_valid_tag("trailing ")); // trailing space
        assert!(!is_valid_tag("has\nnewline")); // control char
        assert!(!is_valid_tag("has\ttab")); // control char
        assert!(!is_valid_tag(&"x".repeat(MAX_TAG_LENGTH + 1))); // too long
        assert!(!is_valid_tag("   ")); // whitespace only
    }

    #[test]
    fn test_tag_at_max_length() {
        let tag = "x".repeat(MAX_TAG_LENGTH);
        assert!(is_valid_tag(&tag));
    }
}
