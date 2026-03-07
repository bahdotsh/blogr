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
/// no consecutive dots, no leading/trailing dots, no whitespace.
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

    // Domain checks
    domain.contains('.')
        && !domain.starts_with('.')
        && !domain.ends_with('.')
        && !domain.contains("..")
        && domain.len() >= 3
}
