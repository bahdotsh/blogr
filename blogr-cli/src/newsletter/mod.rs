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
