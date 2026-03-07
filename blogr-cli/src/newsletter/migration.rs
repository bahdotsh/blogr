//! Migration tools for importing subscribers from external services
//!
//! This module provides functionality to import subscriber lists from popular
//! email marketing services like Mailchimp, ConvertKit, Substack, etc.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufRead, BufReader};

use super::database::{NewsletterDatabase, Subscriber, SubscriberStatus};

/// Supported migration sources
#[derive(Debug, Clone, PartialEq)]
pub enum MigrationSource {
    /// Mailchimp CSV export
    Mailchimp,
    /// ConvertKit CSV export  
    ConvertKit,
    /// Substack CSV export
    Substack,
    /// Beehiiv CSV export
    Beehiiv,
    /// Generic CSV format
    Generic,
    /// JSON format (custom or API exports)
    Json,
}

impl std::str::FromStr for MigrationSource {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "mailchimp" => Ok(Self::Mailchimp),
            "convertkit" => Ok(Self::ConvertKit),
            "substack" => Ok(Self::Substack),
            "beehiiv" => Ok(Self::Beehiiv),
            "generic" | "csv" => Ok(Self::Generic),
            "json" => Ok(Self::Json),
            _ => Err(anyhow::anyhow!("Unsupported migration source: {}", s)),
        }
    }
}

impl MigrationSource {
    /// Get all supported sources as strings
    #[allow(dead_code)]
    pub fn all_sources() -> Vec<&'static str> {
        vec![
            "mailchimp",
            "convertkit",
            "substack",
            "beehiiv",
            "generic",
            "json",
        ]
    }
}

/// Migration configuration for different sources
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MigrationConfig {
    pub source: MigrationSource,
    pub file_path: String,
    pub email_column: Option<String>,
    pub name_column: Option<String>,
    pub status_column: Option<String>,
    pub date_column: Option<String>,
    pub tags_column: Option<String>,
    pub custom_mappings: HashMap<String, String>,
    pub skip_header: bool,
    pub delimiter: char,
}

impl MigrationConfig {
    /// Create default config for a migration source
    pub fn for_source(source: MigrationSource, file_path: String) -> Self {
        match source {
            MigrationSource::Mailchimp => Self {
                source,
                file_path,
                email_column: Some("Email Address".to_string()),
                name_column: Some("FNAME".to_string()),
                status_column: Some("Member Status".to_string()),
                date_column: Some("Timestamp Signup".to_string()),
                tags_column: Some("Tags".to_string()),
                custom_mappings: HashMap::new(),
                skip_header: true,
                delimiter: ',',
            },
            MigrationSource::ConvertKit => Self {
                source,
                file_path,
                email_column: Some("email".to_string()),
                name_column: Some("first_name".to_string()),
                status_column: Some("state".to_string()),
                date_column: Some("created_at".to_string()),
                tags_column: Some("tags".to_string()),
                custom_mappings: HashMap::new(),
                skip_header: true,
                delimiter: ',',
            },
            MigrationSource::Substack => Self {
                source,
                file_path,
                email_column: Some("email".to_string()),
                name_column: Some("name".to_string()),
                status_column: Some("subscription_status".to_string()),
                date_column: Some("created_at".to_string()),
                tags_column: None,
                custom_mappings: HashMap::new(),
                skip_header: true,
                delimiter: ',',
            },
            MigrationSource::Beehiiv => Self {
                source,
                file_path,
                email_column: Some("email".to_string()),
                name_column: Some("name".to_string()),
                status_column: Some("status".to_string()),
                date_column: Some("created".to_string()),
                tags_column: Some("tags".to_string()),
                custom_mappings: HashMap::new(),
                skip_header: true,
                delimiter: ',',
            },
            MigrationSource::Generic => Self {
                source,
                file_path,
                email_column: Some("email".to_string()),
                name_column: Some("name".to_string()),
                status_column: None,
                date_column: None,
                tags_column: None,
                custom_mappings: HashMap::new(),
                skip_header: true,
                delimiter: ',',
            },
            MigrationSource::Json => Self {
                source,
                file_path,
                email_column: Some("email".to_string()),
                name_column: Some("name".to_string()),
                status_column: Some("status".to_string()),
                date_column: Some("created_at".to_string()),
                tags_column: Some("tags".to_string()),
                custom_mappings: HashMap::new(),
                skip_header: false,
                delimiter: ',',
            },
        }
    }
}

/// Imported subscriber data before conversion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImportedSubscriber {
    pub email: String,
    pub name: Option<String>,
    pub status: Option<String>,
    pub subscribed_at: Option<DateTime<Utc>>,
    pub tags: Vec<String>,
    pub custom_fields: HashMap<String, String>,
}

/// Migration result summary
#[derive(Debug)]
pub struct MigrationResult {
    pub total_processed: usize,
    pub successfully_imported: usize,
    pub skipped_duplicates: usize,
    pub errors: Vec<String>,
    pub imported_subscribers: Vec<Subscriber>,
}

/// Main migration handler
pub struct MigrationManager {
    database: NewsletterDatabase,
}

impl MigrationManager {
    /// Create a new migration manager
    pub fn new(database: NewsletterDatabase) -> Self {
        Self { database }
    }

    /// Import subscribers from a file
    pub fn import_from_file(&mut self, config: &MigrationConfig) -> Result<MigrationResult> {
        println!("Starting migration from {:?} source...", config.source);

        let imported_data = match config.source {
            MigrationSource::Json => self.parse_json_file(config)?,
            _ => self.parse_csv_file(config)?,
        };

        println!("Parsed {} subscribers from file", imported_data.len());

        let mut result = MigrationResult {
            total_processed: imported_data.len(),
            successfully_imported: 0,
            skipped_duplicates: 0,
            errors: Vec::new(),
            imported_subscribers: Vec::new(),
        };

        for imported in imported_data {
            match self.import_subscriber(&imported, &config.source) {
                Ok(subscriber) => {
                    // Check if subscriber already exists
                    match self.database.get_subscriber_by_email(&subscriber.email) {
                        Ok(Some(_)) => {
                            result.skipped_duplicates += 1;
                            println!("Skipped duplicate: {}", subscriber.email);
                        }
                        Ok(None) => {
                            // Add new subscriber
                            match self.database.add_subscriber(&subscriber) {
                                Ok(_) => {
                                    result.successfully_imported += 1;
                                    result.imported_subscribers.push(subscriber);
                                    println!("Imported: {}", imported.email);
                                }
                                Err(e) => {
                                    let error = format!("Failed to save {}: {}", imported.email, e);
                                    result.errors.push(error.clone());
                                    eprintln!("Error: {}", error);
                                }
                            }
                        }
                        Err(e) => {
                            let error = format!("Database error for {}: {}", imported.email, e);
                            result.errors.push(error.clone());
                            eprintln!("Error: {}", error);
                        }
                    }
                }
                Err(e) => {
                    let error = format!("Failed to process {}: {}", imported.email, e);
                    result.errors.push(error.clone());
                    eprintln!("Error: {}", error);
                }
            }
        }

        println!("\nMigration completed:");
        println!("  Total processed: {}", result.total_processed);
        println!("  Successfully imported: {}", result.successfully_imported);
        println!("  Skipped duplicates: {}", result.skipped_duplicates);
        println!("  Errors: {}", result.errors.len());

        Ok(result)
    }

    /// Parse CSV file based on configuration
    fn parse_csv_file(&self, config: &MigrationConfig) -> Result<Vec<ImportedSubscriber>> {
        let file = File::open(&config.file_path)
            .with_context(|| format!("Failed to open file: {}", config.file_path))?;
        let reader = BufReader::new(file);
        let mut lines = reader.lines();

        let mut subscribers = Vec::new();
        let mut headers = Vec::new();

        // Parse header if needed
        if config.skip_header {
            if let Some(Ok(header_line)) = lines.next() {
                headers = self.parse_csv_line(&header_line, config.delimiter);
            }
        }

        // Parse data lines
        for (line_num, line) in lines.enumerate() {
            let line = line.with_context(|| format!("Failed to read line {}", line_num + 1))?;

            if line.trim().is_empty() {
                continue;
            }

            match self.parse_csv_subscriber(&line, &headers, config) {
                Ok(subscriber) => subscribers.push(subscriber),
                Err(e) => eprintln!("Warning: Failed to parse line {}: {}", line_num + 1, e),
            }
        }

        Ok(subscribers)
    }

    /// Parse JSON file
    fn parse_json_file(&self, config: &MigrationConfig) -> Result<Vec<ImportedSubscriber>> {
        let file_content = std::fs::read_to_string(&config.file_path)
            .with_context(|| format!("Failed to read JSON file: {}", config.file_path))?;

        // Try to parse as array of objects first
        if let Ok(subscribers) = serde_json::from_str::<Vec<ImportedSubscriber>>(&file_content) {
            return Ok(subscribers);
        }

        // Try to parse as single object
        if let Ok(subscriber) = serde_json::from_str::<ImportedSubscriber>(&file_content) {
            return Ok(vec![subscriber]);
        }

        // Try to parse as generic JSON and extract subscriber data
        let json_value: serde_json::Value =
            serde_json::from_str(&file_content).context("Invalid JSON format")?;

        match json_value {
            serde_json::Value::Array(items) => {
                let mut subscribers = Vec::new();
                for item in items {
                    if let Ok(subscriber) = self.parse_json_subscriber(&item, config) {
                        subscribers.push(subscriber);
                    }
                }
                Ok(subscribers)
            }
            serde_json::Value::Object(_) => {
                // Single subscriber object
                let subscriber = self.parse_json_subscriber(&json_value, config)?;
                Ok(vec![subscriber])
            }
            _ => Err(anyhow::anyhow!(
                "JSON must be an object or array of objects"
            )),
        }
    }

    /// Parse a CSV line into fields
    fn parse_csv_line(&self, line: &str, delimiter: char) -> Vec<String> {
        let mut fields = Vec::new();
        let mut current_field = String::new();
        let mut in_quotes = false;
        let mut chars = line.chars().peekable();

        while let Some(ch) = chars.next() {
            match ch {
                '"' => {
                    if in_quotes && chars.peek() == Some(&'"') {
                        // Escaped quote
                        current_field.push('"');
                        chars.next();
                    } else {
                        in_quotes = !in_quotes;
                    }
                }
                c if c == delimiter && !in_quotes => {
                    fields.push(current_field.trim().to_string());
                    current_field.clear();
                }
                c => current_field.push(c),
            }
        }

        fields.push(current_field.trim().to_string());
        fields
    }

    /// Parse a CSV subscriber from line and headers
    fn parse_csv_subscriber(
        &self,
        line: &str,
        headers: &[String],
        config: &MigrationConfig,
    ) -> Result<ImportedSubscriber> {
        let fields = self.parse_csv_line(line, config.delimiter);
        let mut field_map = HashMap::new();

        // Create field mapping
        if !headers.is_empty() {
            for (i, header) in headers.iter().enumerate() {
                if let Some(value) = fields.get(i) {
                    field_map.insert(header.clone(), value.clone());
                }
            }
        } else {
            // No headers, use positional mapping
            for (i, value) in fields.iter().enumerate() {
                field_map.insert(i.to_string(), value.clone());
            }
        }

        self.extract_subscriber_from_fields(&field_map, config)
    }

    /// Parse a JSON subscriber
    fn parse_json_subscriber(
        &self,
        json: &serde_json::Value,
        config: &MigrationConfig,
    ) -> Result<ImportedSubscriber> {
        let mut field_map = HashMap::new();

        if let serde_json::Value::Object(obj) = json {
            for (key, value) in obj {
                let string_value = match value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    serde_json::Value::Array(arr) => {
                        // Convert array to comma-separated string (useful for tags)
                        arr.iter()
                            .filter_map(|v| v.as_str())
                            .collect::<Vec<_>>()
                            .join(",")
                    }
                    _ => continue,
                };
                field_map.insert(key.clone(), string_value);
            }
        }

        self.extract_subscriber_from_fields(&field_map, config)
    }

    /// Extract subscriber data from field mapping
    fn extract_subscriber_from_fields(
        &self,
        fields: &HashMap<String, String>,
        config: &MigrationConfig,
    ) -> Result<ImportedSubscriber> {
        // Extract email (required)
        let email = self
            .get_field_value(fields, &config.email_column, "email")?
            .ok_or_else(|| anyhow::anyhow!("Email field is required"))?;

        // Validate email format
        if !email.contains('@') {
            return Err(anyhow::anyhow!("Invalid email format: {}", email));
        }

        // Extract optional fields
        let name = self.get_field_value(fields, &config.name_column, "name")?;
        let status = self.get_field_value(fields, &config.status_column, "status")?;
        let date_str = self.get_field_value(fields, &config.date_column, "date")?;
        let tags_str = self.get_field_value(fields, &config.tags_column, "tags")?;

        // Parse date
        let subscribed_at = if let Some(date_str) = date_str {
            self.parse_date(&date_str).ok()
        } else {
            None
        };

        // Parse tags
        let tags = if let Some(tags_str) = tags_str {
            tags_str
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        } else {
            Vec::new()
        };

        // Extract custom fields
        let mut custom_fields = HashMap::new();
        for (key, value) in fields {
            if !self.is_standard_field(key, config) && !value.is_empty() {
                custom_fields.insert(key.clone(), value.clone());
            }
        }

        Ok(ImportedSubscriber {
            email,
            name,
            status,
            subscribed_at,
            tags,
            custom_fields,
        })
    }

    /// Get field value with fallback options
    fn get_field_value(
        &self,
        fields: &HashMap<String, String>,
        primary_column: &Option<String>,
        fallback_key: &str,
    ) -> Result<Option<String>> {
        // Try primary column first
        if let Some(column) = primary_column {
            if let Some(value) = fields.get(column) {
                if !value.is_empty() {
                    return Ok(Some(value.clone()));
                }
            }
        }

        // Try fallback key
        if let Some(value) = fields.get(fallback_key) {
            if !value.is_empty() {
                return Ok(Some(value.clone()));
            }
        }

        // Try case-insensitive matching
        for (key, value) in fields {
            if key.to_lowercase() == fallback_key.to_lowercase() && !value.is_empty() {
                return Ok(Some(value.clone()));
            }
        }

        Ok(None)
    }

    /// Check if field is a standard field
    fn is_standard_field(&self, key: &str, config: &MigrationConfig) -> bool {
        let standard_fields = [
            &config.email_column,
            &config.name_column,
            &config.status_column,
            &config.date_column,
            &config.tags_column,
        ];

        standard_fields
            .iter()
            .any(|field| field.as_ref().map(|f| f == key).unwrap_or(false))
    }

    /// Parse date string in various formats
    fn parse_date(&self, date_str: &str) -> Result<DateTime<Utc>> {
        // Common date formats
        let formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%.3fZ",
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d/%m/%Y",
            "%m-%d-%Y",
            "%d-%m-%Y",
        ];

        for format in &formats {
            if let Ok(dt) = chrono::NaiveDateTime::parse_from_str(date_str, format) {
                return Ok(dt.and_utc());
            }
            if let Ok(date) = chrono::NaiveDate::parse_from_str(date_str, format) {
                return Ok(date.and_hms_opt(0, 0, 0).unwrap().and_utc());
            }
        }

        // Try parsing as RFC3339/ISO8601
        if let Ok(dt) = DateTime::parse_from_rfc3339(date_str) {
            return Ok(dt.with_timezone(&Utc));
        }

        Err(anyhow::anyhow!("Unable to parse date: {}", date_str))
    }

    /// Convert imported subscriber to database subscriber
    fn import_subscriber(
        &self,
        imported: &ImportedSubscriber,
        source: &MigrationSource,
    ) -> Result<Subscriber> {
        // Determine status
        let status = self.map_subscriber_status(&imported.status, source);

        // Use current time if no subscription date provided
        let subscribed_at = imported.subscribed_at.unwrap_or_else(Utc::now);

        // Create notes with migration info
        let mut notes = format!("Migrated from {:?}", source);
        if !imported.tags.is_empty() {
            notes.push_str(&format!(" | Tags: {}", imported.tags.join(", ")));
        }
        if !imported.custom_fields.is_empty() {
            notes.push_str(&format!(" | Custom fields: {:?}", imported.custom_fields));
        }

        Ok(Subscriber {
            id: None,
            email: imported.email.clone(),
            status: status.clone(),
            subscribed_at,
            approved_at: if status == SubscriberStatus::Approved {
                Some(subscribed_at)
            } else {
                None
            },
            declined_at: if status == SubscriberStatus::Declined {
                Some(subscribed_at)
            } else {
                None
            },
            source_email_id: Some(format!("migration-{:?}", source)),
            notes: Some(notes),
        })
    }

    /// Map external status to internal status
    fn map_subscriber_status(
        &self,
        status: &Option<String>,
        source: &MigrationSource,
    ) -> SubscriberStatus {
        let status_str = status.as_ref().map(|s| s.to_lowercase());

        match (source, status_str.as_deref()) {
            (MigrationSource::Mailchimp, Some(s)) => match s {
                "subscribed" => SubscriberStatus::Approved,
                "unsubscribed" | "cleaned" => SubscriberStatus::Declined,
                _ => SubscriberStatus::Pending,
            },
            (MigrationSource::ConvertKit, Some(s)) => match s {
                "active" | "subscribed" => SubscriberStatus::Approved,
                "unsubscribed" | "cancelled" => SubscriberStatus::Declined,
                _ => SubscriberStatus::Pending,
            },
            (MigrationSource::Substack, Some(s)) => match s {
                "active" | "subscribed" => SubscriberStatus::Approved,
                "unsubscribed" => SubscriberStatus::Declined,
                _ => SubscriberStatus::Pending,
            },
            (MigrationSource::Beehiiv, Some(s)) => match s {
                "active" | "subscribed" => SubscriberStatus::Approved,
                "unsubscribed" => SubscriberStatus::Declined,
                _ => SubscriberStatus::Pending,
            },
            _ => {
                // Default mapping for generic or unknown statuses
                match status_str.as_deref() {
                    Some("active") | Some("subscribed") | Some("approved") => {
                        SubscriberStatus::Approved
                    }
                    Some("unsubscribed") | Some("declined") | Some("cancelled") => {
                        SubscriberStatus::Declined
                    }
                    _ => SubscriberStatus::Pending,
                }
            }
        }
    }

    /// Preview migration without importing
    pub fn preview_migration(
        &self,
        config: &MigrationConfig,
        limit: Option<usize>,
    ) -> Result<Vec<ImportedSubscriber>> {
        println!("Previewing migration from {:?} source...", config.source);

        let imported_data = match config.source {
            MigrationSource::Json => self.parse_json_file(config)?,
            _ => self.parse_csv_file(config)?,
        };

        let preview_data = if let Some(limit) = limit {
            imported_data.into_iter().take(limit).collect()
        } else {
            imported_data
        };

        println!("Preview shows {} subscribers", preview_data.len());

        Ok(preview_data)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_csv_line_parsing() {
        let db = NewsletterDatabase::in_memory().unwrap();
        let migration = MigrationManager::new(db);

        // Test simple CSV
        let fields = migration.parse_csv_line("test@example.com,John Doe,subscribed", ',');
        assert_eq!(fields, vec!["test@example.com", "John Doe", "subscribed"]);

        // Test CSV with quotes
        let fields =
            migration.parse_csv_line(r#""test@example.com","John, Doe","subscribed""#, ',');
        assert_eq!(fields, vec!["test@example.com", "John, Doe", "subscribed"]);
    }

    #[test]
    fn test_date_parsing() {
        let db = NewsletterDatabase::in_memory().unwrap();
        let migration = MigrationManager::new(db);

        assert!(migration.parse_date("2023-12-01 10:30:00").is_ok());
        assert!(migration.parse_date("2023-12-01T10:30:00Z").is_ok());
        assert!(migration.parse_date("2023-12-01").is_ok());
        assert!(migration.parse_date("12/01/2023").is_ok());
    }

    #[test]
    fn test_status_mapping() {
        let db = NewsletterDatabase::in_memory().unwrap();
        let migration = MigrationManager::new(db);

        assert_eq!(
            migration.map_subscriber_status(
                &Some("subscribed".to_string()),
                &MigrationSource::Mailchimp
            ),
            SubscriberStatus::Approved
        );
        assert_eq!(
            migration.map_subscriber_status(
                &Some("unsubscribed".to_string()),
                &MigrationSource::Mailchimp
            ),
            SubscriberStatus::Declined
        );
    }
}
