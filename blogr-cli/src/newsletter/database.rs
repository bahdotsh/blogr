//! Database operations for newsletter subscribers

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use rusqlite::{params, Connection, Row};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Mutex;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SubscriberStatus {
    Pending,
    Approved,
    Declined,
}

impl std::fmt::Display for SubscriberStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriberStatus::Pending => write!(f, "pending"),
            SubscriberStatus::Approved => write!(f, "approved"),
            SubscriberStatus::Declined => write!(f, "declined"),
        }
    }
}

impl std::str::FromStr for SubscriberStatus {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(SubscriberStatus::Pending),
            "approved" => Ok(SubscriberStatus::Approved),
            "declined" => Ok(SubscriberStatus::Declined),
            _ => Err(anyhow::anyhow!("Invalid subscriber status: {}", s)),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Subscriber {
    pub id: Option<i64>,
    pub email: String,
    pub status: SubscriberStatus,
    pub subscribed_at: DateTime<Utc>,
    pub approved_at: Option<DateTime<Utc>>,
    pub declined_at: Option<DateTime<Utc>>,
    pub source_email_id: Option<String>,
    pub notes: Option<String>,
}

impl Subscriber {
    pub fn new(email: String, source_email_id: Option<String>) -> Self {
        Self {
            id: None,
            email,
            status: SubscriberStatus::Pending,
            subscribed_at: Utc::now(),
            approved_at: None,
            declined_at: None,
            source_email_id,
            notes: None,
        }
    }

    #[allow(dead_code)]
    pub fn approve(&mut self) {
        self.status = SubscriberStatus::Approved;
        self.approved_at = Some(Utc::now());
    }

    #[allow(dead_code)]
    pub fn decline(&mut self) {
        self.status = SubscriberStatus::Declined;
        self.declined_at = Some(Utc::now());
    }
}

#[derive(Debug)]
pub struct NewsletterDatabase {
    conn: Mutex<Connection>,
}

impl NewsletterDatabase {
    /// Create or open the newsletter database
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(&path)
            .with_context(|| format!("Failed to open database at {}", path.as_ref().display()))?;

        let mut db = Self {
            conn: Mutex::new(conn),
        };
        db.initialize()?;
        Ok(db)
    }

    /// Create an in-memory database (for testing)
    #[cfg(test)]
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().context("Failed to open in-memory database")?;

        let mut db = Self {
            conn: Mutex::new(conn),
        };
        db.initialize()?;
        Ok(db)
    }

    /// Acquire the mutex lock, handling poisoned mutex gracefully
    fn lock_conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>> {
        self.conn
            .lock()
            .map_err(|e| anyhow::anyhow!("Database mutex poisoned: {}", e))
    }

    /// Initialize the database schema
    fn initialize(&mut self) -> Result<()> {
        let conn = self.lock_conn()?;

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'declined')),
                subscribed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                approved_at DATETIME,
                declined_at DATETIME,
                source_email_id TEXT,
                notes TEXT
            );

            CREATE INDEX IF NOT EXISTS idx_subscribers_status ON subscribers(status);
            CREATE INDEX IF NOT EXISTS idx_subscribers_email ON subscribers(email);
            CREATE INDEX IF NOT EXISTS idx_subscribers_subscribed_at ON subscribers(subscribed_at);

            CREATE TABLE IF NOT EXISTS send_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subject TEXT NOT NULL,
                sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                total_recipients INTEGER NOT NULL DEFAULT 0,
                successful_sends INTEGER NOT NULL DEFAULT 0,
                failed_sends INTEGER NOT NULL DEFAULT 0
            );

            CREATE TABLE IF NOT EXISTS send_recipients (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                send_history_id INTEGER NOT NULL,
                subscriber_email TEXT NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('sent', 'failed')),
                error_message TEXT,
                sent_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (send_history_id) REFERENCES send_history(id)
            );
            "#,
        )
        .context("Failed to initialize database schema")?;

        // Migration: add declined_at column if it doesn't exist (for existing databases)
        let has_declined_at: bool = conn
            .prepare(
                "SELECT COUNT(*) FROM pragma_table_info('subscribers') WHERE name='declined_at'",
            )
            .and_then(|mut stmt| stmt.query_row([], |row| row.get::<_, i64>(0)))
            .map(|count| count > 0)
            .unwrap_or(false);

        if !has_declined_at {
            let _ = conn.execute_batch("ALTER TABLE subscribers ADD COLUMN declined_at DATETIME");
        }

        Ok(())
    }

    /// Add a new subscriber to the database
    pub fn add_subscriber(&self, subscriber: &Subscriber) -> Result<i64> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "INSERT INTO subscribers (email, status, subscribed_at, source_email_id, notes)
             VALUES (?1, ?2, ?3, ?4, ?5)",
        )?;

        let id = stmt.insert(params![
            subscriber.email,
            subscriber.status.to_string(),
            subscriber
                .subscribed_at
                .format("%Y-%m-%d %H:%M:%S%.3f")
                .to_string(),
            subscriber.source_email_id,
            subscriber.notes,
        ])?;

        Ok(id)
    }

    /// Update subscriber status
    pub fn update_subscriber_status(&self, id: i64, status: SubscriberStatus) -> Result<()> {
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();

        let conn = self.lock_conn()?;
        match status {
            SubscriberStatus::Approved => {
                conn.execute(
                    "UPDATE subscribers SET status = ?1, approved_at = ?2 WHERE id = ?3",
                    params![status.to_string(), now, id],
                )?;
            }
            SubscriberStatus::Declined => {
                conn.execute(
                    "UPDATE subscribers SET status = ?1, declined_at = ?2 WHERE id = ?3",
                    params![status.to_string(), now, id],
                )?;
            }
            SubscriberStatus::Pending => {
                conn.execute(
                    "UPDATE subscribers SET status = ?1, approved_at = NULL, declined_at = NULL WHERE id = ?2",
                    params![status.to_string(), id],
                )?;
            }
        }

        Ok(())
    }

    /// Update a full subscriber record
    pub fn update_subscriber(&self, subscriber: &Subscriber) -> Result<()> {
        let id = subscriber
            .id
            .ok_or_else(|| anyhow::anyhow!("Cannot update subscriber without an ID"))?;

        let conn = self.lock_conn()?;
        conn.execute(
            "UPDATE subscribers SET email = ?1, status = ?2, notes = ?3, approved_at = ?4, declined_at = ?5 WHERE id = ?6",
            params![
                subscriber.email,
                subscriber.status.to_string(),
                subscriber.notes,
                subscriber.approved_at.map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
                subscriber.declined_at.map(|dt| dt.format("%Y-%m-%d %H:%M:%S%.3f").to_string()),
                id,
            ],
        )?;

        Ok(())
    }

    /// Get all subscribers with optional status filter and optional pagination
    pub fn get_subscribers(
        &self,
        status_filter: Option<SubscriberStatus>,
    ) -> Result<Vec<Subscriber>> {
        self.get_subscribers_paginated(status_filter, None, None)
    }

    /// Get subscribers with pagination support
    pub fn get_subscribers_paginated(
        &self,
        status_filter: Option<SubscriberStatus>,
        limit: Option<usize>,
        offset: Option<usize>,
    ) -> Result<Vec<Subscriber>> {
        let conn = self.lock_conn()?;

        let mut query = String::from(
            "SELECT id, email, status, subscribed_at, approved_at, declined_at, source_email_id, notes
             FROM subscribers"
        );
        let mut params: Vec<String> = Vec::new();

        if let Some(ref status) = status_filter {
            query.push_str(" WHERE status = ?1");
            params.push(status.to_string());
        }

        query.push_str(" ORDER BY subscribed_at DESC");

        if let Some(limit) = limit {
            let idx = params.len() + 1;
            query.push_str(&format!(" LIMIT ?{}", idx));
            params.push(limit.to_string());
        }
        if let Some(offset) = offset {
            let idx = params.len() + 1;
            query.push_str(&format!(" OFFSET ?{}", idx));
            params.push(offset.to_string());
        }

        let mut stmt = conn.prepare(&query)?;
        let subscriber_iter = stmt.query_map(rusqlite::params_from_iter(params.iter()), |row| {
            Self::row_to_subscriber(row)
        })?;

        let mut subscribers = Vec::new();
        for subscriber in subscriber_iter {
            subscribers.push(subscriber?);
        }

        Ok(subscribers)
    }

    /// Get subscriber by email
    pub fn get_subscriber_by_email(&self, email: &str) -> Result<Option<Subscriber>> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, email, status, subscribed_at, approved_at, declined_at, source_email_id, notes
             FROM subscribers WHERE email = ?1",
        )?;

        let result = stmt.query_row(params![email], Self::row_to_subscriber);

        match result {
            Ok(subscriber) => Ok(Some(subscriber)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Remove subscriber by email
    pub fn remove_subscriber(&self, email: &str) -> Result<bool> {
        let rows_affected = self
            .lock_conn()?
            .execute("DELETE FROM subscribers WHERE email = ?1", params![email])?;

        Ok(rows_affected > 0)
    }

    /// Get subscriber count by status
    pub fn get_subscriber_count(&self, status: Option<SubscriberStatus>) -> Result<i64> {
        let (query, params): (String, Vec<String>) = match status {
            Some(status) => (
                "SELECT COUNT(*) FROM subscribers WHERE status = ?1".to_string(),
                vec![status.to_string()],
            ),
            None => ("SELECT COUNT(*) FROM subscribers".to_string(), vec![]),
        };

        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(&query)?;
        let count: i64 =
            stmt.query_row(rusqlite::params_from_iter(params.iter()), |row| row.get(0))?;

        Ok(count)
    }

    /// Check if email already exists
    pub fn email_exists(&self, email: &str) -> Result<bool> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare("SELECT 1 FROM subscribers WHERE email = ?1")?;
        let result = stmt.query_row(params![email], |_| Ok(()));

        match result {
            Ok(_) => Ok(true),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Record a newsletter send in history
    pub fn record_send(
        &self,
        subject: &str,
        total: usize,
        successful: usize,
        failed: usize,
    ) -> Result<i64> {
        let conn = self.lock_conn()?;
        let mut stmt = conn.prepare(
            "INSERT INTO send_history (subject, total_recipients, successful_sends, failed_sends) VALUES (?1, ?2, ?3, ?4)",
        )?;
        let id = stmt.insert(params![
            subject,
            total as i64,
            successful as i64,
            failed as i64
        ])?;
        Ok(id)
    }

    /// Record individual send recipient status
    pub fn record_send_recipient(
        &self,
        send_history_id: i64,
        email: &str,
        status: &str,
        error_message: Option<&str>,
    ) -> Result<()> {
        let conn = self.lock_conn()?;
        conn.execute(
            "INSERT INTO send_recipients (send_history_id, subscriber_email, status, error_message) VALUES (?1, ?2, ?3, ?4)",
            params![send_history_id, email, status, error_message],
        )?;
        Ok(())
    }

    /// Helper function to convert database row to Subscriber
    fn row_to_subscriber(row: &Row) -> rusqlite::Result<Subscriber> {
        let subscribed_at_str: String = row.get(3)?;
        let approved_at_str: Option<String> = row.get(4)?;
        let declined_at_str: Option<String> = row.get(5)?;

        let subscribed_at = parse_datetime(&subscribed_at_str).map_err(|_| {
            rusqlite::Error::InvalidColumnType(
                3,
                "subscribed_at".to_string(),
                rusqlite::types::Type::Text,
            )
        })?;

        let approved_at = approved_at_str
            .as_deref()
            .map(parse_datetime)
            .transpose()
            .map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    4,
                    "approved_at".to_string(),
                    rusqlite::types::Type::Text,
                )
            })?;

        let declined_at = declined_at_str
            .as_deref()
            .map(parse_datetime)
            .transpose()
            .map_err(|_| {
                rusqlite::Error::InvalidColumnType(
                    5,
                    "declined_at".to_string(),
                    rusqlite::types::Type::Text,
                )
            })?;

        let status_str: String = row.get(2)?;
        let status = status_str.parse().map_err(|_| {
            rusqlite::Error::InvalidColumnType(2, "status".to_string(), rusqlite::types::Type::Text)
        })?;

        Ok(Subscriber {
            id: Some(row.get(0)?),
            email: row.get(1)?,
            status,
            subscribed_at,
            approved_at,
            declined_at,
            source_email_id: row.get(6)?,
            notes: row.get(7)?,
        })
    }
}

/// Parse a datetime string in common formats
fn parse_datetime(s: &str) -> Result<DateTime<Utc>> {
    chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S")
        .or_else(|_| chrono::NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S%.3f"))
        .map(|dt| dt.and_utc())
        .map_err(|e| anyhow::anyhow!("Failed to parse datetime '{}': {}", s, e))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_database_operations() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        // Test adding subscriber
        let subscriber =
            Subscriber::new("test@example.com".to_string(), Some("msg-123".to_string()));
        let id = db.add_subscriber(&subscriber)?;
        assert!(id > 0);

        // Test getting subscriber by email
        let retrieved = db.get_subscriber_by_email("test@example.com")?;
        assert!(retrieved.is_some());
        let retrieved = retrieved.unwrap();
        assert_eq!(retrieved.email, "test@example.com");
        assert_eq!(retrieved.status, SubscriberStatus::Pending);

        // Test updating status to approved
        db.update_subscriber_status(id, SubscriberStatus::Approved)?;
        let updated = db.get_subscriber_by_email("test@example.com")?.unwrap();
        assert_eq!(updated.status, SubscriberStatus::Approved);
        assert!(updated.approved_at.is_some());
        assert!(updated.declined_at.is_none());

        // Test updating status to declined
        let subscriber2 = Subscriber::new("test2@example.com".to_string(), None);
        let id2 = db.add_subscriber(&subscriber2)?;
        db.update_subscriber_status(id2, SubscriberStatus::Declined)?;
        let declined = db.get_subscriber_by_email("test2@example.com")?.unwrap();
        assert_eq!(declined.status, SubscriberStatus::Declined);
        assert!(declined.declined_at.is_some());
        assert!(declined.approved_at.is_none());

        // Test getting subscribers by status
        let approved_subscribers = db.get_subscribers(Some(SubscriberStatus::Approved))?;
        assert_eq!(approved_subscribers.len(), 1);

        let pending_subscribers = db.get_subscribers(Some(SubscriberStatus::Pending))?;
        assert_eq!(pending_subscribers.len(), 0);

        // Test subscriber count
        let total_count = db.get_subscriber_count(None)?;
        assert_eq!(total_count, 2);

        let approved_count = db.get_subscriber_count(Some(SubscriberStatus::Approved))?;
        assert_eq!(approved_count, 1);

        // Test email exists
        assert!(db.email_exists("test@example.com")?);
        assert!(!db.email_exists("nonexistent@example.com")?);

        // Test removing subscriber
        assert!(db.remove_subscriber("test@example.com")?);
        assert!(!db.email_exists("test@example.com")?);

        Ok(())
    }

    #[test]
    fn test_update_subscriber() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        let subscriber = Subscriber::new("update@example.com".to_string(), None);
        let id = db.add_subscriber(&subscriber)?;

        let mut sub = db.get_subscriber_by_email("update@example.com")?.unwrap();
        sub.notes = Some("Updated notes".to_string());
        sub.status = SubscriberStatus::Approved;
        sub.approved_at = Some(Utc::now());
        db.update_subscriber(&sub)?;

        let updated = db.get_subscriber_by_email("update@example.com")?.unwrap();
        assert_eq!(updated.id, Some(id));
        assert_eq!(updated.notes, Some("Updated notes".to_string()));
        assert_eq!(updated.status, SubscriberStatus::Approved);
        assert!(updated.approved_at.is_some());

        Ok(())
    }

    #[test]
    fn test_pagination() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        for i in 0..10 {
            let sub = Subscriber::new(format!("user{}@example.com", i), None);
            db.add_subscriber(&sub)?;
        }

        let page = db.get_subscribers_paginated(None, Some(3), Some(0))?;
        assert_eq!(page.len(), 3);

        let page2 = db.get_subscribers_paginated(None, Some(3), Some(3))?;
        assert_eq!(page2.len(), 3);
        assert_ne!(page[0].email, page2[0].email);

        Ok(())
    }

    #[test]
    fn test_send_history() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        let send_id = db.record_send("Test Newsletter", 10, 9, 1)?;
        assert!(send_id > 0);

        db.record_send_recipient(send_id, "user@example.com", "sent", None)?;
        db.record_send_recipient(send_id, "fail@example.com", "failed", Some("SMTP error"))?;

        Ok(())
    }
}
