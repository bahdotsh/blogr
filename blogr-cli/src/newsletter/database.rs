//! Database operations for newsletter subscribers

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;
use rusqlite::{params, Row};
use serde::{Deserialize, Serialize};
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SubscriberStatus {
    Pending,
    Approved,
    Declined,
    Unconfirmed,
}

impl std::fmt::Display for SubscriberStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SubscriberStatus::Pending => write!(f, "pending"),
            SubscriberStatus::Approved => write!(f, "approved"),
            SubscriberStatus::Declined => write!(f, "declined"),
            SubscriberStatus::Unconfirmed => write!(f, "unconfirmed"),
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
            "unconfirmed" => Ok(SubscriberStatus::Unconfirmed),
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

/// Status of a queued send item
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum SendQueueStatus {
    Pending,
    Sent,
    Failed,
    Retry,
}

impl std::fmt::Display for SendQueueStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SendQueueStatus::Pending => write!(f, "pending"),
            SendQueueStatus::Sent => write!(f, "sent"),
            SendQueueStatus::Failed => write!(f, "failed"),
            SendQueueStatus::Retry => write!(f, "retry"),
        }
    }
}

impl std::str::FromStr for SendQueueStatus {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "pending" => Ok(SendQueueStatus::Pending),
            "sent" => Ok(SendQueueStatus::Sent),
            "failed" => Ok(SendQueueStatus::Failed),
            "retry" => Ok(SendQueueStatus::Retry),
            _ => Err(anyhow::anyhow!("Invalid send queue status: {}", s)),
        }
    }
}

/// A queued send item for tracking individual email delivery
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct SendQueueItem {
    pub id: i64,
    pub send_history_id: i64,
    pub subscriber_id: i64,
    pub subscriber_email: String,
    pub status: SendQueueStatus,
    pub attempts: i32,
    pub last_attempt_at: Option<DateTime<Utc>>,
    pub error_message: Option<String>,
}

/// Bounce event types
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum BounceType {
    Hard,
    Soft,
    Complaint,
}

impl std::fmt::Display for BounceType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BounceType::Hard => write!(f, "hard"),
            BounceType::Soft => write!(f, "soft"),
            BounceType::Complaint => write!(f, "complaint"),
        }
    }
}

impl std::str::FromStr for BounceType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self> {
        match s.to_lowercase().as_str() {
            "hard" => Ok(BounceType::Hard),
            "soft" => Ok(BounceType::Soft),
            "complaint" => Ok(BounceType::Complaint),
            _ => Err(anyhow::anyhow!("Invalid bounce type: {}", s)),
        }
    }
}

/// A bounce log entry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BounceRecord {
    pub id: i64,
    pub subscriber_email: String,
    pub bounce_type: BounceType,
    pub reason: Option<String>,
    pub received_at: DateTime<Utc>,
}

/// Initialize a SQLite connection with standard PRAGMAs
fn init_connection(conn: &rusqlite::Connection) -> Result<()> {
    conn.execute_batch(
        "PRAGMA journal_mode=WAL;
         PRAGMA synchronous=NORMAL;
         PRAGMA cache_size=-64000;
         PRAGMA busy_timeout=5000;",
    )
    .context("Failed to set database pragmas")?;
    Ok(())
}

#[derive(Debug)]
pub struct NewsletterDatabase {
    pool: Pool<SqliteConnectionManager>,
}

impl NewsletterDatabase {
    /// Create or open the newsletter database
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let manager = SqliteConnectionManager::file(path.as_ref());
        let pool = Pool::builder()
            .max_size(4)
            .connection_customizer(Box::new(ConnectionCustomizer))
            .build(manager)
            .with_context(|| format!("Failed to open database at {}", path.as_ref().display()))?;

        let db = Self { pool };
        db.initialize()?;
        Ok(db)
    }

    /// Create an in-memory database (for testing)
    #[cfg(test)]
    pub fn in_memory() -> Result<Self> {
        let manager = SqliteConnectionManager::memory();
        let pool = Pool::builder()
            .max_size(1)
            .connection_customizer(Box::new(ConnectionCustomizer))
            .build(manager)
            .context("Failed to open in-memory database")?;

        let db = Self { pool };
        db.initialize()?;
        Ok(db)
    }

    /// Get a connection from the pool
    fn get_conn(&self) -> Result<r2d2::PooledConnection<SqliteConnectionManager>> {
        self.pool
            .get()
            .map_err(|e| anyhow::anyhow!("Failed to get database connection: {}", e))
    }

    /// Initialize the database schema
    fn initialize(&self) -> Result<()> {
        let conn = self.get_conn()?;

        conn.execute_batch(
            r#"
            CREATE TABLE IF NOT EXISTS subscribers (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                email TEXT UNIQUE NOT NULL,
                status TEXT NOT NULL CHECK (status IN ('pending', 'approved', 'declined', 'unconfirmed')),
                subscribed_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                approved_at DATETIME,
                declined_at DATETIME,
                source_email_id TEXT,
                notes TEXT,
                soft_bounce_count INTEGER NOT NULL DEFAULT 0
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

            CREATE TABLE IF NOT EXISTS send_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                send_history_id INTEGER NOT NULL,
                subscriber_id INTEGER NOT NULL,
                subscriber_email TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed', 'retry')),
                attempts INTEGER NOT NULL DEFAULT 0,
                last_attempt_at DATETIME,
                error_message TEXT,
                FOREIGN KEY (send_history_id) REFERENCES send_history(id),
                FOREIGN KEY (subscriber_id) REFERENCES subscribers(id)
            );

            CREATE INDEX IF NOT EXISTS idx_send_queue_status ON send_queue(send_history_id, status);
            CREATE INDEX IF NOT EXISTS idx_send_queue_history ON send_queue(send_history_id);
            CREATE INDEX IF NOT EXISTS idx_send_queue_status_id ON send_queue(send_history_id, status, id);
            CREATE INDEX IF NOT EXISTS idx_send_recipients_sent_at ON send_recipients(sent_at);

            CREATE TABLE IF NOT EXISTS bounce_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscriber_email TEXT NOT NULL,
                bounce_type TEXT NOT NULL CHECK (bounce_type IN ('hard', 'soft', 'complaint')),
                reason TEXT,
                received_at DATETIME DEFAULT CURRENT_TIMESTAMP
            );

            CREATE INDEX IF NOT EXISTS idx_bounce_log_email ON bounce_log(subscriber_email);

            CREATE TABLE IF NOT EXISTS subscriber_tags (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscriber_id INTEGER NOT NULL,
                tag TEXT NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                FOREIGN KEY (subscriber_id) REFERENCES subscribers(id) ON DELETE CASCADE,
                UNIQUE(subscriber_id, tag)
            );

            CREATE INDEX IF NOT EXISTS idx_subscriber_tags_tag ON subscriber_tags(tag);
            CREATE INDEX IF NOT EXISTS idx_subscriber_tags_subscriber ON subscriber_tags(subscriber_id);

            CREATE TABLE IF NOT EXISTS confirmation_tokens (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscriber_id INTEGER NOT NULL,
                token TEXT UNIQUE NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                expires_at DATETIME NOT NULL,
                used_at DATETIME,
                FOREIGN KEY (subscriber_id) REFERENCES subscribers(id) ON DELETE CASCADE
            );

            CREATE INDEX IF NOT EXISTS idx_confirmation_tokens_token ON confirmation_tokens(token);
            "#,
        )
        .context("Failed to initialize database schema")?;

        // Migrations for existing databases
        self.run_migrations(&conn)?;

        Ok(())
    }

    /// Run migrations for existing databases
    fn run_migrations(&self, conn: &rusqlite::Connection) -> Result<()> {
        // Migration: add declined_at column if it doesn't exist
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

        // Migration: add soft_bounce_count column if it doesn't exist
        let has_soft_bounce: bool = conn
            .prepare(
                "SELECT COUNT(*) FROM pragma_table_info('subscribers') WHERE name='soft_bounce_count'",
            )
            .and_then(|mut stmt| stmt.query_row([], |row| row.get::<_, i64>(0)))
            .map(|count| count > 0)
            .unwrap_or(false);

        if !has_soft_bounce {
            let _ = conn.execute_batch(
                "ALTER TABLE subscribers ADD COLUMN soft_bounce_count INTEGER NOT NULL DEFAULT 0",
            );
        }

        Ok(())
    }

    /// Add a new subscriber to the database
    pub fn add_subscriber(&self, subscriber: &Subscriber) -> Result<i64> {
        let conn = self.get_conn()?;
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

    /// Add multiple subscribers in a single transaction (batch insert)
    pub fn add_subscribers_batch(&self, subscribers: &[Subscriber]) -> Result<usize> {
        let conn = self.get_conn()?;
        let tx = conn.unchecked_transaction()?;
        let mut count = 0;

        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO subscribers (email, status, subscribed_at, source_email_id, notes)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
            )?;

            for subscriber in subscribers {
                let rows = stmt.execute(params![
                    subscriber.email,
                    subscriber.status.to_string(),
                    subscriber
                        .subscribed_at
                        .format("%Y-%m-%d %H:%M:%S%.3f")
                        .to_string(),
                    subscriber.source_email_id,
                    subscriber.notes,
                ])?;
                count += rows;
            }
        }

        tx.commit()?;
        Ok(count)
    }

    /// Update subscriber status
    pub fn update_subscriber_status(&self, id: i64, status: SubscriberStatus) -> Result<()> {
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();

        let conn = self.get_conn()?;
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
            SubscriberStatus::Pending | SubscriberStatus::Unconfirmed => {
                conn.execute(
                    "UPDATE subscribers SET status = ?1, approved_at = NULL, declined_at = NULL WHERE id = ?2",
                    params![status.to_string(), id],
                )?;
            }
        }

        Ok(())
    }

    /// Update subscriber status by email
    pub fn update_subscriber_status_by_email(
        &self,
        email: &str,
        status: SubscriberStatus,
    ) -> Result<bool> {
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();

        let conn = self.get_conn()?;
        let rows = match status {
            SubscriberStatus::Approved => conn.execute(
                "UPDATE subscribers SET status = ?1, approved_at = ?2 WHERE email = ?3",
                params![status.to_string(), now, email],
            )?,
            SubscriberStatus::Declined => conn.execute(
                "UPDATE subscribers SET status = ?1, declined_at = ?2 WHERE email = ?3",
                params![status.to_string(), now, email],
            )?,
            SubscriberStatus::Pending | SubscriberStatus::Unconfirmed => conn.execute(
                "UPDATE subscribers SET status = ?1, approved_at = NULL, declined_at = NULL WHERE email = ?2",
                params![status.to_string(), email],
            )?,
        };

        Ok(rows > 0)
    }

    /// Update a full subscriber record
    pub fn update_subscriber(&self, subscriber: &Subscriber) -> Result<()> {
        let id = subscriber
            .id
            .ok_or_else(|| anyhow::anyhow!("Cannot update subscriber without an ID"))?;

        let conn = self.get_conn()?;
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
        let conn = self.get_conn()?;

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
        let conn = self.get_conn()?;
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
            .get_conn()?
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

        let conn = self.get_conn()?;
        let mut stmt = conn.prepare(&query)?;
        let count: i64 =
            stmt.query_row(rusqlite::params_from_iter(params.iter()), |row| row.get(0))?;

        Ok(count)
    }

    /// Check if email already exists
    pub fn email_exists(&self, email: &str) -> Result<bool> {
        let conn = self.get_conn()?;
        let mut stmt = conn.prepare("SELECT 1 FROM subscribers WHERE email = ?1")?;
        let result = stmt.query_row(params![email], |_| Ok(()));

        match result {
            Ok(_) => Ok(true),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(false),
            Err(e) => Err(e.into()),
        }
    }

    /// Check multiple emails for existence in one query (batch dedup)
    pub fn emails_exist_batch(
        &self,
        emails: &[String],
    ) -> Result<std::collections::HashSet<String>> {
        if emails.is_empty() {
            return Ok(std::collections::HashSet::new());
        }

        let conn = self.get_conn()?;
        let mut existing = std::collections::HashSet::new();

        // Process in chunks (SQLite max params is 999, use 900)
        for chunk in emails.chunks(900) {
            let placeholders: Vec<String> = (1..=chunk.len()).map(|i| format!("?{}", i)).collect();
            let query = format!(
                "SELECT email FROM subscribers WHERE email IN ({})",
                placeholders.join(",")
            );

            let mut stmt = conn.prepare(&query)?;
            let rows = stmt.query_map(rusqlite::params_from_iter(chunk.iter()), |row| {
                row.get::<_, String>(0)
            })?;

            for email in rows.flatten() {
                existing.insert(email);
            }
        }

        Ok(existing)
    }

    /// Record a newsletter send in history
    pub fn record_send(
        &self,
        subject: &str,
        total: usize,
        successful: usize,
        failed: usize,
    ) -> Result<i64> {
        let conn = self.get_conn()?;
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

    /// Update send history counts
    pub fn update_send_history(
        &self,
        send_history_id: i64,
        successful: usize,
        failed: usize,
    ) -> Result<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "UPDATE send_history SET successful_sends = ?1, failed_sends = ?2 WHERE id = ?3",
            params![successful as i64, failed as i64, send_history_id],
        )?;
        Ok(())
    }

    /// Record individual send recipient status
    #[allow(dead_code)]
    pub fn record_send_recipient(
        &self,
        send_history_id: i64,
        email: &str,
        status: &str,
        error_message: Option<&str>,
    ) -> Result<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "INSERT INTO send_recipients (send_history_id, subscriber_email, status, error_message) VALUES (?1, ?2, ?3, ?4)",
            params![send_history_id, email, status, error_message],
        )?;
        Ok(())
    }

    /// Record send recipients in a batch transaction
    pub fn record_send_recipients_batch(
        &self,
        send_history_id: i64,
        results: &[(String, &str, Option<String>)], // (email, status, error_message)
    ) -> Result<()> {
        let conn = self.get_conn()?;
        let tx = conn.unchecked_transaction()?;

        {
            let mut stmt = tx.prepare(
                "INSERT INTO send_recipients (send_history_id, subscriber_email, status, error_message) VALUES (?1, ?2, ?3, ?4)",
            )?;

            for (email, status, error_message) in results {
                stmt.execute(params![
                    send_history_id,
                    email,
                    status,
                    error_message.as_deref(),
                ])?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    // --- Send Queue operations for resume/retry support ---

    /// Create a send queue by populating it with all approved subscribers for a send.
    /// Optionally filter by tag.
    #[allow(dead_code)]
    pub fn create_send_queue(&self, send_history_id: i64) -> Result<usize> {
        self.create_send_queue_with_tag(send_history_id, None)
    }

    /// Create a send queue filtered by optional tag
    pub fn create_send_queue_with_tag(
        &self,
        send_history_id: i64,
        tag: Option<&str>,
    ) -> Result<usize> {
        let conn = self.get_conn()?;
        let count = match tag {
            Some(tag) => conn.execute(
                "INSERT INTO send_queue (send_history_id, subscriber_id, subscriber_email, status)
                 SELECT ?1, s.id, s.email, 'pending'
                 FROM subscribers s
                 INNER JOIN subscriber_tags st ON st.subscriber_id = s.id
                 WHERE s.status = 'approved' AND st.tag = ?2",
                params![send_history_id, tag],
            )?,
            None => conn.execute(
                "INSERT INTO send_queue (send_history_id, subscriber_id, subscriber_email, status)
                 SELECT ?1, id, email, 'pending'
                 FROM subscribers WHERE status = 'approved'",
                params![send_history_id],
            )?,
        };
        Ok(count)
    }

    /// Get pending/retry items from the send queue in batches
    pub fn get_send_queue_batch(
        &self,
        send_history_id: i64,
        batch_size: usize,
    ) -> Result<Vec<SendQueueItem>> {
        let conn = self.get_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, send_history_id, subscriber_id, subscriber_email, status, attempts, last_attempt_at, error_message
             FROM send_queue
             WHERE send_history_id = ?1 AND status IN ('pending', 'retry')
             ORDER BY id ASC
             LIMIT ?2",
        )?;

        let items: Vec<SendQueueItem> = stmt
            .query_map(params![send_history_id, batch_size as i64], |row| {
                let status_str: String = row.get(4)?;
                let last_attempt_str: Option<String> = row.get(6)?;

                Ok(SendQueueItem {
                    id: row.get(0)?,
                    send_history_id: row.get(1)?,
                    subscriber_id: row.get(2)?,
                    subscriber_email: row.get(3)?,
                    status: status_str.parse().unwrap_or(SendQueueStatus::Pending),
                    attempts: row.get(5)?,
                    last_attempt_at: last_attempt_str
                        .as_deref()
                        .and_then(|s| parse_datetime(s).ok()),
                    error_message: row.get(7)?,
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(items)
    }

    /// Update a send queue item's status after send attempt
    pub fn update_send_queue_item(
        &self,
        item_id: i64,
        status: SendQueueStatus,
        error_message: Option<&str>,
    ) -> Result<()> {
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let conn = self.get_conn()?;
        conn.execute(
            "UPDATE send_queue SET status = ?1, attempts = attempts + 1, last_attempt_at = ?2, error_message = ?3 WHERE id = ?4",
            params![status.to_string(), now, error_message, item_id],
        )?;
        Ok(())
    }

    /// Update multiple send queue items in a batch transaction
    pub fn update_send_queue_items_batch(
        &self,
        updates: &[(i64, SendQueueStatus, Option<String>)],
    ) -> Result<()> {
        let now = Utc::now().format("%Y-%m-%d %H:%M:%S%.3f").to_string();
        let conn = self.get_conn()?;
        let tx = conn.unchecked_transaction()?;

        {
            let mut stmt = tx.prepare(
                "UPDATE send_queue SET status = ?1, attempts = attempts + 1, last_attempt_at = ?2, error_message = ?3 WHERE id = ?4",
            )?;

            for (item_id, status, error_message) in updates {
                stmt.execute(params![
                    status.to_string(),
                    now,
                    error_message.as_deref(),
                    item_id,
                ])?;
            }
        }

        tx.commit()?;
        Ok(())
    }

    /// Get send queue stats for a given send
    pub fn get_send_queue_stats(
        &self,
        send_history_id: i64,
    ) -> Result<(usize, usize, usize, usize)> {
        let conn = self.get_conn()?;
        let mut stmt = conn.prepare(
            "SELECT status, COUNT(*) FROM send_queue WHERE send_history_id = ?1 GROUP BY status",
        )?;

        let mut pending = 0usize;
        let mut sent = 0usize;
        let mut failed = 0usize;
        let mut retry = 0usize;

        let rows = stmt.query_map(params![send_history_id], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;

        for row in rows {
            let (status, count) = row?;
            match status.as_str() {
                "pending" => pending = count as usize,
                "sent" => sent = count as usize,
                "failed" => failed = count as usize,
                "retry" => retry = count as usize,
                _ => {}
            }
        }

        Ok((pending, sent, failed, retry))
    }

    /// Get the latest incomplete send (for resume)
    pub fn get_incomplete_send(&self) -> Result<Option<i64>> {
        let conn = self.get_conn()?;
        let result = conn.query_row(
            "SELECT sh.id FROM send_history sh
             INNER JOIN send_queue sq ON sq.send_history_id = sh.id
             WHERE sq.status IN ('pending', 'retry')
             GROUP BY sh.id
             ORDER BY sh.id DESC
             LIMIT 1",
            [],
            |row| row.get::<_, i64>(0),
        );

        match result {
            Ok(id) => Ok(Some(id)),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Get the subject for a send history entry
    pub fn get_send_history_subject(&self, send_history_id: i64) -> Result<String> {
        let conn = self.get_conn()?;
        let subject: String = conn.query_row(
            "SELECT subject FROM send_history WHERE id = ?1",
            params![send_history_id],
            |row| row.get(0),
        )?;
        Ok(subject)
    }

    // --- Cleanup operations ---

    /// Delete old send recipients older than the given number of days
    pub fn cleanup_old_send_recipients(&self, days: u32) -> Result<usize> {
        let conn = self.get_conn()?;
        let rows = conn.execute(
            "DELETE FROM send_recipients WHERE sent_at < datetime('now', ?1)",
            params![format!("-{} days", days)],
        )?;
        Ok(rows)
    }

    /// Delete send queue items from completed sends
    pub fn cleanup_completed_send_queue(&self) -> Result<usize> {
        let conn = self.get_conn()?;
        let rows = conn.execute(
            "DELETE FROM send_queue WHERE send_history_id IN (
                SELECT sq.send_history_id FROM send_queue sq
                GROUP BY sq.send_history_id
                HAVING COUNT(*) = SUM(CASE WHEN sq.status IN ('sent', 'failed') THEN 1 ELSE 0 END)
            )",
            [],
        )?;
        Ok(rows)
    }

    // --- Bounce operations ---

    /// Record a bounce event
    pub fn record_bounce(
        &self,
        email: &str,
        bounce_type: &BounceType,
        reason: Option<&str>,
    ) -> Result<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "INSERT INTO bounce_log (subscriber_email, bounce_type, reason) VALUES (?1, ?2, ?3)",
            params![email, bounce_type.to_string(), reason],
        )?;

        match bounce_type {
            BounceType::Hard | BounceType::Complaint => {
                // Immediate decline
                conn.execute(
                    "UPDATE subscribers SET status = 'declined', declined_at = datetime('now') WHERE email = ?1",
                    params![email],
                )?;
            }
            BounceType::Soft => {
                // Increment soft bounce count and decline if >= 3
                conn.execute(
                    "UPDATE subscribers SET soft_bounce_count = soft_bounce_count + 1 WHERE email = ?1",
                    params![email],
                )?;
                conn.execute(
                    "UPDATE subscribers SET status = 'declined', declined_at = datetime('now') WHERE email = ?1 AND soft_bounce_count >= 3",
                    params![email],
                )?;
            }
        }

        Ok(())
    }

    /// Get bounce count for an email
    #[allow(dead_code)]
    pub fn get_bounce_count(&self, email: &str) -> Result<i64> {
        let conn = self.get_conn()?;
        let count: i64 = conn.query_row(
            "SELECT COUNT(*) FROM bounce_log WHERE subscriber_email = ?1",
            params![email],
            |row| row.get(0),
        )?;
        Ok(count)
    }

    /// Get bounce history for an email
    pub fn get_bounces(&self, email: &str) -> Result<Vec<BounceRecord>> {
        let conn = self.get_conn()?;
        let mut stmt = conn.prepare(
            "SELECT id, subscriber_email, bounce_type, reason, received_at FROM bounce_log WHERE subscriber_email = ?1 ORDER BY received_at DESC",
        )?;

        let records = stmt
            .query_map(params![email], |row| {
                let bounce_type_str: String = row.get(2)?;
                let received_at_str: String = row.get(4)?;
                Ok(BounceRecord {
                    id: row.get(0)?,
                    subscriber_email: row.get(1)?,
                    bounce_type: bounce_type_str.parse().unwrap_or(BounceType::Soft),
                    reason: row.get(3)?,
                    received_at: parse_datetime(&received_at_str).unwrap_or_else(|_| Utc::now()),
                })
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(records)
    }

    // --- Tag operations ---

    /// Add a tag to a subscriber
    pub fn add_tag(&self, subscriber_id: i64, tag: &str) -> Result<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "INSERT OR IGNORE INTO subscriber_tags (subscriber_id, tag) VALUES (?1, ?2)",
            params![subscriber_id, tag],
        )?;
        Ok(())
    }

    /// Remove a tag from a subscriber
    pub fn remove_tag(&self, subscriber_id: i64, tag: &str) -> Result<bool> {
        let conn = self.get_conn()?;
        let rows = conn.execute(
            "DELETE FROM subscriber_tags WHERE subscriber_id = ?1 AND tag = ?2",
            params![subscriber_id, tag],
        )?;
        Ok(rows > 0)
    }

    /// Get all tags for a subscriber
    pub fn get_tags(&self, subscriber_id: i64) -> Result<Vec<String>> {
        let conn = self.get_conn()?;
        let mut stmt =
            conn.prepare("SELECT tag FROM subscriber_tags WHERE subscriber_id = ?1 ORDER BY tag")?;
        let tags = stmt
            .query_map(params![subscriber_id], |row| row.get::<_, String>(0))?
            .filter_map(|r| r.ok())
            .collect();
        Ok(tags)
    }

    /// Get subscribers by tag
    pub fn get_subscribers_by_tag(&self, tag: &str) -> Result<Vec<Subscriber>> {
        let conn = self.get_conn()?;
        let mut stmt = conn.prepare(
            "SELECT s.id, s.email, s.status, s.subscribed_at, s.approved_at, s.declined_at, s.source_email_id, s.notes
             FROM subscribers s
             INNER JOIN subscriber_tags st ON st.subscriber_id = s.id
             WHERE st.tag = ?1
             ORDER BY s.subscribed_at DESC",
        )?;

        let subscribers = stmt
            .query_map(params![tag], Self::row_to_subscriber)?
            .filter_map(|r| r.ok())
            .collect();

        Ok(subscribers)
    }

    /// Get all tags with subscriber counts
    pub fn get_all_tags(&self) -> Result<Vec<(String, i64)>> {
        let conn = self.get_conn()?;
        let mut stmt = conn.prepare(
            "SELECT tag, COUNT(*) as cnt FROM subscriber_tags GROUP BY tag ORDER BY tag",
        )?;
        let tags = stmt
            .query_map([], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .filter_map(|r| r.ok())
            .collect();
        Ok(tags)
    }

    /// Add tags in batch for a subscriber
    pub fn add_tags_batch(&self, subscriber_id: i64, tags: &[String]) -> Result<()> {
        if tags.is_empty() {
            return Ok(());
        }
        let conn = self.get_conn()?;
        let tx = conn.unchecked_transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT OR IGNORE INTO subscriber_tags (subscriber_id, tag) VALUES (?1, ?2)",
            )?;
            for tag in tags {
                stmt.execute(params![subscriber_id, tag])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    // --- Confirmation token operations (double opt-in) ---

    /// Create a confirmation token for a subscriber
    pub fn create_confirmation_token(
        &self,
        subscriber_id: i64,
        token: &str,
        expires_hours: i64,
    ) -> Result<()> {
        let conn = self.get_conn()?;
        conn.execute(
            "INSERT INTO confirmation_tokens (subscriber_id, token, expires_at)
             VALUES (?1, ?2, datetime('now', ?3))",
            params![subscriber_id, token, format!("+{} hours", expires_hours)],
        )?;
        Ok(())
    }

    /// Verify a confirmation token and promote the subscriber
    pub fn verify_confirmation_token(&self, token: &str) -> Result<Option<String>> {
        let conn = self.get_conn()?;

        // Find the token and check expiry
        let result = conn.query_row(
            "SELECT ct.id, ct.subscriber_id, s.email
             FROM confirmation_tokens ct
             INNER JOIN subscribers s ON s.id = ct.subscriber_id
             WHERE ct.token = ?1 AND ct.used_at IS NULL AND ct.expires_at > datetime('now')",
            params![token],
            |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, i64>(1)?,
                    row.get::<_, String>(2)?,
                ))
            },
        );

        match result {
            Ok((token_id, subscriber_id, email)) => {
                // Mark token as used
                conn.execute(
                    "UPDATE confirmation_tokens SET used_at = datetime('now') WHERE id = ?1",
                    params![token_id],
                )?;
                // Promote subscriber from Unconfirmed to Pending
                conn.execute(
                    "UPDATE subscribers SET status = 'pending' WHERE id = ?1 AND status = 'unconfirmed'",
                    params![subscriber_id],
                )?;
                Ok(Some(email))
            }
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(e) => Err(e.into()),
        }
    }

    /// Cleanup expired confirmation tokens
    #[allow(dead_code)]
    pub fn cleanup_expired_tokens(&self) -> Result<usize> {
        let conn = self.get_conn()?;
        let rows = conn.execute(
            "DELETE FROM confirmation_tokens WHERE expires_at < datetime('now') AND used_at IS NULL",
            [],
        )?;
        Ok(rows)
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

/// r2d2 connection customizer to run PRAGMAs on each new connection
#[derive(Debug)]
struct ConnectionCustomizer;

impl r2d2::CustomizeConnection<rusqlite::Connection, rusqlite::Error> for ConnectionCustomizer {
    fn on_acquire(&self, conn: &mut rusqlite::Connection) -> Result<(), rusqlite::Error> {
        init_connection(conn).map_err(|e| {
            rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_ERROR),
                Some(e.to_string()),
            )
        })?;
        Ok(())
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

    #[test]
    fn test_batch_insert() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        let subscribers: Vec<Subscriber> = (0..100)
            .map(|i| Subscriber::new(format!("batch{}@example.com", i), None))
            .collect();

        let count = db.add_subscribers_batch(&subscribers)?;
        assert_eq!(count, 100);

        let total = db.get_subscriber_count(None)?;
        assert_eq!(total, 100);

        // Duplicates should be ignored
        let count2 = db.add_subscribers_batch(&subscribers)?;
        assert_eq!(count2, 0);

        Ok(())
    }

    #[test]
    fn test_batch_email_exists() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        for i in 0..5 {
            let sub = Subscriber::new(format!("exists{}@example.com", i), None);
            db.add_subscriber(&sub)?;
        }

        let emails: Vec<String> = (0..10)
            .map(|i| format!("exists{}@example.com", i))
            .collect();
        let existing = db.emails_exist_batch(&emails)?;
        assert_eq!(existing.len(), 5);
        assert!(existing.contains("exists0@example.com"));
        assert!(!existing.contains("exists5@example.com"));

        Ok(())
    }

    #[test]
    fn test_send_queue() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        // Create approved subscribers
        for i in 0..5 {
            let mut sub = Subscriber::new(format!("queue{}@example.com", i), None);
            sub.status = SubscriberStatus::Approved;
            db.add_subscriber(&sub)?;
        }

        // Create a send and populate queue
        let send_id = db.record_send("Queue Test", 5, 0, 0)?;
        let queued = db.create_send_queue(send_id)?;
        assert_eq!(queued, 5);

        // Get a batch from the queue
        let batch = db.get_send_queue_batch(send_id, 3)?;
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].status, SendQueueStatus::Pending);

        // Update items
        db.update_send_queue_item(batch[0].id, SendQueueStatus::Sent, None)?;
        db.update_send_queue_item(batch[1].id, SendQueueStatus::Failed, Some("SMTP timeout"))?;

        // Check stats
        let (pending, sent, failed, _retry) = db.get_send_queue_stats(send_id)?;
        assert_eq!(sent, 1);
        assert_eq!(failed, 1);
        assert_eq!(pending, 3);

        Ok(())
    }

    #[test]
    fn test_batch_send_recipients() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;
        let send_id = db.record_send("Batch Test", 3, 0, 0)?;

        let results = vec![
            ("a@example.com".to_string(), "sent", None),
            ("b@example.com".to_string(), "sent", None),
            (
                "c@example.com".to_string(),
                "failed",
                Some("error".to_string()),
            ),
        ];

        db.record_send_recipients_batch(send_id, &results)?;
        Ok(())
    }

    #[test]
    fn test_tags() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        let sub = Subscriber::new("tagged@example.com".to_string(), None);
        let id = db.add_subscriber(&sub)?;

        db.add_tag(id, "premium")?;
        db.add_tag(id, "beta")?;

        let tags = db.get_tags(id)?;
        assert_eq!(tags, vec!["beta", "premium"]);

        // Duplicate tag should be ignored
        db.add_tag(id, "premium")?;
        let tags = db.get_tags(id)?;
        assert_eq!(tags.len(), 2);

        // Remove tag
        assert!(db.remove_tag(id, "beta")?);
        let tags = db.get_tags(id)?;
        assert_eq!(tags, vec!["premium"]);

        // Get all tags
        let all_tags = db.get_all_tags()?;
        assert_eq!(all_tags.len(), 1);
        assert_eq!(all_tags[0], ("premium".to_string(), 1));

        Ok(())
    }

    #[test]
    fn test_bounces() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        let mut sub = Subscriber::new("bounce@example.com".to_string(), None);
        sub.status = SubscriberStatus::Approved;
        db.add_subscriber(&sub)?;

        // Hard bounce should immediately decline
        db.record_bounce(
            "bounce@example.com",
            &BounceType::Hard,
            Some("550 User not found"),
        )?;
        let updated = db.get_subscriber_by_email("bounce@example.com")?.unwrap();
        assert_eq!(updated.status, SubscriberStatus::Declined);

        // Test soft bounces
        let mut sub2 = Subscriber::new("soft@example.com".to_string(), None);
        sub2.status = SubscriberStatus::Approved;
        db.add_subscriber(&sub2)?;

        db.record_bounce("soft@example.com", &BounceType::Soft, Some("Mailbox full"))?;
        db.record_bounce("soft@example.com", &BounceType::Soft, Some("Mailbox full"))?;
        let still_approved = db.get_subscriber_by_email("soft@example.com")?.unwrap();
        assert_eq!(still_approved.status, SubscriberStatus::Approved);

        db.record_bounce("soft@example.com", &BounceType::Soft, Some("Mailbox full"))?;
        let now_declined = db.get_subscriber_by_email("soft@example.com")?.unwrap();
        assert_eq!(now_declined.status, SubscriberStatus::Declined);

        Ok(())
    }

    #[test]
    fn test_confirmation_tokens() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        let mut sub = Subscriber::new("confirm@example.com".to_string(), None);
        sub.status = SubscriberStatus::Unconfirmed;
        let id = db.add_subscriber(&sub)?;

        db.create_confirmation_token(id, "test-token-123", 24)?;

        // Valid token should verify
        let email = db.verify_confirmation_token("test-token-123")?;
        assert_eq!(email, Some("confirm@example.com".to_string()));

        // Subscriber should now be pending
        let updated = db.get_subscriber_by_email("confirm@example.com")?.unwrap();
        assert_eq!(updated.status, SubscriberStatus::Pending);

        // Token should not be usable again
        let reuse = db.verify_confirmation_token("test-token-123")?;
        assert!(reuse.is_none());

        // Invalid token should return None
        let invalid = db.verify_confirmation_token("nonexistent")?;
        assert!(invalid.is_none());

        Ok(())
    }

    #[test]
    fn test_update_status_by_email() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        let sub = Subscriber::new("status@example.com".to_string(), None);
        db.add_subscriber(&sub)?;

        assert!(
            db.update_subscriber_status_by_email("status@example.com", SubscriberStatus::Declined)?
        );
        let updated = db.get_subscriber_by_email("status@example.com")?.unwrap();
        assert_eq!(updated.status, SubscriberStatus::Declined);

        assert!(!db.update_subscriber_status_by_email(
            "nonexistent@example.com",
            SubscriberStatus::Declined
        )?);

        Ok(())
    }
}
