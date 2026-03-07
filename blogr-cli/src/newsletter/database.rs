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

        // Enable WAL mode for better concurrent read/write performance
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA cache_size=-64000;
             PRAGMA busy_timeout=5000;",
        )
        .context("Failed to set database pragmas")?;

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

    /// Add multiple subscribers in a single transaction (batch insert)
    pub fn add_subscribers_batch(&self, subscribers: &[Subscriber]) -> Result<usize> {
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;
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

    /// Stream approved subscribers in batches for sending (constant memory usage)
    #[allow(dead_code)]
    pub fn iter_approved_batches(&self, batch_size: usize) -> Result<Vec<Vec<Subscriber>>> {
        let conn = self.lock_conn()?;
        let total: i64 = conn.query_row(
            "SELECT COUNT(*) FROM subscribers WHERE status = 'approved'",
            [],
            |row| row.get(0),
        )?;

        let mut batches = Vec::new();
        let mut offset: usize = 0;

        while (offset as i64) < total {
            let mut stmt = conn.prepare(
                "SELECT id, email, status, subscribed_at, approved_at, declined_at, source_email_id, notes
                 FROM subscribers WHERE status = 'approved'
                 ORDER BY id ASC LIMIT ?1 OFFSET ?2",
            )?;

            let batch: Vec<Subscriber> = stmt
                .query_map(params![batch_size as i64, offset as i64], |row| {
                    Self::row_to_subscriber(row)
                })?
                .filter_map(|r| r.ok())
                .collect();

            if batch.is_empty() {
                break;
            }
            offset += batch.len();
            batches.push(batch);
        }

        Ok(batches)
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

    /// Check multiple emails for existence in one query (batch dedup)
    pub fn emails_exist_batch(
        &self,
        emails: &[String],
    ) -> Result<std::collections::HashSet<String>> {
        if emails.is_empty() {
            return Ok(std::collections::HashSet::new());
        }

        let conn = self.lock_conn()?;
        let mut existing = std::collections::HashSet::new();

        // Process in chunks to avoid SQLite variable limit
        for chunk in emails.chunks(500) {
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

    /// Update send history counts
    pub fn update_send_history(
        &self,
        send_history_id: i64,
        successful: usize,
        failed: usize,
    ) -> Result<()> {
        let conn = self.lock_conn()?;
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
        let conn = self.lock_conn()?;
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
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;

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

    /// Create a send queue by populating it with all approved subscribers for a send
    pub fn create_send_queue(&self, send_history_id: i64) -> Result<usize> {
        let conn = self.lock_conn()?;
        let count = conn.execute(
            "INSERT INTO send_queue (send_history_id, subscriber_id, subscriber_email, status)
             SELECT ?1, id, email, 'pending'
             FROM subscribers WHERE status = 'approved'",
            params![send_history_id],
        )?;
        Ok(count)
    }

    /// Get pending/retry items from the send queue in batches
    pub fn get_send_queue_batch(
        &self,
        send_history_id: i64,
        batch_size: usize,
    ) -> Result<Vec<SendQueueItem>> {
        let conn = self.lock_conn()?;
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
        let conn = self.lock_conn()?;
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
        let mut conn = self.lock_conn()?;
        let tx = conn.transaction()?;

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
        let conn = self.lock_conn()?;
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
        let conn = self.lock_conn()?;
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
        let conn = self.lock_conn()?;
        let subject: String = conn.query_row(
            "SELECT subject FROM send_history WHERE id = ?1",
            params![send_history_id],
            |row| row.get(0),
        )?;
        Ok(subject)
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
        assert_eq!(pending, 3); // 2 remaining from first batch + 2 not yet fetched = 3 pending

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
    fn test_iter_approved_batches() -> Result<()> {
        let db = NewsletterDatabase::in_memory()?;

        for i in 0..10 {
            let mut sub = Subscriber::new(format!("iter{}@example.com", i), None);
            sub.status = SubscriberStatus::Approved;
            db.add_subscriber(&sub)?;
        }

        let batches = db.iter_approved_batches(3)?;
        assert_eq!(batches.len(), 4); // 3+3+3+1
        assert_eq!(batches[0].len(), 3);
        assert_eq!(batches[3].len(), 1);

        Ok(())
    }
}
