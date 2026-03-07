//! SMTP email sending for newsletters
//!
//! This module handles sending newsletters via SMTP with:
//! - Async concurrent sending with configurable parallelism
//! - Token bucket rate limiting
//! - Send queue for crash recovery and resume
//! - Batch database writes for tracking
//! - Efficient personalization (avoids full clone per subscriber)

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use lettre::message::{header, Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::client::{Tls, TlsParameters};
use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, SmtpTransport, Tokio1Executor, Transport,
};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::sync::Arc;
use std::time::Duration;
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

use crate::config::SmtpConfig;
use crate::newsletter::composer::Newsletter;
use crate::newsletter::database::{
    NewsletterDatabase, SendQueueStatus, Subscriber, SubscriberStatus,
};

type HmacSha256 = Hmac<Sha256>;

/// Default batch size for processing subscribers
const DEFAULT_BATCH_SIZE: usize = 1000;
/// Default concurrent SMTP connections
const DEFAULT_CONCURRENCY: usize = 10;
/// Default emails per minute rate limit
const DEFAULT_RATE_LIMIT: u32 = 100;
/// Max retries for failed sends
const MAX_RETRIES: i32 = 3;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendReport {
    pub total_subscribers: usize,
    pub successful_sends: usize,
    pub failed_sends: usize,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub errors: Vec<SendError>,
    pub resumed: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendError {
    pub subscriber_email: String,
    pub error_message: String,
    pub timestamp: DateTime<Utc>,
    pub retry_count: u32,
}

impl SendReport {
    pub fn new(total_subscribers: usize) -> Self {
        Self {
            total_subscribers,
            successful_sends: 0,
            failed_sends: 0,
            started_at: Utc::now(),
            completed_at: None,
            errors: Vec::new(),
            resumed: false,
        }
    }

    pub fn add_success(&mut self) {
        self.successful_sends += 1;
    }

    pub fn add_error(&mut self, subscriber_email: String, error_message: String) {
        self.failed_sends += 1;
        self.errors.push(SendError {
            subscriber_email,
            error_message,
            timestamp: Utc::now(),
            retry_count: 0,
        });
    }

    pub fn complete(&mut self) {
        self.completed_at = Some(Utc::now());
    }

    #[allow(dead_code)]
    pub fn is_complete(&self) -> bool {
        self.successful_sends + self.failed_sends >= self.total_subscribers
    }

    pub fn success_rate(&self) -> f64 {
        if self.total_subscribers == 0 {
            return 1.0;
        }
        self.successful_sends as f64 / self.total_subscribers as f64
    }
}

/// Token bucket rate limiter — O(1) per check, no unbounded Vec growth
pub struct RateLimiter {
    emails_per_second: f64,
    tokens: f64,
    max_tokens: f64,
    last_refill: tokio::time::Instant,
}

impl RateLimiter {
    pub fn new(emails_per_minute: u32) -> Self {
        let per_second = emails_per_minute as f64 / 60.0;
        // Allow burst of up to 1 second worth of tokens
        let max_tokens = per_second.max(1.0);
        Self {
            emails_per_second: per_second,
            tokens: max_tokens,
            max_tokens,
            last_refill: tokio::time::Instant::now(),
        }
    }

    /// Wait until a token is available, then consume it
    pub async fn acquire(&mut self) {
        loop {
            self.refill();
            if self.tokens >= 1.0 {
                self.tokens -= 1.0;
                return;
            }
            // Wait for enough time to accumulate 1 token
            let wait = Duration::from_secs_f64(1.0 / self.emails_per_second);
            tokio::time::sleep(wait).await;
        }
    }

    fn refill(&mut self) {
        let now = tokio::time::Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.emails_per_second).min(self.max_tokens);
        self.last_refill = now;
    }
}

/// Configuration for the sending pipeline
#[derive(Debug, Clone)]
pub struct SendConfig {
    pub emails_per_minute: u32,
    pub concurrency: usize,
    pub batch_size: usize,
    pub max_retries: i32,
}

impl Default for SendConfig {
    fn default() -> Self {
        Self {
            emails_per_minute: DEFAULT_RATE_LIMIT,
            concurrency: DEFAULT_CONCURRENCY,
            batch_size: DEFAULT_BATCH_SIZE,
            max_retries: MAX_RETRIES,
        }
    }
}

pub struct NewsletterSender {
    smtp_config: SmtpConfig,
    from_address: Mailbox,
    hmac_secret: String,
    send_config: SendConfig,
}

impl NewsletterSender {
    /// Create a new newsletter sender
    pub fn new(
        smtp_config: SmtpConfig,
        sender_name: Option<String>,
        emails_per_minute: u32,
        hmac_secret: String,
    ) -> Result<Self> {
        let from_name = sender_name.unwrap_or_else(|| "Newsletter".to_string());
        let from_address = format!("{} <{}>", from_name, smtp_config.username)
            .parse()
            .context("Failed to parse from address")?;

        Ok(Self {
            smtp_config,
            from_address,
            hmac_secret,
            send_config: SendConfig {
                emails_per_minute,
                ..Default::default()
            },
        })
    }

    /// Set sending configuration
    #[allow(dead_code)]
    pub fn with_config(mut self, config: SendConfig) -> Self {
        self.send_config = config;
        self
    }

    /// Send newsletter using the send queue for resume support.
    /// This is the primary high-performance sending method.
    pub async fn send_with_queue(
        &self,
        newsletter: &Newsletter,
        database: &NewsletterDatabase,
        smtp_password: &str,
        progress_callback: Option<Box<dyn Fn(usize, usize) + Send + Sync>>,
    ) -> Result<SendReport> {
        // Check for an incomplete send to resume
        let (send_history_id, total, resumed) = match database.get_incomplete_send()? {
            Some(id) => {
                let (pending, sent, failed, retry) = database.get_send_queue_stats(id)?;
                let total = pending + sent + failed + retry;
                let remaining = pending + retry;
                let subject = database.get_send_history_subject(id)?;

                // Validate that the newsletter matches the incomplete send
                if subject != newsletter.subject {
                    return Err(anyhow::anyhow!(
                        "Incomplete send found for '{}', but current newsletter subject is '{}'. \
                         Complete or clear the previous send before starting a new one.",
                        subject,
                        newsletter.subject
                    ));
                }

                println!(
                    "Resuming incomplete send '{}': {}/{} remaining",
                    subject, remaining, total
                );
                (id, total, true)
            }
            None => {
                // Create new send history and queue
                let approved_count =
                    database.get_subscriber_count(Some(SubscriberStatus::Approved))? as usize;
                if approved_count == 0 {
                    println!("No approved subscribers found");
                    let mut report = SendReport::new(0);
                    report.complete();
                    return Ok(report);
                }

                let send_id = database.record_send(&newsletter.subject, approved_count, 0, 0)?;
                let queued = database.create_send_queue(send_id)?;
                println!("Sending newsletter to {} approved subscribers", queued);
                (send_id, queued, false)
            }
        };

        let mut report = SendReport::new(total);
        report.resumed = resumed;

        // Pre-compute the parts of the newsletter that don't change per subscriber
        let newsletter = Arc::new(newsletter.clone());
        let from_address = self.from_address.clone();
        let hmac_secret = self.hmac_secret.clone();
        let smtp_username = self.smtp_config.username.clone();

        // Create a single async SMTP transport to be shared across all tasks
        let transport = Arc::new(create_async_smtp_transport(
            &self.smtp_config,
            smtp_password,
        )?);

        // Rate limiter shared across all tasks
        let rate_limiter = Arc::new(tokio::sync::Mutex::new(RateLimiter::new(
            self.send_config.emails_per_minute,
        )));
        let semaphore = Arc::new(Semaphore::new(self.send_config.concurrency));
        let progress_callback = progress_callback.map(Arc::new);
        let processed = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        // If resuming, count already-sent items
        if resumed {
            let (_, sent, failed, _) = database.get_send_queue_stats(send_history_id)?;
            report.successful_sends = sent;
            report.failed_sends = failed;
            processed.store(sent + failed, std::sync::atomic::Ordering::Relaxed);
        }

        // Process the queue in batches
        loop {
            let batch =
                database.get_send_queue_batch(send_history_id, self.send_config.batch_size)?;

            if batch.is_empty() {
                break;
            }

            let mut tasks = Vec::with_capacity(batch.len());

            for item in batch {
                // Skip items that have exceeded max retries
                if item.attempts >= self.send_config.max_retries {
                    database.update_send_queue_item(
                        item.id,
                        SendQueueStatus::Failed,
                        Some("Max retries exceeded"),
                    )?;
                    report.add_error(
                        item.subscriber_email.clone(),
                        "Max retries exceeded".to_string(),
                    );
                    continue;
                }

                let permit = semaphore.clone().acquire_owned().await?;
                let newsletter = Arc::clone(&newsletter);
                let rate_limiter = Arc::clone(&rate_limiter);
                let transport = Arc::clone(&transport);
                let from_address = from_address.clone();
                let hmac_secret = hmac_secret.clone();
                let smtp_username = smtp_username.clone();
                let progress_callback = progress_callback.clone();
                let processed = Arc::clone(&processed);
                let task_total = total;

                let task = tokio::spawn(async move {
                    // Rate limit
                    rate_limiter.lock().await.acquire().await;

                    let result = send_single_email_async(
                        &transport,
                        &from_address,
                        &newsletter,
                        &item.subscriber_email,
                        &hmac_secret,
                        &smtp_username,
                    )
                    .await;

                    let count = processed.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
                    if let Some(ref cb) = progress_callback {
                        cb(count, task_total);
                    }

                    drop(permit);

                    match result {
                        Ok(()) => (item.id, item.subscriber_email, Ok(())),
                        Err(e) => {
                            let msg = format!("{}", e);
                            (item.id, item.subscriber_email, Err(msg))
                        }
                    }
                });

                tasks.push(task);
            }

            // Collect results and batch-update the database
            let mut db_updates: Vec<(i64, SendQueueStatus, Option<String>)> = Vec::new();
            let mut recipient_records: Vec<(String, &str, Option<String>)> = Vec::new();

            for task in tasks {
                match task.await {
                    Ok((item_id, email, Ok(()))) => {
                        report.add_success();
                        db_updates.push((item_id, SendQueueStatus::Sent, None));
                        recipient_records.push((email, "sent", None));
                    }
                    Ok((item_id, email, Err(error_msg))) => {
                        report.add_error(email.clone(), error_msg.clone());
                        // Mark as retry if under max retries, otherwise failed
                        db_updates.push((item_id, SendQueueStatus::Retry, Some(error_msg.clone())));
                        recipient_records.push((email, "failed", Some(error_msg)));
                    }
                    Err(join_err) => {
                        eprintln!("Task panicked: {}", join_err);
                    }
                }
            }

            // Batch write all updates
            if !db_updates.is_empty() {
                database.update_send_queue_items_batch(&db_updates)?;
            }
            if !recipient_records.is_empty() {
                database.record_send_recipients_batch(send_history_id, &recipient_records)?;
            }
        }

        // Update final send history
        database.update_send_history(
            send_history_id,
            report.successful_sends,
            report.failed_sends,
        )?;

        report.complete();
        self.print_send_summary(&report);

        Ok(report)
    }

    /// Send test email to a single address
    pub fn send_test_email(
        &mut self,
        newsletter: &Newsletter,
        test_email: &str,
        smtp_password: &str,
    ) -> Result<()> {
        println!("Sending test email to {}", test_email);

        let transport = self.create_smtp_transport(smtp_password)?;

        let test_subscriber = Subscriber {
            id: None,
            email: test_email.to_string(),
            status: SubscriberStatus::Approved,
            subscribed_at: Utc::now(),
            approved_at: Some(Utc::now()),
            declined_at: None,
            source_email_id: None,
            notes: Some("Test email".to_string()),
        };

        let unsubscribe_token = self.generate_unsubscribe_token(test_email);
        let personalized_html = personalize_content(
            &newsletter.html_content,
            &unsubscribe_token,
            &self.smtp_config.username,
        );
        let personalized_text = personalize_content(
            &newsletter.text_content,
            &unsubscribe_token,
            &self.smtp_config.username,
        );

        let personalized = Newsletter {
            subject: newsletter.subject.clone(),
            html_content: personalized_html,
            text_content: personalized_text,
            created_at: newsletter.created_at,
            unsubscribe_token: Some(unsubscribe_token),
        };

        self.send_single_email_sync(&transport, &personalized, &test_subscriber)
            .context("Failed to send test email")?;

        println!("Test email sent successfully");
        Ok(())
    }

    /// Create synchronous SMTP transport
    fn create_smtp_transport(&self, password: &str) -> Result<SmtpTransport> {
        let credentials = Credentials::new(self.smtp_config.username.clone(), password.to_string());

        let mut builder = SmtpTransport::relay(&self.smtp_config.server)?;

        if self.smtp_config.port == 465 || self.smtp_config.port == 587 {
            builder = builder.tls(Tls::Required(TlsParameters::new(
                self.smtp_config.server.clone(),
            )?));
        }

        let transport = builder
            .port(self.smtp_config.port)
            .credentials(credentials)
            .build();

        Ok(transport)
    }

    /// Send a single email (synchronous)
    fn send_single_email_sync(
        &self,
        transport: &SmtpTransport,
        newsletter: &Newsletter,
        subscriber: &Subscriber,
    ) -> Result<()> {
        let to_address: Mailbox = subscriber
            .email
            .parse()
            .context("Failed to parse subscriber email address")?;

        let email = Message::builder()
            .from(self.from_address.clone())
            .to(to_address)
            .subject(&newsletter.subject)
            .multipart(
                MultiPart::alternative()
                    .singlepart(
                        SinglePart::builder()
                            .header(header::ContentType::TEXT_PLAIN)
                            .body(newsletter.text_content.clone()),
                    )
                    .singlepart(
                        SinglePart::builder()
                            .header(header::ContentType::TEXT_HTML)
                            .body(newsletter.html_content.clone()),
                    ),
            )
            .context("Failed to build email message")?;

        transport
            .send(&email)
            .context("Failed to send email via SMTP")?;

        Ok(())
    }

    /// Generate cryptographically secure unsubscribe token using HMAC-SHA256
    fn generate_unsubscribe_token(&self, email: &str) -> String {
        generate_unsubscribe_token_with_secret(email, &self.hmac_secret)
    }

    /// Verify an unsubscribe token against an email
    #[allow(dead_code)]
    pub fn verify_unsubscribe_token(&self, email: &str, token: &str) -> bool {
        let expected = self.generate_unsubscribe_token(email);
        expected.as_bytes().ct_eq(token.as_bytes()).into()
    }

    /// Print sending summary
    fn print_send_summary(&self, report: &SendReport) {
        println!("\nNewsletter Sending Summary");
        println!("===========================");
        if report.resumed {
            println!("Mode: Resumed from previous incomplete send");
        }
        println!("Total subscribers: {}", report.total_subscribers);
        println!("Successful sends: {}", report.successful_sends);
        println!("Failed sends: {}", report.failed_sends);
        println!("Success rate: {:.1}%", report.success_rate() * 100.0);

        if let Some(completed_at) = report.completed_at {
            let duration = completed_at - report.started_at;
            println!("Total time: {} seconds", duration.num_seconds());
            if duration.num_seconds() > 0 {
                let rate = report.successful_sends as f64 / duration.num_seconds() as f64;
                println!("Throughput: {:.1} emails/second", rate);
            }
        }

        if !report.errors.is_empty() {
            let display_count = report.errors.len().min(10);
            println!(
                "\nErrors (showing {}/{}):",
                display_count,
                report.errors.len()
            );
            for error in report.errors.iter().take(display_count) {
                println!("  - {}: {}", error.subscriber_email, error.error_message);
            }
            if report.errors.len() > display_count {
                println!(
                    "  ... and {} more errors",
                    report.errors.len() - display_count
                );
            }
        }
        println!();
    }
}

/// Personalize content by replacing template tokens.
/// This avoids cloning the entire Newsletter struct — only the content strings are modified.
fn personalize_content(content: &str, unsubscribe_token: &str, smtp_username: &str) -> String {
    let unsubscribe_url = format!(
        "mailto:{}?subject=Unsubscribe&body=Please unsubscribe me from the newsletter. Token: {}",
        smtp_username, unsubscribe_token
    );

    content
        .replace("{{unsubscribe_token}}", unsubscribe_token)
        .replace("{{unsubscribe_url}}", &unsubscribe_url)
}

/// Generate an unsubscribe token with a given secret (used by both sync and async paths)
fn generate_unsubscribe_token_with_secret(email: &str, secret: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(email.as_bytes());
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

/// Create an async SMTP transport
fn create_async_smtp_transport(
    smtp_config: &SmtpConfig,
    password: &str,
) -> Result<AsyncSmtpTransport<Tokio1Executor>> {
    let credentials = Credentials::new(smtp_config.username.clone(), password.to_string());

    let mut builder = AsyncSmtpTransport::<Tokio1Executor>::relay(&smtp_config.server)?;

    if smtp_config.port == 465 || smtp_config.port == 587 {
        builder = builder.tls(Tls::Required(TlsParameters::new(
            smtp_config.server.clone(),
        )?));
    }

    let transport = builder
        .port(smtp_config.port)
        .credentials(credentials)
        .build();

    Ok(transport)
}

/// Send a single email asynchronously using a shared transport
async fn send_single_email_async(
    transport: &AsyncSmtpTransport<Tokio1Executor>,
    from_address: &Mailbox,
    newsletter: &Newsletter,
    subscriber_email: &str,
    hmac_secret: &str,
    smtp_username: &str,
) -> Result<()> {
    let to_address: Mailbox = subscriber_email
        .parse()
        .context("Failed to parse subscriber email address")?;

    let unsubscribe_token = generate_unsubscribe_token_with_secret(subscriber_email, hmac_secret);
    let html = personalize_content(
        &newsletter.html_content,
        &unsubscribe_token,
        smtp_username,
    );
    let text = personalize_content(
        &newsletter.text_content,
        &unsubscribe_token,
        smtp_username,
    );

    let email = Message::builder()
        .from(from_address.clone())
        .to(to_address)
        .subject(&newsletter.subject)
        .multipart(
            MultiPart::alternative()
                .singlepart(
                    SinglePart::builder()
                        .header(header::ContentType::TEXT_PLAIN)
                        .body(text),
                )
                .singlepart(
                    SinglePart::builder()
                        .header(header::ContentType::TEXT_HTML)
                        .body(html),
                ),
        )
        .context("Failed to build email message")?;

    transport
        .send(email)
        .await
        .context("Failed to send email via SMTP")?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_send_report() {
        let mut report = SendReport::new(5);
        assert_eq!(report.total_subscribers, 5);
        assert_eq!(report.successful_sends, 0);
        assert_eq!(report.failed_sends, 0);
        assert!(!report.is_complete());

        report.add_success();
        report.add_success();
        report.add_error("test@example.com".to_string(), "Test error".to_string());

        assert_eq!(report.successful_sends, 2);
        assert_eq!(report.failed_sends, 1);
        assert_eq!(report.errors.len(), 1);
        assert!(!report.is_complete());

        report.add_success();
        report.add_success();
        assert!(report.is_complete());
        assert_eq!(report.success_rate(), 0.8);
    }

    #[tokio::test]
    async fn test_rate_limiter() {
        let mut limiter = RateLimiter::new(120); // 2 per second

        let start = tokio::time::Instant::now();
        limiter.acquire().await;
        limiter.acquire().await;
        let elapsed = start.elapsed();

        // First two should be nearly instant (burst)
        assert!(elapsed < Duration::from_millis(100));
    }

    #[test]
    fn test_personalize_content() {
        let content = "Hello! Token: {{unsubscribe_token}} URL: {{unsubscribe_url}}";
        let result = personalize_content(content, "abc123", "test@example.com");
        assert!(result.contains("abc123"));
        assert!(result.contains("mailto:test@example.com"));
        assert!(!result.contains("{{unsubscribe_token}}"));
        assert!(!result.contains("{{unsubscribe_url}}"));
    }

    #[test]
    fn test_unsubscribe_token_generation_and_verification() {
        let smtp_config = SmtpConfig {
            server: "smtp.example.com".to_string(),
            port: 587,
            username: "test@example.com".to_string(),
            use_tls: Some(true),
        };

        let sender = NewsletterSender::new(
            smtp_config,
            None,
            10,
            "test-secret-key-for-hmac".to_string(),
        )
        .unwrap();

        let email = "subscriber@example.com";
        let token = sender.generate_unsubscribe_token(email);

        // Token should be a hex string (64 chars for SHA256)
        assert_eq!(token.len(), 64);

        // Verification should pass
        assert!(sender.verify_unsubscribe_token(email, &token));

        // Wrong email should fail
        assert!(!sender.verify_unsubscribe_token("other@example.com", &token));

        // Same email should always produce the same token (deterministic)
        let token2 = sender.generate_unsubscribe_token(email);
        assert_eq!(token, token2);
    }

    #[test]
    fn test_send_config_defaults() {
        let config = SendConfig::default();
        assert_eq!(config.emails_per_minute, DEFAULT_RATE_LIMIT);
        assert_eq!(config.concurrency, DEFAULT_CONCURRENCY);
        assert_eq!(config.batch_size, DEFAULT_BATCH_SIZE);
        assert_eq!(config.max_retries, MAX_RETRIES);
    }
}
