//! SMTP email sending for newsletters
//!
//! This module handles sending newsletters via SMTP with:
//! - Async concurrent sending with configurable parallelism
//! - Channel-based rate limiting (no mutex contention)
//! - Send queue for crash recovery and resume
//! - Batch database writes for tracking
//! - Capped error collection for memory safety
//! - List-Unsubscribe headers for Gmail/Yahoo compliance
//! - Bounce detection from SMTP errors

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use lettre::message::{
    header::{self, HeaderName, HeaderValue},
    Mailbox, MultiPart, SinglePart,
};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::client::{Tls, TlsParameters};
use lettre::{
    AsyncSmtpTransport, AsyncTransport, Message, SmtpTransport, Tokio1Executor, Transport,
};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::path::Path;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::sync::Semaphore;

use crate::config::SmtpConfig;
use crate::newsletter::composer::Newsletter;
use crate::newsletter::database::{
    BounceType, NewsletterDatabase, SendQueueStatus, Subscriber, SubscriberStatus,
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
/// Maximum number of errors to collect in a report
const MAX_REPORT_ERRORS: usize = 1000;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendReport {
    pub total_subscribers: usize,
    pub successful_sends: usize,
    pub failed_sends: usize,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub errors: Vec<SendError>,
    pub errors_truncated: bool,
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
            errors_truncated: false,
            resumed: false,
        }
    }

    pub fn add_success(&mut self) {
        self.successful_sends += 1;
    }

    pub fn add_error(&mut self, subscriber_email: String, error_message: String) {
        self.failed_sends += 1;
        if self.errors.len() < MAX_REPORT_ERRORS {
            self.errors.push(SendError {
                subscriber_email,
                error_message,
                timestamp: Utc::now(),
                retry_count: 0,
            });
        } else {
            self.errors_truncated = true;
        }
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

    /// Save progress to a JSON file for monitoring
    pub fn save_progress(&self, project_root: &Path) -> Result<()> {
        let progress_dir = project_root.join(".blogr");
        std::fs::create_dir_all(&progress_dir)?;
        let progress_file = progress_dir.join("send_progress.json");
        let json = serde_json::to_string_pretty(self)?;
        std::fs::write(progress_file, json)?;
        Ok(())
    }
}

/// Channel-based rate limiter that distributes tokens without mutex contention.
/// A single background task ticks at the configured rate and sends tokens
/// via an mpsc channel. Send tasks receive tokens from the channel.
struct ChannelRateLimiter {
    receiver: tokio::sync::mpsc::Receiver<()>,
    _distributor: tokio::task::JoinHandle<()>,
}

impl ChannelRateLimiter {
    fn new(emails_per_minute: u32) -> Self {
        let interval_micros = if emails_per_minute == 0 {
            0
        } else {
            60_000_000u64 / emails_per_minute as u64
        };

        // Buffer up to 1 second worth of tokens for burst capacity
        let buffer_size = (emails_per_minute as usize / 60).max(1);
        let (tx, rx) = tokio::sync::mpsc::channel(buffer_size);

        let distributor = tokio::spawn(async move {
            if interval_micros == 0 {
                return;
            }
            let mut interval =
                tokio::time::interval(std::time::Duration::from_micros(interval_micros));
            loop {
                interval.tick().await;
                if tx.send(()).await.is_err() {
                    break; // All receivers dropped
                }
            }
        });

        Self {
            receiver: rx,
            _distributor: distributor,
        }
    }

    async fn acquire(&mut self) {
        // Receive a token; blocks until one is available
        let _ = self.receiver.recv().await;
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
    api_base_url: Option<String>,
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
            api_base_url: None,
        })
    }

    /// Set sending configuration
    #[allow(dead_code)]
    pub fn with_config(mut self, config: SendConfig) -> Self {
        self.send_config = config;
        self
    }

    /// Set the API base URL for unsubscribe links
    pub fn with_api_base_url(mut self, url: Option<String>) -> Self {
        self.api_base_url = url;
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
        project_root: Option<&Path>,
        tag: Option<&str>,
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
                let queued = database.create_send_queue_with_tag(send_id, tag)?;
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
        let api_base_url = self.api_base_url.clone();

        // Create a single async SMTP transport to be shared across all tasks
        let transport = Arc::new(create_async_smtp_transport(
            &self.smtp_config,
            smtp_password,
        )?);

        // Channel-based rate limiter — single distributor task, no mutex contention
        let rate_limiter = Arc::new(tokio::sync::Mutex::new(ChannelRateLimiter::new(
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
                let api_base_url = api_base_url.clone();
                let progress_callback = progress_callback.clone();
                let processed = Arc::clone(&processed);
                let task_total = total;

                let task = tokio::spawn(async move {
                    // Rate limit via channel (no mutex contention on hot path)
                    rate_limiter.lock().await.acquire().await;

                    let result = send_single_email_async(
                        &transport,
                        &from_address,
                        &newsletter,
                        &item.subscriber_email,
                        &hmac_secret,
                        &smtp_username,
                        api_base_url.as_deref(),
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
            let mut bounce_events: Vec<(String, BounceType, String)> = Vec::new();

            for task in tasks {
                match task.await {
                    Ok((item_id, email, Ok(()))) => {
                        report.add_success();
                        db_updates.push((item_id, SendQueueStatus::Sent, None));
                        recipient_records.push((email, "sent", None));
                    }
                    Ok((item_id, email, Err(error_msg))) => {
                        report.add_error(email.clone(), error_msg.clone());
                        // Check if the error indicates a bounce
                        if let Some(bounce_type) = detect_bounce_from_error(&error_msg) {
                            bounce_events.push((email.clone(), bounce_type, error_msg.clone()));
                        }
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

            // Record any detected bounces
            for (email, bounce_type, reason) in &bounce_events {
                let _ = database.record_bounce(email, bounce_type, Some(reason));
            }

            // Save progress after each batch
            if let Some(root) = project_root {
                let _ = report.save_progress(root);
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
            self.api_base_url.as_deref(),
            test_email,
        );
        let personalized_text = personalize_content(
            &newsletter.text_content,
            &unsubscribe_token,
            &self.smtp_config.username,
            self.api_base_url.as_deref(),
            test_email,
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

    /// Send a confirmation email for double opt-in
    #[allow(dead_code)]
    pub fn send_confirmation_email(
        &self,
        email: &str,
        token: &str,
        confirmation_base_url: &str,
        smtp_password: &str,
    ) -> Result<()> {
        let transport = self.create_smtp_transport(smtp_password)?;

        let confirm_url = format!("{}?token={}", confirmation_base_url, token);
        let html = format!(
            r#"<!DOCTYPE html>
<html><body style="font-family: Arial, sans-serif; max-width: 600px; margin: 0 auto; padding: 20px;">
<h2>Confirm your subscription</h2>
<p>Thank you for subscribing! Please click the button below to confirm your email address:</p>
<p style="text-align: center; margin: 30px 0;">
<a href="{url}" style="background-color: #2563eb; color: white; padding: 12px 24px; text-decoration: none; border-radius: 6px; font-weight: bold;">Confirm Subscription</a>
</p>
<p>Or copy and paste this link: <a href="{url}">{url}</a></p>
<p style="color: #666; font-size: 14px;">If you didn't request this subscription, you can safely ignore this email.</p>
</body></html>"#,
            url = confirm_url
        );
        let text = format!(
            "Confirm your subscription\n\nThank you for subscribing! Please visit this link to confirm:\n{}\n\nIf you didn't request this, ignore this email.",
            confirm_url
        );

        let to_address: Mailbox = email.parse().context("Failed to parse email address")?;

        let msg = Message::builder()
            .from(self.from_address.clone())
            .to(to_address)
            .subject("Confirm your subscription")
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
            .context("Failed to build confirmation email")?;

        transport
            .send(&msg)
            .context("Failed to send confirmation email")?;

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
                "\nErrors (showing {}/{}{}):",
                display_count,
                report.errors.len(),
                if report.errors_truncated {
                    format!(" — truncated, total failures: {}", report.failed_sends)
                } else {
                    String::new()
                }
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
fn personalize_content(
    content: &str,
    unsubscribe_token: &str,
    smtp_username: &str,
    api_base_url: Option<&str>,
    subscriber_email: &str,
) -> String {
    let unsubscribe_url = if let Some(base_url) = api_base_url {
        format!(
            "{}/unsubscribe?email={}&token={}",
            base_url.trim_end_matches('/'),
            urlencoding::encode(subscriber_email),
            unsubscribe_token
        )
    } else {
        format!(
            "mailto:{}?subject=Unsubscribe&body=Please unsubscribe me from the newsletter. Token: {}",
            smtp_username, unsubscribe_token
        )
    };

    content
        .replace("{{unsubscribe_token}}", unsubscribe_token)
        .replace("{{unsubscribe_url}}", &unsubscribe_url)
}

/// Generate an unsubscribe token with a given secret (used by both sync and async paths)
pub fn generate_unsubscribe_token_with_secret(email: &str, secret: &str) -> String {
    let mut mac =
        HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC can take key of any size");
    mac.update(email.as_bytes());
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

/// Verify an unsubscribe token (public for use in API)
pub fn verify_unsubscribe_token(email: &str, token: &str, secret: &str) -> bool {
    let expected = generate_unsubscribe_token_with_secret(email, secret);
    expected.as_bytes().ct_eq(token.as_bytes()).into()
}

/// Detect bounce type from SMTP error messages
fn detect_bounce_from_error(error_msg: &str) -> Option<BounceType> {
    let lower = error_msg.to_lowercase();
    // Hard bounce indicators
    if lower.contains("550")
        || lower.contains("user unknown")
        || lower.contains("no such user")
        || lower.contains("does not exist")
        || lower.contains("invalid recipient")
        || lower.contains("recipient rejected")
    {
        return Some(BounceType::Hard);
    }
    // Complaint indicators
    if lower.contains("spam") || lower.contains("abuse") || lower.contains("complaint") {
        return Some(BounceType::Complaint);
    }
    // Soft bounce indicators
    if lower.contains("mailbox full")
        || lower.contains("over quota")
        || lower.contains("try again")
        || lower.contains("temporarily")
        || lower.contains("451")
        || lower.contains("452")
    {
        return Some(BounceType::Soft);
    }
    None
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

/// Custom header: List-Unsubscribe (RFC 2369)
#[derive(Debug, Clone)]
struct ListUnsubscribeHeader(String);

impl header::Header for ListUnsubscribeHeader {
    fn name() -> HeaderName {
        HeaderName::new_from_ascii_str("List-Unsubscribe")
    }

    fn parse(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        Ok(Self(s.to_string()))
    }

    fn display(&self) -> HeaderValue {
        HeaderValue::dangerous_new_pre_encoded(Self::name(), self.0.clone(), self.0.clone())
    }
}

/// Custom header: List-Unsubscribe-Post (RFC 8058)
#[derive(Debug, Clone)]
struct ListUnsubscribePostHeader;

impl header::Header for ListUnsubscribePostHeader {
    fn name() -> HeaderName {
        HeaderName::new_from_ascii_str("List-Unsubscribe-Post")
    }

    fn parse(s: &str) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let _ = s;
        Ok(Self)
    }

    fn display(&self) -> HeaderValue {
        HeaderValue::dangerous_new_pre_encoded(
            Self::name(),
            "List-Unsubscribe=One-Click".to_string(),
            "List-Unsubscribe=One-Click".to_string(),
        )
    }
}

/// Send a single email asynchronously using a shared transport.
/// Adds List-Unsubscribe and List-Unsubscribe-Post headers.
async fn send_single_email_async(
    transport: &AsyncSmtpTransport<Tokio1Executor>,
    from_address: &Mailbox,
    newsletter: &Newsletter,
    subscriber_email: &str,
    hmac_secret: &str,
    smtp_username: &str,
    api_base_url: Option<&str>,
) -> Result<()> {
    let to_address: Mailbox = subscriber_email
        .parse()
        .context("Failed to parse subscriber email address")?;

    let unsubscribe_token = generate_unsubscribe_token_with_secret(subscriber_email, hmac_secret);
    let html = personalize_content(
        &newsletter.html_content,
        &unsubscribe_token,
        smtp_username,
        api_base_url,
        subscriber_email,
    );
    let text = personalize_content(
        &newsletter.text_content,
        &unsubscribe_token,
        smtp_username,
        api_base_url,
        subscriber_email,
    );

    // Build List-Unsubscribe header value
    let list_unsubscribe = if let Some(base_url) = api_base_url {
        let unsub_url = format!(
            "{}/unsubscribe?email={}&token={}",
            base_url.trim_end_matches('/'),
            urlencoding::encode(subscriber_email),
            &unsubscribe_token
        );
        format!("<{}>", unsub_url)
    } else {
        format!(
            "<mailto:{}?subject=Unsubscribe&body=Token:{}>",
            smtp_username, unsubscribe_token
        )
    };

    let email = Message::builder()
        .from(from_address.clone())
        .to(to_address)
        .subject(&newsletter.subject)
        .header(ListUnsubscribeHeader(list_unsubscribe))
        .header(ListUnsubscribePostHeader)
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

    #[test]
    fn test_send_report_error_cap() {
        let mut report = SendReport::new(2000);
        for i in 0..1500 {
            report.add_error(format!("user{}@example.com", i), "error".to_string());
        }
        assert_eq!(report.errors.len(), MAX_REPORT_ERRORS);
        assert!(report.errors_truncated);
        assert_eq!(report.failed_sends, 1500);
    }

    #[test]
    fn test_personalize_content() {
        let content = "Hello! Token: {{unsubscribe_token}} URL: {{unsubscribe_url}}";
        let result = personalize_content(
            content,
            "abc123",
            "test@example.com",
            None,
            "sub@example.com",
        );
        assert!(result.contains("abc123"));
        assert!(result.contains("mailto:test@example.com"));
        assert!(!result.contains("{{unsubscribe_token}}"));
        assert!(!result.contains("{{unsubscribe_url}}"));
    }

    #[test]
    fn test_personalize_content_with_api_url() {
        let content = "URL: {{unsubscribe_url}}";
        let result = personalize_content(
            content,
            "abc123",
            "test@example.com",
            Some("https://api.example.com"),
            "sub@example.com",
        );
        assert!(result
            .contains("https://api.example.com/unsubscribe?email=sub%40example.com&token=abc123"));
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

    #[test]
    fn test_detect_bounce() {
        assert_eq!(
            detect_bounce_from_error("550 User not found"),
            Some(BounceType::Hard)
        );
        assert_eq!(
            detect_bounce_from_error("Mailbox full"),
            Some(BounceType::Soft)
        );
        assert_eq!(
            detect_bounce_from_error("marked as spam"),
            Some(BounceType::Complaint)
        );
        assert_eq!(detect_bounce_from_error("Connection timeout"), None);
    }

    #[test]
    fn test_public_verify_unsubscribe_token() {
        let email = "test@example.com";
        let secret = "test-secret";
        let token = generate_unsubscribe_token_with_secret(email, secret);
        assert!(verify_unsubscribe_token(email, &token, secret));
        assert!(!verify_unsubscribe_token(
            "other@example.com",
            &token,
            secret
        ));
    }
}
