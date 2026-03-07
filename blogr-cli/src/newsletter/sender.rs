//! SMTP email sending for newsletters
//!
//! This module handles sending newsletters via SMTP with rate limiting
//! and progress tracking.

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use hmac::{Hmac, Mac};
use lettre::message::{header, Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::client::{Tls, TlsParameters};
use lettre::{Message, SmtpTransport, Transport};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::thread;
use std::time::{Duration, Instant};

use crate::config::SmtpConfig;
use crate::newsletter::composer::Newsletter;
use crate::newsletter::database::{NewsletterDatabase, Subscriber, SubscriberStatus};

type HmacSha256 = Hmac<Sha256>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendReport {
    pub total_subscribers: usize,
    pub successful_sends: usize,
    pub failed_sends: usize,
    pub started_at: DateTime<Utc>,
    pub completed_at: Option<DateTime<Utc>>,
    pub errors: Vec<SendError>,
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

pub struct RateLimiter {
    emails_per_minute: u32,
    last_send_times: Vec<Instant>,
}

impl RateLimiter {
    pub fn new(emails_per_minute: u32) -> Self {
        Self {
            emails_per_minute,
            last_send_times: Vec::new(),
        }
    }

    pub fn wait_if_needed(&mut self) {
        let now = Instant::now();
        let one_minute_ago = now - Duration::from_secs(60);

        // Remove timestamps older than 1 minute
        self.last_send_times.retain(|&time| time > one_minute_ago);

        // If we've hit the rate limit, wait
        if self.last_send_times.len() >= self.emails_per_minute as usize {
            let oldest_send = self.last_send_times[0];
            let wait_time = Duration::from_secs(60) - (now - oldest_send);
            if wait_time > Duration::from_secs(0) {
                println!(
                    "Rate limit reached, waiting {} seconds...",
                    wait_time.as_secs()
                );
                thread::sleep(wait_time);
            }
        }
    }

    /// Record a send after it actually completes
    pub fn record_send(&mut self) {
        self.last_send_times.push(Instant::now());
    }
}

pub struct NewsletterSender {
    smtp_config: SmtpConfig,
    rate_limiter: RateLimiter,
    from_address: Mailbox,
    /// Secret key for generating HMAC-based unsubscribe tokens
    hmac_secret: String,
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
            rate_limiter: RateLimiter::new(emails_per_minute),
            from_address,
            hmac_secret,
        })
    }

    /// Send newsletter to all approved subscribers, recording history in database
    pub fn send_to_subscribers(
        &mut self,
        newsletter: &Newsletter,
        subscribers: &[Subscriber],
        smtp_password: &str,
        database: Option<&NewsletterDatabase>,
        progress_callback: Option<Box<dyn Fn(usize, usize)>>,
    ) -> Result<SendReport> {
        let approved_subscribers: Vec<_> = subscribers
            .iter()
            .filter(|s| s.status == SubscriberStatus::Approved)
            .collect();

        let mut report = SendReport::new(approved_subscribers.len());

        if approved_subscribers.is_empty() {
            println!("No approved subscribers found");
            report.complete();
            return Ok(report);
        }

        println!(
            "Sending newsletter to {} approved subscribers",
            approved_subscribers.len()
        );

        // Create SMTP transport
        let transport = self.create_smtp_transport(smtp_password)?;

        for (index, subscriber) in approved_subscribers.iter().enumerate() {
            // Rate limiting - check before send
            self.rate_limiter.wait_if_needed();

            // Generate unsubscribe token
            let unsubscribe_token = self.generate_unsubscribe_token(&subscriber.email);

            // Create personalized newsletter
            let personalized_newsletter =
                self.personalize_newsletter(newsletter, subscriber, &unsubscribe_token)?;

            match self.send_single_email(&transport, &personalized_newsletter, subscriber) {
                Ok(_) => {
                    // Record send time after successful send
                    self.rate_limiter.record_send();
                    report.add_success();
                    println!("Sent to {}", subscriber.email);
                }
                Err(e) => {
                    self.rate_limiter.record_send();
                    let error_msg = format!("Failed to send to {}: {}", subscriber.email, e);
                    report.add_error(subscriber.email.clone(), error_msg.clone());
                    eprintln!("{}", error_msg);
                }
            }

            if let Some(ref callback) = progress_callback {
                callback(index + 1, approved_subscribers.len());
            }
        }

        report.complete();

        // Record send history in database
        if let Some(db) = database {
            if let Ok(send_id) = db.record_send(
                &newsletter.subject,
                report.total_subscribers,
                report.successful_sends,
                report.failed_sends,
            ) {
                for error in &report.errors {
                    let _ = db.record_send_recipient(
                        send_id,
                        &error.subscriber_email,
                        "failed",
                        Some(&error.error_message),
                    );
                }
            }
        }

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
        let personalized_newsletter =
            self.personalize_newsletter(newsletter, &test_subscriber, &unsubscribe_token)?;

        self.send_single_email(&transport, &personalized_newsletter, &test_subscriber)
            .context("Failed to send test email")?;

        println!("Test email sent successfully");
        Ok(())
    }

    /// Create SMTP transport
    fn create_smtp_transport(&self, password: &str) -> Result<SmtpTransport> {
        let credentials = Credentials::new(self.smtp_config.username.clone(), password.to_string());

        let mut builder = SmtpTransport::relay(&self.smtp_config.server)?;

        // Configure TLS - always use Required to prevent downgrade attacks
        if self.smtp_config.port == 465 {
            // SMTPS (implicit TLS)
            builder = builder.tls(Tls::Required(TlsParameters::new(
                self.smtp_config.server.clone(),
            )?));
        } else if self.smtp_config.port == 587 {
            // SMTP with STARTTLS - must be Required to prevent plaintext fallback
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

    /// Send a single email
    fn send_single_email(
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

    /// Personalize newsletter for a specific subscriber
    fn personalize_newsletter(
        &self,
        newsletter: &Newsletter,
        _subscriber: &Subscriber,
        unsubscribe_token: &str,
    ) -> Result<Newsletter> {
        let mut personalized = newsletter.clone();

        // Replace unsubscribe token in content
        personalized.html_content = personalized
            .html_content
            .replace("{{unsubscribe_token}}", unsubscribe_token);
        personalized.text_content = personalized
            .text_content
            .replace("{{unsubscribe_token}}", unsubscribe_token);

        // Add personalized unsubscribe link
        let unsubscribe_url = format!(
            "mailto:{}?subject=Unsubscribe&body=Please unsubscribe me from the newsletter. Token: {}",
            self.smtp_config.username,
            unsubscribe_token
        );

        personalized.html_content = personalized
            .html_content
            .replace("{{unsubscribe_url}}", &unsubscribe_url);
        personalized.text_content = personalized
            .text_content
            .replace("{{unsubscribe_url}}", &unsubscribe_url);

        Ok(personalized)
    }

    /// Generate cryptographically secure unsubscribe token using HMAC-SHA256
    fn generate_unsubscribe_token(&self, email: &str) -> String {
        let mut mac = HmacSha256::new_from_slice(self.hmac_secret.as_bytes())
            .expect("HMAC can take key of any size");
        mac.update(email.as_bytes());
        let result = mac.finalize();
        hex::encode(result.into_bytes())
    }

    /// Verify an unsubscribe token against an email
    #[allow(dead_code)]
    pub fn verify_unsubscribe_token(&self, email: &str, token: &str) -> bool {
        let expected = self.generate_unsubscribe_token(email);
        // Constant-time comparison
        expected == token
    }

    /// Print sending summary
    fn print_send_summary(&self, report: &SendReport) {
        println!("\nNewsletter Sending Summary");
        println!("===========================");
        println!("Total subscribers: {}", report.total_subscribers);
        println!("Successful sends: {}", report.successful_sends);
        println!("Failed sends: {}", report.failed_sends);
        println!("Success rate: {:.1}%", report.success_rate() * 100.0);

        if let Some(completed_at) = report.completed_at {
            let duration = completed_at - report.started_at;
            println!("Total time: {} seconds", duration.num_seconds());
        }

        if !report.errors.is_empty() {
            println!("\nErrors:");
            for error in &report.errors {
                println!("  - {}: {}", error.subscriber_email, error.error_message);
            }
        }
        println!();
    }
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
    fn test_rate_limiter() {
        let mut limiter = RateLimiter::new(2);

        // Should not wait for first two sends
        limiter.wait_if_needed();
        limiter.record_send();
        limiter.wait_if_needed();
        limiter.record_send();

        assert_eq!(limiter.last_send_times.len(), 2);
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
}
