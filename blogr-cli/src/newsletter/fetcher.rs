//! Email fetching and parsing functionality

use anyhow::{Context, Result};
use imap::Session;
use mailparse::{parse_mail, MailHeaderMap};
use native_tls::{TlsConnector, TlsStream};
use std::collections::HashSet;
use std::net::TcpStream;

use super::database::{NewsletterDatabase, Subscriber};
use crate::config::ImapConfig;

pub struct EmailFetcher {
    session: Option<Session<TlsStream<TcpStream>>>,
}

#[derive(Debug, Clone)]
pub struct FetchedEmail {
    pub id: u32,
    pub from: String,
    pub subject: String,
    pub body: String,
    #[allow(dead_code)]
    pub date: Option<String>,
}

impl EmailFetcher {
    pub fn new() -> Self {
        Self { session: None }
    }

    /// Connect to IMAP server with authentication
    pub fn connect(&mut self, config: &ImapConfig, password: &str) -> Result<()> {
        let tls = TlsConnector::builder()
            .build()
            .context("Failed to create TLS connector")?;

        let client = imap::connect((config.server.as_str(), config.port), &config.server, &tls)
            .context("Failed to connect to IMAP server")?;

        let session = client
            .login(&config.username, password)
            .map_err(|e| anyhow::anyhow!("IMAP login failed: {:?}", e.0))?;

        self.session = Some(session);
        println!(
            "Connected to IMAP server: {}:{}",
            config.server, config.port
        );

        Ok(())
    }

    /// Fetch unseen emails from the inbox
    pub fn fetch_subscription_emails(&mut self) -> Result<Vec<FetchedEmail>> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Not connected to IMAP server"))?;

        // Select INBOX
        session.select("INBOX").context("Failed to select INBOX")?;

        // Search for unseen emails
        let message_ids = session
            .search("UNSEEN")
            .context("Failed to search for unseen emails")?;

        if message_ids.is_empty() {
            println!("No new emails found.");
            return Ok(Vec::new());
        }

        println!("Found {} new emails to process", message_ids.len());

        let mut fetched_emails = Vec::new();

        for &msg_id in &message_ids {
            match Self::fetch_single_email(session, msg_id) {
                Ok(email) => {
                    println!("Processed email from: {}", email.from);
                    fetched_emails.push(email);
                }
                Err(e) => {
                    eprintln!("Failed to process email {}: {}", msg_id, e);
                    continue;
                }
            }
        }

        Ok(fetched_emails)
    }

    /// Fetch a single email by ID
    fn fetch_single_email(
        session: &mut Session<TlsStream<TcpStream>>,
        msg_id: u32,
    ) -> Result<FetchedEmail> {
        let messages = session
            .fetch(msg_id.to_string(), "RFC822")
            .with_context(|| format!("Failed to fetch email {}", msg_id))?;

        let message = messages
            .iter()
            .next()
            .ok_or_else(|| anyhow::anyhow!("No message found for ID {}", msg_id))?;

        let body = message
            .body()
            .ok_or_else(|| anyhow::anyhow!("No body found for message {}", msg_id))?;

        let parsed =
            parse_mail(body).with_context(|| format!("Failed to parse email {}", msg_id))?;

        let from = parsed
            .headers
            .get_first_value("From")
            .unwrap_or_else(|| "unknown@unknown.com".to_string());

        let subject = parsed
            .headers
            .get_first_value("Subject")
            .unwrap_or_else(|| "No Subject".to_string());

        let date = parsed.headers.get_first_value("Date");

        let body_text = Self::extract_body_text(&parsed)?;

        Ok(FetchedEmail {
            id: msg_id,
            from,
            subject,
            body: body_text,
            date,
        })
    }

    /// Extract plain text from email body
    fn extract_body_text(parsed: &mailparse::ParsedMail) -> Result<String> {
        if parsed.subparts.is_empty() {
            // Simple text email
            return parsed.get_body().context("Failed to get email body");
        }

        // Multipart email - find text/plain part
        for part in &parsed.subparts {
            let content_type = part.ctype.mimetype.parse::<String>().unwrap_or_default();
            if content_type == "text/plain" {
                return part.get_body().context("Failed to get text part body");
            }
        }

        // Fallback to first part
        if let Some(first_part) = parsed.subparts.first() {
            return first_part.get_body().context("Failed to get fallback body");
        }

        Ok("No readable content found".to_string())
    }

    /// Extract email addresses from subscription emails
    pub fn extract_subscriber_emails(&self, emails: &[FetchedEmail]) -> Result<Vec<Subscriber>> {
        let mut subscribers = Vec::new();
        let mut seen_emails = HashSet::new();

        for email in emails {
            // Extract email address from "From" field
            let subscriber_email = self.extract_email_address(&email.from)?;

            // Skip duplicates
            if seen_emails.contains(&subscriber_email) {
                continue;
            }
            seen_emails.insert(subscriber_email.clone());

            // Validate email format
            if !self.is_valid_email(&subscriber_email) {
                println!("Warning: Skipping invalid email: {}", subscriber_email);
                continue;
            }

            // Check if this looks like a subscription email
            if self.is_subscription_email(email) {
                let subscriber =
                    Subscriber::new(subscriber_email.clone(), Some(email.id.to_string()));
                println!("Found subscription from: {}", subscriber.email);
                subscribers.push(subscriber);
            }
        }

        Ok(subscribers)
    }

    /// Extract email address from a "From" field
    fn extract_email_address(&self, from_field: &str) -> Result<String> {
        // Handle formats like:
        // "John Doe <john@example.com>"
        // "john@example.com"
        // "<john@example.com>"

        if let Some(start) = from_field.find('<') {
            if let Some(end) = from_field.find('>') {
                if end > start {
                    return Ok(from_field[start + 1..end].trim().to_lowercase());
                }
            }
        }

        // Simple email format
        let email = from_field.trim().to_lowercase();
        if email.contains('@') {
            Ok(email)
        } else {
            Err(anyhow::anyhow!(
                "Could not extract email from: {}",
                from_field
            ))
        }
    }

    /// Basic email validation
    fn is_valid_email(&self, email: &str) -> bool {
        email.contains('@')
            && email.contains('.')
            && !email.starts_with('@')
            && !email.ends_with('@')
            && email.len() > 5
    }

    /// Check if email looks like a subscription request (public wrapper)
    pub fn is_subscription_email_public(&self, email: &FetchedEmail) -> bool {
        self.is_subscription_email(email)
    }

    /// Check if email looks like a subscription request
    fn is_subscription_email(&self, email: &FetchedEmail) -> bool {
        let subject_lower = email.subject.to_lowercase();
        let body_lower = email.body.to_lowercase();

        // Look for subscription-related keywords in the subject primarily
        let subject_keywords = [
            "subscribe",
            "subscription",
            "newsletter",
            "sign up",
            "signup",
            "mailing list",
        ];

        let unsubscribe_keywords = [
            "unsubscribe",
            "remove me",
            "stop sending",
            "opt out",
            "opt-out",
        ];

        // Check for unsubscribe keywords first (takes priority)
        let has_unsubscribe_keywords = unsubscribe_keywords
            .iter()
            .any(|keyword| subject_lower.contains(keyword) || body_lower.contains(keyword));

        if has_unsubscribe_keywords {
            return false;
        }

        // Subject is the primary signal for subscription intent
        let subject_match = subject_keywords
            .iter()
            .any(|keyword| subject_lower.contains(keyword));

        if subject_match {
            return true;
        }

        // Body keywords alone are weaker signals - require stronger patterns
        let body_intent_patterns = [
            "i want to subscribe",
            "please subscribe me",
            "i'd like to subscribe",
            "add me to",
            "sign me up",
        ];

        body_intent_patterns
            .iter()
            .any(|pattern| body_lower.contains(pattern))
    }

    /// Mark processed emails as seen
    pub fn mark_emails_as_seen(&mut self, email_ids: &[u32]) -> Result<()> {
        let session = self
            .session
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("Not connected to IMAP server"))?;

        for &email_id in email_ids {
            session
                .store(format!("{}", email_id), "+FLAGS (\\Seen)")
                .with_context(|| format!("Failed to mark email {} as seen", email_id))?;
        }

        println!("Marked {} emails as seen", email_ids.len());
        Ok(())
    }

    /// Process new subscribers and add them to database
    pub fn process_subscribers(
        &self,
        emails: &[FetchedEmail],
        database: &NewsletterDatabase,
    ) -> Result<Vec<Subscriber>> {
        let new_subscribers = self.extract_subscriber_emails(emails)?;
        let mut added_subscribers = Vec::new();

        for subscriber in new_subscribers {
            // Check if subscriber already exists
            if database.email_exists(&subscriber.email)? {
                println!("Subscriber already exists: {}", subscriber.email);
                continue;
            }

            // Add new subscriber
            match database.add_subscriber(&subscriber) {
                Ok(_) => {
                    println!("Added new subscriber: {}", subscriber.email);
                    added_subscribers.push(subscriber);
                }
                Err(e) => {
                    eprintln!("Failed to add subscriber {}: {}", subscriber.email, e);
                }
            }
        }

        Ok(added_subscribers)
    }

    /// Disconnect from IMAP server
    pub fn disconnect(&mut self) -> Result<()> {
        if let Some(mut session) = self.session.take() {
            session
                .logout()
                .context("Failed to logout from IMAP server")?;
            println!("Disconnected from IMAP server");
        }
        Ok(())
    }
}

impl Drop for EmailFetcher {
    fn drop(&mut self) {
        let _ = self.disconnect();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_email_address_extraction() {
        let fetcher = EmailFetcher::new();

        assert_eq!(
            fetcher.extract_email_address("john@example.com").unwrap(),
            "john@example.com"
        );

        assert_eq!(
            fetcher
                .extract_email_address("John Doe <john@example.com>")
                .unwrap(),
            "john@example.com"
        );

        assert_eq!(
            fetcher.extract_email_address("<john@example.com>").unwrap(),
            "john@example.com"
        );

        assert_eq!(
            fetcher
                .extract_email_address("  JOHN@EXAMPLE.COM  ")
                .unwrap(),
            "john@example.com"
        );
    }

    #[test]
    fn test_email_validation() {
        let fetcher = EmailFetcher::new();

        assert!(fetcher.is_valid_email("john@example.com"));
        assert!(fetcher.is_valid_email("test.email+tag@domain.co.uk"));

        assert!(!fetcher.is_valid_email("invalid"));
        assert!(!fetcher.is_valid_email("@example.com"));
        assert!(!fetcher.is_valid_email("john@"));
        assert!(!fetcher.is_valid_email(""));
    }

    #[test]
    fn test_subscription_email_detection() {
        let fetcher = EmailFetcher::new();

        let subscription_email = FetchedEmail {
            id: 1,
            from: "john@example.com".to_string(),
            subject: "Newsletter Subscription".to_string(),
            body: "I would like to subscribe to your newsletter".to_string(),
            date: None,
        };

        assert!(fetcher.is_subscription_email(&subscription_email));

        let unsubscribe_email = FetchedEmail {
            id: 2,
            from: "john@example.com".to_string(),
            subject: "Unsubscribe Request".to_string(),
            body: "Please unsubscribe me from your newsletter".to_string(),
            date: None,
        };

        assert!(!fetcher.is_subscription_email(&unsubscribe_email));

        let regular_email = FetchedEmail {
            id: 3,
            from: "john@example.com".to_string(),
            subject: "Hello".to_string(),
            body: "Just saying hello".to_string(),
            date: None,
        };

        assert!(!fetcher.is_subscription_email(&regular_email));

        // An email mentioning "newsletter" casually in body shouldn't match
        let casual_mention = FetchedEmail {
            id: 4,
            from: "john@example.com".to_string(),
            subject: "Great blog".to_string(),
            body: "I love your newsletter updates, keep it up!".to_string(),
            date: None,
        };

        assert!(!fetcher.is_subscription_email(&casual_mention));

        // Explicit intent in body should match
        let explicit_intent = FetchedEmail {
            id: 5,
            from: "john@example.com".to_string(),
            subject: "Hello".to_string(),
            body: "I want to subscribe to your mailing list".to_string(),
            date: None,
        };

        assert!(fetcher.is_subscription_email(&explicit_intent));
    }
}
