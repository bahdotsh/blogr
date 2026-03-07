//! Bounce event types and SMTP error parsing for newsletter bounce handling

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use super::database::BounceType;

/// A bounce event received from webhook or SMTP error detection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BounceEvent {
    pub email: String,
    pub bounce_type: BounceType,
    pub reason: Option<String>,
    pub timestamp: DateTime<Utc>,
}

/// Webhook bounce notification payload
#[derive(Debug, Deserialize)]
pub struct BounceWebhookPayload {
    pub email: String,
    pub bounce_type: Option<String>,
    pub reason: Option<String>,
}

impl BounceWebhookPayload {
    /// Convert the webhook payload to a BounceEvent
    pub fn to_event(&self) -> BounceEvent {
        let bounce_type = self
            .bounce_type
            .as_deref()
            .and_then(|t| t.parse().ok())
            .unwrap_or(BounceType::Soft);

        BounceEvent {
            email: self.email.clone(),
            bounce_type,
            reason: self.reason.clone(),
            timestamp: Utc::now(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bounce_webhook_payload() {
        let payload = BounceWebhookPayload {
            email: "test@example.com".to_string(),
            bounce_type: Some("hard".to_string()),
            reason: Some("550 User not found".to_string()),
        };

        let event = payload.to_event();
        assert_eq!(event.email, "test@example.com");
        assert_eq!(event.bounce_type, BounceType::Hard);
        assert_eq!(event.reason, Some("550 User not found".to_string()));
    }

    #[test]
    fn test_bounce_webhook_default_type() {
        let payload = BounceWebhookPayload {
            email: "test@example.com".to_string(),
            bounce_type: None,
            reason: None,
        };

        let event = payload.to_event();
        assert_eq!(event.bounce_type, BounceType::Soft);
    }
}
