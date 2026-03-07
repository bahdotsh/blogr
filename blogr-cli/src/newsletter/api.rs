//! REST API server for newsletter operations
//!
//! This module provides an optional HTTP REST API that allows external tools
//! and services to interact with the newsletter system programmatically.

use anyhow::{Context, Result};
use axum::{
    extract::{Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{Json, Response},
    routing::{delete, get, post, put},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::TcpListener;
use tower_http::cors::{Any, CorsLayer};

use super::{NewsletterManager, Subscriber, SubscriberStatus};
use crate::config::Config;

/// API server configuration
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct ApiConfig {
    pub host: String,
    pub port: u16,
    pub api_key: Option<String>,
    pub cors_enabled: bool,
    pub rate_limit: Option<u32>,
}

impl Default for ApiConfig {
    fn default() -> Self {
        Self {
            host: "127.0.0.1".to_string(),
            port: 3001,
            api_key: None,
            cors_enabled: true,
            rate_limit: Some(100),
        }
    }
}

/// API server application state
#[derive(Clone)]
#[allow(dead_code)]
pub struct ApiState {
    pub newsletter_manager: Arc<NewsletterManager>,
    pub config: Arc<Config>,
    pub api_config: Arc<ApiConfig>,
}

/// API response wrapper
#[derive(Debug, Serialize)]
pub struct ApiResponse<T> {
    pub success: bool,
    pub data: Option<T>,
    pub error: Option<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

impl<T> ApiResponse<T> {
    pub fn success(data: T) -> Self {
        Self {
            success: true,
            data: Some(data),
            error: None,
            timestamp: chrono::Utc::now(),
        }
    }
}

impl ApiResponse<()> {
    pub fn error(message: String) -> Self {
        ApiResponse {
            success: false,
            data: None,
            error: Some(message),
            timestamp: chrono::Utc::now(),
        }
    }
}

/// Subscriber list query parameters
#[derive(Deserialize)]
pub struct SubscriberQuery {
    pub status: Option<String>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
}

/// Subscriber creation request
#[derive(Deserialize)]
pub struct CreateSubscriberRequest {
    pub email: String,
    pub status: Option<SubscriberStatus>,
    pub notes: Option<String>,
}

/// Subscriber update request
#[derive(Deserialize)]
pub struct UpdateSubscriberRequest {
    pub status: Option<SubscriberStatus>,
    pub notes: Option<String>,
}

/// Statistics response
#[derive(Serialize)]
pub struct StatsResponse {
    pub total_subscribers: i64,
    pub approved_subscribers: i64,
    pub pending_subscribers: i64,
    pub declined_subscribers: i64,
}

/// Newsletter API server
pub struct NewsletterApiServer {
    state: ApiState,
}

impl NewsletterApiServer {
    /// Create a new API server
    pub fn new(
        newsletter_manager: NewsletterManager,
        config: Config,
        api_config: ApiConfig,
    ) -> Self {
        let state = ApiState {
            newsletter_manager: Arc::new(newsletter_manager),
            config: Arc::new(config),
            api_config: Arc::new(api_config),
        };

        Self { state }
    }

    /// Start the API server
    pub async fn start(self) -> Result<()> {
        let addr = format!(
            "{}:{}",
            self.state.api_config.host, self.state.api_config.port
        );
        let app = self.create_router();

        println!("Starting Newsletter API server on {}", addr);

        let listener = TcpListener::bind(&addr)
            .await
            .with_context(|| format!("Failed to bind to address {}", addr))?;

        axum::serve(listener, app)
            .await
            .context("API server error")?;

        Ok(())
    }

    /// Create the router with all endpoints
    fn create_router(self) -> Router {
        let api_key = self.state.api_config.api_key.clone();
        let cors_enabled = self.state.api_config.cors_enabled;

        let mut app = Router::new()
            // Health check (no auth required)
            .route("/health", get(health_check))
            // Subscriber management
            .route("/subscribers", get(list_subscribers))
            .route("/subscribers", post(create_subscriber))
            .route("/subscribers/{email}", get(get_subscriber))
            .route("/subscribers/{email}", put(update_subscriber))
            .route("/subscribers/{email}", delete(delete_subscriber))
            // Import/export
            .route("/export", get(export_subscribers))
            // Statistics
            .route("/stats", get(get_stats))
            .with_state(self.state);

        // Add API key authentication middleware if configured
        if let Some(key) = api_key {
            let key = Arc::new(key);
            app = app.layer(middleware::from_fn(move |req, next| {
                let key = Arc::clone(&key);
                auth_middleware(req, next, key)
            }));
        }

        // Add CORS if enabled
        if cors_enabled {
            app = app.layer(
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods(Any)
                    .allow_headers(Any),
            );
        }

        app
    }
}

/// Authentication middleware that checks for a valid API key
async fn auth_middleware(req: Request, next: Next, api_key: Arc<String>) -> Response {
    // Allow health check without auth
    if req.uri().path() == "/health" {
        return next.run(req).await;
    }

    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    match auth_header {
        Some(header)
            if header
                .strip_prefix("Bearer ")
                .is_some_and(|token| token == api_key.as_str()) =>
        {
            next.run(req).await
        }
        _ => {
            let body = serde_json::to_string(&ApiResponse::<()>::error(
                "Unauthorized: invalid or missing API key".to_string(),
            ))
            .unwrap_or_default();

            Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap()
        }
    }
}

/// Basic email format validation
fn is_valid_email(email: &str) -> bool {
    email.contains('@')
        && email.contains('.')
        && !email.starts_with('@')
        && !email.ends_with('@')
        && email.len() > 5
        && email.len() < 255
        && !email.contains(' ')
}

/// Health check endpoint
async fn health_check() -> Json<ApiResponse<HashMap<String, String>>> {
    let mut data = HashMap::new();
    data.insert("status".to_string(), "healthy".to_string());
    data.insert("service".to_string(), "blogr-newsletter-api".to_string());
    data.insert("version".to_string(), env!("CARGO_PKG_VERSION").to_string());

    Json(ApiResponse::success(data))
}

/// List subscribers endpoint
async fn list_subscribers(
    State(state): State<ApiState>,
    Query(params): Query<SubscriberQuery>,
) -> Result<Json<ApiResponse<Vec<Subscriber>>>, (StatusCode, Json<ApiResponse<()>>)> {
    let status_filter = params
        .status
        .as_deref()
        .and_then(|s| match s.to_lowercase().as_str() {
            "pending" => Some(SubscriberStatus::Pending),
            "approved" => Some(SubscriberStatus::Approved),
            "declined" => Some(SubscriberStatus::Declined),
            _ => None,
        });

    match state
        .newsletter_manager
        .database()
        .get_subscribers_paginated(status_filter, params.limit, params.offset)
    {
        Ok(subscribers) => Ok(Json(ApiResponse::success(subscribers))),
        Err(e) => {
            eprintln!("Failed to list subscribers: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error(format!(
                    "Failed to list subscribers: {}",
                    e
                ))),
            ))
        }
    }
}

/// Create subscriber endpoint
async fn create_subscriber(
    State(state): State<ApiState>,
    Json(request): Json<CreateSubscriberRequest>,
) -> Result<Json<ApiResponse<Subscriber>>, (StatusCode, Json<ApiResponse<()>>)> {
    if !is_valid_email(&request.email) {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::error(format!(
                "Invalid email format: {}",
                request.email
            ))),
        ));
    }

    let subscriber = Subscriber {
        id: None,
        email: request.email,
        status: request.status.unwrap_or(SubscriberStatus::Pending),
        subscribed_at: chrono::Utc::now(),
        approved_at: None,
        declined_at: None,
        source_email_id: Some("api".to_string()),
        notes: request.notes,
    };

    match state
        .newsletter_manager
        .database()
        .add_subscriber(&subscriber)
    {
        Ok(_) => {
            match state
                .newsletter_manager
                .database()
                .get_subscriber_by_email(&subscriber.email)
            {
                Ok(Some(created_subscriber)) => Ok(Json(ApiResponse::success(created_subscriber))),
                Ok(None) => Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse::error(
                        "Failed to retrieve created subscriber".to_string(),
                    )),
                )),
                Err(e) => {
                    eprintln!("Failed to fetch created subscriber: {}", e);
                    Err((
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ApiResponse::error(format!("Database error: {}", e))),
                    ))
                }
            }
        }
        Err(e) => {
            eprintln!("Failed to create subscriber: {}", e);
            if e.to_string().contains("UNIQUE constraint failed") {
                Err((
                    StatusCode::CONFLICT,
                    Json(ApiResponse::error("Subscriber already exists".to_string())),
                ))
            } else {
                Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse::error(format!(
                        "Failed to create subscriber: {}",
                        e
                    ))),
                ))
            }
        }
    }
}

/// Get subscriber endpoint
async fn get_subscriber(
    State(state): State<ApiState>,
    Path(email): Path<String>,
) -> Result<Json<ApiResponse<Subscriber>>, (StatusCode, Json<ApiResponse<()>>)> {
    match state
        .newsletter_manager
        .database()
        .get_subscriber_by_email(&email)
    {
        Ok(Some(subscriber)) => Ok(Json(ApiResponse::success(subscriber))),
        Ok(None) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiResponse::error(format!(
                "Subscriber '{}' not found",
                email
            ))),
        )),
        Err(e) => {
            eprintln!("Failed to get subscriber: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error(format!("Database error: {}", e))),
            ))
        }
    }
}

/// Update subscriber endpoint
async fn update_subscriber(
    State(state): State<ApiState>,
    Path(email): Path<String>,
    Json(request): Json<UpdateSubscriberRequest>,
) -> Result<Json<ApiResponse<Subscriber>>, (StatusCode, Json<ApiResponse<()>>)> {
    let mut subscriber = match state
        .newsletter_manager
        .database()
        .get_subscriber_by_email(&email)
    {
        Ok(Some(subscriber)) => subscriber,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiResponse::error(format!(
                    "Subscriber '{}' not found",
                    email
                ))),
            ))
        }
        Err(e) => {
            eprintln!("Failed to get subscriber: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error(format!("Database error: {}", e))),
            ));
        }
    };

    if let Some(status) = request.status {
        match status {
            SubscriberStatus::Approved => {
                subscriber.status = SubscriberStatus::Approved;
                if subscriber.approved_at.is_none() {
                    subscriber.approved_at = Some(chrono::Utc::now());
                }
            }
            SubscriberStatus::Declined => {
                subscriber.status = SubscriberStatus::Declined;
                if subscriber.declined_at.is_none() {
                    subscriber.declined_at = Some(chrono::Utc::now());
                }
            }
            SubscriberStatus::Pending => {
                subscriber.status = SubscriberStatus::Pending;
                subscriber.approved_at = None;
                subscriber.declined_at = None;
            }
        }
    }

    if let Some(notes) = request.notes {
        subscriber.notes = Some(notes);
    }

    match state
        .newsletter_manager
        .database()
        .update_subscriber(&subscriber)
    {
        Ok(_) => Ok(Json(ApiResponse::success(subscriber))),
        Err(e) => {
            eprintln!("Failed to update subscriber: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error(format!(
                    "Failed to update subscriber: {}",
                    e
                ))),
            ))
        }
    }
}

/// Delete subscriber endpoint
async fn delete_subscriber(
    State(state): State<ApiState>,
    Path(email): Path<String>,
) -> Result<Json<ApiResponse<()>>, (StatusCode, Json<ApiResponse<()>>)> {
    match state
        .newsletter_manager
        .database()
        .remove_subscriber(&email)
    {
        Ok(true) => Ok(Json(ApiResponse::success(()))),
        Ok(false) => Err((
            StatusCode::NOT_FOUND,
            Json(ApiResponse::error(format!(
                "Subscriber '{}' not found",
                email
            ))),
        )),
        Err(e) => {
            eprintln!("Failed to delete subscriber: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error(format!(
                    "Failed to delete subscriber: {}",
                    e
                ))),
            ))
        }
    }
}

/// Export subscribers endpoint
async fn export_subscribers(
    State(state): State<ApiState>,
    Query(params): Query<SubscriberQuery>,
) -> Result<Json<ApiResponse<Vec<Subscriber>>>, (StatusCode, Json<ApiResponse<()>>)> {
    list_subscribers(State(state), Query(params)).await
}

/// Get statistics endpoint (uses COUNT queries, not loading all subscribers)
async fn get_stats(
    State(state): State<ApiState>,
) -> Result<Json<ApiResponse<StatsResponse>>, (StatusCode, Json<ApiResponse<()>>)> {
    let db = state.newsletter_manager.database();

    let total = db.get_subscriber_count(None).unwrap_or(0);
    let approved = db
        .get_subscriber_count(Some(SubscriberStatus::Approved))
        .unwrap_or(0);
    let pending = db
        .get_subscriber_count(Some(SubscriberStatus::Pending))
        .unwrap_or(0);
    let declined = db
        .get_subscriber_count(Some(SubscriberStatus::Declined))
        .unwrap_or(0);

    let stats = StatsResponse {
        total_subscribers: total,
        approved_subscribers: approved,
        pending_subscribers: pending,
        declined_subscribers: declined,
    };

    Ok(Json(ApiResponse::success(stats)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    async fn create_test_state() -> (ApiState, tempfile::TempDir) {
        let temp_dir = tempdir().unwrap();

        let config = Config::default();
        let newsletter_manager = NewsletterManager::new(config.clone(), temp_dir.path()).unwrap();

        let state = ApiState {
            newsletter_manager: Arc::new(newsletter_manager),
            config: Arc::new(config),
            api_config: Arc::new(ApiConfig::default()),
        };
        (state, temp_dir)
    }

    #[tokio::test]
    async fn test_health_check() {
        let response = health_check().await;
        assert!(response.0.success);
        assert!(response.0.data.is_some());
    }

    #[test]
    fn test_email_validation() {
        assert!(is_valid_email("test@example.com"));
        assert!(is_valid_email("user.name@domain.co.uk"));
        assert!(!is_valid_email("invalid"));
        assert!(!is_valid_email("@example.com"));
        assert!(!is_valid_email("user@"));
        assert!(!is_valid_email("has space@example.com"));
        assert!(!is_valid_email(""));
        assert!(!is_valid_email("ab@c"));
    }

    #[tokio::test]
    async fn test_create_and_get_subscriber() {
        let (state, _dir) = create_test_state().await;

        let request = CreateSubscriberRequest {
            email: "test@example.com".to_string(),
            status: Some(SubscriberStatus::Pending),
            notes: Some("Test subscriber".to_string()),
        };

        let result = create_subscriber(State(state.clone()), Json(request)).await;
        assert!(result.is_ok());

        let get_result = get_subscriber(State(state), Path("test@example.com".to_string())).await;
        assert!(get_result.is_ok());
    }

    #[tokio::test]
    async fn test_create_subscriber_invalid_email() {
        let (state, _dir) = create_test_state().await;

        let request = CreateSubscriberRequest {
            email: "not-an-email".to_string(),
            status: None,
            notes: None,
        };

        let result = create_subscriber(State(state), Json(request)).await;
        assert!(result.is_err());
        let (status, _) = result.unwrap_err();
        assert_eq!(status, StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_update_subscriber() {
        let (state, _dir) = create_test_state().await;

        // Create first
        let create_req = CreateSubscriberRequest {
            email: "update@example.com".to_string(),
            status: None,
            notes: None,
        };
        let _ = create_subscriber(State(state.clone()), Json(create_req))
            .await
            .unwrap();

        // Update
        let update_req = UpdateSubscriberRequest {
            status: Some(SubscriberStatus::Approved),
            notes: Some("approved via api".to_string()),
        };
        let result = update_subscriber(
            State(state.clone()),
            Path("update@example.com".to_string()),
            Json(update_req),
        )
        .await;
        assert!(result.is_ok());

        let sub = result.unwrap().0.data.unwrap();
        assert_eq!(sub.status, SubscriberStatus::Approved);
        assert!(sub.approved_at.is_some());
    }

    #[tokio::test]
    async fn test_stats_endpoint() {
        let (state, _dir) = create_test_state().await;

        let result = get_stats(State(state)).await;
        assert!(result.is_ok());
        let stats = result.unwrap().0.data.unwrap();
        assert_eq!(stats.total_subscribers, 0);
    }
}
