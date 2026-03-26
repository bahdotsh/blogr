//! REST API server for newsletter operations
//!
//! This module provides an optional HTTP REST API that allows external tools
//! and services to interact with the newsletter system programmatically.

use anyhow::{Context, Result};
use axum::{
    extract::{Path, Query, Request, State},
    http::StatusCode,
    middleware::{self, Next},
    response::{Html, IntoResponse, Json, Response},
    routing::{delete, get, post, put},
    Router,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tokio::net::TcpListener;
use tokio::sync::Mutex;
use tower_http::cors::{Any, CorsLayer};

use super::database::BounceRecord;
use super::is_valid_tag;
use super::webhooks::BounceWebhookPayload;
use super::{NewsletterManager, Subscriber, SubscriberStatus};
use crate::config::Config;
use crate::newsletter::sender;

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

/// Maximum concurrent background confirmation email sends.
const MAX_CONFIRMATION_TASKS: usize = 10;

/// Maximum page size for list/export endpoints to prevent OOM on large databases.
const MAX_PAGE_SIZE: usize = 1000;

/// Per-IP sliding-window rate limiter.
/// Tracks request timestamps per client IP within a 60-second window
/// and rejects requests that exceed the configured limit.
#[derive(Clone)]
struct RateLimiter {
    buckets: Arc<Mutex<HashMap<IpAddr, VecDeque<std::time::Instant>>>>,
    max_requests: u32,
}

impl RateLimiter {
    fn new(max_requests_per_minute: u32) -> Self {
        Self {
            buckets: Arc::new(Mutex::new(HashMap::new())),
            max_requests: max_requests_per_minute,
        }
    }

    async fn check(&self, ip: IpAddr) -> bool {
        let mut buckets = self.buckets.lock().await;
        let now = std::time::Instant::now();
        let window = std::time::Duration::from_secs(60);

        let timestamps = buckets.entry(ip).or_default();

        // Remove timestamps outside the window
        while timestamps
            .front()
            .is_some_and(|t| now.duration_since(*t) > window)
        {
            timestamps.pop_front();
        }

        if timestamps.len() >= self.max_requests as usize {
            return false;
        }

        timestamps.push_back(now);
        true
    }

    /// Remove entries for IPs with no recent requests to prevent unbounded growth.
    async fn cleanup_stale_entries(&self) {
        let mut buckets = self.buckets.lock().await;
        let now = std::time::Instant::now();
        let window = std::time::Duration::from_secs(60);
        buckets.retain(|_, timestamps| {
            timestamps
                .back()
                .is_some_and(|t| now.duration_since(*t) <= window)
        });
    }
}

/// Rate limiting middleware
async fn rate_limit_middleware(req: Request, next: Next, limiter: Arc<RateLimiter>) -> Response {
    let ip = req
        .extensions()
        .get::<axum::extract::ConnectInfo<SocketAddr>>()
        .map(|ci| ci.0.ip())
        .unwrap_or(IpAddr::V4(std::net::Ipv4Addr::UNSPECIFIED));

    if !limiter.check(ip).await {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(ApiResponse::<()>::error(
                "Rate limit exceeded. Try again later.".to_string(),
            )),
        )
            .into_response();
    }
    next.run(req).await
}

/// API server application state
#[derive(Clone)]
#[allow(dead_code)]
pub struct ApiState {
    pub newsletter_manager: Arc<NewsletterManager>,
    pub config: Arc<Config>,
    pub api_config: Arc<ApiConfig>,
    /// Semaphore to bound concurrent confirmation email sends.
    confirmation_semaphore: Arc<tokio::sync::Semaphore>,
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
    pub tag: Option<String>,
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

/// Tag update request
#[derive(Deserialize)]
pub struct UpdateTagsRequest {
    pub tags: Vec<String>,
}

/// Statistics response
#[derive(Serialize)]
pub struct StatsResponse {
    pub total_subscribers: i64,
    pub approved_subscribers: i64,
    pub pending_subscribers: i64,
    pub declined_subscribers: i64,
}

/// Tag with count
#[derive(Serialize)]
pub struct TagInfo {
    pub tag: String,
    pub count: i64,
}

/// Unsubscribe query parameters
#[derive(Deserialize)]
pub struct UnsubscribeQuery {
    pub email: String,
    pub token: String,
}

/// Confirm query parameters
#[derive(Deserialize)]
pub struct ConfirmQuery {
    pub token: String,
}

/// Bounce query parameters
#[derive(Deserialize)]
pub struct BounceQuery {
    pub email: String,
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
            confirmation_semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_CONFIRMATION_TASKS)),
        };

        Self { state }
    }

    /// Start the API server
    pub async fn start(self) -> Result<()> {
        let addr = format!(
            "{}:{}",
            self.state.api_config.host, self.state.api_config.port
        );

        // Spawn periodic database cleanup (every 6 hours)
        let db_manager = Arc::clone(&self.state.newsletter_manager);
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_secs(6 * 3600));
            interval.tick().await; // skip the immediate first tick
            loop {
                interval.tick().await;
                let db = db_manager.database();
                if let Err(e) = db.cleanup_expired_tokens() {
                    eprintln!("Warning: periodic cleanup of expired tokens failed: {}", e);
                }
                if let Err(e) = db.cleanup_completed_send_queue() {
                    eprintln!("Warning: periodic cleanup of send queue failed: {}", e);
                }
                if let Err(e) = db.cleanup_old_send_recipients(90) {
                    eprintln!(
                        "Warning: periodic cleanup of old send recipients failed: {}",
                        e
                    );
                }
            }
        });

        let app = self.create_router();

        println!("Starting Newsletter API server on {}", addr);

        let listener = TcpListener::bind(&addr)
            .await
            .with_context(|| format!("Failed to bind to address {}", addr))?;

        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .await
        .context("API server error")?;

        Ok(())
    }

    /// Create the router with all endpoints
    fn create_router(self) -> Router {
        let api_key = self.state.api_config.api_key.clone();
        let cors_enabled = self.state.api_config.cors_enabled;
        let rate_limit = self.state.api_config.rate_limit;

        // Build CORS layer before self.state is moved — restrict to configured
        // api_base_url origin if available, otherwise permissive for local dev.
        let cors_layer = if cors_enabled {
            let cors = if let Some(ref base_url) = self.state.config.newsletter.api_base_url {
                if let Ok(origin) = base_url.parse::<axum::http::HeaderValue>() {
                    CorsLayer::new()
                        .allow_origin(origin)
                        .allow_methods(Any)
                        .allow_headers(Any)
                } else {
                    CorsLayer::new()
                        .allow_origin(Any)
                        .allow_methods(Any)
                        .allow_headers(Any)
                }
            } else {
                CorsLayer::new()
                    .allow_origin(Any)
                    .allow_methods(Any)
                    .allow_headers(Any)
            };
            Some(cors)
        } else {
            None
        };

        // Public routes (no auth required)
        let public_routes = Router::new()
            .route("/health", get(health_check))
            .route("/unsubscribe", get(handle_unsubscribe))
            .route("/unsubscribe", post(handle_unsubscribe_post))
            .route("/confirm", get(handle_confirm));

        // Protected routes (auth required)
        let mut protected_routes = Router::new()
            .route("/subscribers", get(list_subscribers))
            .route("/subscribers", post(create_subscriber))
            .route("/subscribers/{email}", get(get_subscriber))
            .route("/subscribers/{email}", put(update_subscriber))
            .route("/subscribers/{email}", delete(delete_subscriber))
            .route("/subscribers/{email}/tags", put(update_subscriber_tags))
            .route("/subscribers/{email}/tags", get(get_subscriber_tags))
            .route("/export", get(export_subscribers))
            .route("/stats", get(get_stats))
            .route("/tags", get(list_tags))
            .route("/webhooks/bounce", post(handle_bounce_webhook))
            .route("/bounces", get(get_bounces));

        // Add API key authentication middleware if configured
        if let Some(key) = api_key {
            let key = Arc::new(key);
            protected_routes = protected_routes.layer(middleware::from_fn(move |req, next| {
                let key = Arc::clone(&key);
                auth_middleware(req, next, key)
            }));
        } else {
            eprintln!(
                "WARNING: Newsletter API starting without authentication. \
                 Set newsletter.api_key in blogr.toml or NEWSLETTER_API_KEY env var to secure endpoints."
            );
        }

        let mut app = public_routes.merge(protected_routes).with_state(self.state);

        // Apply rate limiting if configured
        if let Some(limit) = rate_limit {
            if limit > 0 {
                let limiter = Arc::new(RateLimiter::new(limit));
                // Periodically evict stale per-IP entries (every 5 minutes)
                let cleanup_limiter = Arc::clone(&limiter);
                tokio::spawn(async move {
                    let mut interval = tokio::time::interval(std::time::Duration::from_secs(300));
                    loop {
                        interval.tick().await;
                        cleanup_limiter.cleanup_stale_entries().await;
                    }
                });
                app = app.layer(middleware::from_fn(move |req, next| {
                    let limiter = Arc::clone(&limiter);
                    rate_limit_middleware(req, next, limiter)
                }));
            }
        }

        if let Some(cors) = cors_layer {
            app = app.layer(cors);
        }

        app
    }
}

/// Authentication middleware that checks for a valid API key
async fn auth_middleware(req: Request, next: Next, api_key: Arc<String>) -> Response {
    let auth_header = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok());

    match auth_header {
        Some(header)
            if header
                .strip_prefix("Bearer ")
                .is_some_and(|token| bool::from(token.as_bytes().ct_eq(api_key.as_bytes()))) =>
        {
            next.run(req).await
        }
        _ => {
            let body = serde_json::to_string(&ApiResponse::<()>::error(
                "Unauthorized: invalid or missing API key".to_string(),
            ))
            .unwrap_or_else(|_| r#"{"success":false,"error":"Unauthorized"}"#.to_string());

            Response::builder()
                .status(StatusCode::UNAUTHORIZED)
                .header("content-type", "application/json")
                .body(axum::body::Body::from(body))
                .unwrap_or_else(|_| Response::new(axum::body::Body::from("Unauthorized")))
        }
    }
}

use super::is_valid_email;

/// Health check endpoint
async fn health_check() -> Json<ApiResponse<HashMap<String, String>>> {
    let mut data = HashMap::new();
    data.insert("status".to_string(), "healthy".to_string());
    data.insert("service".to_string(), "blogr-newsletter-api".to_string());
    data.insert("version".to_string(), env!("CARGO_PKG_VERSION").to_string());

    Json(ApiResponse::success(data))
}

// --- Public endpoints ---

/// GET /unsubscribe?email={}&token={} — public unsubscribe endpoint
async fn handle_unsubscribe(
    State(state): State<ApiState>,
    Query(params): Query<UnsubscribeQuery>,
) -> Html<String> {
    let hmac_secret = match get_hmac_secret(&state) {
        Ok(s) => s,
        Err(_) => {
            return Html(
                "<html><body><h2>Error</h2><p>Server configuration error.</p></body></html>"
                    .to_string(),
            )
        }
    };
    if !sender::verify_unsubscribe_token(&params.email, &params.token, &hmac_secret) {
        return Html(
            "<html><body><h2>Invalid unsubscribe link</h2><p>This link is invalid or has expired.</p></body></html>"
                .to_string(),
        );
    }

    // Use a uniform response regardless of whether the email was found to avoid
    // leaking subscriber existence to unauthenticated callers.
    match state
        .newsletter_manager
        .database()
        .update_subscriber_status_by_email(&params.email, SubscriberStatus::Declined)
    {
        Ok(_) => Html(
            "<html><body><h2>Unsubscribed</h2><p>If this email was subscribed, it has been removed from the newsletter.</p></body></html>"
                .to_string(),
        ),
        Err(_) => Html(
            "<html><body><h2>Error</h2><p>An error occurred while processing your request. Please try again later.</p></body></html>"
                .to_string(),
        ),
    }
}

/// POST /unsubscribe — RFC 8058 one-click unsubscribe
async fn handle_unsubscribe_post(
    State(state): State<ApiState>,
    Query(params): Query<UnsubscribeQuery>,
) -> StatusCode {
    let hmac_secret = match get_hmac_secret(&state) {
        Ok(s) => s,
        Err(status) => return status,
    };
    if !sender::verify_unsubscribe_token(&params.email, &params.token, &hmac_secret) {
        return StatusCode::FORBIDDEN;
    }

    match state
        .newsletter_manager
        .database()
        .update_subscriber_status_by_email(&params.email, SubscriberStatus::Declined)
    {
        Ok(_) => StatusCode::OK,
        Err(_) => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// GET /confirm?token={} — public confirmation endpoint for double opt-in
async fn handle_confirm(
    State(state): State<ApiState>,
    Query(params): Query<ConfirmQuery>,
) -> Html<String> {
    match state
        .newsletter_manager
        .database()
        .verify_confirmation_token(&params.token)
    {
        Ok(Some(email)) => {
            let escaped_email = email
                .replace('&', "&amp;")
                .replace('<', "&lt;")
                .replace('>', "&gt;")
                .replace('"', "&quot;")
                .replace('\'', "&#x27;");
            Html(format!(
                "<html><body><h2>Confirmed!</h2><p>Your email address ({}) has been confirmed. You are now subscribed to our newsletter.</p></body></html>",
                escaped_email
            ))
        }
        Ok(None) => Html(
            "<html><body><h2>Invalid link</h2><p>This confirmation link is invalid or has expired.</p></body></html>"
                .to_string(),
        ),
        Err(_) => Html(
            "<html><body><h2>Error</h2><p>An error occurred. Please try again later.</p></body></html>"
                .to_string(),
        ),
    }
}

// --- Protected endpoints ---

/// List subscribers endpoint
async fn list_subscribers(
    State(state): State<ApiState>,
    Query(params): Query<SubscriberQuery>,
) -> Result<Json<ApiResponse<Vec<Subscriber>>>, (StatusCode, Json<ApiResponse<()>>)> {
    // Cap the page size to prevent OOM on large databases
    let limit = Some(params.limit.unwrap_or(MAX_PAGE_SIZE).min(MAX_PAGE_SIZE));
    let offset = params.offset;

    // If tag filter is provided, use tag-based query
    if let Some(ref tag) = params.tag {
        match state
            .newsletter_manager
            .database()
            .get_subscribers_by_tag_paginated(tag, limit, offset)
        {
            Ok(subscribers) => return Ok(Json(ApiResponse::success(subscribers))),
            Err(e) => {
                eprintln!("Failed to list subscribers by tag: {}", e);
                return Err((
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ApiResponse::error("Failed to list subscribers".to_string())),
                ));
            }
        }
    }

    let status_filter = params
        .status
        .as_deref()
        .and_then(|s| match s.to_lowercase().as_str() {
            "pending" => Some(SubscriberStatus::Pending),
            "approved" => Some(SubscriberStatus::Approved),
            "declined" => Some(SubscriberStatus::Declined),
            "unconfirmed" => Some(SubscriberStatus::Unconfirmed),
            _ => None,
        });

    match state
        .newsletter_manager
        .database()
        .get_subscribers_paginated(status_filter, limit, offset)
    {
        Ok(subscribers) => Ok(Json(ApiResponse::success(subscribers))),
        Err(e) => {
            eprintln!("Failed to list subscribers: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("Failed to list subscribers".to_string())),
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
            Json(ApiResponse::error("Invalid email format".to_string())),
        ));
    }

    // If double opt-in is enabled, insert as Unconfirmed
    let status = if state.config.newsletter.double_optin {
        SubscriberStatus::Unconfirmed
    } else {
        request.status.unwrap_or(SubscriberStatus::Pending)
    };

    let subscriber = Subscriber {
        id: None,
        email: request.email.clone(),
        status,
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
        Ok(id) => {
            // If double opt-in, create confirmation token and send confirmation email
            if state.config.newsletter.double_optin {
                let token = uuid::Uuid::new_v4().to_string();
                if let Err(e) = state
                    .newsletter_manager
                    .database()
                    .create_confirmation_token(id, &token, 48)
                {
                    eprintln!("Failed to create confirmation token: {}", e);
                } else {
                    // Determine the confirmation URL base
                    let confirm_base =
                        state
                            .config
                            .newsletter
                            .confirmation_url
                            .clone()
                            .or_else(|| {
                                state
                                    .config
                                    .newsletter
                                    .api_base_url
                                    .as_ref()
                                    .map(|u| format!("{}/confirm", u.trim_end_matches('/')))
                            });

                    if let Some(confirm_url) = confirm_base {
                        let manager = state.newsletter_manager.clone();
                        let email_addr = request.email.clone();
                        let semaphore = state.confirmation_semaphore.clone();
                        // Send confirmation email in a bounded background task
                        tokio::spawn(async move {
                            // Acquire permit to bound concurrent SMTP connections
                            let _permit = match semaphore.acquire().await {
                                Ok(p) => p,
                                Err(_) => {
                                    eprintln!("Confirmation semaphore closed, cannot send email");
                                    return;
                                }
                            };
                            let result = tokio::task::spawn_blocking(move || {
                                let sender_instance = manager.create_sender(None)?;
                                let password = manager.get_smtp_password()?;
                                sender_instance.send_confirmation_email(
                                    &email_addr,
                                    &token,
                                    &confirm_url,
                                    &password,
                                )
                            })
                            .await;
                            match result {
                                Ok(Err(e)) => {
                                    eprintln!("Failed to send confirmation email: {}", e);
                                }
                                Err(e) => {
                                    eprintln!("Confirmation email task panicked: {}", e);
                                }
                                Ok(Ok(())) => {}
                            }
                        });
                    }
                }
            }

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
                        Json(ApiResponse::error("Internal database error".to_string())),
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
                    Json(ApiResponse::error(
                        "Failed to create subscriber".to_string(),
                    )),
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
            Json(ApiResponse::error("Subscriber not found".to_string())),
        )),
        Err(e) => {
            eprintln!("Failed to get subscriber: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("Internal database error".to_string())),
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
    let db = state.newsletter_manager.database();

    let db_err = |msg: &str, e: anyhow::Error| -> (StatusCode, Json<ApiResponse<()>>) {
        eprintln!("{}: {}", msg, e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error(msg.to_string())),
        )
    };
    let not_found = || -> (StatusCode, Json<ApiResponse<()>>) {
        (
            StatusCode::NOT_FOUND,
            Json(ApiResponse::error("Subscriber not found".to_string())),
        )
    };

    // Verify subscriber exists
    db.get_subscriber_by_email(&email)
        .map_err(|e| db_err("Internal database error", e))?
        .ok_or_else(not_found)?;

    // Delegate status transitions to the canonical DB method (single source of truth
    // for clearing stale timestamps on approve/decline/reset).
    if let Some(status) = request.status {
        db.update_subscriber_status_by_email(&email, status)
            .map_err(|e| db_err("Failed to update subscriber", e))?;
    }

    // Update notes if provided — re-fetch to pick up correct timestamps from status
    // transition above, then write the full record.
    if let Some(notes) = request.notes {
        let mut subscriber = db
            .get_subscriber_by_email(&email)
            .map_err(|e| db_err("Internal database error", e))?
            .ok_or_else(not_found)?;
        subscriber.notes = Some(notes);
        db.update_subscriber(&subscriber)
            .map_err(|e| db_err("Failed to update subscriber", e))?;
        return Ok(Json(ApiResponse::success(subscriber)));
    }

    // No notes update — fetch and return current state
    let subscriber = db
        .get_subscriber_by_email(&email)
        .map_err(|e| db_err("Internal database error", e))?
        .ok_or_else(not_found)?;
    Ok(Json(ApiResponse::success(subscriber)))
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
            Json(ApiResponse::error("Subscriber not found".to_string())),
        )),
        Err(e) => {
            eprintln!("Failed to delete subscriber: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error(
                    "Failed to delete subscriber".to_string(),
                )),
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

/// Get statistics endpoint
async fn get_stats(
    State(state): State<ApiState>,
) -> Result<Json<ApiResponse<StatsResponse>>, (StatusCode, Json<ApiResponse<()>>)> {
    let db = state.newsletter_manager.database();

    fn stats_err(e: anyhow::Error) -> (StatusCode, Json<ApiResponse<()>>) {
        eprintln!("Failed to get subscriber stats: {}", e);
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error("Failed to get statistics".to_string())),
        )
    }

    let total = db.get_subscriber_count(None).map_err(stats_err)?;
    let approved = db
        .get_subscriber_count(Some(SubscriberStatus::Approved))
        .map_err(stats_err)?;
    let pending = db
        .get_subscriber_count(Some(SubscriberStatus::Pending))
        .map_err(stats_err)?;
    let declined = db
        .get_subscriber_count(Some(SubscriberStatus::Declined))
        .map_err(stats_err)?;

    let stats = StatsResponse {
        total_subscribers: total,
        approved_subscribers: approved,
        pending_subscribers: pending,
        declined_subscribers: declined,
    };

    Ok(Json(ApiResponse::success(stats)))
}

// --- Tag endpoints ---

/// PUT /subscribers/{email}/tags — set tags for a subscriber
async fn update_subscriber_tags(
    State(state): State<ApiState>,
    Path(email): Path<String>,
    Json(request): Json<UpdateTagsRequest>,
) -> Result<Json<ApiResponse<Vec<String>>>, (StatusCode, Json<ApiResponse<()>>)> {
    // Validate all tags before querying the database
    for tag in &request.tags {
        if !is_valid_tag(tag) {
            return Err((
                StatusCode::BAD_REQUEST,
                Json(ApiResponse::error(format!(
                    "Invalid tag {:?}: must be non-empty, at most {} chars, no control characters",
                    tag,
                    super::MAX_TAG_LENGTH,
                ))),
            ));
        }
    }
    if request.tags.len() > super::MAX_TAGS_PER_SUBSCRIBER {
        return Err((
            StatusCode::BAD_REQUEST,
            Json(ApiResponse::error(format!(
                "Too many tags: {} provided (max {})",
                request.tags.len(),
                super::MAX_TAGS_PER_SUBSCRIBER,
            ))),
        ));
    }

    let subscriber = match state
        .newsletter_manager
        .database()
        .get_subscriber_by_email(&email)
    {
        Ok(Some(s)) => s,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiResponse::error("Subscriber not found".to_string())),
            ))
        }
        Err(e) => {
            eprintln!("Failed to get subscriber for tags: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("Internal database error".to_string())),
            ));
        }
    };

    let id = subscriber.id.ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error("Subscriber has no ID".to_string())),
        )
    })?;
    let db = state.newsletter_manager.database();

    // Atomically replace all tags in a single transaction
    if let Err(e) = db.set_tags(id, &request.tags) {
        eprintln!("Failed to set tags: {}", e);
        return Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ApiResponse::error("Failed to set tags".to_string())),
        ));
    }

    match db.get_tags(id) {
        Ok(tags) => Ok(Json(ApiResponse::success(tags))),
        Err(e) => {
            eprintln!("Failed to get tags: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("Failed to get tags".to_string())),
            ))
        }
    }
}

/// GET /subscribers/{email}/tags — get tags for a subscriber
async fn get_subscriber_tags(
    State(state): State<ApiState>,
    Path(email): Path<String>,
) -> Result<Json<ApiResponse<Vec<String>>>, (StatusCode, Json<ApiResponse<()>>)> {
    let subscriber = match state
        .newsletter_manager
        .database()
        .get_subscriber_by_email(&email)
    {
        Ok(Some(s)) => s,
        Ok(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                Json(ApiResponse::error("Subscriber not found".to_string())),
            ))
        }
        Err(e) => {
            eprintln!("Failed to get subscriber for tags: {}", e);
            return Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("Internal database error".to_string())),
            ));
        }
    };

    match state
        .newsletter_manager
        .database()
        .get_tags(subscriber.id.ok_or_else(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("Subscriber has no ID".to_string())),
            )
        })?) {
        Ok(tags) => Ok(Json(ApiResponse::success(tags))),
        Err(e) => {
            eprintln!("Failed to get tags: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("Failed to get tags".to_string())),
            ))
        }
    }
}

/// GET /tags — list all tags with counts
async fn list_tags(
    State(state): State<ApiState>,
) -> Result<Json<ApiResponse<Vec<TagInfo>>>, (StatusCode, Json<ApiResponse<()>>)> {
    match state.newsletter_manager.database().get_all_tags() {
        Ok(tags) => {
            let tag_infos: Vec<TagInfo> = tags
                .into_iter()
                .map(|(tag, count)| TagInfo { tag, count })
                .collect();
            Ok(Json(ApiResponse::success(tag_infos)))
        }
        Err(e) => {
            eprintln!("Failed to list tags: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("Failed to list tags".to_string())),
            ))
        }
    }
}

// --- Bounce endpoints ---

/// POST /webhooks/bounce — receive bounce notifications
async fn handle_bounce_webhook(
    State(state): State<ApiState>,
    Json(payload): Json<BounceWebhookPayload>,
) -> Result<Json<ApiResponse<()>>, (StatusCode, Json<ApiResponse<()>>)> {
    let event = payload.to_event();

    match state.newsletter_manager.database().record_bounce(
        &event.email,
        &event.bounce_type,
        event.reason.as_deref(),
    ) {
        Ok(_) => Ok(Json(ApiResponse::success(()))),
        Err(e) => {
            eprintln!("Failed to record bounce: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("Failed to record bounce".to_string())),
            ))
        }
    }
}

/// GET /bounces?email={} — get bounce history
async fn get_bounces(
    State(state): State<ApiState>,
    Query(params): Query<BounceQuery>,
) -> Result<Json<ApiResponse<Vec<BounceRecord>>>, (StatusCode, Json<ApiResponse<()>>)> {
    match state
        .newsletter_manager
        .database()
        .get_bounces(&params.email)
    {
        Ok(bounces) => Ok(Json(ApiResponse::success(bounces))),
        Err(e) => {
            eprintln!("Failed to get bounces: {}", e);
            Err((
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ApiResponse::error("Failed to get bounces".to_string())),
            ))
        }
    }
}

/// Get the HMAC secret via the NewsletterManager (single source of truth).
fn get_hmac_secret(state: &ApiState) -> Result<String, StatusCode> {
    state.newsletter_manager.get_hmac_secret().map_err(|e| {
        eprintln!("HMAC secret not configured: {}", e);
        StatusCode::INTERNAL_SERVER_ERROR
    })
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
            confirmation_semaphore: Arc::new(tokio::sync::Semaphore::new(MAX_CONFIRMATION_TASKS)),
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
        // Multiple @ signs must be rejected
        assert!(!is_valid_email("a@@b.com"));
        assert!(!is_valid_email("a@b@c.com"));
        // Domain must not start or end with dot
        assert!(!is_valid_email("user@.example.com"));
        assert!(!is_valid_email("user@example."));
        // Local part must not start/end with dot or have consecutive dots
        assert!(!is_valid_email(".user@example.com"));
        assert!(!is_valid_email("user.@example.com"));
        assert!(!is_valid_email("user..name@example.com"));
        // Domain must not have consecutive dots
        assert!(!is_valid_email("user@example..com"));
        // Local part max 64 chars
        let long_local = format!("{}@example.com", "a".repeat(65));
        assert!(!is_valid_email(&long_local));
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

    #[tokio::test]
    async fn test_rate_limiter_allows_within_limit() {
        let limiter = RateLimiter::new(5);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        for _ in 0..5 {
            assert!(
                limiter.check(ip).await,
                "should allow requests within limit"
            );
        }
    }

    #[tokio::test]
    async fn test_rate_limiter_rejects_over_limit() {
        let limiter = RateLimiter::new(3);
        let ip: IpAddr = "127.0.0.1".parse().unwrap();

        assert!(limiter.check(ip).await);
        assert!(limiter.check(ip).await);
        assert!(limiter.check(ip).await);
        assert!(!limiter.check(ip).await, "should reject the 4th request");
    }

    #[tokio::test]
    async fn test_rate_limiter_isolates_ips() {
        let limiter = RateLimiter::new(2);
        let ip1: IpAddr = "10.0.0.1".parse().unwrap();
        let ip2: IpAddr = "10.0.0.2".parse().unwrap();

        // Exhaust limit for ip1
        assert!(limiter.check(ip1).await);
        assert!(limiter.check(ip1).await);
        assert!(!limiter.check(ip1).await);

        // ip2 should still be allowed
        assert!(
            limiter.check(ip2).await,
            "different IP should have its own bucket"
        );
    }

    #[tokio::test]
    async fn test_rate_limiter_cleanup_stale_entries() {
        let limiter = RateLimiter::new(100);
        let ip: IpAddr = "192.168.1.1".parse().unwrap();

        // Add a request so the IP has an entry
        assert!(limiter.check(ip).await);
        {
            let buckets = limiter.buckets.lock().await;
            assert!(buckets.contains_key(&ip));
        }

        // Manually expire the entry by clearing timestamps
        {
            let mut buckets = limiter.buckets.lock().await;
            buckets.get_mut(&ip).unwrap().clear();
        }

        limiter.cleanup_stale_entries().await;

        let buckets = limiter.buckets.lock().await;
        assert!(
            !buckets.contains_key(&ip),
            "stale IP entry should be cleaned up"
        );
    }

    #[tokio::test]
    async fn test_rate_limit_middleware_per_ip() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let limiter = Arc::new(RateLimiter::new(2));
        let limiter_clone = Arc::clone(&limiter);

        let app = Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(middleware::from_fn(move |req, next| {
                let limiter = Arc::clone(&limiter_clone);
                rate_limit_middleware(req, next, limiter)
            }));

        let addr1: SocketAddr = "10.0.0.1:1234".parse().unwrap();
        let addr2: SocketAddr = "10.0.0.2:1234".parse().unwrap();

        // 2 requests from IP1 should succeed
        for _ in 0..2 {
            let req = Request::builder()
                .uri("/test")
                .extension(axum::extract::ConnectInfo(addr1))
                .body(Body::empty())
                .unwrap();
            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }

        // 3rd request from IP1 should be rate limited
        let req = Request::builder()
            .uri("/test")
            .extension(axum::extract::ConnectInfo(addr1))
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "3rd request from same IP should be rejected"
        );

        // Request from IP2 should still succeed (per-IP isolation)
        let req = Request::builder()
            .uri("/test")
            .extension(axum::extract::ConnectInfo(addr2))
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "different IP should have its own bucket"
        );
    }

    #[tokio::test]
    async fn test_rate_limit_middleware_fallback_without_connect_info() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let limiter = Arc::new(RateLimiter::new(1));
        let limiter_clone = Arc::clone(&limiter);

        let app = Router::new()
            .route("/test", get(|| async { "ok" }))
            .layer(middleware::from_fn(move |req, next| {
                let limiter = Arc::clone(&limiter_clone);
                rate_limit_middleware(req, next, limiter)
            }));

        // Request without ConnectInfo falls back to 0.0.0.0
        let req = Request::builder()
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        // Second request without ConnectInfo shares the same fallback bucket
        let req = Request::builder()
            .uri("/test")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::TOO_MANY_REQUESTS,
            "requests without ConnectInfo share a single fallback bucket"
        );
    }
}
