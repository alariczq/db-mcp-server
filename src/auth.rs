//! Authentication module for MCP server HTTP transport.

use axum::{
    body::Body,
    extract::State,
    http::{Request, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::Serialize;
use std::collections::HashSet;
use std::sync::Arc;
use subtle::ConstantTimeEq;
use tracing::warn;

/// Authentication configuration for the MCP server.
#[derive(Debug, Clone)]
pub struct AuthConfig {
    enabled: bool,
    tokens: HashSet<String>,
}

impl AuthConfig {
    /// Create a new AuthConfig from a list of tokens.
    pub fn from_tokens(tokens: Vec<String>) -> Result<Self, String> {
        let mut valid_tokens = HashSet::new();
        for token in tokens {
            let trimmed = token.trim().to_string();
            if trimmed.is_empty() {
                return Err("Empty token value in configuration".to_string());
            }
            valid_tokens.insert(trimmed);
        }
        let enabled = !valid_tokens.is_empty();
        Ok(Self { enabled, tokens: valid_tokens })
    }

    pub fn disabled() -> Self {
        Self { enabled: false, tokens: HashSet::new() }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }

    pub fn token_count(&self) -> usize {
        self.tokens.len()
    }
}

impl Default for AuthConfig {
    fn default() -> Self {
        Self::disabled()
    }
}

/// Authentication middleware for HTTP requests.
pub async fn auth_middleware(
    State(auth_config): State<Arc<AuthConfig>>,
    request: Request<Body>,
    next: Next,
) -> Response {
    let token = match extract_bearer_token(&request) {
        Ok(Some(token)) => token,
        Ok(None) => {
            warn!("Authentication failed: missing Authorization header");
            return unauthorized_response(
                "Missing Bearer token in Authorization header",
                "Include a valid token: 'Authorization: Bearer <token>'",
            );
        }
        Err(msg) => {
            warn!("Authentication failed: invalid header format");
            return unauthorized_response(msg, "Use the format: 'Authorization: Bearer <your-token>'");
        }
    };

    if verify_token(&auth_config, token) {
        next.run(request).await
    } else {
        warn!(token_prefix = %mask_token(token), "Authentication failed: invalid token");
        unauthorized_response(
            "Invalid Bearer token",
            "Check that you are using a valid token configured on the server",
        )
    }
}

fn extract_bearer_token(request: &Request<Body>) -> Result<Option<&str>, &'static str> {
    let Some(auth_header) = request.headers().get(header::AUTHORIZATION) else {
        return Ok(None);
    };

    let auth_str = auth_header
        .to_str()
        .map_err(|_| "Authorization header contains invalid characters")?;

    if !auth_str.starts_with("Bearer ") {
        return Err("Invalid Authorization header format. Expected 'Bearer <token>'");
    }

    let token = &auth_str[7..];
    if token.is_empty() {
        return Err("Bearer token is empty");
    }

    Ok(Some(token))
}

fn verify_token(config: &AuthConfig, provided: &str) -> bool {
    let mut found = false;
    for expected in &config.tokens {
        if constant_time_eq(provided.as_bytes(), expected.as_bytes()) {
            found = true;
        }
    }
    found
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.ct_eq(b).into()
}

fn mask_token(token: &str) -> String {
    if token.len() <= 3 {
        "***".to_string()
    } else {
        format!("{}***", &token[..3])
    }
}

fn unauthorized_response(message: impl Into<String>, suggestion: impl Into<String>) -> Response {
    #[derive(Serialize)]
    struct ErrorResponse { error: ErrorDetail }
    #[derive(Serialize)]
    struct ErrorDetail { code: &'static str, message: String, suggestion: String }

    let body = ErrorResponse {
        error: ErrorDetail {
            code: "unauthorized",
            message: message.into(),
            suggestion: suggestion.into(),
        },
    };
    let json = serde_json::to_string(&body).unwrap_or_else(|_| {
        r#"{"error":{"code":"unauthorized","message":"Authentication failed"}}"#.to_string()
    });

    (StatusCode::UNAUTHORIZED, [(header::CONTENT_TYPE, "application/json")], json).into_response()
}
