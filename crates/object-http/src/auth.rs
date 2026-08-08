//! Pluggable authentication and authorization for the S3 gateway.
//!
//! The [`AuthProvider`] trait abstracts over credential verification so
//! callers can plug in API-key, JWT, or AWS Signature v4 backends without
//! touching the HTTP handlers.
//!
//! # Architecture
//!
//! 1. An axum middleware ([`auth_middleware`]) runs **before** every handler.
//! 2. It calls [`AuthProvider::authenticate`] to turn request headers into a
//!    [`Principal`].
//! 3. The resulting `Principal` is stored in the request extensions so handlers
//!    can read it.
//! 4. Optionally, [`AuthProvider::authorize`] is called to check per-operation
//!    permissions.
//!
//! When no `AuthProvider` is configured the middleware is a no-op and an
//! anonymous principal is injected.

use std::sync::Arc;

use axum::{
    body::Body,
    extract::Request,
    http::{header, StatusCode},
    middleware::Next,
    response::{IntoResponse, Response},
};

use crate::xml;

// ── Principal ────────────────────────────────────────────────────────────────

/// An authenticated identity attached to every request.
#[derive(Debug, Clone)]
pub struct Principal {
    /// Unique identifier for this principal (e.g. user-id, service-account).
    pub id: String,
    /// Human-readable display name (optional).
    pub display_name: Option<String>,
    /// Set of permission strings (e.g. `s3:GetObject`, `s3:PutObject`).
    pub permissions: Vec<String>,
}

impl Principal {
    /// Create an anonymous principal (used when auth is disabled).
    pub fn anonymous() -> Self {
        Self {
            id: "anonymous".into(),
            display_name: None,
            permissions: vec!["s3:*".into()],
        }
    }

    /// Check whether this principal holds `permission`.
    pub fn has_permission(&self, permission: &str) -> bool {
        self.permissions.iter().any(|p| p == "s3:*" || p == permission)
    }
}

// ── AuthProvider trait ───────────────────────────────────────────────────────

/// Trait for pluggable authentication and authorization.
///
/// Implement this to provide custom credential verification for the S3
/// gateway. The default [`NoAuth`] implementation allows all requests.
#[async_trait::async_trait]
pub trait AuthProvider: Send + Sync + 'static {
    /// Authenticate a request and return the [`Principal`].
    ///
    /// Implementations should inspect request headers (e.g. `Authorization`,
    /// `X-Api-Key`) and return `Ok(Principal)` on success or `Err(AuthError)`
    /// on failure.
    async fn authenticate(&self, headers: &axum::http::HeaderMap) -> Result<Principal, AuthError>;

    /// Check whether `principal` is allowed to perform `action` on `resource`.
    ///
    /// `action` follows S3 conventions (e.g. `s3:GetObject`, `s3:PutObject`).
    /// `resource` is the S3 resource path (e.g. `/bucket/key`).
    ///
    /// The default implementation checks `principal.has_permission(action)`.
    async fn authorize(
        &self,
        principal: &Principal,
        action: &str,
        _resource: &str,
    ) -> Result<(), AuthError> {
        if principal.has_permission(action) {
            Ok(())
        } else {
            Err(AuthError::Forbidden(format!(
                "principal '{}' lacks permission '{}'",
                principal.id, action
            )))
        }
    }
}

// ── AuthError ────────────────────────────────────────────────────────────────

/// Errors produced by the auth layer.
#[derive(Debug, thiserror::Error)]
pub enum AuthError {
    /// Credentials are missing or invalid — maps to HTTP 403.
    #[error("access denied: {0}")]
    AccessDenied(String),
    /// Principal lacks required permissions — maps to HTTP 403.
    #[error("forbidden: {0}")]
    Forbidden(String),
}

impl IntoResponse for AuthError {
    fn into_response(self) -> Response {
        let (status, code, message) = match &self {
            AuthError::AccessDenied(msg) => (StatusCode::FORBIDDEN, "AccessDenied", msg.as_str()),
            AuthError::Forbidden(msg) => (StatusCode::FORBIDDEN, "AccessDenied", msg.as_str()),
        };
        let body = xml::error_response(code, message, "");
        (status, [("content-type", "application/xml")], body).into_response()
    }
}

// ── NoAuth (default) ─────────────────────────────────────────────────────────

/// A no-op auth provider that allows all requests with an anonymous principal.
#[derive(Debug, Clone, Default)]
pub struct NoAuth;

#[async_trait::async_trait]
impl AuthProvider for NoAuth {
    async fn authenticate(&self, _headers: &axum::http::HeaderMap) -> Result<Principal, AuthError> {
        Ok(Principal::anonymous())
    }
}

// ── BearerTokenAuth ──────────────────────────────────────────────────────────

/// Simple bearer-token auth provider for development and testing.
///
/// Validates an `Authorization` header with a ****** against a static set
/// of known tokens. Each token maps to a [`Principal`].
#[derive(Debug, Clone)]
pub struct BearerTokenAuth {
    /// Mapping of token → principal.
    tokens: std::collections::HashMap<String, Principal>,
}

impl BearerTokenAuth {
    /// Create a new `BearerTokenAuth` with the given token → principal mapping.
    pub fn new(tokens: std::collections::HashMap<String, Principal>) -> Self {
        Self { tokens }
    }
}

#[async_trait::async_trait]
impl AuthProvider for BearerTokenAuth {
    async fn authenticate(&self, headers: &axum::http::HeaderMap) -> Result<Principal, AuthError> {
        let auth_header = headers
            .get(header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .ok_or_else(|| AuthError::AccessDenied("missing Authorization header".into()))?;

        let token = auth_header
            .strip_prefix("Bearer ")
            .ok_or_else(|| AuthError::AccessDenied("invalid Authorization scheme".into()))?;

        self.tokens
            .get(token)
            .cloned()
            .ok_or_else(|| AuthError::AccessDenied("invalid token".into()))
    }
}

// ── Middleware ────────────────────────────────────────────────────────────────

/// Axum middleware that authenticates every request via the configured
/// [`AuthProvider`] and injects the resulting [`Principal`] into
/// request extensions.
pub async fn auth_middleware(
    auth: axum::extract::State<Arc<dyn AuthProvider>>,
    mut req: Request<Body>,
    next: Next,
) -> Response {
    match auth.authenticate(req.headers()).await {
        Ok(principal) => {
            req.extensions_mut().insert(principal);
            next.run(req).await
        }
        Err(err) => err.into_response(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anonymous_principal_has_wildcard() {
        let p = Principal::anonymous();
        assert!(p.has_permission("s3:GetObject"));
        assert!(p.has_permission("s3:PutObject"));
        assert!(p.has_permission("s3:DeleteObject"));
    }

    #[test]
    fn scoped_principal_permission_check() {
        let p = Principal {
            id: "user-1".into(),
            display_name: Some("Alice".into()),
            permissions: vec!["s3:GetObject".into(), "s3:ListBucket".into()],
        };
        assert!(p.has_permission("s3:GetObject"));
        assert!(p.has_permission("s3:ListBucket"));
        assert!(!p.has_permission("s3:PutObject"));
        assert!(!p.has_permission("s3:DeleteObject"));
    }

    #[tokio::test]
    async fn no_auth_returns_anonymous() {
        let provider = NoAuth;
        let headers = axum::http::HeaderMap::new();
        let principal = provider.authenticate(&headers).await.unwrap();
        assert_eq!(principal.id, "anonymous");
        assert!(principal.has_permission("s3:GetObject"));
    }

    /// Helper to build an Authorization header value for ******
    fn bearer_header(token: &str) -> String {
        let mut s = String::from("Bearer ");
        s.push_str(token);
        s
    }

    #[tokio::test]
    async fn bearer_auth_valid_token() {
        let token_value = "test-value-abc123";
        let mut tokens = std::collections::HashMap::new();
        tokens.insert(
            token_value.to_string(),
            Principal {
                id: "user-1".into(),
                display_name: Some("Alice".into()),
                permissions: vec!["s3:GetObject".into()],
            },
        );
        let provider = BearerTokenAuth::new(tokens);

        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            bearer_header(token_value).parse().unwrap(),
        );

        let principal = provider.authenticate(&headers).await.unwrap();
        assert_eq!(principal.id, "user-1");
    }

    #[tokio::test]
    async fn bearer_auth_invalid_token() {
        let provider = BearerTokenAuth::new(std::collections::HashMap::new());

        let mut headers = axum::http::HeaderMap::new();
        headers.insert(
            header::AUTHORIZATION,
            bearer_header("wrong-token").parse().unwrap(),
        );

        let result = provider.authenticate(&headers).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn bearer_auth_missing_header() {
        let provider = BearerTokenAuth::new(std::collections::HashMap::new());
        let headers = axum::http::HeaderMap::new();
        let result = provider.authenticate(&headers).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn default_authorize_checks_permissions() {
        let provider = NoAuth;
        let principal = Principal {
            id: "user-1".into(),
            display_name: None,
            permissions: vec!["s3:GetObject".into()],
        };

        assert!(provider.authorize(&principal, "s3:GetObject", "/bucket/key").await.is_ok());
        assert!(provider.authorize(&principal, "s3:PutObject", "/bucket/key").await.is_err());
    }
}
