//! HTTP error response types for the S3-compatible API.

use axum::{
    http::StatusCode,
    response::{IntoResponse, Response},
};

use plures_object_core::ObjectError;

use crate::xml;

/// An S3-compatible HTTP error response.
///
/// Implements [`IntoResponse`] so it can be returned from axum handlers directly.
pub struct ApiError {
    pub status: StatusCode,
    pub code: &'static str,
    pub message: String,
    pub resource: String,
}

impl ApiError {
    pub fn not_found(resource: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            code: "NoSuchKey",
            message: "The specified key does not exist.".into(),
            resource: resource.into(),
        }
    }

    pub fn conflict(resource: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::CONFLICT,
            code: "OperationAborted",
            message: message.into(),
            resource: resource.into(),
        }
    }

    pub fn internal(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            code: "InternalError",
            message: message.into(),
            resource: String::new(),
        }
    }

    pub fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            code: "InvalidArgument",
            message: message.into(),
            resource: String::new(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = xml::error_response(self.code, &self.message, &self.resource);
        (
            self.status,
            [("content-type", "application/xml")],
            body,
        )
            .into_response()
    }
}

/// Convert an [`ObjectError`] into an [`ApiError`].
pub fn object_error_to_api(err: ObjectError, resource: &str) -> ApiError {
    match err {
        ObjectError::NotFound(_) | ObjectError::ChunkNotFound(_) => {
            ApiError::not_found(resource.to_string())
        }
        ObjectError::Conflict(msg) => ApiError::conflict(resource.to_string(), msg),
        ObjectError::InvalidRequest(msg) => ApiError::bad_request(msg),
        ObjectError::Storage(msg) => ApiError::internal(msg),
    }
}
