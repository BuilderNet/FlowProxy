use alloy_primitives::bytes::{BufMut as _, BytesMut};
use axum::{
    extract::{FromRequest, Request},
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::str::FromStr;

/// Supported JSON-RPC version 2.0.
pub const JSONRPC_VERSION_2: &str = "2.0";

/// JSON-RPC request object.
/// Spec: <https://www.jsonrpc.org/specification#request_object>.
///
/// NOTE: This request object is more restrictive. It diverges from the spec
/// by accepting only a single type in the `params` field.
#[derive(Debug, Deserialize)]
pub struct JsonRpcRequest<T> {
    /// An identifier established by the client.
    pub id: u64,
    /// A String specifying the version of the JSON-RPC protocol. MUST be exactly "2.0".
    pub jsonrpc: String,
    /// A String containing the name of the method to be invoked.
    pub method: String,
    /// A Structured value that holds the parameter values to be used during the invocation of the
    /// method. This member MAY be omitted.
    pub params: Option<Vec<T>>,
}

impl<T> JsonRpcRequest<T> {
    pub fn from_json(Json(json): Json<JsonRpcRequest<T>>) -> Result<Self, JsonRpcError> {
        if json.jsonrpc != JSONRPC_VERSION_2 {
            return Err(JsonRpcError::InvalidRequest)
        }
        Ok(json)
    }

    /// Removes and returns a parameter only if it's the only one.
    pub fn take_single_param(&mut self) -> Option<T> {
        self.params.take_if(|p| p.len() == 1)?.pop()
    }
}

impl<T: DeserializeOwned> JsonRpcRequest<T> {
    pub fn from_bytes(bytes: &[u8]) -> Result<Self, JsonRpcError> {
        match Json::from_bytes(bytes) {
            Ok(json) => Self::from_json(json),
            Err(_error) => Err(JsonRpcError::ParseError),
        }
    }
}

impl<T, S> FromRequest<S> for JsonRpcRequest<T>
where
    T: DeserializeOwned,
    S: Send + Sync,
{
    type Rejection = JsonRpcResponse<()>;

    async fn from_request(req: Request, state: &S) -> Result<Self, Self::Rejection> {
        match Json::<JsonRpcRequest<T>>::from_request(req, state).await {
            Ok(json) => {
                let id = json.id;
                match Self::from_json(json) {
                    Ok(request) => Ok(request),
                    Err(error) => Err(JsonRpcResponse::error(Some(id), error)),
                }
            }
            Err(_error) => Err(JsonRpcResponse::error(None, JsonRpcError::ParseError)),
        }
    }
}

/// JSON-RPC response object.
/// Spec: <https://www.jsonrpc.org/specification#response_object>.
#[derive(Debug, Serialize, Deserialize)]
pub struct JsonRpcResponse<T> {
    pub id: Option<u64>,
    pub jsonrpc: String,
    #[serde(flatten)]
    pub result_or_error: JsonRpcResponseTy<T>,
}

impl<T> JsonRpcResponse<T> {
    pub fn new(id: Option<u64>, result_or_error: JsonRpcResponseTy<T>) -> Self {
        Self { id, result_or_error, jsonrpc: JSONRPC_VERSION_2.to_owned() }
    }

    pub fn result(id: u64, result: T) -> Self {
        Self {
            id: Some(id),
            result_or_error: JsonRpcResponseTy::Result(result),
            jsonrpc: JSONRPC_VERSION_2.to_owned(),
        }
    }

    pub fn error(id: Option<u64>, error: JsonRpcError) -> Self {
        Self {
            id,
            result_or_error: JsonRpcResponseTy::Error { code: error.code(), message: error },
            jsonrpc: JSONRPC_VERSION_2.to_owned(),
        }
    }
}

impl<T: Serialize> IntoResponse for JsonRpcResponse<T> {
    fn into_response(self) -> Response {
        // Use a small initial capacity of 128 bytes like serde_json::to_vec
        // https://docs.rs/serde_json/1.0.82/src/serde_json/ser.rs.html#2189
        let mut buf = BytesMut::with_capacity(128).writer();
        match serde_json::to_writer(&mut buf, &self) {
            Ok(()) => {
                let status_code = match &self.result_or_error {
                    JsonRpcResponseTy::Result(_) => StatusCode::OK,
                    JsonRpcResponseTy::Error { message, .. } => message.http_status_code(),
                };
                let headers = [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(mime::APPLICATION_JSON.as_ref()),
                )];

                (status_code, headers, buf.into_inner().freeze()).into_response()
            }
            Err(err) => {
                let status_code = StatusCode::INTERNAL_SERVER_ERROR;
                let headers = [(
                    header::CONTENT_TYPE,
                    HeaderValue::from_static(mime::TEXT_PLAIN_UTF_8.as_ref()),
                )];
                (status_code, headers, err.to_string()).into_response()
            }
        }
    }
}

/// JSON-RPC response type.
/// Either the result member or error member MUST be included, but both members MUST NOT be
/// included.
#[derive(PartialEq, Eq, Debug, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum JsonRpcResponseTy<T> {
    Result(T),
    Error { code: i32, message: JsonRpcError },
}

#[derive(PartialEq, Eq, Debug, thiserror::Error)]
pub enum JsonRpcError {
    #[error("Parse error")]
    ParseError,
    #[error("Invalid Request")]
    InvalidRequest,
    #[error("Invalid signature")]
    InvalidSignature,
    #[error("Method not found")]
    MethodNotFound,
    #[error("Invalid params")]
    InvalidParams,
    #[error("Rate limited")]
    RateLimited,
    #[error("Internal error")]
    Internal,
    #[error("{0}")]
    Unknown(String),
}

impl FromStr for JsonRpcError {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let error = match s {
            "Parse error" => Self::ParseError,
            "Invalid Request" => Self::InvalidRequest,
            "Invalid signature" => Self::InvalidSignature,
            "Method not found" => Self::MethodNotFound,
            "Invalid params" => Self::InvalidParams,
            "Rate limited" => Self::RateLimited,
            "Internal error" => Self::Internal,
            s => Self::Unknown(s.to_string()),
        };
        Ok(error)
    }
}

impl Serialize for JsonRpcError {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for JsonRpcError {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        JsonRpcError::from_str(String::deserialize(deserializer)?.as_str())
            .map_err(|_| serde::de::Error::custom("unknown"))
    }
}

impl JsonRpcError {
    /// Create new unknown error.
    pub fn unknown(error: impl Into<String>) -> Self {
        Self::Unknown(error.into())
    }

    /// The error code for JSON-RPC error.
    /// Spec: <https://www.jsonrpc.org/specification#error_object>.
    pub fn code(&self) -> i32 {
        match self {
            Self::ParseError => -32700,
            Self::InvalidRequest => -32600,
            Self::MethodNotFound => -32601,
            Self::InvalidParams => -32602,
            Self::RateLimited | Self::Internal | Self::Unknown(_) | Self::InvalidSignature => {
                -32603
            }
        }
    }

    /// The HTTP status code for JSON-RPC error.
    pub fn http_status_code(&self) -> StatusCode {
        match self {
            Self::ParseError |
            Self::InvalidRequest |
            Self::InvalidParams |
            Self::InvalidSignature => StatusCode::BAD_REQUEST,
            Self::MethodNotFound => StatusCode::NOT_FOUND,
            Self::RateLimited => StatusCode::TOO_MANY_REQUESTS,
            Self::Internal | Self::Unknown(_) => StatusCode::INTERNAL_SERVER_ERROR,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn jsonrpc_error_str_roundtrip() {
        let errors = [
            JsonRpcError::ParseError,
            JsonRpcError::InvalidRequest,
            JsonRpcError::MethodNotFound,
            JsonRpcError::InvalidParams,
            JsonRpcError::RateLimited,
            JsonRpcError::Internal,
            JsonRpcError::unknown("unknown"),
        ];
        for error in errors {
            assert_eq!(error, JsonRpcError::from_str(&error.to_string()).unwrap());
        }
    }
}
