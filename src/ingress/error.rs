use crate::{jsonrpc::JsonRpcError, validation::ValidationError};
use alloy_consensus::crypto::RecoveryError;
use alloy_eips::eip2718::Eip2718Error;
use rbuilder_primitives::serialize::{RawBundleConvertError, RawShareBundleConvertError};

#[derive(Debug, thiserror::Error)]
pub enum IngressError {
    /// Empty raw transaction data.
    #[error("empty transaction data")]
    EmptyRawTransaction,
    /// Validation error.
    #[error(transparent)]
    Validation(#[from] ValidationError),
    /// Error decoding EIP-2718 encoded transaction.
    #[error(transparent)]
    Decode2718(#[from] Eip2718Error),
    /// Bundle decoding error.
    #[error(transparent)]
    BundleDecode(#[from] RawBundleConvertError),
    /// MEV Share bundle decoding error.
    #[error(transparent)]
    ShareBundleDecode(#[from] RawShareBundleConvertError),
    /// ECDSA signature recovery error.
    #[error(transparent)]
    Recovery(#[from] RecoveryError),
    /// Serde error.
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

impl IngressError {
    /// Convert [`IngressError`] into [`JsonRpcError`].
    pub fn into_jsonrpc_error(self) -> JsonRpcError {
        match self {
            Self::EmptyRawTransaction |
            Self::Validation(_) |
            Self::Decode2718(_) |
            Self::BundleDecode(_) |
            Self::ShareBundleDecode(_) |
            Self::Recovery(_) => JsonRpcError::InvalidParams,
            Self::Serde(_) => JsonRpcError::ParseError,
        }
    }

    /// Returns `true` if it is validation error.
    pub fn is_validation(&self) -> bool {
        matches!(self, Self::Validation(_))
    }
}
