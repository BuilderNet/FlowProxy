use std::{ops::Deref, sync::Arc};

use alloy_consensus::{
    crypto::RecoveryError,
    transaction::{PooledTransaction, Recovered, SignerRecoverable as _},
};
use alloy_eips::{
    eip2718::{Eip2718Error, Eip2718Result},
    Decodable2718 as _, Encodable2718,
};
use alloy_primitives::{Address, Bytes};
use rbuilder_primitives::{
    serialize::{RawBundle, RawBundleConvertError, RawBundleDecodeResult, TxEncoding},
    Bundle, BundleReplacementData,
};
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

/// Bundle type that is used for the system API. It contains the verified signer with the original
/// bundle.
#[derive(PartialEq, Eq, Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemBundle {
    #[serde(rename = "signingAddress")]
    pub signer: Address,
    /// The inner bundle. Wrapped in [`Arc`] to make cloning cheaper.
    #[serde(flatten)]
    pub raw_bundle: Arc<RawBundle>,

    /// The decoded bundle.
    #[serde(skip)]
    pub decoded_bundle: Arc<DecodedBundle>,
}

/// Decoded bundle type. Either a new, full bundle or a replacement bundle.
#[allow(clippy::large_enum_variant)]
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum DecodedBundle {
    /// A new, full bundle.
    Bundle(Bundle),
    /// A replacement bundle.
    Replacement(BundleReplacementData),
}

impl From<RawBundleDecodeResult> for DecodedBundle {
    fn from(value: RawBundleDecodeResult) -> Self {
        match value {
            RawBundleDecodeResult::NewBundle(bundle) => Self::Bundle(bundle),
            RawBundleDecodeResult::CancelBundle(replacement_data) => {
                Self::Replacement(replacement_data)
            }
        }
    }
}

impl SystemBundle {
    /// Create a new system bundle from a raw bundle and a signer.
    /// Returns an error if the bundle fails to decode.
    pub fn try_from_bundle_and_signer(
        bundle: RawBundle,
        signer: Address,
    ) -> Result<Self, RawBundleConvertError> {
        let decoded = bundle.clone().decode(TxEncoding::WithBlobData)?;

        Ok(Self { signer, raw_bundle: Arc::new(bundle), decoded_bundle: Arc::new(decoded.into()) })
    }

    /// Returns `true` if the bundle is a replacement.
    pub fn is_replacement(&self) -> bool {
        matches!(self.decoded_bundle.as_ref(), DecodedBundle::Replacement(_))
    }

    /// Returns the bundle UUID if it is a bundle, otherwise the replacement UUID.
    pub fn uuid(&self) -> Uuid {
        match self.decoded_bundle.as_ref() {
            DecodedBundle::Bundle(bundle) => bundle.uuid,
            DecodedBundle::Replacement(replacement_data) => replacement_data.key.key().id,
        }
    }

    /// Returns the bundle if it is a new bundle.
    pub fn bundle(&self) -> Option<&Bundle> {
        match self.decoded_bundle.as_ref() {
            DecodedBundle::Bundle(bundle) => Some(bundle),
            DecodedBundle::Replacement(_) => None,
        }
    }

    /// Returns the replacement data if it is a replacement bundle.
    pub fn replacement_data(&self) -> Option<&BundleReplacementData> {
        match self.decoded_bundle.as_ref() {
            DecodedBundle::Replacement(replacement_data) => Some(replacement_data),
            DecodedBundle::Bundle(_) => None,
        }
    }

    /// Encode the inner bundle (no signer).
    pub fn encode_local(self) -> Vec<u8> {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_sendBundle",
            "params": [self.raw_bundle]
        });

        serde_json::to_vec(&json).unwrap()
    }

    /// Encode the full system bundle.
    pub fn encode(self) -> Vec<u8> {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_sendBundle",
            "params": [self]
        });

        serde_json::to_vec(&json).unwrap()
    }
}

/// Internally processed transaction.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SystemTransaction {
    pub transaction: Arc<PooledTransaction>,
    /// The original transaction signer.
    pub signer: Address,
}

impl Deref for SystemTransaction {
    type Target = PooledTransaction;

    fn deref(&self) -> &Self::Target {
        &self.transaction
    }
}

impl SystemTransaction {
    /// Create a new system transaction from a transaction and a signer.
    pub fn from_transaction_and_signer(transaction: PooledTransaction, signer: Address) -> Self {
        Self { transaction: Arc::new(transaction), signer }
    }

    /// Encode the transaction as EIP-2718 encoded bytes.
    pub fn encode(&self) -> Vec<u8> {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [self.transaction.encoded_2718()]
        });

        serde_json::to_vec(&json).unwrap()
    }
}

/// Decode pooled Ethereum transaction from raw bytes.
pub fn decode_transaction(raw: &Bytes) -> Eip2718Result<PooledTransaction> {
    if raw.is_empty() {
        return Err(Eip2718Error::RlpError(alloy_rlp::Error::InputTooShort))
    }
    PooledTransaction::decode_2718(&mut &raw[..])
}

/// Recover ECDSA signer of the transaction.
pub fn recover_transaction(
    transaction: PooledTransaction,
) -> Result<Recovered<PooledTransaction>, RecoveryError> {
    let signer = transaction.recover_signer()?;
    Ok(Recovered::new_unchecked(transaction, signer))
}
