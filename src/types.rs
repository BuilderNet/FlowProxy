use std::{
    hash::{Hash as _, Hasher as _},
    sync::Arc,
    time::{Duration, Instant},
};

use alloy_consensus::{
    crypto::RecoveryError,
    transaction::{PooledTransaction, Recovered, SignerRecoverable as _},
};
use alloy_eips::{
    eip2718::{Eip2718Error, Eip2718Result},
    Decodable2718 as _,
};
use alloy_primitives::{Address, Bytes};
use derive_more::Deref;
use rbuilder_primitives::{
    serialize::{RawBundle, RawBundleConvertError, RawBundleDecodeResult, TxEncoding},
    Bundle, BundleReplacementData,
};
use revm_primitives::B256;
use serde::Serialize;
use serde_json::json;
use time::UtcDateTime;
use uuid::Uuid;

use crate::priority::Priority;

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

    /// The bundle hash.
    #[serde(skip)]
    pub bundle_hash: B256,

    /// The time at which the bundle has first been seen from the local operator.
    #[serde(skip)]
    pub received_at: UtcInstant,
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

pub trait BundleHash {
    fn bundle_hash(&self) -> B256;
}

impl BundleHash for RawBundle {
    fn bundle_hash(&self) -> B256 {
        fn hash(bundle: &RawBundle, state: &mut wyhash::WyHash) {
            // We destructure here so we never miss any fields if new fields are added in the
            // future.
            let RawBundle {
                block_number,
                txs,
                reverting_tx_hashes,
                dropping_tx_hashes,
                replacement_uuid,
                refund_percent,
                refund_recipient,
                refund_tx_hashes,
                first_seen_at: _,
                signing_address: _,
                uuid: _,
                version: _,
                min_timestamp: _,
                max_timestamp: _,
                replacement_nonce: _,
                delayed_refund: _,
            } = bundle;

            block_number.hash(state);
            txs.hash(state);

            let reverting_tx_hashes = if !reverting_tx_hashes.is_empty() {
                Some(reverting_tx_hashes.iter().map(|hash| format!("{hash:?}")).collect::<Vec<_>>())
            } else {
                None
            };

            reverting_tx_hashes.hash(state);

            let dropping_tx_hashes = if !dropping_tx_hashes.is_empty() {
                Some(dropping_tx_hashes.iter().map(|hash| format!("{hash:?}")).collect::<Vec<_>>())
            } else {
                None
            };

            dropping_tx_hashes.hash(state);

            let replacement_uuid = replacement_uuid.map(|uuid| uuid.to_string());
            replacement_uuid.hash(state);

            let refund_percent = refund_percent.map(|percent| percent as u64);
            refund_percent.hash(state);

            refund_recipient.hash(state);

            let refund_tx_hashes = refund_tx_hashes
                .as_ref()
                .map(|hashes| hashes.iter().map(|hash| format!("{hash:?}")).collect::<Vec<_>>());
            refund_tx_hashes.hash(state);
        }

        let mut hasher = wyhash::WyHash::default();
        let mut bytes = [0u8; 32];
        for i in 0..4 {
            hash(self, &mut hasher);
            let hash = hasher.finish();
            bytes[(i * 8)..((i + 1) * 8)].copy_from_slice(&hash.to_be_bytes());
        }

        B256::from(bytes)
    }
}

impl SystemBundle {
    /// Create a new system bundle from a raw bundle and a signer.
    /// Returns an error if the bundle fails to decode.
    pub fn try_from_bundle_and_signer(
        mut bundle: RawBundle,
        signer: Address,
        received_at: UtcInstant,
    ) -> Result<Self, RawBundleConvertError> {
        bundle.signing_address = Some(signer);

        let bundle_hash = bundle.bundle_hash();

        let mut decoded = bundle.clone().decode(TxEncoding::WithBlobData)?.into();

        if let DecodedBundle::Bundle(bundle) = &mut decoded {
            bundle.signer = Some(signer);
        }

        Ok(Self {
            signer,
            raw_bundle: Arc::new(bundle),
            decoded_bundle: Arc::new(decoded),
            bundle_hash,
            received_at,
        })
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

    /// Returns the bundle hash.
    pub fn bundle_hash(&self) -> B256 {
        self.bundle_hash
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

/// A wrapper around ethereum transaction containing decoded information as well as original raw
/// bytes.
#[derive(PartialEq, Eq, Debug, Deref)]
pub struct EthereumTransaction {
    /// Decoded pooled transaction.
    #[deref]
    pub decoded: PooledTransaction,
    /// Original raw transaction bytes.
    pub raw: Bytes,
}

impl EthereumTransaction {
    /// Create new ethereum transaction.
    pub fn new(decoded: PooledTransaction, raw: Bytes) -> Self {
        Self { decoded, raw }
    }
}

/// Internally processed transaction.
#[derive(PartialEq, Eq, Clone, Debug, Deref)]
pub struct SystemTransaction {
    /// Ethereum transaction.
    #[deref]
    pub transaction: Arc<EthereumTransaction>,
    /// The original transaction signer.
    pub signer: Address,
    /// The timestamp at which the bundle has first been seen from the local operator.
    pub received_at: UtcInstant,
}

impl SystemTransaction {
    /// Create a new system transaction from a transaction and a signer.
    pub fn from_transaction_and_signer(
        transaction: EthereumTransaction,
        signer: Address,
        received_at: UtcInstant,
    ) -> Self {
        Self { transaction: Arc::new(transaction), signer, received_at }
    }

    /// Encode the transaction as EIP-2718 encoded bytes.
    pub fn encode(&self) -> Vec<u8> {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [&self.transaction.raw]
        });

        serde_json::to_vec(&json).unwrap()
    }

    pub fn tx_hash(&self) -> B256 {
        *self.transaction.tx_hash()
    }
}

/// The receipt of a bundle received from the system endpoint, to be indexed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BundleReceipt {
    /// The hash of the raw bundle.
    pub bundle_hash: B256,
    /// The time the bundle has been sent, according to the field provided in the JSON-RPC request
    /// header. `None` if the bundle was sent on the user endpoint.
    pub sent_at: Option<UtcDateTime>,
    /// The time the bundle has been received.
    pub received_at: UtcDateTime,
    /// The name of the operator which sent us the bundle.
    pub src_builder_name: String,
    /// The name of the local operator which received the bundle, for indexing
    /// purposes. Can be left unset and will be set by the indexer.
    pub dst_builder_name: Option<String>,
    /// The size in bytes of the payload.
    pub payload_size: u32,
    /// The priority of the bundle.
    pub priority: Priority,
}

/// Decode pooled Ethereum transaction from raw bytes.
pub fn decode_transaction(raw: &Bytes) -> Eip2718Result<PooledTransaction> {
    if raw.is_empty() {
        return Err(Eip2718Error::RlpError(alloy_rlp::Error::InputTooShort));
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

/// Response for the eth_sendBundle and eth_sendRawTransaction methods.
#[derive(Debug, Clone, Copy, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum EthResponse {
    BundleHash(B256),
    #[serde(untagged)]
    TxHash(B256),
}

/// A UTC timestamp along with a monotonic `Instant` to measure elapsed time.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct UtcInstant {
    pub instant: Instant,
    pub utc: UtcDateTime,
}

impl UtcInstant {
    /// Create a new `UtcInstant` from an `Instant` and a `UtcDateTime`.
    pub fn now() -> Self {
        Self { instant: Instant::now(), utc: UtcDateTime::now() }
    }

    pub fn elapsed(&self) -> Duration {
        self.instant.elapsed()
    }
}

impl From<UtcInstant> for UtcDateTime {
    fn from(value: UtcInstant) -> Self {
        value.utc
    }
}

impl From<UtcInstant> for Instant {
    fn from(value: UtcInstant) -> Self {
        value.instant
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_hash_response() {
        let hash = B256::from([1; 32]);
        let response = EthResponse::BundleHash(hash);

        let json = serde_json::to_value(response).unwrap();

        assert_eq!(
            json,
            json!({
                "bundleHash": hash
            })
        );
    }

    #[test]
    fn test_tx_hash_response() {
        let hash = B256::from([1; 32]);
        let response = EthResponse::TxHash(hash);
        let json = serde_json::to_value(response).unwrap();
        assert_eq!(json, json!(hash));
    }
}
