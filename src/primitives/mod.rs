use std::{
    hash::{Hash as _, Hasher as _},
    sync::Arc,
    time::{Duration, Instant},
};

use ::time::UtcDateTime;
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
    serialize::{
        CancelShareBundle, RawBundle, RawBundleConvertError, RawBundleDecodeResult,
        RawBundleMetadata, RawShareBundle, RawShareBundleConvertError, RawShareBundleDecodeResult,
        TxEncoding,
    },
    Bundle, BundleReplacementData, ShareBundle,
};
use revm_primitives::B256;
use serde::Serialize;
use serde_json::json;
use uuid::Uuid;

use crate::{
    consts::{ETH_SEND_BUNDLE_METHOD, MEV_SEND_BUNDLE_METHOD},
    priority::Priority,
};

/// Metadata about a [`SystemBundle`].
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SystemBundleMetadata {
    pub signer: Address,
    /// The time at which the bundle has first been seen from the local operator.
    pub received_at: UtcInstant,
    /// The priority of the bundle.
    pub priority: Priority,
}

/// Bundle type that is used for the system API. It contains the verified signer with the original
/// bundle.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SystemBundle {
    /// The inner bundle. Wrapped in [`Arc`] to make cloning cheaper.
    pub raw_bundle: Arc<RawBundle>,
    /// The decoded bundle.
    pub decoded_bundle: Arc<DecodedBundle>,
    /// Metadata about the bundle.
    pub metadata: SystemBundleMetadata,
}

#[derive(Debug, thiserror::Error)]
pub enum SystemBundleDecodingError {
    #[error(transparent)]
    RawBundleConvertError(#[from] RawBundleConvertError),
    #[error("bundle contains too many transactions")]
    TooManyTransactions,
}

/// Decoded bundle type. Either a new, full bundle or an empty replacement bundle.
#[allow(clippy::large_enum_variant)]
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum DecodedBundle {
    /// A new, full bundle.
    Bundle(Bundle),
    /// A replacement bundle.
    EmptyReplacement(BundleReplacementData),
}

impl From<RawBundleDecodeResult> for DecodedBundle {
    fn from(value: RawBundleDecodeResult) -> Self {
        match value {
            RawBundleDecodeResult::NewBundle(bundle) => Self::Bundle(bundle),
            RawBundleDecodeResult::CancelBundle(replacement_data) => {
                Self::EmptyReplacement(replacement_data)
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
                txs,
                metadata:
                    RawBundleMetadata {
                        reverting_tx_hashes,
                        dropping_tx_hashes,
                        replacement_uuid,
                        refund_percent,
                        refund_recipient,
                        refund_tx_hashes,
                        uuid,
                        replacement_nonce,
                        refund_identity,
                        version,
                        min_timestamp,
                        max_timestamp,
                        delayed_refund,
                        block_number,
                        signing_address: _,
                        // NOTE: If we call `hash`, this should not be set.
                        bundle_hash: _,
                    },
            } = bundle;

            if let Some(block_number) = block_number {
                block_number.hash(state);
            }

            if !txs.is_empty() {
                txs.hash(state);
            }

            if !reverting_tx_hashes.is_empty() {
                reverting_tx_hashes.hash(state);
            }

            if !dropping_tx_hashes.is_empty() {
                dropping_tx_hashes.hash(state);
            }

            // NOTE: Use uuid field if it is set, otherwise use replacement_uuid.
            let replacement_uuid = uuid.or(*replacement_uuid).map(|uuid| uuid.to_string());
            if let Some(replacement_uuid) = replacement_uuid {
                replacement_uuid.hash(state);
                if let Some(replacement_nonce) = replacement_nonce {
                    // NOTE: By the time this is called, we expect the replacement nonce to be set
                    // at least on the user endpoint. We have no guarantees however on the system
                    // endpoint.
                    replacement_nonce.hash(state);
                } else {
                    tracing::warn!("Expected replacement_nonce along with uuid/replacement_uuid for calculating bundle hash");
                }
            }

            if let Some(refund_percent) = refund_percent.map(|percent| percent as u64) {
                refund_percent.hash(state);
            }

            if let Some(refund_recipient) = refund_recipient {
                refund_recipient.hash(state);
            }

            if let Some(refund_tx_hashes) = refund_tx_hashes {
                if !refund_tx_hashes.is_empty() {
                    refund_tx_hashes.hash(state);
                }
            }

            if let Some(refund_identity) = refund_identity {
                refund_identity.hash(state);
            }

            if let Some(version) = version {
                version.hash(state);
            }

            if let Some(min_timestamp) = min_timestamp {
                min_timestamp.hash(state);
            }

            if let Some(max_timestamp) = max_timestamp {
                max_timestamp.hash(state);
            }

            if let Some(delayed_refund) = delayed_refund {
                delayed_refund.hash(state);
            }
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

impl BundleHash for RawShareBundle {
    fn bundle_hash(&self) -> B256 {
        fn hash(bundle: &RawShareBundle, state: &mut wyhash::WyHash) {
            let RawShareBundle { version, inclusion, body, validity, metadata, replacement_uuid } =
                bundle;

            version.hash(state);

            inclusion.block.to::<u64>().hash(state);
            if let Some(max_block) = &inclusion.max_block {
                max_block.to::<u64>().hash(state);
            }

            for entry in body {
                if let Some(tx) = &entry.tx {
                    tx.hash(state);
                }
                entry.can_revert.hash(state);
                if let Some(mode) = &entry.revert_mode {
                    mode.hash(state);
                }
                if let Some(bundle) = &entry.bundle {
                    bundle.hash(state);
                }
            }

            if let Some(validity) = validity {
                validity.hash(state);
            }

            if let Some(metadata) = metadata {
                metadata.hash(state);
            }

            if let Some(uuid) = replacement_uuid {
                uuid.hash(state);
            }
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

/// Decoder for system bundles with additional constraints.
#[derive(Debug, Clone, Copy)]
pub struct SystemBundleDecoder {
    /// Maximum number of transactions allowed in a bundle.
    pub max_txs_per_bundle: usize,
}

impl Default for SystemBundleDecoder {
    fn default() -> Self {
        Self { max_txs_per_bundle: Self::DEFAULT_MAX_TXS_PER_BUNDLE }
    }
}

impl SystemBundleDecoder {
    /// The maximum number of transactions allowed in a bundle received via `eth_sendBundle`.
    pub const DEFAULT_MAX_TXS_PER_BUNDLE: usize = 100;

    /// Create a new system bundle from a raw bundle and additional data.
    /// Returns an error if the raw bundle fails to decode.
    pub fn try_decode(
        &self,
        bundle: RawBundle,
        metadata: SystemBundleMetadata,
    ) -> Result<SystemBundle, SystemBundleDecodingError> {
        self.try_decode_inner(bundle, metadata, None::<fn(B256) -> Option<Address>>)
    }

    /// Create a new system bundle from a raw bundle and additional data, using a signer lookup
    /// function for the transaction signers.
    pub fn try_decode_with_lookup(
        &self,
        bundle: RawBundle,
        metadata: SystemBundleMetadata,
        lookup: impl Fn(B256) -> Option<Address>,
    ) -> Result<SystemBundle, SystemBundleDecodingError> {
        self.try_decode_inner(bundle, metadata, Some(lookup))
    }

    /// Create a new system bundle from a raw bundle and additional data, using a signer lookup
    /// function for the transaction signers. Returns an error if the raw bundle fails to decode.
    fn try_decode_inner(
        &self,
        bundle: RawBundle,
        metadata: SystemBundleMetadata,
        lookup: Option<impl Fn(B256) -> Option<Address>>,
    ) -> Result<SystemBundle, SystemBundleDecodingError> {
        if bundle.txs.len() > self.max_txs_per_bundle {
            return Err(SystemBundleDecodingError::TooManyTransactions);
        }

        let mut decoded = if let Some(lookup) = lookup {
            bundle.clone().decode_with_signer_lookup(TxEncoding::WithBlobData, lookup)?.into()
        } else {
            bundle.clone().decode(TxEncoding::WithBlobData)?.into()
        };

        if let DecodedBundle::Bundle(bundle) = &mut decoded {
            bundle.signer = Some(metadata.signer);
        }

        Ok(SystemBundle {
            raw_bundle: Arc::new(bundle),
            decoded_bundle: Arc::new(decoded),
            metadata,
        })
    }
}

impl SystemBundle {
    /// Returns `true` if the bundle is a replacement.
    pub fn is_replacement(&self) -> bool {
        matches!(self.decoded_bundle.as_ref(), DecodedBundle::EmptyReplacement(_))
    }

    /// Returns the bundle UUID if it is a bundle, otherwise the replacement UUID.
    pub fn uuid(&self) -> Uuid {
        match self.decoded_bundle.as_ref() {
            DecodedBundle::Bundle(bundle) => bundle.uuid,
            DecodedBundle::EmptyReplacement(replacement_data) => replacement_data.key.key().id,
        }
    }

    /// Returns the bundle hash.
    pub fn bundle_hash(&self) -> B256 {
        // Use the provided bundle hash if available, otherwise compute it.
        self.raw_bundle.metadata.bundle_hash.unwrap_or_else(|| self.raw_bundle.bundle_hash())
    }

    /// Returns the bundle if it is a new bundle.
    pub fn bundle(&self) -> Option<&Bundle> {
        match self.decoded_bundle.as_ref() {
            DecodedBundle::Bundle(bundle) => Some(bundle),
            DecodedBundle::EmptyReplacement(_) => None,
        }
    }

    /// Returns the replacement data if it is a replacement bundle.
    pub fn replacement_data(&self) -> Option<&BundleReplacementData> {
        match self.decoded_bundle.as_ref() {
            DecodedBundle::EmptyReplacement(replacement_data) => Some(replacement_data),
            DecodedBundle::Bundle(_) => None,
        }
    }

    /// Returns `true` if the bundle is an empty replacement bundle.
    pub fn is_empty(&self) -> bool {
        matches!(self.decoded_bundle.as_ref(), DecodedBundle::EmptyReplacement(_))
    }

    /// Encode the system bundle in a JSON-RPC payload with params EIP-2718 encoded bytes.
    pub fn encode(self) -> WithEncoding<Self> {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": ETH_SEND_BUNDLE_METHOD,
            "params": [self.raw_bundle]
        });

        let encoding = serde_json::to_vec(&json).expect("to JSON serialize bundle");
        WithEncoding { inner: self, encoding: Arc::new(encoding) }
    }
}

#[derive(Debug, Clone)]
/// Metadata about a raw order received from the system endpoint.
pub struct RawOrderMetadata {
    pub priority: Priority,
    pub received_at: UtcInstant,
    pub hash: B256,
}

/// Decoded MEV Share bundle.
#[allow(clippy::large_enum_variant)]
#[derive(PartialEq, Eq, Clone, Debug)]
pub enum DecodedShareBundle {
    /// New bundle.
    New(ShareBundle),
    /// Bundle cancellation.
    Cancel(CancelShareBundle),
}

impl DecodedShareBundle {
    /// Create new decoded share bundle from [`RawShareBundleDecodeResult`].
    pub fn from_result(bundle: RawShareBundleDecodeResult) -> Self {
        match bundle {
            RawShareBundleDecodeResult::NewShareBundle(bundle) => Self::New(*bundle),
            RawShareBundleDecodeResult::CancelShareBundle(bundle) => Self::Cancel(bundle),
        }
    }
}

/// A wrapper around MEV share bundle.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct SystemMevShareBundle {
    /// The decoded MEV Share bundle.
    pub decoded: Arc<DecodedShareBundle>,

    /// The raw bundle.
    pub raw: Arc<RawShareBundle>,

    /// Signer address.
    pub signer: Address,

    /// The bundle hash.
    pub bundle_hash: B256,

    /// The priority of the bundle.
    pub priority: Priority,

    /// The timestamp at which the bundle has first been seen from the local operator.
    pub received_at: UtcInstant,
}

impl SystemMevShareBundle {
    /// Create a new system bundle from a raw bundle and a signer.
    /// Returns an error if the bundle fails to decode.
    pub fn try_from_bundle_and_signer(
        raw: RawShareBundle,
        signer: Address,
        received_at: UtcInstant,
        priority: Priority,
    ) -> Result<Self, RawShareBundleConvertError> {
        let decoded =
            DecodedShareBundle::from_result(raw.clone().decode(TxEncoding::WithBlobData)?);
        let bundle_hash = raw.bundle_hash();
        Ok(Self {
            decoded: Arc::new(decoded),
            raw: Arc::new(raw),
            signer,
            bundle_hash,
            received_at,
            priority,
        })
    }

    /// Returns the bundle hash.
    pub fn bundle_hash(&self) -> B256 {
        self.bundle_hash
    }
}

impl SystemMevShareBundle {
    /// Encode the inner bundle (no signer).
    pub fn encode(self) -> WithEncoding<Self> {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": MEV_SEND_BUNDLE_METHOD,
            "params": [self.raw]
        });

        let encoding = serde_json::to_vec(&json).expect("to JSON serialize bundle");
        WithEncoding { inner: self, encoding: Arc::new(encoding) }
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
    pub priority: Priority,
}

impl SystemTransaction {
    /// Create a new system transaction from a transaction and additional context data.
    pub fn from_transaction(
        transaction: EthereumTransaction,
        signer: Address,
        received_at: UtcInstant,
        priority: Priority,
    ) -> Self {
        Self { transaction: Arc::new(transaction), signer, received_at, priority }
    }

    /// Encode the system transaction in a JSON-RPC payload with params EIP-2718 encoded bytes.
    pub fn encode(self) -> WithEncoding<SystemTransaction> {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_sendRawTransaction",
            "params": [self.transaction.raw]
        });

        let encoding = serde_json::to_vec(&json).expect("to JSON serialize transaction");
        WithEncoding { inner: self, encoding: Arc::new(encoding) }
    }

    pub fn tx_hash(&self) -> B256 {
        *self.transaction.tx_hash()
    }
}

/// The receipt of a bundle received from the system endpoint, to be indexed.
#[derive(Debug, Clone, PartialEq, Eq)]
/// The hash of the raw bundle.
pub struct BundleReceipt {
    pub bundle_hash: B256,
    /// The time the bundle has been sent, according to the field provided in the JSON-RPC request
    /// header. `None` if the bundle was sent on the user endpoint.
    pub sent_at: Option<UtcDateTime>,
    /// The time the bundle has been received.
    pub received_at: UtcDateTime,
    /// The name of the operator which sent us the bundle.
    pub src_builder_name: String,
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

impl EthResponse {
    /// Returns the hash contained in the response.
    pub fn hash(&self) -> B256 {
        match self {
            EthResponse::BundleHash(hash) => *hash,
            EthResponse::TxHash(hash) => *hash,
        }
    }
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

/// A wrapper around a type `T` that includes its encoding (e.g. JSON-RPC) as bytes.
#[derive(Debug, Clone)]
pub struct WithEncoding<T> {
    pub inner: T,
    pub encoding: Arc<Vec<u8>>,
}

/// An order that can be either a bundle or a transaction, along with its JSON-RPC encoding, ready
/// to be sent on the wire.
#[derive(Debug, Clone)]
pub enum EncodedOrder {
    /// Raw order bytes received from the system endpoint, already ready to be forwarded.
    SystemOrder(WithEncoding<RawOrderMetadata>),
    /// A bundle along with its JSON-RPC encoding.
    Bundle(WithEncoding<SystemBundle>),
    /// A MEV Share bundle along with its JSON-RPC encoding.
    MevShareBundle(WithEncoding<SystemMevShareBundle>),
    /// A transaction along with its JSON-RPC encoding.
    Transaction(WithEncoding<SystemTransaction>),
}

impl EncodedOrder {
    /// Returns the JSON-RPC encoding of the order.
    pub fn encoding(&self) -> &[u8] {
        match self {
            EncodedOrder::SystemOrder(order) => &order.encoding,
            EncodedOrder::Bundle(bundle) => &bundle.encoding,
            EncodedOrder::MevShareBundle(bundle) => &bundle.encoding,
            EncodedOrder::Transaction(tx) => &tx.encoding,
        }
    }

    /// Returns the priority of the order.
    pub fn priority(&self) -> Priority {
        match self {
            EncodedOrder::SystemOrder(order) => order.inner.priority,
            EncodedOrder::Bundle(bundle) => bundle.inner.metadata.priority,
            EncodedOrder::MevShareBundle(bundle) => bundle.inner.priority,
            EncodedOrder::Transaction(tx) => tx.inner.priority,
        }
    }

    pub fn received_at(&self) -> UtcInstant {
        match self {
            EncodedOrder::SystemOrder(order) => order.inner.received_at,
            EncodedOrder::Bundle(bundle) => bundle.inner.metadata.received_at,
            EncodedOrder::MevShareBundle(bundle) => bundle.inner.received_at,
            EncodedOrder::Transaction(tx) => tx.inner.received_at,
        }
    }

    /// Returns the type of the order.
    pub fn order_type(&self) -> &'static str {
        match self {
            EncodedOrder::SystemOrder(_) => "system",
            EncodedOrder::Bundle(_) => "bundle",
            EncodedOrder::MevShareBundle(_) => "mev_share_bundle",
            EncodedOrder::Transaction(_) => "transaction",
        }
    }

    /// Returns the hash of the order.
    pub fn hash(&self) -> B256 {
        match self {
            EncodedOrder::SystemOrder(order) => order.inner.hash,
            EncodedOrder::Bundle(bundle) => bundle.inner.bundle_hash(),
            EncodedOrder::MevShareBundle(bundle) => bundle.inner.bundle_hash(),
            EncodedOrder::Transaction(tx) => tx.inner.tx_hash(),
        }
    }
}

impl From<WithEncoding<SystemBundle>> for EncodedOrder {
    fn from(value: WithEncoding<SystemBundle>) -> Self {
        Self::Bundle(value)
    }
}

impl From<WithEncoding<SystemMevShareBundle>> for EncodedOrder {
    fn from(value: WithEncoding<SystemMevShareBundle>) -> Self {
        Self::MevShareBundle(value)
    }
}

impl From<WithEncoding<SystemTransaction>> for EncodedOrder {
    fn from(value: WithEncoding<SystemTransaction>) -> Self {
        Self::Transaction(value)
    }
}

/// A trait for types that can be sampled (simple modulo based sampling).
pub trait Samplable {
    fn sample(&self, every: usize) -> bool;
}

impl Samplable for B256 {
    /// Sample the hash if the first 8 bytes are divisible by the given number.
    #[inline]
    fn sample(&self, every: usize) -> bool {
        let mut first = [0; 8];
        first.copy_from_slice(&self.0[..8]);
        let every = every as u64;
        u64::from_be_bytes(first) % every == 0
    }
}

/// Contains information about another proxy peer instance, as returned by the `infoz` endpoint.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerProxyInfo {
    /// The port where the peer receives TCP-only, system flow.
    pub system_api_port: u16,
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

    #[test]
    fn too_many_txs_error() {
        let decoder = SystemBundleDecoder::default();
        let raw_bundle = RawBundle {
            txs: vec![Bytes::from(vec![0u8; 8]); decoder.max_txs_per_bundle + 1],
            metadata: RawBundleMetadata {
                version: None,
                block_number: None,
                reverting_tx_hashes: vec![],
                dropping_tx_hashes: vec![],
                replacement_uuid: None,
                uuid: None,
                signing_address: None,
                refund_identity: None,
                min_timestamp: None,
                max_timestamp: None,
                replacement_nonce: None,
                refund_percent: None,
                refund_recipient: None,
                refund_tx_hashes: None,
                delayed_refund: None,
                bundle_hash: None,
            },
        };

        let metadata = SystemBundleMetadata {
            signer: Address::ZERO,
            received_at: UtcInstant::now(),
            priority: Priority::Medium,
        };

        // This should be the first reason decoding of such garbage data fails.
        let result = decoder.try_decode(raw_bundle.clone(), metadata.clone());
        assert!(matches!(result, Err(SystemBundleDecodingError::TooManyTransactions)));
    }
}
