use std::{
    collections::HashMap,
    fmt::Display,
    hash::{Hash, Hasher as _},
    sync::Arc,
    time::{Duration, Instant},
};

use ::time::UtcDateTime;
use alloy_consensus::{
    EthereumTxEnvelope, TxEip4844Variant,
    crypto::RecoveryError,
    transaction::{Recovered, SignerRecoverable as _},
};
use alloy_eips::{
    Decodable2718 as _,
    eip2718::{Eip2718Error, Eip2718Result},
    eip7594::BlobTransactionSidecarVariant,
};
use alloy_primitives::{Address, Bytes, U64};
use bitcode::{Decode, Encode};
use derive_more::{Deref, From};
use openssl::{
    pkey::PKey,
    ssl::{SslAcceptor, SslAcceptorBuilder, SslMethod, SslVerifyMode, SslVersion},
    x509::{X509, store::X509StoreBuilder},
};
use rbuilder_primitives::{
    Bundle, BundleReplacementData,
    serialize::{
        RawBundle, RawBundleConvertError, RawBundleDecodeResult, RawBundleMetadata, TxEncoding,
    },
};
use revm_primitives::{B256, hex};
use serde::Serialize;
use serde_json::json;
use strum::AsRefStr;
use uuid::Uuid;

use crate::{
    builderhub::DEFAULT_TLS_CIPHERS,
    consts::{ETH_SEND_BUNDLE_METHOD, ETH_SEND_RAW_TRANSACTION_METHOD},
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
                    tracing::warn!(
                        "Expected replacement_nonce along with uuid/replacement_uuid for calculating bundle hash"
                    );
                }
            }

            if let Some(refund_percent) = refund_percent.map(|percent| percent as u64) {
                refund_percent.hash(state);
            }

            if let Some(refund_recipient) = refund_recipient {
                refund_recipient.hash(state);
            }

            if let Some(refund_tx_hashes) = refund_tx_hashes
                && !refund_tx_hashes.is_empty()
            {
                refund_tx_hashes.hash(state);
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
            DecodedBundle::EmptyReplacement(replacement_data) => replacement_data.key.id,
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
    pub fn encode(self) -> WithEncoding<SystemBundle> {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": ETH_SEND_BUNDLE_METHOD,
            "params": [self.raw_bundle]
        });

        let encoding = serde_json::to_vec(&json).expect("to JSON serialize bundle");
        WithEncoding { inner: self, encoding: Arc::new(encoding), encoding_tcp_forwarder: None }
    }
}

#[derive(Debug, Clone)]
/// Metadata about a raw order received from the system endpoint.
pub struct RawOrderMetadata {
    pub priority: Priority,
    pub received_at: UtcInstant,
    pub hash: B256,
}

/// A wrapper around ethereum transaction containing decoded information as well as original raw
/// bytes.
#[derive(PartialEq, Eq, Debug, Deref)]
pub struct EthereumTransaction {
    /// Decoded pooled transaction.
    #[deref]
    pub decoded: EthPooledTransaction,
    /// Original raw transaction bytes.
    pub raw: Bytes,
}

impl EthereumTransaction {
    /// Create new ethereum transaction.
    pub fn new(decoded: EthPooledTransaction, raw: Bytes) -> Self {
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
        WithEncoding { inner: self, encoding: Arc::new(encoding), encoding_tcp_forwarder: None }
    }

    pub fn tx_hash(&self) -> B256 {
        *self.transaction.tx_hash()
    }
}

/// An enumeration of the orders that can be sent via system endopints.
#[derive(Debug, Clone, From)]
pub enum SystemOrder {
    Bundle(SystemBundle),
    Transaction(SystemTransaction),
}

impl SystemOrder {
    /// Returns the priority of the order.
    pub fn priority(&self) -> Priority {
        match self {
            SystemOrder::Bundle(bundle) => bundle.metadata.priority,
            SystemOrder::Transaction(tx) => tx.priority,
        }
    }

    pub fn encode(self) -> WithEncoding<SystemOrder> {
        match self {
            SystemOrder::Bundle(bundle) => WithEncoding {
                inner: SystemOrder::Bundle(bundle.clone()),
                encoding: bundle.encode().encoding,
                encoding_tcp_forwarder: None,
            },
            SystemOrder::Transaction(tx) => WithEncoding {
                inner: SystemOrder::Transaction(tx.clone()),
                encoding: tx.encode().encoding,
                encoding_tcp_forwarder: None,
            },
        }
    }

    pub fn method_name(&self) -> &'static str {
        match self {
            SystemOrder::Bundle(_) => ETH_SEND_BUNDLE_METHOD,
            SystemOrder::Transaction(_) => ETH_SEND_RAW_TRANSACTION_METHOD,
        }
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

/// Ethereum pooled transaction that supports both EIP-4844 and EIP-7594 style blob sidecars.
pub type EthPooledTransaction = EthereumTxEnvelope<TxEip4844Variant<BlobTransactionSidecarVariant>>;

/// Decode pooled Ethereum transaction from raw bytes.
pub fn decode_transaction(raw: &[u8]) -> Eip2718Result<EthPooledTransaction> {
    if raw.is_empty() {
        return Err(Eip2718Error::RlpError(alloy_rlp::Error::InputTooShort));
    }
    EthPooledTransaction::decode_2718_exact(raw)
}

/// Recover ECDSA signer of the transaction.
pub fn into_recovered_transaction(
    transaction: EthPooledTransaction,
) -> Result<Recovered<EthPooledTransaction>, RecoveryError> {
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
    /// The encoding to be used for by the TCP forwarder.
    pub encoding_tcp_forwarder: Option<Vec<u8>>,
}

/// An order that can be either a bundle or a transaction, along with its JSON-RPC encoding, ready
/// to be sent on the wire.
#[derive(Debug, Clone)]
pub enum EncodedOrder {
    /// Raw order bytes received from the system endpoint, already ready to be forwarded.
    Raw(WithEncoding<RawOrderMetadata>),
    /// A bundle along with its JSON-RPC encoding.
    Bundle(WithEncoding<SystemBundle>),
    /// A transaction along with its JSON-RPC encoding.
    Transaction(WithEncoding<SystemTransaction>),
}

impl From<WithEncoding<SystemOrder>> for EncodedOrder {
    fn from(value: WithEncoding<SystemOrder>) -> Self {
        match value.inner {
            SystemOrder::Bundle(bundle) => EncodedOrder::Bundle(WithEncoding {
                inner: bundle,
                encoding: value.encoding,
                encoding_tcp_forwarder: value.encoding_tcp_forwarder,
            }),
            SystemOrder::Transaction(tx) => EncodedOrder::Transaction(WithEncoding {
                inner: tx,
                encoding: value.encoding,
                encoding_tcp_forwarder: value.encoding_tcp_forwarder,
            }),
        }
    }
}

impl EncodedOrder {
    /// Returns the JSON-RPC encoding of the order.
    pub fn encoding(&self) -> &[u8] {
        match self {
            EncodedOrder::Raw(order) => &order.encoding,
            EncodedOrder::Bundle(bundle) => &bundle.encoding,
            EncodedOrder::Transaction(tx) => &tx.encoding,
        }
    }

    pub fn encoding_tcp_forwarder(&self) -> Option<&Vec<u8>> {
        match self {
            EncodedOrder::Raw(order) => order.encoding_tcp_forwarder.as_ref(),
            EncodedOrder::Bundle(bundle) => bundle.encoding_tcp_forwarder.as_ref(),
            EncodedOrder::Transaction(tx) => tx.encoding_tcp_forwarder.as_ref(),
        }
    }

    /// Returns the priority of the order.
    pub fn priority(&self) -> Priority {
        match self {
            EncodedOrder::Raw(order) => order.inner.priority,
            EncodedOrder::Bundle(bundle) => bundle.inner.metadata.priority,
            EncodedOrder::Transaction(tx) => tx.inner.priority,
        }
    }

    pub fn received_at(&self) -> UtcInstant {
        match self {
            EncodedOrder::Raw(order) => order.inner.received_at,
            EncodedOrder::Bundle(bundle) => bundle.inner.metadata.received_at,
            EncodedOrder::Transaction(tx) => tx.inner.received_at,
        }
    }

    /// Returns the type of the order.
    pub fn order_type(&self) -> &'static str {
        match self {
            EncodedOrder::Raw(_) => "system",
            EncodedOrder::Bundle(_) => "bundle",
            EncodedOrder::Transaction(_) => "transaction",
        }
    }

    /// Returns the hash of the order.
    pub fn hash(&self) -> B256 {
        match self {
            EncodedOrder::Raw(order) => order.inner.hash,
            EncodedOrder::Bundle(bundle) => bundle.inner.bundle_hash(),
            EncodedOrder::Transaction(tx) => tx.inner.tx_hash(),
        }
    }
}

impl From<WithEncoding<SystemBundle>> for EncodedOrder {
    fn from(value: WithEncoding<SystemBundle>) -> Self {
        Self::Bundle(value)
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize, bitcode::Encode, bitcode::Decode)]
pub struct WithHeaders<T, H = HashMap<String, String>> {
    pub headers: H,
    pub data: T,
}

/// Bitcode-friendly representation of [`RawBundle`].
#[derive(Debug, Clone, PartialEq, Eq, Encode, Decode)]
pub struct RawBundleBitcode {
    pub metadata: RawBundleMetadataBitcode,
    pub txs: Vec<Vec<u8>>, // equivalent to Bytes
}

/// Bitcode-friendly representation of [`RawBundleMetadata`].
#[derive(Debug, Clone, PartialEq, Eq, bitcode::Encode, bitcode::Decode)]
pub struct RawBundleMetadataBitcode {
    pub version: Option<String>,
    pub block_number: Option<u64>,

    pub reverting_tx_hashes: Vec<[u8; 32]>,
    pub dropping_tx_hashes: Vec<[u8; 32]>,

    pub replacement_uuid: Option<[u8; 16]>,
    pub uuid: Option<[u8; 16]>,

    pub signing_address: Option<[u8; 20]>,
    pub refund_identity: Option<[u8; 20]>,

    pub min_timestamp: Option<u64>,
    pub max_timestamp: Option<u64>,
    pub replacement_nonce: Option<u64>,

    pub refund_percent: Option<u8>,
    pub refund_recipient: Option<[u8; 20]>,

    pub refund_tx_hashes: Option<Vec<[u8; 32]>>,
    pub delayed_refund: Option<bool>,

    pub bundle_hash: Option<[u8; 32]>,
}

impl From<&RawBundleMetadata> for RawBundleMetadataBitcode {
    fn from(r: &RawBundleMetadata) -> Self {
        Self {
            version: r.version.clone(),
            block_number: r.block_number.map(|v| v.try_into().expect("U64 fits into u64")),

            reverting_tx_hashes: r.reverting_tx_hashes.clone().into_iter().map(|h| *h).collect(),
            dropping_tx_hashes: r.dropping_tx_hashes.clone().into_iter().map(|h| *h).collect(),

            replacement_uuid: r.replacement_uuid.map(|u| *u.as_bytes()),
            uuid: r.uuid.map(|u| *u.as_bytes()),

            signing_address: r.signing_address.map(|a| a.into()),
            refund_identity: r.refund_identity.map(|a| a.into()),

            min_timestamp: r.min_timestamp,
            max_timestamp: r.max_timestamp,
            replacement_nonce: r.replacement_nonce,

            refund_percent: r.refund_percent,
            refund_recipient: r.refund_recipient.map(|a| a.into()),

            refund_tx_hashes: r
                .refund_tx_hashes
                .clone()
                .map(|hashes| hashes.into_iter().map(|h| *h).collect()),

            delayed_refund: r.delayed_refund,
            bundle_hash: r.bundle_hash.map(|b| b.0),
        }
    }
}

impl From<RawBundleMetadataBitcode> for RawBundleMetadata {
    fn from(r: RawBundleMetadataBitcode) -> Self {
        Self {
            version: r.version,
            block_number: r.block_number.map(U64::from),

            reverting_tx_hashes: r.reverting_tx_hashes.into_iter().map(B256::from).collect(),
            dropping_tx_hashes: r.dropping_tx_hashes.into_iter().map(B256::from).collect(),

            replacement_uuid: r.replacement_uuid.map(Uuid::from_bytes),
            uuid: r.uuid.map(Uuid::from_bytes),

            signing_address: r.signing_address.map(Address::from),
            refund_identity: r.refund_identity.map(Address::from),

            min_timestamp: r.min_timestamp,
            max_timestamp: r.max_timestamp,
            replacement_nonce: r.replacement_nonce,

            refund_percent: r.refund_percent,
            refund_recipient: r.refund_recipient.map(Address::from),

            refund_tx_hashes: r
                .refund_tx_hashes
                .map(|hashes| hashes.into_iter().map(B256::from).collect()),

            delayed_refund: r.delayed_refund,
            bundle_hash: r.bundle_hash.map(B256::from),
        }
    }
}

impl From<&RawBundle> for RawBundleBitcode {
    fn from(r: &RawBundle) -> Self {
        Self {
            metadata: RawBundleMetadataBitcode::from(&r.metadata),
            txs: r.txs.iter().map(|b| b.to_vec()).collect(),
        }
    }
}

impl From<RawBundleBitcode> for RawBundle {
    fn from(r: RawBundleBitcode) -> Self {
        Self {
            metadata: RawBundleMetadata::from(r.metadata),
            txs: r.txs.iter().map(|v| v.clone().into()).collect(),
        }
    }
}

/// The type of data contained in a [`TcpResponse`].
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode, From)]
pub enum TcpReponseType {
    NoData,
    String(String),
    Hash([u8; 32]),
}

impl Display for TcpReponseType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            TcpReponseType::NoData => write!(f, "no data"),
            TcpReponseType::String(s) => write!(f, "{s}"),
            TcpReponseType::Hash(h) => write!(f, "0x{}", hex::encode(h)),
        }
    }
}

/// Status codes for a [`TcpResponse`].
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode, AsRefStr, From, PartialEq, Eq)]
pub enum TcpResponseStatus {
    Success = 0,
    ErrorInternal = 1,
    ErrorDecoding = 2,
    ErrorMissingArguments = 3,
    ErrorInvalidSignature = 4,
    ErrorUnknownArgument = 5,
}

/// A flexible, versioned response structure to use for communication over TCP for the system API.
///
/// To allow for future extensibility, the response contains a [`TcpResponse::status`] code and a
/// [`TcpResponse::data`] tagged field to identify the type of response.
#[derive(Debug, Clone, bitcode::Encode, bitcode::Decode)]
pub struct TcpResponse {
    /// The stauts code of the response. A value of zero is reserved for success.
    pub status: TcpResponseStatus,
    /// The data payload of the response.
    pub data: TcpReponseType,
}

impl TcpResponse {
    /// Create a no-data response with the given status.
    pub fn no_data(status: TcpResponseStatus) -> Self {
        Self { status, data: TcpReponseType::NoData }
    }

    /// Create an acknowledgment response with no data.
    pub fn ack() -> Self {
        Self::no_data(TcpResponseStatus::Success)
    }

    /// Create a success response with the given data.
    pub fn success(data: impl Into<TcpReponseType>) -> Self {
        Self { status: TcpResponseStatus::Success, data: data.into() }
    }

    /// Create an internal error response.
    pub fn error_internal() -> Self {
        Self::no_data(TcpResponseStatus::ErrorInternal)
    }

    /// Create an error response with the given status code and message.
    pub fn error_message(status: TcpResponseStatus, message: impl Into<String>) -> Self {
        Self { status, data: message.into().into() }
    }

    pub fn error_decoding(message: String) -> Self {
        Self::error_message(TcpResponseStatus::ErrorDecoding, message)
    }

    pub fn error_unknown_argument(data: impl Into<TcpReponseType>) -> Self {
        Self { status: TcpResponseStatus::ErrorUnknownArgument, data: data.into() }
    }
}

/// Structure that holds raw PEM bytes of a key and cert pair, with utilities
/// for easily creating a [`SslAcceptorBuilder`] with client authentication (mTLS).
#[derive(Debug, Clone)]
pub struct AcceptorBuilder {
    raw_certificate: Vec<u8>,
}

impl AcceptorBuilder {
    pub fn new(cert: impl Into<Vec<u8>>) -> Self {
        Self { raw_certificate: cert.into() }
    }

    /// Creates a [`SslAcceptorBuilder`] with support for client authentication (mTLS).
    pub fn ssl(&self) -> Result<SslAcceptorBuilder, openssl::error::ErrorStack> {
        let mut acceptor_builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;
        acceptor_builder.set_min_proto_version(Some(SslVersion::TLS1_3))?;
        acceptor_builder.set_ciphersuites(DEFAULT_TLS_CIPHERS)?;

        let cert = X509::from_pem(&self.raw_certificate)?;
        acceptor_builder.set_certificate(&cert)?;

        let key = PKey::private_key_from_pem(&self.raw_certificate)?;
        acceptor_builder.set_private_key(&key)?;

        // acceptor_builder.verify_param_mut().clear_flags(X509VerifyFlags::X509_STRICT)?;
        // acceptor_builder.verify_param_mut().set_flags(X509VerifyFlags::X509_STRICT)?;

        acceptor_builder.set_verify_callback(
            SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT,
            |success, store_ctx| {
                if success {
                    return true;
                }

                // TODO: uncomment this
                // If we don't have enough verbosity, just propagate the error and don't log
                // further.
                // if !tracing::enabled!(tracing::Level::DEBUG) {
                //     return false;
                // }

                let verify_result = store_ctx.error();

                let error_code = verify_result.as_raw();
                let error_string = verify_result.error_string();
                let error_depth = store_ctx.error_depth();

                // TODO: demote this to debug span
                let _span = tracing::info_span!(
                    "openssl_verify",
                    ?error_code,
                    ?error_string,
                    ?error_depth,
                    chain_len = tracing::field::Empty,
                )
                .entered();

                // Get the certificate that caused the error
                if let Some(cert) = store_ctx.current_cert() {
                    let subject_name = cert.subject_name();
                    tracing::error!(?subject_name, "failed");
                } else {
                    tracing::error!("failed and no certificate relevant to error");
                }

                if let Some(chain) = store_ctx.chain()
                    && chain.len() > 1
                {
                    _span.record("chain_len", chain.len());
                    for (idx, cert) in chain.iter().enumerate() {
                        let subject = cert.subject_name();
                        tracing::debug!(?idx, ?subject, "certificate");
                    }
                }

                false
            },
        );

        Ok(acceptor_builder)
    }
}

/// Extension trait to override certificate store with the provided root certificates.
pub trait SslAcceptorBuilderExt: Sized {
    fn add_trusted_certs(self, certs: Vec<X509>) -> Result<Self, openssl::error::ErrorStack>;
}

impl SslAcceptorBuilderExt for SslAcceptorBuilder {
    /// Replaces current [`X509StoreBuilder`] with one created using these trusted certificates.
    fn add_trusted_certs(mut self, certs: Vec<X509>) -> Result<Self, openssl::error::ErrorStack> {
        let mut i = 0;
        let len = certs.len();
        let mut store_builder = X509StoreBuilder::new()?;
        for cert in certs.into_iter() {
            if let Err(e) = store_builder.add_cert(cert) {
                tracing::error!(?e, "failed to add trusted cert");
            }
            i += 1;
        }
        self.set_verify_cert_store(store_builder.build())?;
        tracing::info!(certs = len, added = i, "added certs to store");
        Ok(self)
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
