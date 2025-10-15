//! Contains the model used for storing data inside Clickhouse.

use crate::{
    indexer::{
        click::BuilderName,
        ser::{address, addresses, hash, hashes, u256es},
    },
    primitives::BundleReceipt,
};
use alloy_consensus::Transaction;
use alloy_eips::Typed2718;
use alloy_primitives::{Address, Keccak256, B256, U256};
use alloy_rlp::Encodable;
use rbuilder_primitives::BundleVersion;
use time::OffsetDateTime;
use uuid::Uuid;

use crate::primitives::{DecodedBundle, SystemBundle};

/// Model representing Clickhouse bundle row.
///
/// NOTE: Make sure the fields are in the same order as the columns in the Clickhouse table.
#[derive(Clone, clickhouse::Row, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct BundleRow {
    /// The timestamp at which the bundle was observed.
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub received_at: OffsetDateTime,
    #[serde(rename = "transactions.hash", with = "hashes")]
    /// Collection of hashes for transactions in the bundle.
    pub transactions_hash: Vec<B256>,
    /// Collection of from addresses for transactions in the bundle.
    #[serde(rename = "transactions.from", with = "addresses")]
    pub transactions_from: Vec<Address>,
    /// Collection of nonces for transactions in the bundle.
    #[serde(rename = "transactions.nonce")]
    pub transactions_nonce: Vec<u64>,
    /// Collection of signature `r` values for transactions in the bundle.
    #[serde(rename = "transactions.r", with = "u256es")]
    pub transactions_r: Vec<U256>,
    /// Collection of signature `s` values for transactions in the bundle.
    #[serde(rename = "transactions.s", with = "u256es")]
    pub transactions_s: Vec<U256>,
    /// Collection of signature `v` values for transactions in the bundle.
    #[serde(rename = "transactions.v")]
    pub transactions_v: Vec<u8>,
    /// Collection of to addresses for transactions in the bundle.
    #[serde(rename = "transactions.to", with = "addresses::option")]
    pub transactions_to: Vec<Option<Address>>,
    /// Collection of gas limit values for transactions in the bundle.
    #[serde(rename = "transactions.gas")]
    pub transactions_gas: Vec<u64>,
    /// Collection of transaction types for transactions in the bundle.
    #[serde(rename = "transactions.type")]
    pub transactions_type: Vec<u8>,
    /// Collection of inputs for transactions in the bundle.
    #[serde(rename = "transactions.input")]
    pub transactions_input: Vec<Vec<u8>>,
    /// Collection of values for transactions in the bundle.
    #[serde(rename = "transactions.value", with = "u256es")]
    pub transactions_value: Vec<U256>,
    /// Collection of gas prices for transactions in the bundle.
    #[serde(rename = "transactions.gasPrice")]
    pub transactions_gas_price: Vec<Option<u128>>,
    /// Collection of max fee per gas values for transactions in the bundle.
    #[serde(rename = "transactions.maxFeePerGas")]
    pub transactions_max_fee_per_gas: Vec<Option<u128>>,
    /// Collection of max priority fee per gas values for transactions in the bundle.
    #[serde(rename = "transactions.maxPriorityFeePerGas")]
    pub transactions_max_priority_fee_per_gas: Vec<Option<u128>>,
    /// Collection of access lists for transactions in the bundle.
    #[serde(rename = "transactions.accessList")]
    pub transactions_access_list: Vec<Option<Vec<u8>>>,
    /// Collection of authorization lists for transactions in the bundle.
    #[serde(rename = "transactions.authorizationList")]
    pub transactions_authorization_list: Vec<Option<Vec<u8>>>,
    #[serde(rename = "transactions.raw")]
    pub transactions_raw: Vec<Vec<u8>>,

    /// Bundle block number.
    pub block_number: u64,
    /// Minimum timestamp for the bundle.
    pub min_timestamp: Option<u64>,
    /// Maximum timestamp for the bundle.
    pub max_timestamp: Option<u64>,

    /// Collection of reverting transaction hashes.
    #[serde(with = "hashes")]
    pub reverting_tx_hashes: Vec<B256>,
    /// Collection of dropping transaction hashes.
    #[serde(with = "hashes")]
    pub dropping_tx_hashes: Vec<B256>,
    /// Collection of refund transaction hashes.
    #[serde(with = "hashes")]
    pub refund_tx_hashes: Vec<B256>,

    /// Replacement bundle uuid.
    #[serde(with = "clickhouse::serde::uuid::option")]
    pub replacement_uuid: Option<Uuid>,
    pub replacement_nonce: Option<u64>,
    /// Bundle refund percent.
    pub refund_percent: Option<u8>,
    /// Bundle refund recipient.
    #[serde(with = "address::option")]
    pub refund_recipient: Option<Address>,
    /// Whether the bundle has a delayed refund.
    pub delayed_refund: Option<bool>,
    /// For 2nd price refunds done by buildernet
    #[serde(with = "address::option")]
    pub refund_identity: Option<Address>,

    /// The signer of the bundle,
    #[serde(with = "address::option")]
    pub signer_address: Option<Address>,

    /// The hash of the bundle (unique identifier)
    #[serde(with = "hash")]
    pub hash: B256,
    /// Bundle uuid.
    #[serde(with = "clickhouse::serde::uuid")]
    pub internal_uuid: Uuid,

    /// Builder name.
    pub builder_name: String,
    /// The bundle version, or `None` in case of a replacement bundle with no transactions.
    pub version: u8,
}

/// Adapted from <https://github.com/scpresearch/bundles-forwarder-external/blob/4f13f737f856755df5c39e3e6307f36bff4dd3a9/src/lib.rs#L552-L692>
impl From<(SystemBundle, BuilderName)> for BundleRow {
    fn from((bundle, builder_name): (SystemBundle, BuilderName)) -> Self {
        let bundle_row = match bundle.decoded_bundle.as_ref() {
            DecodedBundle::Bundle(ref decoded) => {
                let micros = bundle.metadata.received_at.utc.microsecond();
                BundleRow {
                    received_at: bundle
                        .metadata
                        .received_at
                        .utc
                        // Needed so that the `BundleRow` created has the same timestamp precision
                        // (micros) as the row written on clickhouse db.
                        .replace_microsecond(micros)
                        .expect("to replace microseconds")
                        .into(),
                    transactions_hash: decoded.txs.iter().map(|tx| tx.hash()).collect(),
                    transactions_from: decoded.txs.iter().map(|tx| tx.signer()).collect(),
                    transactions_nonce: decoded.txs.iter().map(|tx| tx.nonce()).collect(),
                    transactions_r: decoded
                        .txs
                        .iter()
                        .map(|tx| tx.as_ref().signature().r())
                        .collect(),
                    transactions_s: decoded
                        .txs
                        .iter()
                        .map(|tx| tx.as_ref().signature().s())
                        .collect(),
                    transactions_v: decoded
                        .txs
                        .iter()
                        .map(|tx| tx.as_ref().signature().v().into())
                        .collect(),
                    transactions_to: decoded.txs.iter().map(|tx| tx.to()).collect(),
                    transactions_gas: decoded
                        .txs
                        .iter()
                        .map(|tx| tx.as_ref().gas_limit())
                        .collect(),
                    transactions_type: decoded
                        .txs
                        .iter()
                        .map(|tx| tx.as_ref().tx_type() as u8)
                        .collect(),
                    transactions_input: decoded
                        .txs
                        .iter()
                        .map(|tx| tx.as_ref().input().to_vec())
                        .collect(),
                    transactions_value: decoded.txs.iter().map(|tx| tx.value()).collect(),
                    transactions_gas_price: decoded
                        .txs
                        .iter()
                        .map(|tx| tx.as_ref().gas_price())
                        .collect(),
                    transactions_max_fee_per_gas: decoded
                        .txs
                        .iter()
                        .map(|tx| {
                            if tx.is_legacy() {
                                None
                            } else {
                                Some(tx.as_ref().max_fee_per_gas())
                            }
                        })
                        .collect(),
                    transactions_max_priority_fee_per_gas: decoded
                        .txs
                        .iter()
                        .map(|tx| {
                            if tx.is_legacy() {
                                None
                            } else {
                                tx.as_ref().max_priority_fee_per_gas()
                            }
                        })
                        .collect(),
                    transactions_access_list: decoded
                        .txs
                        .iter()
                        .map(|tx| {
                            let access_list = tx.as_ref().access_list()?;

                            if access_list.is_empty() {
                                return None;
                            }
                            let mut buf: Vec<u8> = Vec::new();
                            access_list.encode(&mut buf);
                            Some(buf)
                        })
                        .collect(),
                    transactions_authorization_list: decoded
                        .txs
                        .iter()
                        .map(|tx| {
                            let authorization_list = tx.as_ref().authorization_list()?;
                            let mut buf: Vec<u8> = Vec::new();
                            authorization_list.to_vec().encode(&mut buf);
                            Some(buf)
                        })
                        .collect(),
                    transactions_raw: bundle.raw_bundle.txs.iter().map(|tx| tx.to_vec()).collect(),
                    block_number: bundle
                        .raw_bundle
                        .metadata
                        .block_number
                        .map_or(0, |b| b.to::<u64>()),
                    min_timestamp: bundle.raw_bundle.metadata.min_timestamp,
                    max_timestamp: bundle.raw_bundle.metadata.max_timestamp,
                    reverting_tx_hashes: bundle.raw_bundle.metadata.reverting_tx_hashes.clone(),
                    dropping_tx_hashes: bundle.raw_bundle.metadata.dropping_tx_hashes.clone(),
                    delayed_refund: bundle.raw_bundle.metadata.delayed_refund,
                    refund_tx_hashes: bundle
                        .raw_bundle
                        .metadata
                        .refund_tx_hashes
                        .clone()
                        .unwrap_or_default(),
                    // Decoded bundles always have a uuid.
                    internal_uuid: decoded.uuid,
                    replacement_uuid: decoded.replacement_data.clone().map(|r| r.key.key().id),
                    replacement_nonce: bundle.raw_bundle.metadata.replacement_nonce,
                    signer_address: Some(bundle.metadata.signer),
                    builder_name,
                    refund_percent: bundle.raw_bundle.metadata.refund_percent,
                    refund_recipient: bundle.raw_bundle.metadata.refund_recipient,
                    refund_identity: bundle.raw_bundle.metadata.refund_identity,
                    hash: bundle.bundle_hash,
                    version: match decoded.version {
                        BundleVersion::V1 => 1,
                        BundleVersion::V2 => 2,
                    },
                }
            }
            // This is in particular a cancellation bundle i.e. a replacement bundle with no
            // transactions.
            DecodedBundle::EmptyReplacement(ref replacement) => {
                let micros = bundle.metadata.received_at.utc.microsecond();
                BundleRow {
                    received_at: bundle
                        .metadata
                        .received_at
                        .utc
                        // Needed so that the `BundleRow` created has the same timestamp precision
                        // (micros) as the row written on clickhouse db.
                        .replace_microsecond(micros)
                        .expect("to replace microseconds")
                        .into(),
                    transactions_hash: Vec::new(),
                    transactions_from: Vec::new(),
                    transactions_nonce: Vec::new(),
                    transactions_r: Vec::new(),
                    transactions_s: Vec::new(),
                    transactions_v: Vec::new(),
                    transactions_to: Vec::new(),
                    transactions_gas: Vec::new(),
                    transactions_type: Vec::new(),
                    transactions_input: Vec::new(),
                    transactions_value: Vec::new(),
                    transactions_gas_price: Vec::new(),
                    transactions_max_fee_per_gas: Vec::new(),
                    transactions_max_priority_fee_per_gas: Vec::new(),
                    transactions_access_list: Vec::new(),
                    transactions_authorization_list: Vec::new(),
                    transactions_raw: Vec::new(),
                    block_number: 0,
                    min_timestamp: None,
                    max_timestamp: None,
                    reverting_tx_hashes: Vec::new(),
                    dropping_tx_hashes: Vec::new(),
                    refund_tx_hashes: Vec::new(),
                    // NOTE: For now, replacement bundles don't have a uuid, so we set the
                    // user-provided replacement-uuid instead.
                    internal_uuid: replacement.key.key().id,
                    replacement_uuid: Some(replacement.key.key().id),
                    replacement_nonce: bundle.raw_bundle.metadata.replacement_nonce,
                    signer_address: Some(bundle.metadata.signer),
                    builder_name,
                    delayed_refund: bundle.raw_bundle.metadata.delayed_refund,
                    refund_percent: bundle.raw_bundle.metadata.refund_percent,
                    refund_recipient: bundle.raw_bundle.metadata.refund_recipient,
                    refund_identity: bundle.raw_bundle.metadata.refund_identity,
                    hash: bundle.bundle_hash,
                    // NOTE: For now, replacement bundles don't have a version, so we set v2.
                    version: 2,
                }
            }
        };

        bundle_row
    }
}

/// The clickhouse model representing a [`crate::primitives::BundleReceipt`].
#[derive(Debug, Clone, clickhouse::Row, serde::Serialize, serde::Deserialize)]
pub(crate) struct BundleReceiptRow {
    #[serde(with = "hash")]
    bundle_hash: B256,
    /// The hash of the bundle hash.
    #[serde(with = "hash")]
    double_bundle_hash: B256,
    #[serde(with = "clickhouse::serde::time::datetime64::micros::option")]
    sent_at: Option<OffsetDateTime>,
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    received_at: OffsetDateTime,
    /// The name of the local operator indexing this data.
    dst_builder_name: String,
    src_builder_name: String,
    payload_size: u32,
    priority: u8,
}

impl From<(BundleReceipt, BuilderName)> for BundleReceiptRow {
    fn from((receipt, dst_builder_name): (BundleReceipt, BuilderName)) -> Self {
        let mut hasher = Keccak256::new();
        hasher.update(receipt.bundle_hash);
        let micros = receipt.received_at.microsecond();

        BundleReceiptRow {
            bundle_hash: receipt.bundle_hash,
            double_bundle_hash: hasher.finalize(),
            sent_at: receipt
                .sent_at
                .map(|dt| dt.replace_microsecond(micros).expect("to replace microseconds").into()),
            received_at: receipt
                .received_at
                .replace_microsecond(micros)
                .expect("to replace microseconds")
                .into(),
            dst_builder_name,
            src_builder_name: receipt.src_builder_name,
            payload_size: receipt.payload_size,
            priority: receipt.priority as u8,
        }
    }
}

/// Tests to make sure round-trip conversion between raw bundle and clickhouse bundle types is
/// feasible.
#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use alloy_primitives::{Bytes, U64};
    use rbuilder_primitives::serialize::{RawBundle, RawBundleMetadata};

    use crate::{
        indexer::{
            self,
            click::models::{BundleReceiptRow, BundleRow},
            tests::bundle_receipt_example,
        },
        primitives::{BundleReceipt, SystemBundle},
        priority::Priority,
    };

    impl From<BundleRow> for RawBundle {
        fn from(value: BundleRow) -> Self {
            RawBundle {
                txs: value.transactions_raw.into_iter().map(Bytes::from).collect(),
                metadata: RawBundleMetadata {
                    bundle_hash: Some(value.hash),
                    block_number: Some(U64::from(value.block_number)),
                    min_timestamp: value.min_timestamp,
                    max_timestamp: value.max_timestamp,
                    reverting_tx_hashes: value.reverting_tx_hashes.clone(),
                    dropping_tx_hashes: value.dropping_tx_hashes.clone(),
                    // NOTE: we don't really know whether this was `None` or `Some(vec![])` when it
                    // was written, because in Clickhouse we cannot have
                    // `Nullable(Array(T))`.
                    refund_tx_hashes: Some(value.refund_tx_hashes.clone()),
                    // NOTE: we'll always consider this unset, and set the `replacement_uuid`
                    // instead.
                    uuid: None,
                    replacement_uuid: value.replacement_uuid,
                    replacement_nonce: value.replacement_nonce,
                    refund_percent: value.refund_percent,
                    refund_recipient: value.refund_recipient,
                    refund_identity: value.refund_identity,
                    version: if value.version == 1 {
                        Some("v1".to_string())
                    } else {
                        Some("v2".to_string())
                    },
                    signing_address: value.signer_address,
                    delayed_refund: None,
                },
            }
        }
    }

    impl From<u8> for Priority {
        fn from(value: u8) -> Self {
            match value {
                0 => Priority::High,
                1 => Priority::Medium,
                2 => Priority::Low,
                _ => panic!("invalid priority value: {}", value),
            }
        }
    }

    impl From<BundleReceiptRow> for BundleReceipt {
        fn from(value: BundleReceiptRow) -> Self {
            BundleReceipt {
                bundle_hash: value.bundle_hash,
                sent_at: value.sent_at.map(|dt| dt.into()),
                received_at: value.received_at.into(),
                src_builder_name: value.src_builder_name,
                payload_size: value.payload_size,
                priority: value.priority.into(),
            }
        }
    }

    #[test]
    fn clickhouse_bundle_row_conversion_round_trip_works() {
        let system_bundle = indexer::tests::system_bundle_example();
        println!("Bundle hash: {:?}", system_bundle.bundle_hash());
        let bundle_row: BundleRow = (system_bundle.clone(), "buildernet".to_string()).into();
        println!("Bundle row hash: {:?}", bundle_row.hash);

        let mut raw_bundle_round_trip: RawBundle = bundle_row.into();
        raw_bundle_round_trip.metadata.signing_address = None;

        assert_eq!(system_bundle.raw_bundle, Arc::new(raw_bundle_round_trip.clone()));

        let system_bundle_round_trip =
            SystemBundle::try_decode(raw_bundle_round_trip, system_bundle.metadata.clone())
                .unwrap();

        assert_eq!(system_bundle, system_bundle_round_trip);
    }

    #[test]
    fn clickhouse_cancel_bundle_row_conversion_round_trip_works() {
        let system_bundle = indexer::tests::system_cancel_bundle_example();
        let bundle_row: BundleRow = (system_bundle.clone(), "buildernet".to_string()).into();

        let raw_bundle_round_trip: RawBundle = bundle_row.into();

        assert_eq!(system_bundle.raw_bundle, Arc::new(raw_bundle_round_trip.clone()));
        let system_bundle_round_trip =
            SystemBundle::try_decode(raw_bundle_round_trip, system_bundle.metadata.clone())
                .unwrap();

        assert_eq!(system_bundle, system_bundle_round_trip);
    }

    #[test]
    fn clickhouse_bundle_receipt_row_conversion_round_trip_works() {
        let bundle_receipt = bundle_receipt_example();

        let receipt_row: BundleReceiptRow =
            (bundle_receipt.clone(), "buildernet".to_string()).into();
        let bundle_receipt_round_trip: BundleReceipt = receipt_row.into();

        assert_eq!(bundle_receipt, bundle_receipt_round_trip);
    }
}
