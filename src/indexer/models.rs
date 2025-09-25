//! Contains the model used for storing data inside Clickhouse.

use crate::indexer::serde::{deserialize_vec_u256, hashes, serialize_vec_u256};
use alloy_consensus::Transaction;
use alloy_eips::Typed2718;
use alloy_primitives::{Address, B256, U256};
use time::OffsetDateTime;
use uuid::Uuid;

use crate::types::{DecodedBundle, SystemBundle};

/// Model representing Clickhouse bundle row.
#[derive(Clone, clickhouse::Row, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct BundleRow {
    /// The timestamp at which the bundle was observed.
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub time: OffsetDateTime,
    #[serde(rename = "transactions.hash")]
    /// Collection of hashes for transactions in the bundle.
    pub transactions_hash: Vec<B256>,
    /// Collection of from addresses for transactions in the bundle.
    #[serde(rename = "transactions.from")]
    pub transactions_from: Vec<Address>,
    /// Collection of nonces for transactions in the bundle.
    #[serde(rename = "transactions.nonce")]
    pub transactions_nonce: Vec<u64>,
    /// Collection of signature `r` values for transactions in the bundle.
    #[serde(
        rename = "transactions.r",
        serialize_with = "serialize_vec_u256",
        deserialize_with = "deserialize_vec_u256"
    )]
    pub transactions_r: Vec<U256>,
    /// Collection of signature `s` values for transactions in the bundle.
    #[serde(
        rename = "transactions.s",
        serialize_with = "serialize_vec_u256",
        deserialize_with = "deserialize_vec_u256"
    )]
    pub transactions_s: Vec<U256>,
    /// Collection of signature `v` values for transactions in the bundle.
    #[serde(rename = "transactions.v")]
    pub transactions_v: Vec<u8>,
    /// Collection of to addresses for transactions in the bundle.
    #[serde(rename = "transactions.to")]
    pub transactions_to: Vec<Option<Address>>,
    /// Collection of gas limit values for transactions in the bundle.
    #[serde(rename = "transactions.gas")]
    pub transactions_gas: Vec<u64>,
    /// Collection of transaction types for transactions in the bundle.
    #[serde(rename = "transactions.type")]
    pub transactions_type: Vec<u64>,
    /// Collection of inputs for transactions in the bundle.
    #[serde(rename = "transactions.input")]
    pub transactions_input: Vec<String>,
    /// Collection of values for transactions in the bundle.
    #[serde(
        rename = "transactions.value",
        serialize_with = "serialize_vec_u256",
        deserialize_with = "deserialize_vec_u256"
    )]
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
    pub transactions_access_list: Vec<Option<String>>,
    /// Collection of authorization lists for transactions in the bundle.
    #[serde(rename = "transactions.authorizationList")]
    pub transactions_authorization_list: Vec<Option<String>>,

    /// Bundle block number.
    pub block_number: Option<u64>,
    /// Minimum timestamp for the bundle.
    pub min_timestamp: Option<u64>,
    /// Maximum timestamp for the bundle.
    pub max_timestamp: Option<u64>,

    /// Collection of reverting transaction hashes.
    pub reverting_tx_hashes: Vec<B256>,
    /// Collection of dropping transaction hashes.
    pub dropping_tx_hashes: Vec<B256>,
    /// Collection of refund transaction hashes.
    pub refund_tx_hashes: Vec<B256>,

    /// Bundle uuid.
    #[serde(with = "clickhouse::serde::uuid::option")]
    pub uuid: Option<Uuid>,
    /// Replacement bundle uuid.
    #[serde(with = "clickhouse::serde::uuid::option")]
    pub replacement_uuid: Option<Uuid>,
    pub replacement_nonce: Option<u64>,
    /// Bundle refund percent.
    pub refund_percent: Option<u8>,
    /// Bundle refund recipient.
    pub refund_recipient: Option<Address>,
    /// The signer of the bundle,
    pub signer_address: Option<Address>,
    /// For 2nd price refunds done by buildernet
    pub refund_identity: Option<Address>,

    /// Builder name.
    pub builder_name: String,

    /// The hash of the bundle (unique identifier)
    pub hash: String,
}

/// Adapted from <https://github.com/scpresearch/bundles-forwarder-external/blob/4f13f737f856755df5c39e3e6307f36bff4dd3a9/src/lib.rs#L552-L692>
impl From<(SystemBundle, String)> for BundleRow {
    fn from((bundle, builder_name): (SystemBundle, String)) -> Self {
        let bundle_row = match bundle.decoded_bundle.as_ref() {
            DecodedBundle::Bundle(ref decoded) => {
                BundleRow {
                    time: bundle.received_at.into(),
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
                    transactions_to: decoded.txs.iter().map(|tx| tx.to().map(|t| t)).collect(),
                    transactions_gas: decoded
                        .txs
                        .iter()
                        .map(|tx| tx.as_ref().gas_limit())
                        .collect(),
                    transactions_type: decoded
                        .txs
                        .iter()
                        .map(|tx| tx.as_ref().tx_type() as u64)
                        .collect(),
                    transactions_input: decoded
                        .txs
                        .iter()
                        .map(|tx| alloy_primitives::hex::encode_prefixed(tx.as_ref().input()))
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
                            tx.as_ref().access_list().as_ref().map(|access_list| {
                                serde_json::to_string(&access_list)
                                    .expect("serde_json serialization doesn't fail")
                            })
                        })
                        .collect(),
                    transactions_authorization_list: decoded
                        .txs
                        .iter()
                        .map(|tx| {
                            tx.as_ref().authorization_list().as_ref().map(|access_list| {
                                serde_json::to_string(&access_list)
                                    .expect("serde_json serialization doesn't fail")
                            })
                        })
                        .collect(),
                    block_number: bundle.raw_bundle.block_number.map(|b| b.to::<u64>()),
                    min_timestamp: bundle.raw_bundle.min_timestamp,
                    max_timestamp: bundle.raw_bundle.max_timestamp,
                    reverting_tx_hashes: bundle.raw_bundle.reverting_tx_hashes.clone(),
                    dropping_tx_hashes: bundle.raw_bundle.dropping_tx_hashes.clone(),
                    refund_tx_hashes: bundle
                        .raw_bundle
                        .refund_tx_hashes
                        .clone()
                        .unwrap_or_default(),
                    // Decoded bundles always have a uuid.
                    uuid: Some(decoded.uuid),
                    replacement_uuid: decoded.replacement_data.clone().map(|r| r.key.key().id),
                    replacement_nonce: bundle.raw_bundle.replacement_nonce,
                    signer_address: Some(bundle.signer),
                    builder_name,
                    refund_percent: bundle.raw_bundle.refund_percent,
                    refund_recipient: bundle.raw_bundle.refund_recipient,
                    refund_identity: None,
                    hash: format!("{:?}", decoded.hash),
                }
            }
            // This is in particular a cancellation bundle i.e. a replacement bundle with no
            // transactions.
            DecodedBundle::Replacement(ref replacement) => {
                BundleRow {
                    time: bundle.received_at.into(),
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
                    block_number: None,
                    min_timestamp: None,
                    max_timestamp: None,
                    reverting_tx_hashes: Vec::new(),
                    dropping_tx_hashes: Vec::new(),
                    refund_tx_hashes: Vec::new(),
                    // Cancellation bundles don't have the uuid set.
                    uuid: None,
                    replacement_uuid: Some(replacement.key.key().id),
                    replacement_nonce: bundle.raw_bundle.replacement_nonce,
                    signer_address: Some(bundle.signer),
                    builder_name,
                    refund_percent: bundle.raw_bundle.refund_percent,
                    refund_recipient: bundle.raw_bundle.refund_recipient,
                    refund_identity: None,
                    hash: format!("{:?}", bundle.bundle_hash), // always lowercase
                }
            }
        };

        bundle_row
    }
}

/// Model representing clickhouse private transaction row.
#[derive(clickhouse::Row, Debug, serde::Serialize)]
#[allow(dead_code)]
pub(crate) struct PrivateTxRow {
    /// The timestamp at which private transaction was observed.
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub time: OffsetDateTime,
    /// Transaction hash.
    pub hash: String,
    /// Transaction from address.
    pub from: Address,
    /// Transaction nonce.
    pub nonce: u64,
    /// Signature `r` value.
    pub r: U256,
    /// Signature `s` value.
    pub s: U256,
    /// Signature `v` value.
    pub v: U256,
    /// Transaction to addresses if present.
    pub to: Option<Address>,
    /// Transaction gas limit.
    pub gas: U256,
    /// Transaction type.
    #[serde(rename = "type")]
    pub tx_type: u64,
    /// Transaction input field.
    pub input: String,
    /// Transaction value.
    pub value: U256,
    /// Transaction gas price.
    pub gas_price: Option<U256>,
    /// Transaction max fee per gas value.
    pub max_fee_per_gas: Option<U256>,
    /// Transaction max priority fee per gas value.
    pub max_priority_fee_per_gas: Option<U256>,
    /// Transaction access list.
    pub access_list: Option<String>,
    /// The IP from which the transaction request was observed.
    pub source_ip: Option<String>,
    /// The `Host` value in the header if it exists
    pub host: Option<String>,
    /// Builder name.
    pub builder_name: String,
    /// The score calculated by the forwarder.
    pub forwarder_score: f32,
}

/// Tests to make sure round-trip conversion between raw bundle and clickhouse bundle types is
/// feasible.
#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use alloy_consensus::{
        Signed, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEip7702, TxEnvelope, TxLegacy,
    };
    use alloy_eips::{eip2930::AccessList, eip7702::SignedAuthorization, Encodable2718};
    use alloy_primitives::{hex, Address, Bytes, Signature, TxKind, B256, U256, U64};
    use rbuilder_primitives::serialize::RawBundle;

    use crate::{
        indexer::{self, models::BundleRow},
        types::SystemBundle,
    };

    impl From<BundleRow> for RawBundle {
        fn from(value: BundleRow) -> Self {
            let tx_envelopes = convert_clickhouse_data_to_tx_envelopes(
                value.transactions_hash.clone(),
                value.transactions_nonce.clone(),
                value.transactions_r.clone(),
                value.transactions_s.clone(),
                value.transactions_v.clone(),
                value.transactions_to.clone(),
                value.transactions_gas.clone(),
                value.transactions_type.clone(),
                value.transactions_input.clone(),
                value.transactions_value.clone(),
                value.transactions_gas_price.clone(),
                value.transactions_max_fee_per_gas.clone(),
                value.transactions_max_priority_fee_per_gas.clone(),
                value.transactions_access_list.clone(),
                value.transactions_authorization_list.clone(),
            )
            .unwrap();

            let raw_bundle = RawBundle {
                block_number: value.block_number.map(|b| U64::from(b)),
                min_timestamp: value.min_timestamp,
                max_timestamp: value.max_timestamp,
                txs: tx_envelopes.into_iter().map(|tx| Bytes::from(tx.encoded_2718())).collect(),
                reverting_tx_hashes: value.reverting_tx_hashes.clone(),
                dropping_tx_hashes: value.dropping_tx_hashes.clone(),
                // NOTE: we don't really know whether this was `None` or `Some(vec![])` when it was
                // written, because in Clickhouse we cannot have `Nullable(Array(T))`.
                refund_tx_hashes: Some(value.refund_tx_hashes.clone()),
                // NOTE: we'll always consider this unset, and set the `replacement_uuid` instead.
                uuid: None,
                replacement_uuid: value.replacement_uuid,
                replacement_nonce: value.replacement_nonce,
                refund_percent: value.refund_percent,
                refund_recipient: value.refund_recipient,
                first_seen_at: None,
                version: None,
                signing_address: value.signer_address,
            };

            raw_bundle
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn convert_clickhouse_data_to_tx_envelopes(
        transactions_hash: Vec<B256>,
        transactions_nonce: Vec<u64>,
        transactions_r: Vec<U256>,
        transactions_s: Vec<U256>,
        transactions_v: Vec<u8>,
        transactions_to: Vec<Option<Address>>,
        transactions_gas: Vec<u64>,
        transactions_type: Vec<u64>,
        transactions_input: Vec<String>,
        transactions_value: Vec<U256>,
        transactions_gas_price: Vec<Option<u128>>,
        transactions_max_fee_per_gas: Vec<Option<u128>>,
        transactions_max_priority_fee_per_gas: Vec<Option<u128>>,
        transactions_access_list: Vec<Option<String>>,
        transactions_authorization_list: Vec<Option<String>>,
    ) -> Result<Vec<TxEnvelope>, Box<dyn std::error::Error>> {
        let tx_count = transactions_hash.len();
        let mut envelopes = Vec::with_capacity(tx_count);

        for i in 0..tx_count {
            // Parse signature
            let signature =
                Signature::new(transactions_r[i], transactions_s[i], transactions_v[i] != 0);

            // Parse destination address
            let to = match &transactions_to[i] {
                Some(addr) => TxKind::Call(*addr),
                None => TxKind::Create,
            };

            // Parse input data
            let input = Bytes::from(hex::decode(&transactions_input[i])?);

            // Parse access list if present
            let access_list = match &transactions_access_list[i] {
                Some(al_str) => serde_json::from_str::<AccessList>(al_str)?,
                None => AccessList::default(),
            };

            // Parse authorization list if present
            let authorization_list = match &transactions_authorization_list[i] {
                Some(auth_str) => serde_json::from_str::<Vec<SignedAuthorization>>(auth_str)?,
                None => Vec::new(),
            };

            // Create transaction based on type
            let tx_envelope = match transactions_type[i] {
                0 => {
                    // Legacy transaction
                    let tx = TxLegacy {
                        chain_id: Some(1),
                        nonce: transactions_nonce[i],
                        gas_price: transactions_gas_price[i].unwrap_or(0),
                        gas_limit: transactions_gas[i],
                        to,
                        value: transactions_value[i],
                        input,
                    };
                    TxEnvelope::Legacy(Signed::new_unchecked(tx, signature, transactions_hash[i]))
                }
                1 => {
                    // EIP-2930 transaction
                    let tx = TxEip2930 {
                        chain_id: 1,
                        nonce: transactions_nonce[i],
                        gas_price: transactions_gas_price[i].unwrap_or(0),
                        gas_limit: transactions_gas[i],
                        to,
                        value: transactions_value[i],
                        input,
                        access_list,
                    };
                    TxEnvelope::Eip2930(Signed::new_unchecked(tx, signature, transactions_hash[i]))
                }
                2 => {
                    // EIP-1559 transaction
                    let tx = TxEip1559 {
                        chain_id: 1,
                        nonce: transactions_nonce[i],
                        max_fee_per_gas: transactions_max_fee_per_gas[i].unwrap_or(0),
                        max_priority_fee_per_gas: transactions_max_priority_fee_per_gas[i]
                            .unwrap_or(0),
                        gas_limit: transactions_gas[i],
                        to,
                        value: transactions_value[i],
                        input,
                        access_list,
                    };
                    TxEnvelope::Eip1559(Signed::new_unchecked(tx, signature, transactions_hash[i]))
                }
                3 => {
                    // EIP-4844 transaction (blob transaction)
                    let tx = TxEip4844 {
                        chain_id: 1,
                        nonce: transactions_nonce[i],
                        max_fee_per_gas: transactions_max_fee_per_gas[i].unwrap_or(0),
                        max_priority_fee_per_gas: transactions_max_priority_fee_per_gas[i]
                            .unwrap_or(0),
                        gas_limit: transactions_gas[i],
                        to: match to {
                            TxKind::Call(addr) => addr,
                            TxKind::Create => {
                                return Err(
                                    "EIP-4844 transactions cannot be contract creation".into()
                                )
                            }
                        },
                        value: transactions_value[i],
                        input,
                        access_list,
                        blob_versioned_hashes: Vec::new(),
                        max_fee_per_blob_gas: 0,
                    };
                    TxEnvelope::Eip4844(Signed::new_unchecked(
                        TxEip4844Variant::TxEip4844(tx),
                        signature,
                        transactions_hash[i],
                    ))
                }
                4 => {
                    // EIP-7702 transaction
                    let tx = TxEip7702 {
                        chain_id: 1,
                        nonce: transactions_nonce[i],
                        max_fee_per_gas: transactions_max_fee_per_gas[i].unwrap_or(0),
                        max_priority_fee_per_gas: transactions_max_priority_fee_per_gas[i]
                            .unwrap_or(0),
                        gas_limit: transactions_gas[i],
                        to: match to {
                            TxKind::Call(addr) => addr,
                            TxKind::Create => {
                                return Err(
                                    "EIP-7702 transactions cannot be contract creation".into()
                                )
                            }
                        },
                        value: transactions_value[i],
                        input,
                        access_list,
                        authorization_list,
                    };
                    TxEnvelope::Eip7702(Signed::new_unchecked(tx, signature, transactions_hash[i]))
                }
                _ => {
                    return Err(
                        format!("Unsupported transaction type: {}", transactions_type[i]).into()
                    )
                }
            };

            envelopes.push(tx_envelope);
        }

        Ok(envelopes)
    }

    #[test]
    fn clickhouse_bundle_row_conversion_round_trip_works() {
        let system_bundle = indexer::tests::system_bundle_example();
        let bundle_row: BundleRow = (system_bundle.clone(), "buildernet".to_string()).into();
        let signer = *bundle_row.signer_address.as_ref().unwrap();

        let mut raw_bundle_round_trip: RawBundle = bundle_row.into();
        // For this bundle in particular, ensure it set to `None`, and don't populate it with the
        // value saved from the bundle saved in db.
        raw_bundle_round_trip.signing_address = None;

        assert_eq!(system_bundle.raw_bundle, Arc::new(raw_bundle_round_trip.clone()));
        let system_bundle_round_trip = SystemBundle::try_from_bundle_and_signer(
            raw_bundle_round_trip,
            signer,
            system_bundle.received_at,
        )
        .unwrap();

        assert_eq!(system_bundle, system_bundle_round_trip);
    }

    #[test]
    fn clickhouse_cancel_bundle_row_conversion_round_trip_works() {
        let system_bundle = indexer::tests::system_cancel_bundle_example();
        let bundle_row: BundleRow = (system_bundle.clone(), "buildernet".to_string()).into();
        let signer = *bundle_row.signer_address.as_ref().unwrap();

        let raw_bundle_round_trip: RawBundle = bundle_row.into();

        assert_eq!(system_bundle.raw_bundle, Arc::new(raw_bundle_round_trip.clone()));
        let system_bundle_round_trip = SystemBundle::try_from_bundle_and_signer(
            raw_bundle_round_trip,
            signer,
            system_bundle.received_at,
        )
        .unwrap();

        assert_eq!(system_bundle, system_bundle_round_trip);
    }
}
