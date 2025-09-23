use alloy_consensus::Transaction;
use alloy_eips::Typed2718;
use alloy_primitives::U256;
use clickhouse_derive::Row;
use time::OffsetDateTime;

use crate::types::{DecodedBundle, IndexableSystemBundle};

/// Model representing clickhouse bundle row.
#[derive(Clone, Row, Debug, serde::Serialize, serde::Deserialize)]
#[cfg_attr(test, derive(PartialEq, Eq))]
pub(crate) struct BundleRow {
    /// The timestamp at which the bundle was observed.
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub time: OffsetDateTime,
    #[serde(rename = "transactions.hash")]
    /// Collection of hashes for transactions in the bundle.
    pub transactions_hash: Vec<String>,
    /// Collection of from addresses for transactions in the bundle.
    #[serde(rename = "transactions.from")]
    pub transactions_from: Vec<String>,
    /// Collection of nonces for transactions in the bundle.
    #[serde(rename = "transactions.nonce")]
    pub transactions_nonce: Vec<u64>,
    /// Collection of signature `r` values for transactions in the bundle.
    #[serde(rename = "transactions.r")]
    pub transactions_r: Vec<U256>,
    /// Collection of signature `s` values for transactions in the bundle.
    #[serde(rename = "transactions.s")]
    pub transactions_s: Vec<U256>,
    /// Collection of signature `v` values for transactions in the bundle.
    #[serde(rename = "transactions.v")]
    pub transactions_v: Vec<u8>,
    /// Collection of to addresses for transactions in the bundle.
    #[serde(rename = "transactions.to")]
    pub transactions_to: Vec<Option<String>>,
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
    #[serde(rename = "transactions.value")]
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
    pub block_number: u64,
    /// Minimum timestamp for the bundle.
    pub min_timestamp: u64,
    /// Maximum timestamp for the bundle.
    pub max_timestamp: u64,

    /// Collection of reverting transaction hashes.
    pub reverting_tx_hashes: Vec<String>,
    /// Collection of dropping transaction hashes.
    pub dropping_tx_hashes: Vec<String>,
    /// Collection of refund transaction hashes.
    pub refund_tx_hashes: Vec<String>,

    /// Bundle uuid.
    pub uuid: Option<String>,
    pub replacement_nonce: Option<u64>,
    /// Bundle refund percent.
    pub refund_percent: Option<u8>,
    /// Bundle refund recipient.
    pub refund_recipient: Option<String>,
    /// The signer of the bundle,
    pub signer_address: Option<String>,
    /// For 2nd price refunds done by buildernet
    pub refund_identity: Option<String>,

    /// Builder name.
    pub builder_name: String,

    /// The hash of the bundle (unique identifier)
    pub hash: String,
}

/// Adapted from <https://github.com/scpresearch/bundles-forwarder-external/blob/4f13f737f856755df5c39e3e6307f36bff4dd3a9/src/lib.rs#L552-L692>
impl From<IndexableSystemBundle> for BundleRow {
    fn from(indexable_bundle: IndexableSystemBundle) -> Self {
        let IndexableSystemBundle { system_bundle: bundle, timestamp, builder_name } =
            indexable_bundle;
        let DecodedBundle::Bundle(ref decoded) = bundle.decoded_bundle.as_ref() else {
            unreachable!("expecting decoded bundle")
        };

        let bundle = BundleRow {
            // Ensure microsecond accuracy
            time: timestamp.replace_nanosecond(0).unwrap().into(),
            transactions_hash: decoded.txs.iter().map(|tx| format!("{:?}", tx.hash())).collect(),
            transactions_from: decoded.txs.iter().map(|tx| format!("{:?}", tx.signer())).collect(),
            transactions_nonce: decoded.txs.iter().map(|tx| tx.nonce()).collect(),
            transactions_r: decoded.txs.iter().map(|tx| tx.as_ref().signature().r()).collect(),
            transactions_s: decoded.txs.iter().map(|tx| tx.as_ref().signature().s()).collect(),
            transactions_v: decoded
                .txs
                .iter()
                .map(|tx| tx.as_ref().signature().v().into())
                .collect(),
            transactions_to: decoded
                .txs
                .iter()
                .map(|tx| tx.to().map(|t| format!("{t:?}")))
                .collect(),
            transactions_gas: decoded.txs.iter().map(|tx| tx.as_ref().gas_limit()).collect(),
            transactions_type: decoded.txs.iter().map(|tx| tx.as_ref().tx_type() as u64).collect(),
            transactions_input: decoded
                .txs
                .iter()
                .map(|tx| alloy_primitives::hex::encode_prefixed(tx.as_ref().input()))
                .collect(),
            transactions_value: decoded.txs.iter().map(|tx| tx.value()).collect(),
            transactions_gas_price: decoded.txs.iter().map(|tx| tx.as_ref().gas_price()).collect(),
            transactions_max_fee_per_gas: decoded
                .txs
                .iter()
                .map(|tx| if tx.is_legacy() { None } else { Some(tx.as_ref().max_fee_per_gas()) })
                .collect(),
            transactions_max_priority_fee_per_gas: decoded
                .txs
                .iter()
                .map(
                    |tx| {
                        if tx.is_legacy() {
                            None
                        } else {
                            tx.as_ref().max_priority_fee_per_gas()
                        }
                    },
                )
                .collect(),
            transactions_access_list: decoded
                .txs
                .iter()
                .map(|tx| {
                    tx.as_ref()
                        .access_list()
                        .as_ref()
                        .map(|access_list| serde_json::to_string(&access_list).unwrap())
                })
                .collect(),
            transactions_authorization_list: decoded
                .txs
                .iter()
                .map(|tx| {
                    tx.as_ref()
                        .authorization_list()
                        .as_ref()
                        .map(|access_list| serde_json::to_string(&access_list).unwrap())
                })
                .collect(),
            block_number: bundle.raw_bundle.block_number.unwrap_or_default().to::<u64>(),
            min_timestamp: bundle.raw_bundle.min_timestamp.unwrap_or_default(),
            max_timestamp: bundle.raw_bundle.max_timestamp.unwrap_or_default(),
            reverting_tx_hashes: bundle
                .raw_bundle
                .reverting_tx_hashes
                .clone()
                .iter()
                .map(|h| format!("{h:?}"))
                .collect(),
            dropping_tx_hashes: bundle
                .raw_bundle
                .dropping_tx_hashes
                .clone()
                .iter()
                .map(|h| format!("{h:?}"))
                .collect(),
            refund_tx_hashes: bundle
                .raw_bundle
                .refund_tx_hashes
                .clone()
                .iter()
                .map(|h| format!("{h:?}"))
                .collect(),
            uuid: bundle
                .raw_bundle
                .replacement_uuid
                .or(bundle.raw_bundle.uuid)
                .map(|u| u.to_string()),
            replacement_nonce: bundle.raw_bundle.replacement_nonce,
            signer_address: Some(format!("{:?}", bundle.signer)),
            builder_name,
            refund_percent: bundle.raw_bundle.refund_percent,
            refund_recipient: bundle
                .raw_bundle
                .refund_recipient
                .map(|recipient| format!("{recipient:x}")),
            refund_identity: None,
            hash: format!("{:?}", decoded.hash), // always lowercase
        };

        bundle
    }
}

/// Model representing clickhouse private transaction row.
#[derive(Row, Debug, serde::Serialize)]
#[allow(dead_code)]
pub(crate) struct PrivateTxRow {
    /// The timestamp at which private transaction was observed.
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub time: OffsetDateTime,
    /// Transaction hash.
    pub hash: String,
    /// Transaction from address.
    pub from: String,
    /// Transaction nonce.
    pub nonce: u64,
    /// Signature `r` value.
    pub r: U256,
    /// Signature `s` value.
    pub s: U256,
    /// Signature `v` value.
    pub v: U256,
    /// Transaction to addresses if present.
    pub to: Option<String>,
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
