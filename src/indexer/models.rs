use alloy_primitives::U256;
use clickhouse_derive::Row;
use time::{OffsetDateTime, UtcDateTime};

use crate::types::{DecodedBundle, SystemBundle};

/// Model representing clickhouse bundle row.
#[derive(Row, Debug, serde::Serialize)]
pub struct BundleRow {
    /// The timestamp at which the bundle was observed.
    #[serde(with = "clickhouse::serde::time::datetime64::micros")]
    pub time: OffsetDateTime,
    #[serde(rename = "transactions.hash")]
    /// Collection of hashes for transactions in the bundle.
    pub tx_hash: Vec<String>,
    /// Collection of from addresses for transactions in the bundle.
    #[serde(rename = "transactions.from")]
    pub tx_from: Vec<String>,
    /// Collection of nonces for transactions in the bundle.
    #[serde(rename = "transactions.nonce")]
    pub tx_nonce: Vec<u64>,
    /// Collection of signature `r` values for transactions in the bundle.
    #[serde(rename = "transactions.r")]
    pub tx_r: Vec<U256>,
    /// Collection of signature `s` values for transactions in the bundle.
    #[serde(rename = "transactions.s")]
    pub tx_s: Vec<U256>,
    /// Collection of signature `v` values for transactions in the bundle.
    #[serde(rename = "transactions.v")]
    pub tx_v: Vec<U256>,
    /// Collection of to addresses for transactions in the bundle.
    #[serde(rename = "transactions.to")]
    pub tx_to: Vec<Option<String>>,
    /// Collection of gas limit values for transactions in the bundle.
    #[serde(rename = "transactions.gas")]
    pub tx_gas: Vec<U256>,
    /// Collection of transaction types for transactions in the bundle.
    #[serde(rename = "transactions.type")]
    pub tx_type: Vec<u64>,
    /// Collection of inputs for transactions in the bundle.
    #[serde(rename = "transactions.input")]
    pub tx_input: Vec<String>,
    /// Collection of values for transactions in the bundle.
    #[serde(rename = "transactions.value")]
    pub tx_value: Vec<U256>,
    /// Collection of gas prices for transactions in the bundle.
    #[serde(rename = "transactions.gasPrice")]
    pub tx_gas_price: Vec<Option<U256>>,
    /// Collection of max fee per gas values for transactions in the bundle.
    #[serde(rename = "transactions.maxFeePerGas")]
    pub tx_max_fee_per_gas: Vec<Option<U256>>,
    /// Collection of max priority fee per gas values for transactions in the bundle.
    #[serde(rename = "transactions.maxPriorityFeePerGas")]
    pub tx_max_priority_fee_per_gas: Vec<Option<U256>>,
    /// Collection of access lists for transactions in the bundle.
    #[serde(rename = "transactions.accessList")]
    pub tx_access_list: Vec<Option<String>>,
    /// Bundle type: top (default), bottom.
    #[serde(rename = "type")]
    pub bundle_type: String,
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
    /// The IP from which the bundle request was observed.
    pub source_ip: Option<String>,
    /// The `Host` value in the header
    pub host: Option<String>,
    /// Builder name.
    pub builder_name: String,
    /// Bundle refund percent.
    pub refund_percent: Option<u8>,
    /// Bundle refund recipient.
    pub refund_recipient: Option<String>,
    /// The score calculated by the forwarder.
    pub forwarder_score: f32,
    /// The hash of the bundle (unique identifier)
    pub hash: String,
    /// The bx refund parameters are specified in eth_sendPriorityFeeRefundBundle bundles
    pub bx_first_refund_recipient: Option<String>,
    pub bx_first_refund_percent: Option<u8>,
    pub bx_second_refund_recipient: Option<String>,
    pub bx_second_refund_percent: Option<u8>,
}

impl From<(SystemBundle, UtcDateTime)> for BundleRow {
    fn from(bundle_with_time: (SystemBundle, UtcDateTime)) -> Self {
        let DecodedBundle::Bundle(ref decoded) = bundle_with_time.0.decoded_bundle.as_ref() else {
            todo!()
        };

        // let bundle = BundleRow {
        //     time: OffsetDateTime::now_utc(),
        //     tx_hash: decoded.txs.iter().map(|tx| format!("{:?}", tx.hash())).collect(),
        //     tx_from: decoded.txs.iter().map(|tx| format!("{:?}", tx.signer())).collect(),
        //     tx_nonce: decoded.txs.iter().map(|tx| tx.nonce()).collect(),
        //     tx_r: decoded.txs.iter().map(|tx| tx.as_ref().signature().r()).collect(),
        //     tx_s: decoded.txs.iter().map(|tx| tx.as_ref().signature().s()).collect(),
        //     tx_v: txs.iter().map(|tx| to_klickhouse_u256(tx.signature().v().to_u64())).collect(),
        //     tx_to: txs.iter().map(|tx| tx.to().to().map(|t| format!("{t:?}"))).collect(),
        //     tx_gas: txs.iter().map(|tx| to_klickhouse_u256(tx.gas_limit())).collect(),
        //     tx_type: txs.iter().map(|tx| tx.tx_type() as u64).collect(),
        //     tx_input: txs
        //         .iter()
        //         .map(|tx| alloy_primitives::hex::encode_prefixed(tx.input()))
        //         .collect(),
        //     tx_value: txs.iter().map(|tx| to_klickhouse_u256(tx.value())).collect(),
        //     tx_gas_price: txs.iter().map(|tx| tx.gas_price().map(to_klickhouse_u256)).collect(),
        //     tx_max_fee_per_gas: txs
        //         .iter()
        //         .map(|tx| tx.legacy_max_fee_per_gas().map(to_klickhouse_u256))
        //         .collect(),
        //     tx_max_priority_fee_per_gas: txs
        //         .iter()
        //         .map(|tx| tx.max_priority_fee_per_gas().map(to_klickhouse_u256))
        //         .collect(),
        //     tx_access_list: txs
        //         .iter()
        //         .map(|tx| {
        //             tx.access_list()
        //                 .as_ref()
        //                 .map(|access_list| serde_json::to_string(&access_list).unwrap())
        //         })
        //         .collect(),
        //     bundle_type,
        //     // TODO(palkeo): This is not ideal, we are using 0 as block number, if not defined.
        //     block_number: raw_bundle.block_number.unwrap_or_default().to::<u64>(),
        //     min_timestamp: raw_bundle.min_timestamp.unwrap_or_default().to::<u64>(),
        //     max_timestamp: raw_bundle.max_timestamp.unwrap_or_default().to::<u64>(),
        //     reverting_tx_hashes: raw_bundle
        //         .reverting_tx_hashes
        //         .clone()
        //         .unwrap_or_default()
        //         .iter()
        //         .map(|h| format!("{h:?}"))
        //         .collect(),
        //     dropping_tx_hashes: raw_bundle
        //         .dropping_tx_hashes
        //         .clone()
        //         .unwrap_or_default()
        //         .iter()
        //         .map(|h| format!("{h:?}"))
        //         .collect(),
        //     refund_tx_hashes: raw_bundle
        //         .refund_tx_hashes
        //         .clone()
        //         .unwrap_or_default()
        //         .iter()
        //         .map(|h| format!("{h:?}"))
        //         .collect(),
        //     uuid: raw_bundle.replacement_uuid.or(raw_bundle.uuid).map(|u| u.to_string()),
        //     source_ip: Some(source_ip.to_owned()),
        //     host: Some(host.to_owned()),
        //     builder_name: builder_name.to_owned(),
        //     refund_percent: raw_bundle
        //         .refund_percent
        //         .map(|pct| u8::try_from(pct.to::<u64>()).unwrap_or(255)),
        //     refund_recipient: raw_bundle
        //         .refund_recipient
        //         .clone()
        //         .map(|recipient| recipient.to_lowercase()),
        //     forwarder_score: self.compute_forwarder_score(&txs, source_ip),
        //     hash: format!("{bundle_hash:?}"), // always lowercase
        //     bx_first_refund_recipient: extra
        //         .first_refund_recipient
        //         .map(|recipient| recipient.to_lowercase()),
        //     bx_first_refund_percent: extra
        //         .first_refund_percent
        //         .map(|pct| u8::try_from(pct.to::<u64>()).unwrap_or(255)),
        //     bx_second_refund_recipient: extra
        //         .second_refund_recipient
        //         .map(|recipient| recipient.to_lowercase()),
        //     bx_second_refund_percent: extra
        //         .second_refund_percent
        //         .map(|pct| u8::try_from(pct.to::<u64>()).unwrap_or(255)),
        // };
        todo!();
    }
}

/// Model representing clickhouse private transaction row.
#[derive(Row, Debug, serde::Serialize)]
pub struct PrivateTxRow {
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
