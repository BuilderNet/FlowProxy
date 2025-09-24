use alloy_consensus::Transaction;
use alloy_eips::Typed2718;
use alloy_primitives::U256;
use clickhouse_derive::Row;
use time::OffsetDateTime;

use crate::types::{DecodedBundle, SystemBundle};

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
    pub block_number: Option<u64>,
    /// Minimum timestamp for the bundle.
    pub min_timestamp: Option<u64>,
    /// Maximum timestamp for the bundle.
    pub max_timestamp: Option<u64>,

    /// Collection of reverting transaction hashes.
    pub reverting_tx_hashes: Vec<String>,
    /// Collection of dropping transaction hashes.
    pub dropping_tx_hashes: Vec<String>,
    /// Collection of refund transaction hashes.
    pub refund_tx_hashes: Option<Vec<String>>,

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
impl From<(SystemBundle, String)> for BundleRow {
    fn from((bundle, builder_name): (SystemBundle, String)) -> Self {
        let DecodedBundle::Bundle(ref decoded) = bundle.decoded_bundle.as_ref() else {
            unreachable!("expecting decoded bundle")
        };

        let bundle = BundleRow {
            // Ensure microsecond accuracy
            time: bundle
                .received_at
                .replace_nanosecond(0)
                .expect("to cancel nanosecond accuracy")
                .into(),
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
                .map(|v| v.iter().map(|h| format!("{h:?}")).collect()),
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

/// Tests to make sure round-trip conversion between raw bundle and clickhouse bundle types is
/// feasible.
#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;

    use alloy_consensus::{
        Signed, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEip7702, TxEnvelope, TxLegacy,
    };
    use alloy_eips::{eip2930::AccessList, eip7702::SignedAuthorization, Encodable2718};
    use alloy_primitives::{hex, Address, Bytes, FixedBytes, Signature, TxKind, B256, U256, U64};
    use rbuilder_primitives::serialize::RawBundle;

    use crate::{
        indexer::{self, models::BundleRow},
        types::SystemBundle,
    };

    /// Decodes the hex string to the provided number of bytes.
    ///
    /// Panics if it's not a valid hex string.
    fn decode_bytes<const N: usize>(hex: &str) -> FixedBytes<N> {
        let bytes = alloy_primitives::hex::decode(hex.strip_prefix("0x").unwrap_or(hex)).unwrap();
        let mut addr = [0u8; N];
        addr.copy_from_slice(&bytes);
        addr.into()
    }

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
                reverting_tx_hashes: value
                    .reverting_tx_hashes
                    .iter()
                    .map(|h| decode_bytes(h))
                    .collect(),
                dropping_tx_hashes: value
                    .dropping_tx_hashes
                    .iter()
                    .map(|h| decode_bytes(h))
                    .collect(),
                refund_tx_hashes: value
                    .refund_tx_hashes
                    .map(|v| v.iter().map(|h| decode_bytes(h)).collect()),
                uuid: value.uuid.and_then(|u| uuid::Uuid::parse_str(&u).ok()),
                replacement_uuid: None,
                replacement_nonce: value.replacement_nonce,
                refund_percent: value.refund_percent,
                refund_recipient: value.refund_recipient.map(|r| decode_bytes(&r).into()),
                first_seen_at: None,
                version: None,
                signing_address: value.signer_address.map(|h| decode_bytes(&h).into()),
            };

            raw_bundle
        }
    }

    #[allow(clippy::too_many_arguments)]
    fn convert_clickhouse_data_to_tx_envelopes(
        transactions_hash: Vec<String>,
        transactions_nonce: Vec<u64>,
        transactions_r: Vec<U256>,
        transactions_s: Vec<U256>,
        transactions_v: Vec<u8>,
        transactions_to: Vec<Option<String>>,
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
                Some(addr_str) => TxKind::Call(addr_str.parse::<Address>()?),
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
                        chain_id: None, // Will be derived from signature
                        nonce: transactions_nonce[i],
                        gas_price: transactions_gas_price[i].unwrap_or(0),
                        gas_limit: transactions_gas[i],
                        to,
                        value: transactions_value[i],
                        input,
                    };
                    TxEnvelope::Legacy(Signed::new_unchecked(
                        tx,
                        signature,
                        transactions_hash[i].parse::<B256>()?,
                    ))
                }
                1 => {
                    // EIP-2930 transaction
                    let tx = TxEip2930 {
                        chain_id: 1, // You may need to extract this from context
                        nonce: transactions_nonce[i],
                        gas_price: transactions_gas_price[i].unwrap_or(0),
                        gas_limit: transactions_gas[i],
                        to,
                        value: transactions_value[i],
                        input,
                        access_list,
                    };
                    TxEnvelope::Eip2930(Signed::new_unchecked(
                        tx,
                        signature,
                        transactions_hash[i].parse::<B256>()?,
                    ))
                }
                2 => {
                    // EIP-1559 transaction
                    let tx = TxEip1559 {
                        chain_id: 1, // You may need to extract this from context
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
                    TxEnvelope::Eip1559(Signed::new_unchecked(
                        tx,
                        signature,
                        transactions_hash[i].parse::<B256>()?,
                    ))
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
                        blob_versioned_hashes: Vec::new(), // You'll need to extract this
                        max_fee_per_blob_gas: 0,           // You'll need to extract this
                    };
                    TxEnvelope::Eip4844(Signed::new_unchecked(
                        TxEip4844Variant::TxEip4844(tx),
                        signature,
                        transactions_hash[i].parse::<B256>()?,
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
                    TxEnvelope::Eip7702(Signed::new_unchecked(
                        tx,
                        signature,
                        transactions_hash[i].parse::<B256>()?,
                    ))
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
        let signer = decode_bytes(bundle_row.signer_address.as_ref().unwrap()).into();

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
}
