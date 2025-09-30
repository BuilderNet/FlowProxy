use std::{str::FromStr as _, time::SystemTime};

use alloy_consensus::{
    Signed, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEip7702, TxEnvelope, TxLegacy,
};
use alloy_eips::{Encodable2718 as _, eip2930::AccessList, eip7702::SignedAuthorization};
use alloy_primitives::{Address, B256, Bytes, Signature, TxKind, U64, U256};
use alloy_rlp::Decodable as _;
use rbuilder_primitives::serialize::RawBundle;
use uuid::Uuid;

#[derive(Debug, Clone)]
#[allow(dead_code)]
pub(crate) struct BundleRow {
    /// The timestamp at which the bundle was observed.
    pub time: i64,
    // #[serde(rename = "transactions.hash", with = "hashes")]
    /// Collection of hashes for transactions in the bundle.
    pub transactions_hash: Vec<B256>,
    /// Collection of from addresses for transactions in the bundle.
    // #[serde(rename = "transactions.from", with = "addresses")]
    pub transactions_from: Vec<Address>,
    /// Collection of nonces for transactions in the bundle.
    // #[serde(rename = "transactions.nonce")]
    pub transactions_nonce: Vec<u64>,
    /// Collection of signature `r` values for transactions in the bundle.
    // #[serde(rename = "transactions.r", with = "u256es")]
    pub transactions_r: Vec<U256>,
    /// Collection of signature `s` values for transactions in the bundle.
    // #[serde(rename = "transactions.s", with = "u256es")]
    pub transactions_s: Vec<U256>,
    /// Collection of signature `v` values for transactions in the bundle.
    // #[serde(rename = "transactions.v")]
    pub transactions_v: Vec<u8>,
    /// Collection of to addresses for transactions in the bundle.
    // #[serde(rename = "transactions.to", with = "addresses::option")]
    pub transactions_to: Vec<Option<Address>>,
    /// Collection of gas limit values for transactions in the bundle.
    // #[serde(rename = "transactions.gas")]
    pub transactions_gas: Vec<u64>,
    /// Collection of transaction types for transactions in the bundle.
    // #[serde(rename = "transactions.type")]
    pub transactions_type: Vec<u8>,
    /// Collection of inputs for transactions in the bundle.
    // #[serde(rename = "transactions.input")]
    pub transactions_input: Vec<Vec<u8>>,
    /// Collection of values for transactions in the bundle.
    // #[serde(rename = "transactions.value", with = "u256es")]
    pub transactions_value: Vec<U256>,
    /// Collection of gas prices for transactions in the bundle.
    // #[serde(rename = "transactions.gasPrice")]
    pub transactions_gas_price: Vec<Option<u128>>,
    /// Collection of max fee per gas values for transactions in the bundle.
    // #[serde(rename = "transactions.maxFeePerGas")]
    pub transactions_max_fee_per_gas: Vec<Option<u128>>,
    /// Collection of max priority fee per gas values for transactions in the bundle.
    // #[serde(rename = "transactions.maxPriorityFeePerGas")]
    pub transactions_max_priority_fee_per_gas: Vec<Option<u128>>,
    /// Collection of access lists for transactions in the bundle.
    // #[serde(rename = "transactions.accessList")]
    pub transactions_access_list: Vec<Option<Vec<u8>>>,
    /// Collection of authorization lists for transactions in the bundle.
    // #[serde(rename = "transactions.authorizationList")]
    pub transactions_authorization_list: Vec<Option<Vec<u8>>>,

    pub block_number: Option<u64>,
    pub min_timestamp: Option<u64>,
    pub max_timestamp: Option<u64>,

    /// Collection of reverting transaction hashes.
    // #[serde(with = "hashes")]
    pub reverting_tx_hashes: Vec<B256>,
    /// Collection of dropping transaction hashes.
    // #[serde(with = "hashes")]
    pub dropping_tx_hashes: Vec<B256>,
    /// Collection of refund transaction hashes.
    // #[serde(with = "hashes")]
    pub refund_tx_hashes: Vec<B256>,

    /// The hash of the bundle (unique identifier)
    // #[serde(with = "hash")]
    // pub hash: B256,
    /// Bundle uuid.
    // #[serde(with = "clickhouse::serde::uuid::option")]
    // pub internal_uuid: Option<String>,
    /// Replacement bundle uuid.
    // #[serde(with = "clickhouse::serde::uuid::option")]
    pub replacement_uuid: Option<String>,
    // pub replacement_nonce: Option<u64>,
    /// Bundle refund percent.
    pub refund_percent: Option<u8>,
    /// Bundle refund recipient.
    // #[serde(with = "address::option")]
    pub refund_recipient: Option<Address>,
    /// The signer of the bundle,
    // #[serde(with = "address::option")]
    pub signer_address: Option<Address>,
    /// For 2nd price refunds done by buildernet
    // #[serde(with = "address::option")]
    pub refund_identity: Option<Address>,
    // pub builder_name: String,
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
        );

        RawBundle {
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
            replacement_uuid: value.replacement_uuid.map(|uuid| Uuid::from_str(&uuid).unwrap()),
            replacement_nonce: None,
            refund_percent: value.refund_percent,
            refund_recipient: value.refund_recipient,
            first_seen_at: Some(
                SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs() as f64,
            ),
            version: None,
            signing_address: value.signer_address,
        }
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
    transactions_type: Vec<u8>,
    transactions_input: Vec<Vec<u8>>,
    transactions_value: Vec<U256>,
    transactions_gas_price: Vec<Option<u128>>,
    transactions_max_fee_per_gas: Vec<Option<u128>>,
    transactions_max_priority_fee_per_gas: Vec<Option<u128>>,
    transactions_access_list: Vec<Option<Vec<u8>>>,
    transactions_authorization_list: Vec<Option<Vec<u8>>>,
) -> Vec<TxEnvelope> {
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
        let input = Bytes::from(transactions_input[i].clone());

        // Parse access list if present
        let access_list = match &transactions_access_list[i] {
            Some(al_bytes) => AccessList::decode(&mut al_bytes.as_slice()).unwrap(),
            None => AccessList::default(),
        };

        // Parse authorization list if present
        let authorization_list = match &transactions_authorization_list[i] {
            Some(auth_bytes) => {
                Vec::<SignedAuthorization>::decode(&mut auth_bytes.as_slice()).unwrap()
            }
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
                    max_priority_fee_per_gas: transactions_max_priority_fee_per_gas[i].unwrap_or(0),
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
                    max_priority_fee_per_gas: transactions_max_priority_fee_per_gas[i].unwrap_or(0),
                    gas_limit: transactions_gas[i],
                    to: match to {
                        TxKind::Call(addr) => addr,
                        TxKind::Create => {
                            panic!("EIP-4844 transactions cannot be contract creation")
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
                    max_priority_fee_per_gas: transactions_max_priority_fee_per_gas[i].unwrap_or(0),
                    gas_limit: transactions_gas[i],
                    to: match to {
                        TxKind::Call(addr) => addr,
                        TxKind::Create => {
                            panic!("EIP-7702 transactions cannot be contract creation")
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
                panic!("Unsupported transaction type: {}", transactions_type[i])
            }
        };

        envelopes.push(tx_envelope);
    }

    envelopes
}
