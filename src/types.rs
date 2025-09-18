use std::{ops::Deref, sync::Arc};

use alloy_consensus::{
    crypto::RecoveryError,
    transaction::{PooledTransaction, Recovered, SignerRecoverable as _},
};
use alloy_eips::{
    eip2718::{Eip2718Error, Eip2718Result},
    Decodable2718 as _, Encodable2718,
};
use alloy_primitives::{Address, Bytes, Keccak256, B256, U64};
use serde::{Deserialize, Serialize};
use serde_json::json;
use strum_macros::{Display, EnumString};
use uuid::Uuid;

use crate::{ingress::error::IngressError, validation::validate_transaction};

/// An MEV bundle type that can be submitted via `eth_sendBundle` JSON-RPC method.
pub type RpcBundle = Bundle<Bytes>;

/// An MEV bundle type.
#[derive(PartialEq, Eq, Clone, Default, Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Bundle<T = Bytes> {
    /// A string, stringified or hex-encoded block number for which this bundle is valid.
    pub block_number: Option<U64>,
    /// A list of signed transactions to execute in an atomic bundle, list can be empty for bundle
    /// cancellations.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub txs: Vec<T>,
    /// The minimum UNIX timestamp for which this bundle is valid, in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub min_timestamp: Option<u64>,
    /// The maximum UNIX timestamp for which this bundle is valid, in seconds.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub max_timestamp: Option<u64>,
    /// A list of transaction hashes that are allowed to revert.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub reverting_tx_hashes: Vec<B256>,
    /// A list of transaction hashes that are allowed to be discarded, but may not revert on chain.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub dropping_tx_hashes: Vec<B256>,
    /// A v4 UUID that can be used to replace or cancel this bundle
    #[serde(alias = "replacementUuid")]
    pub uuid: Option<Uuid>,
    /// A refund percent from the bundle value to be returned to the user.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refund_percent: Option<u64>,
    /// A refund address to send the refund value to.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refund_recipient: Option<Address>,
    /// A list of transaction hashes which should be refunded.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub refund_tx_hashes: Vec<B256>,
    /// Refund identity. Address used by refund calculation to identify refund receiver
    #[serde(skip_serializing_if = "Option::is_none")]
    pub refund_identity: Option<Address>,
    /// Bundle version. Defaults to [`BundleVersion::V2`].
    #[serde(default)]
    pub version: BundleVersion,
}

impl<T> Bundle<T> {
    /// Map transactions in the bundle to a new type. Mapping function is fallible.
    pub fn try_map_transactions<D, E>(
        self,
        f: impl FnMut(T) -> Result<D, E>,
    ) -> Result<Bundle<D>, E> {
        Ok(Bundle {
            txs: self.txs.into_iter().map(f).collect::<Result<_, E>>()?,
            block_number: self.block_number,
            min_timestamp: self.min_timestamp,
            max_timestamp: self.max_timestamp,
            refund_tx_hashes: self.refund_tx_hashes,
            dropping_tx_hashes: self.dropping_tx_hashes,
            uuid: self.uuid,
            refund_percent: self.refund_percent,
            reverting_tx_hashes: self.reverting_tx_hashes,
            refund_recipient: self.refund_recipient,
            refund_identity: self.refund_identity,
            version: self.version,
        })
    }
}

impl Bundle<PooledTransaction> {
    /// Calculate bundle hash to return to the user.
    /// Ref: <https://github.com/flashbots/go-utils/blob/f7f7f220f37b25ec3ad407c65ef7e685606b82ad/rpctypes/types.go#L224-L233>
    pub fn bundle_hash(&self) -> B256 {
        let mut hasher = Keccak256::new();
        for tx in &self.txs {
            hasher.update(tx.tx_hash());
        }
        hasher.finalize()
    }

    /// Convert the bundle to a system bundle. The `signer` is assumed to be validated.
    pub fn into_system(self, signer: Address) -> SystemBundle {
        SystemBundle { signer, bundle: Arc::new(self) }
    }
}

/// Bundle type that is used for the system API. It contains the verified signer with the original
/// bundle.
#[derive(PartialEq, Eq, Clone, Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SystemBundle {
    #[serde(rename = "signingAddress")]
    pub signer: Address,
    /// The inner bundle. Wrapped in [`Arc`] to make cloning cheaper.
    #[serde(flatten)]
    pub bundle: Arc<Bundle<PooledTransaction>>,
}

impl Deref for SystemBundle {
    type Target = Bundle<PooledTransaction>;

    fn deref(&self) -> &Self::Target {
        &self.bundle
    }
}

impl SystemBundle {
    /// Validates all transactions in the bundle.
    pub fn validate(&self) -> Result<(), IngressError> {
        self.bundle.txs.iter().try_for_each(|tx| {
            validate_transaction(tx)?;
            tx.recover_signer()?;

            Ok(())
        })
    }

    /// Encode the inner bundle (no signer).
    pub fn encode_inner(self) -> Vec<u8> {
        let json = json!({
            "id": 1,
            "jsonrpc": "2.0",
            "method": "eth_sendBundle",
            "params": [self.bundle]
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

/// Bundle version.
#[derive(PartialEq, Eq, Clone, Default, Debug, Display, EnumString, Serialize, Deserialize)]
#[strum(serialize_all = "lowercase")]
pub enum BundleVersion {
    V1,
    #[default]
    V2,
}

impl BundleVersion {
    /// Returns [`BundleVersion::V2`].
    pub const fn v2() -> Self {
        Self::V2
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloy_consensus::{Signed, TxEip1559};
    use alloy_eips::Encodable2718;
    use alloy_primitives::{bytes::BytesMut, hex, Signature, U256};
    use serde_json::json;
    use std::str::FromStr;

    #[test]
    fn bundle_version_str_roundtrip() {
        assert_eq!(BundleVersion::V1.to_string(), "v1");
        assert_eq!(BundleVersion::V2.to_string(), "v2");

        for version in [BundleVersion::V1, BundleVersion::V2] {
            assert_eq!(version, BundleVersion::from_str(&version.to_string()).unwrap());
        }
    }

    #[test]
    fn bundle_ser_deser() {
        let block_number = 0x0123_u64;
        let uuid = Uuid::new_v4();

        let mut encoded = BytesMut::new();
        PooledTransaction::Eip1559(Signed::new_unchecked(
            TxEip1559::default(),
            Signature::new(U256::ZERO, U256::ZERO, false),
            B256::ZERO,
        ))
        .encode_2718(&mut encoded);
        let tx_bytes: Bytes = encoded.freeze().into();
        let tx = hex::encode_prefixed(&tx_bytes);

        // Decode regular bundle
        let bundle = json!({
            "blockNumber": block_number,
            "txs": [tx],
        });
        assert_eq!(
            serde_json::from_value::<Bundle>(bundle).unwrap(),
            Bundle {
                txs: Vec::from([tx_bytes.clone()]),
                block_number: Some(U64::from(block_number)),
                ..Default::default()
            }
        );

        // Decode bundle with stringified block number.
        let bundle = json!({
            "blockNumber": format!("{block_number}"),
            "txs": [tx],
        });
        assert_eq!(
            serde_json::from_value::<Bundle<Bytes>>(bundle).unwrap(),
            Bundle {
                txs: Vec::from([tx_bytes.clone()]),
                block_number: Some(U64::from(block_number)),
                ..Default::default()
            }
        );

        // Decode bundle with hex-encoded block number.
        let bundle = json!({
            "blockNumber": format!("0x{block_number:x}"),
            "txs": [tx],
        });
        assert_eq!(
            serde_json::from_value::<Bundle>(bundle).unwrap(),
            Bundle {
                txs: Vec::from([tx_bytes]),
                block_number: Some(U64::from(block_number)),
                ..Default::default()
            }
        );

        // Decodes bundle with uuid.
        let bundle = json!({
            "uuid": uuid,
            "blockNumber": block_number,
            "txs": [],
        });
        assert_eq!(
            serde_json::from_value::<Bundle>(bundle).unwrap(),
            Bundle {
                uuid: Some(uuid),
                block_number: Some(U64::from(block_number)),
                ..Default::default()
            }
        );

        // Decodes bundle with `replacementUuid` field as uuid.
        let bundle = json!({
            "replacementUuid": uuid,
            "blockNumber": block_number,
            "txs": [],
        });
        assert_eq!(
            serde_json::from_value::<Bundle<Bytes>>(bundle).unwrap(),
            Bundle {
                uuid: Some(uuid),
                block_number: Some(U64::from(block_number)),
                ..Default::default()
            }
        );
    }
}
