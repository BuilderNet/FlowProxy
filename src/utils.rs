use alloy_eips::eip2718::EIP4844_TX_TYPE_ID;
use alloy_primitives::Bytes;
use alloy_rlp::{Buf as _, Header};
use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use crate::{builderhub::LocalPeerStore, validation::MAINNET_CHAIN_ID};

/// An artificial timestamp used for duration clamping.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Clamp the instant to duration bucket since the start time.
pub fn clamp_to_duration_bucket(time: Instant, duration: Duration) -> Instant {
    let full_durations =
        (time.duration_since(*START).as_secs_f64() / duration.as_secs_f64()).floor();

    // Convert that back to a Duration.
    let clamped_duration = Duration::from_secs_f64(full_durations * duration.as_secs_f64());

    // Add that Duration to the start time to get the clamped time.
    *START + clamped_duration
}

pub static LOCAL_PEER_STORE: LazyLock<LocalPeerStore> = LazyLock::new(LocalPeerStore::new);

pub fn looks_like_canonical_blob_tx(raw_tx: &Bytes) -> bool {
    // For full check we could call TransactionSigned::decode_enveloped and fully try to decode it
    // is way more expensive. We expect EIP4844_TX_TYPE_ID + rlp(chainId = 01,.....)
    let mut tx_slice = raw_tx.as_ref();
    if let Some(tx_type) = tx_slice.first() {
        if *tx_type == EIP4844_TX_TYPE_ID {
            tx_slice.advance(1);
            if let Ok(outer_header) = Header::decode(&mut tx_slice) {
                if outer_header.list {
                    if let Some(chain_id) = tx_slice.first() {
                        return (*chain_id as u64) == MAINNET_CHAIN_ID;
                    }
                }
            }
        }
    }
    false
}

pub mod testutils {
    use alloy_consensus::{
        BlobTransactionSidecar, EthereumTypedTransaction, SidecarBuilder, SignableTransaction as _,
        SimpleCoder, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEip4844WithSidecar,
        TxEip7702, TxEnvelope, TxLegacy,
    };
    use alloy_eips::Encodable2718 as _;
    use alloy_primitives::{Address, Bytes, TxKind, U256};
    use alloy_signer::SignerSync as _;
    use alloy_signer_local::PrivateKeySigner;
    use rand::Rng;
    use rbuilder_primitives::serialize::RawBundle;

    /// A trait for types that can be randomly generated.
    pub trait Random {
        fn random<R: Rng>(rng: &mut R) -> Self;
    }

    impl Random for Bytes {
        fn random<R: Rng>(rng: &mut R) -> Self {
            let len = rng.random_range(0..=1024);
            let mut bytes = vec![0u8; len];
            rng.fill_bytes(&mut bytes);
            bytes.into()
        }
    }

    impl Random for TxLegacy {
        fn random<R: Rng>(rng: &mut R) -> Self {
            Self {
                chain_id: Some(1),
                nonce: 0,
                gas_price: rng.random(),
                gas_limit: 100_000,
                to: TxKind::Call(Address::random_with(rng)),
                value: U256::random_with(rng),
                input: Bytes::random(rng),
            }
        }
    }

    impl Random for TxEip2930 {
        fn random<R: Rng>(rng: &mut R) -> Self {
            Self {
                chain_id: 1,
                nonce: 0,
                gas_price: rng.random(),
                gas_limit: 100_000,
                to: TxKind::Call(Address::random_with(rng)),
                value: U256::random_with(rng),
                input: Bytes::random(rng),
                access_list: Default::default(),
            }
        }
    }

    impl Random for TxEip1559 {
        fn random<R: Rng>(rng: &mut R) -> Self {
            let max_fee_per_gas = rng.random();
            let max_priority_fee_per_gas = rng.random_range(0..max_fee_per_gas);

            Self {
                chain_id: 1,
                nonce: 0,
                gas_limit: 100_000,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                to: TxKind::Call(Address::random_with(rng)),
                value: U256::random_with(rng),
                access_list: Default::default(),
                input: Bytes::random(rng),
            }
        }
    }

    impl Random for TxEip4844 {
        fn random<R: Rng>(rng: &mut R) -> Self {
            let max_fee_per_gas = rng.random();
            let max_priority_fee_per_gas = rng.random_range(0..max_fee_per_gas);

            Self {
                chain_id: 1,
                nonce: 0,
                gas_limit: 100_000,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                value: U256::random_with(rng),
                access_list: Default::default(),
                input: Bytes::random(rng),
                blob_versioned_hashes: Default::default(),
                max_fee_per_blob_gas: rng.random(),
                to: Address::random_with(rng),
            }
        }
    }

    impl Random for TxEip4844WithSidecar {
        fn random<R: Rng>(rng: &mut R) -> Self {
            let mut tx = TxEip4844::random(rng);
            let sidecar = BlobTransactionSidecar::random(rng);

            tx.blob_versioned_hashes = sidecar.versioned_hashes().collect();

            Self { tx, sidecar }
        }
    }

    impl Random for BlobTransactionSidecar {
        fn random<R: Rng>(rng: &mut R) -> Self {
            let mut data = [0u8; 1024];
            rng.fill_bytes(&mut data);
            let sidecar: SidecarBuilder<SimpleCoder> = SidecarBuilder::from_slice(&data);

            sidecar.build().unwrap()
        }
    }

    impl Random for TxEip7702 {
        fn random<R: Rng>(rng: &mut R) -> Self {
            let max_fee_per_gas = rng.random();
            let max_priority_fee_per_gas = rng.random_range(0..max_fee_per_gas);

            Self {
                chain_id: 1,
                nonce: 0,
                gas_limit: 100_000,
                max_fee_per_gas,
                max_priority_fee_per_gas,
                value: U256::random_with(rng),
                access_list: Default::default(),
                input: Bytes::random(rng),
                to: Address::random_with(rng),
                authorization_list: Default::default(),
            }
        }
    }

    impl Random for EthereumTypedTransaction<TxEip4844Variant> {
        fn random<R: Rng>(rng: &mut R) -> Self {
            let tx_type = rng.random_range(0..=3);
            match tx_type {
                0 => EthereumTypedTransaction::Legacy(TxLegacy::random(rng)),
                1 => EthereumTypedTransaction::Eip2930(TxEip2930::random(rng)),
                2 => EthereumTypedTransaction::Eip1559(TxEip1559::random(rng)),
                3 => EthereumTypedTransaction::Eip4844(TxEip4844WithSidecar::random(rng).into()),
                _ => unreachable!(),
            }
        }
    }

    impl Random for TxEnvelope {
        fn random<R: Rng>(rng: &mut R) -> Self {
            let signer = PrivateKeySigner::random();
            let transaction = EthereumTypedTransaction::random(rng);

            let sighash = transaction.signature_hash();
            let signature = signer.sign_hash_sync(&sighash).unwrap();

            TxEnvelope::new_unhashed(transaction, signature)
        }
    }

    impl Random for RawBundle {
        /// Generate a random bundle with transactions of type Eip1559.
        fn random<R: Rng>(rng: &mut R) -> Self {
            let txs_len = rng.random_range(1..=10);
            // We only generate Eip1559 here.
            let txs = (0..txs_len)
                .map(|_| {
                    let signer = PrivateKeySigner::random();
                    let tx = EthereumTypedTransaction::Eip1559(TxEip1559::random(rng));
                    let sighash = tx.signature_hash();
                    let signature = signer.sign_hash_sync(&sighash).unwrap();
                    TxEnvelope::new_unhashed(tx, signature).encoded_2718().into()
                })
                .collect();

            Self {
                txs,
                reverting_tx_hashes: vec![],
                dropping_tx_hashes: vec![],
                refund_tx_hashes: None,
                signing_address: None,
                version: Some("v2".to_string()),
                block_number: None,
                replacement_uuid: None,
                uuid: None,
                min_timestamp: None,
                max_timestamp: None,
                replacement_nonce: Some(rng.random()),
                refund_percent: Some(rng.random_range(0..100)),
                refund_recipient: Some(Address::random_with(rng)),
                first_seen_at: None,
            }
        }
    }
}
