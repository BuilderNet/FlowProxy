use alloy_eips::eip2718::EIP4844_TX_TYPE_ID;
use alloy_primitives::Bytes;
use alloy_rlp::{Buf as _, Header};
use futures::{StreamExt, stream::FuturesUnordered};
use std::time::{Duration, Instant};
use time::UtcDateTime;
use tokio::task::JoinHandle;
use tracing::{error, info};
use uuid::Uuid;

use crate::{statics::START, validation::MAINNET_CHAIN_ID};

/// Clamp the instant to duration bucket since the start time.
pub fn clamp_to_duration_bucket(time: Instant, duration: Duration) -> Instant {
    let full_durations =
        (time.duration_since(*START).as_secs_f64() / duration.as_secs_f64()).floor();

    // Convert that back to a Duration.
    let clamped_duration = Duration::from_secs_f64(full_durations * duration.as_secs_f64());

    // Add that Duration to the start time to get the clamped time.
    *START + clamped_duration
}

pub fn looks_like_canonical_blob_tx(raw_tx: &Bytes) -> bool {
    // For full check we could call TransactionSigned::decode_enveloped and fully try to decode it
    // is way more expensive. We expect EIP4844_TX_TYPE_ID + rlp(chainId = 01,.....)
    let mut tx_slice = raw_tx.as_ref();
    if let Some(tx_type) = tx_slice.first() &&
        *tx_type == EIP4844_TX_TYPE_ID
    {
        tx_slice.advance(1);
        if let Ok(outer_header) = Header::decode(&mut tx_slice) &&
            outer_header.list &&
            let Some(chain_id) = tx_slice.first()
        {
            return (*chain_id as u64) == MAINNET_CHAIN_ID;
        }
    }
    false
}

/// A trait for types that can be formatted and parsed as a UNIX timestamp in microseconds header
/// value.
pub trait UtcDateTimeHeader: Sized {
    fn format_header(&self) -> String;
    fn parse_header(value: &str) -> Option<Self>;
}

impl UtcDateTimeHeader for UtcDateTime {
    /// Format a [`UtcDateTime`] as a UNIX timestamp in microseconds header value.
    fn format_header(&self) -> String {
        (self.unix_timestamp_nanos() / 1_000).to_string()
    }

    /// Parse a [`UtcDateTime`] from a UNIX timestamp in microseconds header value.
    fn parse_header(value: &str) -> Option<Self> {
        let micros: i128 = value.parse().ok()?;
        UtcDateTime::from_unix_timestamp_nanos(micros * 1_000).ok()
    }
}

/// A trait for types that can be formatted as a human-readable size in bytes.
pub trait FormatBytes {
    fn format_bytes(&self) -> String;
}

impl FormatBytes for u64 {
    fn format_bytes(&self) -> String {
        if *self < 1024 {
            format!("{}B", self)
        } else if *self < 1024 * 1024 {
            format!("{}KiB", self / 1024)
        } else if *self < 1024 * 1024 * 1024 {
            format!("{}MiB", self / 1024 / 1024)
        } else {
            format!("{}GiB", self / 1024 / 1024 / 1024)
        }
    }
}

/// Generate a short UUID v4 string (8 characters).
pub fn short_uuid_v4() -> String {
    Uuid::new_v4().as_simple().to_string()[..8].to_string()
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
    use rbuilder_primitives::serialize::{RawBundle, RawBundleMetadata};

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
            let input_len = rng.random_range(0..=1024);
            create_tx_eip1559_with_input_size(rng, input_len)
        }
    }

    pub fn create_tx_eip1559_with_input_size<R: Rng>(rng: &mut R, data_size: usize) -> TxEip1559 {
        let max_fee_per_gas = rng.random();
        let max_priority_fee_per_gas = rng.random_range(0..max_fee_per_gas);
        let mut bytes = vec![0u8; data_size];
        rng.fill_bytes(&mut bytes);
        TxEip1559 {
            chain_id: 1,
            nonce: 0,
            gas_limit: 100_000,
            max_fee_per_gas,
            max_priority_fee_per_gas,
            to: TxKind::Call(Address::random_with(rng)),
            value: U256::random_with(rng),
            access_list: Default::default(),
            input: bytes.into(),
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

    /// Create a random [`RawBundle`] with a fixed number of EIP-1559 transactions.
    /// When `input_size` is `Some(n)`, each transaction's input has exactly `n` bytes (via
    /// [`create_tx_eip1559_with_input_size`]). When `None`, each transaction uses random input
    /// size (same as [`TxEip1559::random`]).
    pub fn random_raw_bundle_with_tx_count_and_input_size<R: Rng>(
        rng: &mut R,
        tx_count: usize,
        input_size: Option<usize>,
    ) -> RawBundle {
        let txs = (0..tx_count)
            .map(|_| {
                let signer = PrivateKeySigner::random();
                let tx = EthereumTypedTransaction::Eip1559(match input_size {
                    Some(n) => create_tx_eip1559_with_input_size(rng, n),
                    None => TxEip1559::random(rng),
                });
                let sighash = tx.signature_hash();
                let signature = signer.sign_hash_sync(&sighash).unwrap();
                TxEnvelope::new_unhashed(tx, signature).encoded_2718().into()
            })
            .collect();

        RawBundle {
            txs,
            metadata: RawBundleMetadata {
                reverting_tx_hashes: vec![],
                dropping_tx_hashes: vec![],
                refund_tx_hashes: None,
                signing_address: None,
                version: Some("v2".to_string()),
                block_number: None,
                replacement_uuid: None,
                refund_identity: None,
                uuid: None,
                min_timestamp: None,
                max_timestamp: None,
                replacement_nonce: Some(rng.random()),
                refund_percent: Some(rng.random_range(0..100)),
                refund_recipient: Some(Address::random_with(rng)),
                delayed_refund: None,
                bundle_hash: None,
            },
        }
    }

    impl Random for RawBundle {
        /// Generate a random bundle with transactions of type Eip1559.
        fn random<R: Rng>(rng: &mut R) -> Self {
            let txs_len = rng.random_range(1..=10);
            random_raw_bundle_with_tx_count_and_input_size(rng, txs_len, None)
        }
    }
}

/// This time out should be enough for the inserter to flush all pending clickhouse data (timeout is
/// clickhouse usually a few secs) and local DB data (disk flush time).
pub const SHUTDOWN_TIMEOUT: Duration = Duration::from_secs(20);

/// Consider move this to rbuilder-utils.
/// Waits for critical_tasks to finish by themselves up to grateful_timeout.
pub async fn wait_for_critical_tasks(
    critical_tasks: Vec<JoinHandle<()>>,
    grateful_timeout: Duration,
) {
    let mut critical_tasks: FuturesUnordered<_> = critical_tasks.into_iter().collect();
    let critical_deadline = tokio::time::Instant::now() + grateful_timeout;
    loop {
        tokio::select! {
            biased;
            result = critical_tasks.next() => {
                match result {
                    Some(Err(err)) => error!(?err, "Critical task handle await error"),
                    Some(Ok(())) => {}
                    None => {
                        info!("All critical tasks finished ok");
                        break;
                    }
                }
            }
            _ = tokio::time::sleep_until(critical_deadline) => {
                error!(pending_task_count = critical_tasks.len(), "Critical tasks shutdown timeout reached");
                break;
            }
        }
    }
}
