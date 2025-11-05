use alloy_eips::eip2718::EIP4844_TX_TYPE_ID;
use alloy_primitives::Bytes;
use alloy_rlp::{Buf as _, Header};
use axum::http::HeaderValue;
use std::time::{Duration, Instant};
use time::UtcDateTime;
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

/// A trait for types that can be formatted and parsed as a UNIX timestamp in microseconds header
/// value.
pub trait UtcDateTimeHeader: Sized {
    fn format_header(&self) -> HeaderValue;
    fn parse_header(value: &HeaderValue) -> Option<Self>;
}

impl UtcDateTimeHeader for UtcDateTime {
    /// Format a [`UtcDateTime`] as a UNIX timestamp in microseconds header value.
    fn format_header(&self) -> HeaderValue {
        (self.unix_timestamp_nanos() / 1_000).to_string().parse().unwrap()
    }

    /// Parse a [`UtcDateTime`] from a UNIX timestamp in microseconds header value.
    fn parse_header(value: &HeaderValue) -> Option<Self> {
        let micros: i128 = value.to_str().ok()?.parse().ok()?;
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

/// A module for the concurrent connections limit layer.
/// Adapted from [`tower::limit::concurrency`] but adds metrics.
pub mod limit {
    use std::{
        future::Future,
        pin::Pin,
        sync::Arc,
        task::{Context, Poll},
    };

    use pin_project_lite::pin_project;
    use tokio::sync::{OwnedSemaphorePermit, Semaphore};
    use tokio_util::sync::PollSemaphore;
    use tower::{Layer, Service};

    use crate::metrics::HTTP_METRICS;

    /// Enforces a limit on the concurrent number of requests the underlying
    /// service can handle.
    #[derive(Debug, Clone)]
    pub struct ConnectionLimiterLayer {
        max: usize,
        id: String,
    }

    impl ConnectionLimiterLayer {
        /// Create a new concurrency limit layer.
        pub const fn new(max: usize, id: String) -> Self {
            ConnectionLimiterLayer { max, id }
        }
    }

    impl<S> Layer<S> for ConnectionLimiterLayer {
        type Service = ConnectionLimiter<S>;

        fn layer(&self, service: S) -> Self::Service {
            ConnectionLimiter::new(service, self.max, self.id.clone())
        }
    }

    /// Enforces a limit on the concurrent number of requests the underlying
    /// service can handle.
    #[derive(Debug)]
    pub struct ConnectionLimiter<T> {
        inner: T,
        semaphore: PollSemaphore,
        /// The currently acquired semaphore permit, if there is sufficient
        /// concurrency to send a new request.
        ///
        /// The permit is acquired in `poll_ready`, and taken in `call` when sending
        /// a new request.
        permit: Option<OwnedSemaphorePermit>,
        max: usize,
        /// The ID of the connection limiter to identify the host.
        id: String,
    }

    impl<T> ConnectionLimiter<T> {
        /// Create a new concurrency limiter.
        pub fn new(inner: T, max: usize, id: String) -> Self {
            Self::with_semaphore(inner, Arc::new(Semaphore::new(max)), id)
        }

        /// Create a new concurrency limiter with a provided shared semaphore
        pub fn with_semaphore(inner: T, semaphore: Arc<Semaphore>, id: String) -> Self {
            let max = semaphore.available_permits();
            ConnectionLimiter {
                inner,
                semaphore: PollSemaphore::new(semaphore),
                permit: None,
                max,
                id,
            }
        }

        /// Get a reference to the inner service
        pub fn get_ref(&self) -> &T {
            &self.inner
        }

        /// Get a mutable reference to the inner service
        pub fn get_mut(&mut self) -> &mut T {
            &mut self.inner
        }

        /// Consume `self`, returning the inner service
        pub fn into_inner(self) -> T {
            self.inner
        }

        pub fn current_connections(&self) -> usize {
            self.max - self.semaphore.available_permits()
        }
    }

    impl<S, Request> Service<Request> for ConnectionLimiter<S>
    where
        S: Service<Request>,
    {
        type Response = S::Response;
        type Error = S::Error;
        type Future = ResponseFuture<S::Future>;

        fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
            // If we haven't already acquired a permit from the semaphore, try to
            // acquire one first.
            if self.permit.is_none() {
                self.permit = futures::ready!(self.semaphore.poll_acquire(cx));
                debug_assert!(
                    self.permit.is_some(),
                    "ConcurrencyLimit semaphore is never closed, so `poll_acquire` \
                 should never fail",
                );
            }

            HTTP_METRICS
                .open_http_connections(self.id.clone())
                .set(self.current_connections() as i64);

            // Once we've acquired a permit (or if we already had one), poll the
            // inner service.
            self.inner.poll_ready(cx)
        }

        fn call(&mut self, request: Request) -> Self::Future {
            // Take the permit
            let permit = self
                .permit
                .take()
                .expect("max requests in-flight; poll_ready must be called first");

            // Call the inner service
            let future = self.inner.call(request);

            ResponseFuture::new(future, permit)
        }
    }

    impl<T: Clone> Clone for ConnectionLimiter<T> {
        fn clone(&self) -> Self {
            // Since we hold an `OwnedSemaphorePermit`, we can't derive `Clone`.
            // Instead, when cloning the service, create a new service with the
            // same semaphore, but with the permit in the un-acquired state.
            Self {
                inner: self.inner.clone(),
                semaphore: self.semaphore.clone(),
                permit: None,
                max: self.max,
                id: self.id.clone(),
            }
        }
    }

    pin_project! {
        /// Future for the [`ConcurrencyLimit`] service.
        ///
        /// [`ConcurrencyLimit`]: crate::limit::ConcurrencyLimit
        #[derive(Debug)]
        pub struct ResponseFuture<T> {
            #[pin]
            inner: T,
            // Keep this around so that it is dropped when the future completes
            _permit: OwnedSemaphorePermit,
        }
    }

    impl<T> ResponseFuture<T> {
        fn new(inner: T, _permit: OwnedSemaphorePermit) -> Self {
            Self { inner, _permit }
        }
    }

    impl<F, T, E> Future for ResponseFuture<F>
    where
        F: Future<Output = Result<T, E>>,
    {
        type Output = Result<T, E>;

        fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
            Poll::Ready(futures::ready!(self.project().inner.poll(cx)))
        }
    }
}

pub mod testutils {
    use alloy_consensus::{
        BlobTransactionSidecar, EthereumTypedTransaction, SidecarBuilder, SignableTransaction as _,
        SimpleCoder, TxEip1559, TxEip2930, TxEip4844, TxEip4844Variant, TxEip4844WithSidecar,
        TxEip7702, TxEnvelope, TxLegacy,
    };
    use alloy_eips::Encodable2718 as _;
    use alloy_primitives::{Address, Bytes, TxKind, U256, U64};
    use alloy_signer::SignerSync as _;
    use alloy_signer_local::PrivateKeySigner;
    use rand::Rng;
    use rbuilder_primitives::serialize::{
        RawBundle, RawBundleMetadata, RawShareBundle, RawShareBundleBody, RawShareBundleInclusion,
    };

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
            let txs_len = rng.random_range(1..=2);
            // We only generate Eip1559 here.
            let txs = (0..txs_len)
                .map(|_| {
                    let signer =
                        PrivateKeySigner::from_bytes(&alloy_primitives::B256::random_with(rng))
                            .unwrap();
                    let tx = EthereumTypedTransaction::Eip1559(TxEip1559::random(rng));
                    let sighash = tx.signature_hash();
                    let signature = signer.sign_hash_sync(&sighash).unwrap();
                    TxEnvelope::new_unhashed(tx, signature).encoded_2718().into()
                })
                .collect();

            Self {
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
    }

    impl Random for RawShareBundle {
        fn random<R: Rng>(rng: &mut R) -> Self {
            let txs_len = rng.random_range(1..=10);
            // We only generate Eip1559 here.
            let bodies = (0..txs_len)
                .map(|_| {
                    let signer = PrivateKeySigner::random();
                    let tx = EthereumTypedTransaction::Eip1559(TxEip1559::random(rng));
                    let sighash = tx.signature_hash();
                    let signature = signer.sign_hash_sync(&sighash).unwrap();
                    let bytes = TxEnvelope::new_unhashed(tx, signature).encoded_2718().into();

                    RawShareBundleBody {
                        tx: Some(bytes),
                        can_revert: rng.random_range(0..=1) == 0,
                        revert_mode: None,
                        bundle: None,
                    }
                })
                .collect();

            Self {
                version: "v0.1".to_string(),
                inclusion: RawShareBundleInclusion {
                    block: U64::random_with(rng),
                    max_block: None,
                },
                body: bodies,
                validity: None,
                metadata: None,
                replacement_uuid: None,
            }
        }
    }
}
