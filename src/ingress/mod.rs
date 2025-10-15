use crate::{
    cache::{OrderCache, SignerCache},
    consts::{
        BUILDERNET_PRIORITY_HEADER, BUILDERNET_SENT_AT_HEADER, BUILDERNET_SIGNATURE_HEADER,
        DEFAULT_BUNDLE_VERSION, DEFAULT_HTTP_TIMEOUT_SECS, ETH_SEND_BUNDLE_METHOD,
        ETH_SEND_RAW_TRANSACTION_METHOD, FLASHBOTS_SIGNATURE_HEADER, MEV_SEND_BUNDLE_METHOD,
        USE_LEGACY_SIGNATURE,
    },
    entity::{Entity, EntityBuilderStats, EntityData, EntityRequest, EntityScores, SpamThresholds},
    forwarder::IngressForwarders,
    indexer::{IndexerHandle, OrderIndexer as _},
    jsonrpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse},
    metrics::{IngressHandlerMetricsExt as _, IngressSystemMetrics, IngressUserMetrics},
    primitives::{
        decode_transaction, BundleHash as _, BundleReceipt, DecodedBundle, DecodedShareBundle,
        EthResponse, EthereumTransaction, Samplable, SystemBundle, SystemBundleMetadata,
        SystemMevShareBundle, SystemTransaction, UtcInstant,
    },
    priority::{pqueue::PriorityQueues, Priority},
    rate_limit::CounterOverTime,
    utils::UtcDateTimeHeader as _,
    validation::validate_transaction,
};
use alloy_consensus::{crypto::secp256k1::recover_signer, transaction::SignerRecoverable};
use alloy_primitives::{eip191_hash_message, keccak256, Address, Bytes, B256};
use alloy_signer::Signature;
use axum::{
    body::Body,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::Response,
    Json,
};
use dashmap::DashMap;
use flate2::read::GzDecoder;
use rbuilder_primitives::serialize::{RawBundle, RawShareBundle};
use reqwest::Url;
use std::{
    collections::HashMap,
    io::Read as _,
    str::FromStr as _,
    sync::Arc,
    time::{Duration, Instant},
};
use time::UtcDateTime;
use tracing::*;

pub mod error;
use error::IngressError;

#[derive(Debug)]
pub struct OrderflowIngress {
    pub gzip_enabled: bool,
    pub rate_limiting_enabled: bool,
    pub rate_limit_lookback_s: u64,
    pub rate_limit_count: u64,
    pub score_lookback_s: u64,
    pub score_bucket_s: u64,
    pub spam_thresholds: SpamThresholds,
    pub pqueues: PriorityQueues,
    pub entities: DashMap<Entity, EntityData>,
    pub order_cache: OrderCache,
    pub signer_cache: SignerCache,
    pub forwarders: IngressForwarders,
    pub flashbots_signer: Option<Address>,
    /// The URL of the local builder. Used to send readyz requests.
    /// Optional for testing.
    pub local_builder_url: Option<Url>,
    pub builder_ready_endpoint: Option<Url>,
    pub indexer_handle: IndexerHandle,
}

impl OrderflowIngress {
    /// Return the score for the give entity. Unknown entities are not expected to be scored.
    ///
    /// # Panics
    ///
    /// If debug assertions are enabled and entity is [`Entity::Unknown`].
    fn entity_data(
        &self,
        entity: Entity,
    ) -> Option<dashmap::mapref::one::RefMut<'_, Entity, EntityData>> {
        if entity.is_unknown() {
            return None;
        }

        Some(self.entities.entry(entity).or_insert_with(|| EntityData {
            rate_limit: CounterOverTime::new(Duration::from_secs(self.rate_limit_lookback_s), 8),
            scores: EntityScores::new(
                Duration::from_secs(self.score_lookback_s),
                Duration::from_secs(self.score_bucket_s),
            ),
        }))
    }

    /// Returns priority for the given entity based
    fn priority_for(&self, entity: Entity, request: EntityRequest<'_>) -> Priority {
        if let Some(mut data) = self.entity_data(entity) {
            entity.priority(request, &mut data.scores, &self.spam_thresholds)
        } else {
            Priority::Low
        }
    }

    /// A maintenance (upkeep) task for internal orderflow ingress state.
    pub async fn maintain(self: Arc<Self>, interval: Duration) {
        loop {
            tokio::time::sleep(interval).await;
            let len_before = self.entities.len();
            info!(target: "ingress::state", entries = len_before, "Starting state maintenance");
            self.entities.retain(|_, c| c.rate_limit.count() > 0 || !c.scores.is_empty());
            let len_after = self.entities.len();
            let num_removed = len_before.saturating_sub(len_after);

            IngressUserMetrics::set_entity_count(len_after);
            info!(target: "ingress::state", entries = len_after, num_removed, "Finished state maintenance");
        }
    }

    pub async fn user_handler(
        State(ingress): State<Arc<Self>>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> JsonRpcResponse<EthResponse> {
        let received_at = UtcInstant::now();

        let body = match maybe_decompress(ingress.gzip_enabled, &headers, body) {
            Ok(decompressed) => decompressed,
            Err(error) => return JsonRpcResponse::error(None, error),
        };

        // NOTE: Signature is mandatory
        let Some(signer) = maybe_verify_signature(&headers, &body, USE_LEGACY_SIGNATURE) else {
            trace!(target: "ingress", "Error verifying signature");
            return JsonRpcResponse::error(None, JsonRpcError::InvalidSignature);
        };

        let entity = Entity::Signer(signer);

        if ingress.rate_limiting_enabled {
            if let Some(mut data) = ingress.entity_data(entity) {
                if data.rate_limit.count() > ingress.rate_limit_count {
                    trace!(target: "ingress", "Rate limited request");
                    IngressUserMetrics::increment_requests_rate_limited();
                    return JsonRpcResponse::error(None, JsonRpcError::RateLimited);
                }
                data.rate_limit.inc();
            }
        }

        let mut request: JsonRpcRequest<serde_json::Value> = match JsonRpcRequest::from_bytes(&body)
        {
            Ok(request) => request,
            Err(error) => {
                trace!(target: "ingress", "Error parsing JSON-RPC request");
                IngressUserMetrics::increment_json_rpc_parse_errors();
                return JsonRpcResponse::error(None, error);
            }
        };

        // Explicitly change the mutability of the `entity` variable.
        if let Some(mut data) = ingress.entity_data(entity) {
            data.scores.score_mut(received_at.into()).number_of_requests += 1;
        }

        trace!(target: "ingress", ?entity, id = request.id, method = request.method, params = ?request.params, "Serving user JSON-RPC request");
        let result = match request.method.as_str() {
            ETH_SEND_BUNDLE_METHOD => {
                let Some(Ok(bundle)) =
                    request.take_single_param().map(serde_json::from_value::<RawBundle>)
                else {
                    IngressUserMetrics::increment_json_rpc_parse_errors();
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                ingress.on_bundle(entity, bundle, received_at).await.map(EthResponse::BundleHash)
            }
            ETH_SEND_RAW_TRANSACTION_METHOD => {
                let Some(Ok(tx)) =
                    request.take_single_param().map(|value| -> Result<_, IngressError> {
                        let raw = serde_json::from_value::<Bytes>(value)?;
                        let decoded = decode_transaction(&raw)?;
                        Ok(EthereumTransaction::new(decoded, raw))
                    })
                else {
                    IngressUserMetrics::increment_json_rpc_parse_errors();
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                ingress.send_raw_transaction(entity, tx, received_at).await.map(EthResponse::TxHash)
            }
            MEV_SEND_BUNDLE_METHOD => {
                let Some(Ok(bundle)) =
                    request.take_single_param().map(serde_json::from_value::<RawShareBundle>)
                else {
                    IngressUserMetrics::increment_json_rpc_parse_errors();
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                ingress
                    .on_mev_share_bundle(entity, bundle, received_at)
                    .await
                    .map(EthResponse::BundleHash)
            }
            other => {
                error!(target: "ingress", %other, "Method not supported");
                IngressUserMetrics::increment_json_rpc_unknown_method(other.to_owned());
                return JsonRpcResponse::error(
                    Some(request.id),
                    JsonRpcError::MethodNotFound(other.to_owned()),
                );
            }
        };

        let response = match result {
            Ok(eth) => JsonRpcResponse::result(request.id, eth),
            Err(error) => {
                if error.is_validation() {
                    if let Some(mut data) = ingress.entity_data(entity) {
                        data.scores.score_mut(received_at.into()).invalid_requests += 1;
                    }
                }
                JsonRpcResponse::error(Some(request.id), error.into_jsonrpc_error())
            }
        };

        response
    }

    /// Handler for the `/readyz` endpoint. Used to check if the local builder is ready. Always
    /// returns 200 if the local builder is not configured.
    pub async fn ready_handler(State(ingress): State<Arc<Self>>) -> Response {
        if let Some(ref url) = ingress.builder_ready_endpoint {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
                .build()
                .unwrap();
            let url = url.join("readyz").unwrap();

            let Ok(response) = client.get(url.clone()).send().await else {
                error!(target: "ingress", %url, "Error sending readyz request");
                return Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(Body::from("not ready"))
                    .unwrap();
            };

            if response.status().is_success() {
                info!(target: "ingress", %url, "Local builder is ready");
                return Response::builder().status(StatusCode::OK).body(Body::from("OK")).unwrap();
            } else {
                error!(target: "ingress", %url, status = %response.status(), "Local builder is not ready");
                return Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(Body::from("not ready"))
                    .unwrap();
            }
        }

        Response::builder().status(StatusCode::OK).body(Body::from("OK")).unwrap()
    }

    pub async fn system_handler(
        State(ingress): State<Arc<Self>>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> JsonRpcResponse<EthResponse> {
        let received_at = UtcInstant::now();
        let payload_size = body.len();

        let body = match maybe_decompress(ingress.gzip_enabled, &headers, body) {
            Ok(decompressed) => decompressed,
            Err(error) => return JsonRpcResponse::error(None, error),
        };

        let peer = 'peer: {
            if let Some(address) = maybe_verify_signature(&headers, &body, USE_LEGACY_SIGNATURE) {
                if ingress.flashbots_signer.is_some_and(|addr| addr == address) {
                    break 'peer "flashbots".to_string();
                }

                if let Some(peer) = ingress.forwarders.find_peer(address) {
                    break 'peer peer;
                }
            }

            error!(
                target: "ingress",
                buildernet_signature_header = ?headers.get(BUILDERNET_SIGNATURE_HEADER),
                flashbots_signature_header = ?headers.get(FLASHBOTS_SIGNATURE_HEADER),
                "Error verifying peer signature"
            );
            return JsonRpcResponse::error(None, JsonRpcError::Internal);
        };

        let mut request: JsonRpcRequest<serde_json::Value> = match JsonRpcRequest::from_bytes(&body)
        {
            Ok(request) => request,
            Err(error) => {
                error!(target: "ingress", "Error parsing JSON-RPC request");
                IngressSystemMetrics::increment_json_rpc_parse_errors();
                return JsonRpcResponse::error(None, error);
            }
        };

        let sent_at = headers.get(BUILDERNET_SENT_AT_HEADER).and_then(UtcDateTime::parse_header);

        // TODO: Change to Low once Go proxy is updated / everyone is running Rust proxy.
        let mut priority = Priority::Medium;
        if let Some(priority_) = maybe_buildernet_priority(&headers) {
            priority = priority_;
        } else {
            trace!(target: "ingress", %peer, "Error retrieving priority from system request, defaulting to {priority}");
        }

        trace!(target: "ingress", %peer, id = request.id, method = request.method, params = ?request.params, "Serving system JSON-RPC request");
        let (raw, response) = match request.method.as_str() {
            ETH_SEND_BUNDLE_METHOD => {
                let Some(raw) = request.take_single_param() else {
                    error!(target: "ingress", "Error parsing bundle from system request");
                    IngressSystemMetrics::increment_json_rpc_parse_errors();
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                let Ok(bundle) = serde_json::from_value::<RawBundle>(raw.clone()) else {
                    error!(target: "ingress", "Error parsing bundle from system request");
                    IngressSystemMetrics::increment_json_rpc_parse_errors();
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                let bundle_hash = match bundle.metadata.bundle_hash {
                    Some(bundle_hash) => bundle_hash,
                    None => {
                        debug!(target: "ingress", "Bundle hash is not set");
                        bundle.bundle_hash()
                    }
                };

                // Deduplicate bundles.
                if ingress.order_cache.contains(&bundle_hash) {
                    trace!(target: "ingress", bundle_hash = %bundle_hash, "Bundle already processed");
                    IngressSystemMetrics::increment_order_cache_hit("bundle");

                    // Sample the order cache hit ratio.
                    if bundle_hash.sample(10) {
                        IngressUserMetrics::set_order_cache_hit_ratio(
                            ingress.order_cache.hit_ratio(),
                        );
                        IngressUserMetrics::set_order_cache_entry_count(
                            ingress.order_cache.entry_count(),
                        );
                    }

                    return JsonRpcResponse::result(
                        request.id,
                        EthResponse::BundleHash(bundle_hash),
                    );
                }

                ingress.order_cache.insert(bundle_hash);

                let receipt = BundleReceipt {
                    bundle_hash,
                    sent_at,
                    received_at: received_at.utc,
                    src_builder_name: peer,
                    payload_size: payload_size as u32,
                    priority,
                };

                ingress.indexer_handle.index_bundle_receipt(receipt);

                IngressSystemMetrics::record_bundle_rpc_duration(priority, received_at.elapsed());
                IngressSystemMetrics::record_txs_per_bundle(bundle.txs.len());

                (raw, EthResponse::BundleHash(bundle_hash))
            }
            ETH_SEND_RAW_TRANSACTION_METHOD => {
                let Some(raw) = request.take_single_param() else {
                    IngressSystemMetrics::increment_json_rpc_parse_errors();
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                let Ok(tx) =
                    decode_transaction(&serde_json::from_value::<Bytes>(raw.clone()).unwrap())
                else {
                    IngressSystemMetrics::increment_json_rpc_parse_errors();
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                let tx_hash = *tx.tx_hash();
                if ingress.order_cache.contains(&tx_hash) {
                    trace!(target: "ingress", tx_hash = %tx_hash, "Transaction already processed");
                    IngressSystemMetrics::increment_order_cache_hit("transaction");

                    // Sample the order cache hit ratio.
                    if tx_hash.sample(10) {
                        IngressUserMetrics::set_order_cache_hit_ratio(
                            ingress.order_cache.hit_ratio(),
                        );
                    }

                    return JsonRpcResponse::result(request.id, EthResponse::TxHash(tx_hash));
                }

                ingress.order_cache.insert(tx_hash);

                // TODO: Index transaction receipt
                _ = sent_at;

                IngressSystemMetrics::record_transaction_rpc_duration(
                    priority,
                    received_at.elapsed(),
                );

                (raw, EthResponse::TxHash(tx_hash))
            }
            MEV_SEND_BUNDLE_METHOD => {
                let Some(raw) = request.take_single_param() else {
                    IngressSystemMetrics::increment_json_rpc_parse_errors();
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                let Ok(bundle) = serde_json::from_value::<RawShareBundle>(raw.clone()) else {
                    IngressSystemMetrics::increment_json_rpc_parse_errors();
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                let bundle_hash = bundle.bundle_hash();
                if ingress.order_cache.contains(&bundle_hash) {
                    trace!(target: "ingress", bundle_hash = %bundle_hash, "Share bundle already processed");
                    IngressSystemMetrics::increment_order_cache_hit("mev_share_bundle");

                    // Sample the order cache hit ratio.
                    if bundle_hash.sample(10) {
                        IngressUserMetrics::set_order_cache_hit_ratio(
                            ingress.order_cache.hit_ratio(),
                        );
                    }

                    return JsonRpcResponse::result(
                        request.id,
                        EthResponse::BundleHash(bundle_hash),
                    );
                }

                ingress.order_cache.insert(bundle_hash);

                IngressSystemMetrics::record_txs_per_mev_share_bundle(bundle.body.len());
                IngressSystemMetrics::record_mev_share_bundle_rpc_duration(
                    priority,
                    received_at.elapsed(),
                );

                (raw, EthResponse::BundleHash(bundle_hash))
            }
            other => {
                error!(target: "ingress", %other, "Method not supported");
                IngressSystemMetrics::increment_json_rpc_unknown_method(other.to_owned());
                return JsonRpcResponse::error(
                    Some(request.id),
                    JsonRpcError::MethodNotFound(other.to_owned()),
                );
            }
        };

        // Send request only to the local builder forwarder.
        ingress.forwarders.send_to_local(priority, &request.method, raw, received_at);

        JsonRpcResponse::result(request.id, response)
    }

    /// Handles a new bundle.
    async fn on_bundle(
        &self,
        entity: Entity,
        mut bundle: RawBundle,
        received_at: UtcInstant,
    ) -> Result<B256, IngressError> {
        let start = Instant::now();
        trace!(target: "ingress", ?entity, "Processing bundle");
        // Convert to system bundle.
        let Entity::Signer(signer) = entity else { unreachable!() };
        bundle.metadata.signing_address = Some(signer);

        let priority = self.priority_for(entity, EntityRequest::Bundle(&bundle));

        // Set replacement nonce if it is not set and we have a replacement UUID or UUID. This is
        // needed to decode the replacement data correctly in
        // [`SystemBundle::try_from_bundle_and_signer`].
        if (bundle.metadata.uuid.or(bundle.metadata.replacement_uuid).is_some()) &&
            bundle.metadata.replacement_nonce.is_none()
        {
            let timestamp = received_at.utc.unix_timestamp_nanos() / 1000;
            bundle.metadata.replacement_nonce =
                Some(timestamp.try_into().expect("Timestamp too large"));
        }

        // Deduplicate bundles.
        // IMPORTANT: For correct cancellation deduplication, the replacement nonce must be set (see
        // above).
        let bundle_hash = bundle.bundle_hash();
        let sample = bundle_hash.sample(10);
        if self.order_cache.contains(&bundle_hash) {
            trace!(target: "ingress", %bundle_hash, "Bundle already processed");
            IngressUserMetrics::increment_order_cache_hit("bundle");

            if sample {
                IngressUserMetrics::set_order_cache_hit_ratio(self.order_cache.hit_ratio());
                IngressUserMetrics::set_order_cache_entry_count(self.order_cache.entry_count());
            }

            return Ok(bundle_hash);
        }

        self.order_cache.insert(bundle_hash);
        let signer_cache = self.signer_cache.clone();
        let lookup = move |hash: B256| signer_cache.get(&hash);

        if bundle.metadata.version.is_none() {
            bundle.metadata.version = Some(DEFAULT_BUNDLE_VERSION.to_string());
        }

        // Decode and validate the bundle.
        let bundle = self
            .pqueues
            .spawn_with_priority(priority, move || {
                let metadata = SystemBundleMetadata { signer, received_at, priority };
                SystemBundle::try_decode_with_lookup(bundle, metadata, lookup)
            })
            .await
            .inspect_err(|e| {
                error!(target: "ingress", ?e, "Error decoding bundle");
                IngressUserMetrics::increment_validation_errors(&e);
            })?;

        match bundle.decoded_bundle.as_ref() {
            DecodedBundle::Bundle(bundle) => {
                debug!(target: "ingress", bundle_hash = %bundle.hash, "New bundle decoded");
                for tx in &bundle.txs {
                    self.signer_cache.insert(tx.hash(), tx.signer());
                }
            }
            DecodedBundle::EmptyReplacement(replacement_data) => {
                debug!(target: "ingress", ?replacement_data, "Replacement bundle decoded");
            }
        }

        // Sample the signer cache hit ratio.
        if sample {
            IngressUserMetrics::set_signer_cache_hit_ratio(self.signer_cache.hit_ratio());
            IngressUserMetrics::set_signer_cache_entry_count(self.signer_cache.entry_count());
        }

        if bundle.is_empty() {
            IngressUserMetrics::increment_empty_bundles();
        } else {
            IngressUserMetrics::record_txs_per_bundle(bundle.raw_bundle.txs.len());
        }

        let elapsed = start.elapsed();
        debug!(target: "ingress", bundle_uuid = %bundle.uuid(), ?elapsed, "Bundle validated");

        self.indexer_handle.index_bundle(bundle.clone());

        self.send_bundle(bundle).await
    }

    /// Handles a new MEV Share bundle.
    async fn on_mev_share_bundle(
        &self,
        entity: Entity,
        bundle: RawShareBundle,
        received_at: UtcInstant,
    ) -> Result<B256, IngressError> {
        let start = Instant::now();
        trace!(target: "ingress", ?entity, "Processing MEV Share bundle");
        // Convert to system bundle.
        let Entity::Signer(signer) = entity else { unreachable!() };

        let priority = self.priority_for(entity, EntityRequest::MevShareBundle(&bundle));

        // Deduplicate bundles.
        let bundle_hash = bundle.bundle_hash();
        if self.order_cache.contains(&bundle_hash) {
            trace!(target: "ingress", %bundle_hash, "Bundle already processed");
            IngressUserMetrics::increment_order_cache_hit("mev_share_bundle");

            if bundle_hash.sample(10) {
                IngressUserMetrics::set_order_cache_hit_ratio(self.order_cache.hit_ratio());
            }

            return Ok(bundle_hash);
        }

        self.order_cache.insert(bundle_hash);

        // Decode and validate the bundle.
        let bundle = self
            .pqueues
            .spawn_with_priority(priority, move || {
                SystemMevShareBundle::try_from_bundle_and_signer(
                    bundle,
                    signer,
                    received_at,
                    priority,
                )
            })
            .await
            .inspect_err(|e| {
                error!(target: "ingress", ?e, "Error decoding bundle");
                IngressUserMetrics::increment_validation_errors(&e);
            })?;

        match bundle.decoded.as_ref() {
            DecodedShareBundle::New(bundle) => {
                debug!(target: "ingress", bundle_hash = %bundle.hash, "New bundle decoded");
            }
            DecodedShareBundle::Cancel(cancellation) => {
                debug!(target: "ingress", ?cancellation, "Cancellation bundle decoded");
            }
        }

        IngressUserMetrics::record_txs_per_mev_share_bundle(bundle.raw.body.len());

        let elapsed = start.elapsed();
        debug!(target: "ingress", %bundle_hash, ?elapsed, "MEV Share bundle validated");

        self.send_mev_share_bundle(priority, bundle).await
    }

    async fn send_bundle(&self, bundle: SystemBundle) -> Result<B256, IngressError> {
        let bundle_uuid = bundle.uuid();
        let bundle_hash = bundle.bundle_hash();
        let priority = bundle.metadata.priority;
        let received_at = bundle.metadata.received_at;

        // Send request to all forwarders.
        self.forwarders.broadcast_bundle(bundle);
        debug!(target: "ingress", %bundle_uuid, %bundle_hash, "Bundle processed");

        IngressUserMetrics::record_bundle_rpc_duration(priority, received_at.elapsed());
        Ok(bundle_hash)
    }

    async fn send_mev_share_bundle(
        &self,
        priority: Priority,
        bundle: SystemMevShareBundle,
    ) -> Result<B256, IngressError> {
        let bundle_hash = bundle.bundle_hash();
        let received_at = bundle.received_at;

        self.forwarders.broadcast_mev_share_bundle(priority, bundle);
        debug!(target: "ingress", %bundle_hash, "MEV Share bundle processed");

        IngressUserMetrics::record_mev_share_bundle_rpc_duration(priority, received_at.elapsed());
        Ok(bundle_hash)
    }

    async fn send_raw_transaction(
        &self,
        entity: Entity,
        transaction: EthereumTransaction,
        received_at: UtcInstant,
    ) -> Result<B256, IngressError> {
        let start = Instant::now();
        let tx_hash = *transaction.hash();

        // Deduplicate transactions.
        if self.order_cache.contains(&tx_hash) {
            trace!(target: "ingress", tx_hash = %tx_hash, "Transaction already processed");
            IngressUserMetrics::increment_order_cache_hit("transaction");

            if tx_hash.sample(10) {
                IngressUserMetrics::set_order_cache_hit_ratio(self.order_cache.hit_ratio());
            }

            return Ok(tx_hash);
        }

        self.order_cache.insert(tx_hash);

        let Entity::Signer(signer) = entity else { unreachable!() };
        let priority = self.priority_for(entity, EntityRequest::PrivateTx(&transaction));

        let system_transaction =
            SystemTransaction::from_transaction(transaction, signer, received_at, priority);

        let tx = system_transaction.transaction.clone();

        // Spawn expensive operations like ECDSA recovery and consensus validation.
        self.pqueues
            .spawn_with_priority(priority, move || {
                validate_transaction(&tx.decoded)?;
                tx.recover_signer()?;
                Ok::<(), IngressError>(())
            })
            .await
            .inspect_err(|e| {
                error!(target: "ingress", ?e, "Error validating transaction");
                IngressUserMetrics::increment_validation_errors(e);
            })?;

        // Send request to all forwarders.
        self.forwarders.broadcast_transaction(system_transaction);

        let elapsed = start.elapsed();
        debug!(target: "ingress", tx_hash = %tx_hash, elapsed = ?elapsed, "Raw transaction processed");

        IngressUserMetrics::record_transaction_rpc_duration(priority, received_at.elapsed());

        Ok(tx_hash)
    }

    pub async fn builder_handler(
        State(ingress): State<Arc<Self>>,
        Json(data): Json<HashMap<Entity, EntityBuilderStats>>,
    ) {
        let received_at = Instant::now();
        info!(target: "ingress", count = data.len(), "Updating entity stats with builder data");
        for (entity, stats) in data {
            if let Some(mut data) = ingress.entity_data(entity) {
                data.scores.score_mut(received_at).builder_stats.extend(stats);
            }
        }
    }
}

/// Attempt to decompress the header if `content-encoding` header is set to `gzip`.
pub fn maybe_decompress(
    gzip_enabled: bool,
    headers: &HeaderMap,
    body: axum::body::Bytes,
) -> Result<Vec<u8>, JsonRpcError> {
    if gzip_enabled && headers.get(header::CONTENT_ENCODING).is_some_and(|enc| enc == "gzip") {
        let mut decompressed = Vec::new();
        GzDecoder::new(&body[..])
            .read_to_end(&mut decompressed)
            .map_err(|_| JsonRpcError::ParseError)?;
        Ok(decompressed)
    } else {
        Ok(body.to_vec())
    }
}

/// Parse the signature from [`BUILDERNET_SIGNATURE_HEADER`] header and verify the signer of the
/// request. [`FLASHBOTS_SIGNATURE_HEADER`] is supported for backwards compatibility.
pub fn maybe_verify_signature(headers: &HeaderMap, body: &[u8], legacy: bool) -> Option<Address> {
    let signature_header = headers
        .get(BUILDERNET_SIGNATURE_HEADER)
        .or_else(|| headers.get(FLASHBOTS_SIGNATURE_HEADER))?;
    let (address, signature) = signature_header.to_str().ok()?.split_once(':')?;
    let signature = Signature::from_str(signature).ok()?;

    if legacy {
        let hash_str = format!("{:?}", keccak256(body));
        let message_hash = eip191_hash_message(hash_str.as_bytes());
        let signer = recover_signer(&signature, message_hash).ok()?;

        Some(signer).filter(|signer| Some(signer) == Address::from_str(address).ok().as_ref())
    } else {
        let body_hash = keccak256(body);
        let signer = recover_signer(&signature, body_hash).ok()?;
        Some(signer).filter(|signer| Some(signer) == Address::from_str(address).ok().as_ref())
    }
}

/// Attempt to retrieve BuilderNet priority set by other ingresses.
fn maybe_buildernet_priority(headers: &HeaderMap) -> Option<Priority> {
    let priority_header = headers.get(BUILDERNET_PRIORITY_HEADER)?;
    priority_header.to_str().ok()?.parse().ok()
}
