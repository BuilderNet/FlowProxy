use crate::{
    cache::OrderCache,
    entity::{Entity, EntityBuilderStats, EntityData, EntityRequest, EntityScores, SpamThresholds},
    forwarder::IngressForwarders,
    jsonrpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse},
    priority::{pqueue::PriorityQueues, Priority},
    rate_limit::CounterOverTime,
    types::{
        decode_transaction, BundleHash as _, DecodedBundle, EthResponse, SystemBundle,
        SystemTransaction,
    },
    validation::validate_transaction,
};
use alloy_consensus::{
    crypto::secp256k1::recover_signer,
    transaction::{PooledTransaction, SignerRecoverable},
};
use alloy_primitives::{keccak256, Address, Bytes, B256};
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
use metrics::{Counter, Histogram};
use metrics_derive::Metrics;
use rbuilder_primitives::serialize::RawBundle;
use reqwest::Url;
use std::{
    collections::HashMap,
    io::Read as _,
    str::FromStr as _,
    sync::Arc,
    time::{Duration, Instant},
};
use tracing::*;

pub mod error;
use error::IngressError;

/// Header name for flashbots signature.
pub const FLASHBOTS_SIGNATURE_HEADER: &str = "X-Flashbots-Signature";

/// Header name for flashbots priority.
pub const BUILDERNET_PRIORITY_HEADER: &str = "X-Buildernet-Priority";

/// Header name for XFF header.
pub const XFF_HEADER: &str = "X-Forwarded-For";

/// JSON-RPC method name for sending bundles.
pub const ETH_SEND_BUNDLE_METHOD: &str = "eth_sendBundle";

/// JSON-RPC method name for sending raw transactions.
pub const ETH_SEND_RAW_TRANSACTION_METHOD: &str = "eth_sendRawTransaction";

#[derive(Debug)]
pub struct OrderflowIngress {
    pub gzip_enabled: bool,
    pub rate_limit_lookback_s: u64,
    pub rate_limit_count: u64,
    pub score_lookback_s: u64,
    pub score_bucket_s: u64,
    pub spam_thresholds: SpamThresholds,
    pub pqueues: PriorityQueues,
    pub entities: DashMap<Entity, EntityData>,
    pub order_cache: OrderCache,
    pub forwarders: IngressForwarders,
    /// The URL of the local builder. Used to send readyz requests.
    /// Optional for testing.
    pub local_builder_url: Option<Url>,
    pub metrics: OrderflowIngressMetrics,
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
            info!(target: "ingress::state", entries = len_after, num_removed, "Finished state maintenance");
        }
    }

    pub async fn user_handler(
        State(ingress): State<Arc<Self>>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> JsonRpcResponse<EthResponse> {
        let received_at = Instant::now();
        ingress.metrics.user.requests_received.increment(1);

        let body = match maybe_decompress(ingress.gzip_enabled, &headers, body) {
            Ok(decompressed) => decompressed,
            Err(error) => return JsonRpcResponse::error(None, error),
        };

        // NOTE: Signature is mandatory
        let Some(signer) = maybe_verify_signature(&headers, &body) else {
            return JsonRpcResponse::error(None, JsonRpcError::InvalidSignature);
        };

        let entity = Entity::Signer(signer);

        if let Some(mut data) = ingress.entity_data(entity) {
            if data.rate_limit.count() > ingress.rate_limit_count {
                ingress.metrics.user.requests_rate_limited.increment(1);
                return JsonRpcResponse::error(None, JsonRpcError::RateLimited);
            }
            data.rate_limit.inc();
        }

        let mut request: JsonRpcRequest<serde_json::Value> = match JsonRpcRequest::from_bytes(&body)
        {
            Ok(request) => request,
            Err(error) => {
                ingress.metrics.user.json_rpc_parse_errors.increment(1);
                return JsonRpcResponse::error(None, error);
            }
        };

        // Explicitly change the mutability of the `entity` variable.
        if let Some(mut data) = ingress.entity_data(entity) {
            data.scores.score_mut(received_at).number_of_requests += 1;
        }

        trace!(target: "ingress", ?entity, id = request.id, method = request.method, params = ?request.params, "Serving user JSON-RPC request");
        let result = match request.method.as_str() {
            ETH_SEND_BUNDLE_METHOD => {
                let Some(Ok(bundle)) =
                    request.take_single_param().map(serde_json::from_value::<RawBundle>)
                else {
                    ingress.metrics.user.json_rpc_parse_errors.increment(1);
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                ingress.on_bundle(entity, bundle).await.map(EthResponse::BundleHash)
            }
            ETH_SEND_RAW_TRANSACTION_METHOD => {
                let Some(Ok(tx)) = request.take_single_param().map(|value| {
                    decode_transaction(&serde_json::from_value::<Bytes>(value).unwrap())
                }) else {
                    ingress.metrics.user.json_rpc_parse_errors.increment(1);
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                ingress.send_raw_transaction(entity, tx).await.map(EthResponse::TxHash)
            }
            _ => return JsonRpcResponse::error(Some(request.id), JsonRpcError::MethodNotFound),
        };

        let response = match result {
            Ok(eth) => JsonRpcResponse::result(request.id, eth),
            Err(error) => {
                if error.is_validation() {
                    if let Some(mut data) = ingress.entity_data(entity) {
                        data.scores.score_mut(received_at).invalid_requests += 1;
                    }
                }
                JsonRpcResponse::error(Some(request.id), error.into_jsonrpc_error())
            }
        };

        ingress.metrics.user.record_method_metrics(&request.method, received_at);

        response
    }

    /// Handler for the `/readyz` endpoint. Used to check if the local builder is ready. Always
    /// returns 200 if the local builder is not configured.
    pub async fn ready_handler(State(ingress): State<Arc<Self>>) -> Response {
        if let Some(ref url) = ingress.local_builder_url {
            let client =
                reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();
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
        let received_at = Instant::now();
        ingress.metrics.system.requests_received.increment(1);

        let body = match maybe_decompress(ingress.gzip_enabled, &headers, body) {
            Ok(decompressed) => decompressed,
            Err(error) => return JsonRpcResponse::error(None, error),
        };

        let peer = 'peer: {
            if let Some(address) = maybe_verify_signature(&headers, &body) {
                if let Some(peer) = ingress.forwarders.find_peer(address) {
                    break 'peer peer;
                }
            }

            error!(target: "ingress", "Error verifying signature peer signatures");
            return JsonRpcResponse::error(None, JsonRpcError::Internal);
        };

        let mut request: JsonRpcRequest<serde_json::Value> = match JsonRpcRequest::from_bytes(&body)
        {
            Ok(request) => request,
            Err(error) => {
                ingress.metrics.system.json_rpc_parse_errors.increment(1);
                return JsonRpcResponse::error(None, error);
            }
        };

        let mut priority = Priority::Low;
        if let Some(priority_) = maybe_buildernet_priority(&headers) {
            priority = priority_;
        } else {
            error!(target: "ingress", %peer, "Error retrieving priority from system request, defaulting to low");
        }

        trace!(target: "ingress", %peer, id = request.id, method = request.method, params = ?request.params, "Serving system JSON-RPC request");
        let (raw, response) = match request.method.as_str() {
            ETH_SEND_BUNDLE_METHOD => {
                let Some(raw) = request.take_single_param() else {
                    ingress.metrics.system.json_rpc_parse_errors.increment(1);
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                let Ok(bundle) = serde_json::from_value::<RawBundle>(raw.clone()) else {
                    ingress.metrics.system.json_rpc_parse_errors.increment(1);
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                // Deduplicate bundles.
                let bundle_hash = bundle.bundle_hash();
                if ingress.order_cache.contains(&bundle_hash) {
                    trace!(target: "ingress", bundle_hash = %bundle_hash, "Bundle already processed");
                    return JsonRpcResponse::result(
                        request.id,
                        EthResponse::BundleHash(bundle_hash),
                    );
                }

                ingress.order_cache.insert(bundle_hash);

                (raw, EthResponse::BundleHash(bundle_hash))
            }
            ETH_SEND_RAW_TRANSACTION_METHOD => {
                let Some(raw) = request.take_single_param() else {
                    ingress.metrics.system.json_rpc_parse_errors.increment(1);
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                let Ok(tx) =
                    decode_transaction(&serde_json::from_value::<Bytes>(raw.clone()).unwrap())
                else {
                    ingress.metrics.system.json_rpc_parse_errors.increment(1);
                    return JsonRpcResponse::error(Some(request.id), JsonRpcError::InvalidParams);
                };

                let tx_hash = *tx.tx_hash();
                if ingress.order_cache.contains(&tx_hash) {
                    trace!(target: "ingress", tx_hash = %tx_hash, "Transaction already processed");
                    return JsonRpcResponse::result(request.id, EthResponse::TxHash(tx_hash));
                }

                ingress.order_cache.insert(tx_hash);

                (raw, EthResponse::TxHash(tx_hash))
            }
            _ => return JsonRpcResponse::error(Some(request.id), JsonRpcError::MethodNotFound),
        };

        // Send request only to the local builder forwarder.
        ingress.forwarders.send_to_local(priority, &request.method, raw);

        ingress.metrics.system.record_method_metrics(&request.method, received_at);

        JsonRpcResponse::result(request.id, response)
    }

    /// Handles a new bundle.
    async fn on_bundle(&self, entity: Entity, bundle: RawBundle) -> Result<B256, IngressError> {
        let start = Instant::now();
        trace!(target: "ingress", ?entity, "Processing bundle");
        // Convert to system bundle.
        let Entity::Signer(signer) = entity else { unreachable!() };

        let priority = self.priority_for(entity, EntityRequest::Bundle(&bundle));

        // Deduplicate bundles.
        let bundle_hash = bundle.bundle_hash();
        if self.order_cache.contains(&bundle_hash) {
            trace!(target: "ingress", bundle_hash = %bundle_hash, "Bundle already processed");
            return Ok(bundle_hash);
        }

        self.order_cache.insert(bundle_hash);

        // Decode and validate the bundle.
        let bundle = self
            .pqueues
            .spawn_with_priority(priority, move || {
                SystemBundle::try_from_bundle_and_signer(bundle, signer)
            })
            .await?;

        match bundle.decoded_bundle.as_ref() {
            DecodedBundle::Bundle(bundle) => {
                debug!(target: "ingress", bundle_hash = %bundle.hash, "New bundle decoded");
            }
            DecodedBundle::Replacement(replacement_data) => {
                debug!(target: "ingress", replacement_data = ?replacement_data, "Replacement bundle decoded");
            }
        }

        let elapsed = start.elapsed();
        debug!(target: "ingress", bundle_uuid = %bundle.uuid(), elapsed = ?elapsed, "Bundle validated");

        // TODO: Index here

        self.send_bundle(priority, bundle).await
    }

    async fn send_bundle(
        &self,
        priority: Priority,
        bundle: SystemBundle,
    ) -> Result<B256, IngressError> {
        let uuid = bundle.uuid();
        let bundle_hash = bundle.bundle_hash();
        // Send request to all forwarders.
        self.forwarders.broadcast_bundle(priority, bundle);

        debug!(target: "ingress", bundle_uuid = %uuid, bundle_hash = %bundle_hash, "Bundle processed");

        // TODO: Return bundle UUID or hash or both?
        Ok(todo!("Return bundle UUID or hash or both?"))
    }

    async fn send_raw_transaction(
        &self,
        entity: Entity,
        transaction: PooledTransaction,
    ) -> Result<B256, IngressError> {
        let start = Instant::now();
        let tx_hash = *transaction.hash();

        // Deduplicate transactions.
        if self.order_cache.contains(&tx_hash) {
            trace!(target: "ingress", tx_hash = %tx_hash, "Transaction already processed");
            return Ok(tx_hash);
        }

        self.order_cache.insert(tx_hash);

        let Entity::Signer(signer) = entity else { unreachable!() };
        let transaction = SystemTransaction::from_transaction_and_signer(transaction, signer);

        let unique_key = transaction.unique_key();
        if self.order_cache.contains(unique_key) {
            trace!(target: "ingress", unique_key = %unique_key, "Transaction already processed");
            return Ok(tx_hash);
        }

        self.order_cache.insert(tx_hash);

        let Entity::Signer(signer) = entity else { unreachable!() };
        let transaction = SystemTransaction::from_transaction_and_signer(transaction, signer);

        // Determine priority for processing given request.
        let priority = self.priority_for(entity, EntityRequest::PrivateTx(&transaction));

        let tx = transaction.transaction.clone();

        // Spawn expensive operations like ECDSA recovery and consensus validation.
        self.pqueues
            .spawn_with_priority(priority, move || {
                validate_transaction(tx.as_ref())?;
                tx.recover_signer()?;
                Ok::<(), IngressError>(())
            })
            .await?;

        // Send request to all forwarders.
        self.forwarders.broadcast_transaction(priority, transaction);

        let elapsed = start.elapsed();
        debug!(target: "ingress", tx_hash = %tx_hash, elapsed = ?elapsed, "Raw transaction processed");

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

/// Parse [`FLASHBOTS_SIGNATURE_HEADER`] header and verify the signer of the request.
pub fn maybe_verify_signature(headers: &HeaderMap, body: &[u8]) -> Option<Address> {
    let signature_header = headers.get(FLASHBOTS_SIGNATURE_HEADER)?;
    let (address, signature) = signature_header.to_str().ok()?.split_once(':')?;
    let signature = Signature::from_str(signature).ok()?;
    let body_hash = keccak256(body);
    let signer = recover_signer(&signature, body_hash).ok()?;
    Some(signer).filter(|signer| Some(signer) == Address::from_str(address).ok().as_ref())
}

/// Attempt to retrieve BuilderNet priority set by other ingresses.
fn maybe_buildernet_priority(headers: &HeaderMap) -> Option<Priority> {
    let priority_header = headers.get(BUILDERNET_PRIORITY_HEADER)?;
    priority_header.to_str().ok()?.parse().ok()
}

#[derive(Clone, Debug)]
pub struct OrderflowIngressMetrics {
    user: OrderflowHandlerMetrics,
    system: OrderflowHandlerMetrics,
}

impl Default for OrderflowIngressMetrics {
    fn default() -> Self {
        Self {
            user: OrderflowHandlerMetrics::new_with_labels(&[("handler", "user")]),
            system: OrderflowHandlerMetrics::new_with_labels(&[("handler", "system")]),
        }
    }
}

#[derive(Clone, Metrics)]
#[metrics(scope = "handler")]
pub struct OrderflowHandlerMetrics {
    /// The total number of requests received.
    requests_received: Counter,
    /// The total number of requests that were rate limited.
    requests_rate_limited: Counter,
    /// The total number of JSON-RPC requests that couldn't be parsed.
    json_rpc_parse_errors: Counter,
    /// The total number of JSON-RPC requests with unknown method.
    json_rpc_unknown_method: Counter,
    /// The total number of bundles received.
    bundles_received: Counter,
    /// The total number of raw transactions received.
    raw_transactions_received: Counter,
    /// The number of seconds in which the request was processed.
    processed_in: Histogram,
    /// The number of seconds in which bundle was processed.
    bundle_processed_in: Histogram,
    /// The number of seconds in which raw transaction was processed.
    raw_transaction_processed_in: Histogram,
}

impl OrderflowHandlerMetrics {
    fn record_method_metrics(&self, method: &str, received_at: Instant) {
        let elapsed_secs = received_at.elapsed().as_secs_f64();
        self.processed_in.record(elapsed_secs);
        match method {
            ETH_SEND_BUNDLE_METHOD => {
                self.bundle_processed_in.record(elapsed_secs);
                self.bundles_received.increment(1);
            }
            ETH_SEND_RAW_TRANSACTION_METHOD => {
                self.raw_transaction_processed_in.record(elapsed_secs);
                self.raw_transactions_received.increment(1);
            }
            _ => {
                self.json_rpc_unknown_method.increment(1);
            }
        };
    }
}
