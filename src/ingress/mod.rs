use crate::{
    cache::{OrderCache, SignerCache},
    consts::{
        BUILDERNET_PRIORITY_HEADER, BUILDERNET_SENT_AT_HEADER, BUILDERNET_SIGNATURE_HEADER,
        DEFAULT_BUNDLE_VERSION, DEFAULT_HTTP_TIMEOUT_SECS, ETH_SEND_BUNDLE_METHOD,
        ETH_SEND_RAW_TRANSACTION_METHOD, FLASHBOTS_SIGNATURE_HEADER, MEV_SEND_BUNDLE_METHOD,
        UNKNOWN, USE_LEGACY_SIGNATURE,
    },
    entity::{Entity, EntityBuilderStats, EntityData, EntityRequest, EntityScores, SpamThresholds},
    forwarder::IngressForwarders,
    indexer::{IndexerHandle, OrderIndexer as _},
    jsonrpc::{JsonRpcError, JsonRpcRequest, JsonRpcResponse},
    metrics::{IngressMetrics, SYSTEM_METRICS},
    primitives::{
        decode_transaction, BundleHash as _, BundleReceipt, DecodedBundle, DecodedShareBundle,
        EthResponse, EthereumTransaction, PeerProxyInfo, Samplable, SystemBundle,
        SystemBundleDecoder, SystemBundleMetadata, SystemMevShareBundle, SystemTransaction,
        UtcInstant,
    },
    priority::{workers::PriorityWorkers, Priority},
    rate_limit::CounterOverTime,
    utils::{short_uuid_v4, UtcDateTimeHeader as _},
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
use serde_json::Value;
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
    pub system_bundle_decoder: SystemBundleDecoder,
    pub spam_thresholds: SpamThresholds,
    pub pqueues: PriorityWorkers,
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

    /// The system api port, TCP-only
    pub system_api_port: u16,

    // Metrics
    pub(crate) user_metrics: IngressMetrics,
    pub(crate) system_metrics: IngressMetrics,
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

    /// Record queue capacity metrics for the given priority.
    fn record_queue_capacity_metrics(&self, priority: Priority) {
        let available_permits = self.pqueues.available_permits_for(priority);
        let total_permits = self.pqueues.total_permits_for(priority);
        if available_permits == 0 {
            SYSTEM_METRICS.queue_capacity_hits(priority.as_str()).inc();
        }

        // Record queue capacity almost hit if the queue is at 75% of capacity.
        if available_permits <= total_permits / 4 {
            SYSTEM_METRICS.queue_capacity_almost_hits(priority.as_str()).inc();
        }
    }

    /// Perform maintenance task for internal orderflow ingress state.
    #[tracing::instrument(skip_all, name = "ingress_maintanance")]
    pub async fn maintenance(&self) {
        let len_before = self.entities.len();
        tracing::info!(entries = len_before, "starting state maintenance");

        self.entities.retain(|_, c| c.rate_limit.count() > 0 || !c.scores.is_empty());
        let len_after = self.entities.len();
        let num_removed = len_before.saturating_sub(len_after);

        self.user_metrics.entity_count().set(len_after);
        tracing::info!(entries = len_after, num_removed, "finished state maintenance");
    }

    #[tracing::instrument(skip_all, name = "ingress",
        fields(
            handler = "user",
            id = %short_uuid_v4(),
            method = tracing::field::Empty,
        ))]
    pub async fn user_handler(
        State(ingress): State<Arc<Self>>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> JsonRpcResponse<EthResponse> {
        let received_at = UtcInstant::now();

        let body = match maybe_decompress(ingress.gzip_enabled, &headers, body) {
            Ok(decompressed) => decompressed,
            Err(error) => return JsonRpcResponse::error(Value::Null, error),
        };

        // NOTE: Signature is mandatory
        let body_clone = body.clone();
        let Some(signer) = ingress
            .pqueues
            .spawn_with_priority(Priority::Medium, move || {
                maybe_verify_signature(&headers, &body_clone, USE_LEGACY_SIGNATURE)
            })
            .await
        else {
            tracing::trace!("failed to verify signature");
            return JsonRpcResponse::error(Value::Null, JsonRpcError::InvalidSignature);
        };

        let entity = Entity::Signer(signer);

        if ingress.rate_limiting_enabled {
            if let Some(mut data) = ingress.entity_data(entity) {
                if data.rate_limit.count() > ingress.rate_limit_count {
                    tracing::trace!("rate limited request");
                    ingress.user_metrics.requests_rate_limited().inc();
                    return JsonRpcResponse::error(Value::Null, JsonRpcError::RateLimited);
                }
                data.rate_limit.inc();
            }
        }

        // Since this performs UTF-8 validation, only do it if tracing is enabled at trace level.
        let body_utf8 = if span_enabled!(Level::TRACE) {
            str::from_utf8(&body).unwrap_or("<invalid utf8>")
        } else {
            ""
        };

        let mut request: JsonRpcRequest<serde_json::Value> = match JsonRpcRequest::from_bytes(&body)
        {
            Ok(request) => request,
            Err(e) => {
                tracing::trace!(?e, body_utf8, "failed to parse json-rpc request");
                ingress.user_metrics.json_rpc_parse_errors(UNKNOWN).inc();
                return JsonRpcResponse::error(Value::Null, e);
            }
        };
        tracing::Span::current().record("method", tracing::field::display(&request.method));

        // Explicitly change the mutability of the `entity` variable.
        if let Some(mut data) = ingress.entity_data(entity) {
            data.scores.score_mut(received_at.into()).number_of_requests += 1;
        }

        tracing::trace!(?entity, "serving user json-rpc request");

        let result = match request.method.as_str() {
            ETH_SEND_BUNDLE_METHOD => {
                let Some(Ok(bundle)) = request.take_single_param().map(|p| {
                    serde_json::from_value::<RawBundle>(p)
                        .inspect_err(|e| tracing::trace!(?e, body_utf8, "failed to parse bundle"))
                }) else {
                    ingress.user_metrics.json_rpc_parse_errors(ETH_SEND_BUNDLE_METHOD).inc();
                    return JsonRpcResponse::error(request.id, JsonRpcError::InvalidParams);
                };

                ingress
                    .user_metrics
                    .request_body_size(ETH_SEND_BUNDLE_METHOD)
                    .observe(body.len() as f64);

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
                    ingress
                        .user_metrics
                        .json_rpc_parse_errors(ETH_SEND_RAW_TRANSACTION_METHOD)
                        .inc();
                    return JsonRpcResponse::error(request.id, JsonRpcError::InvalidParams);
                };

                ingress
                    .user_metrics
                    .request_body_size(ETH_SEND_RAW_TRANSACTION_METHOD)
                    .observe(body.len() as f64);

                ingress.on_raw_transaction(entity, tx, received_at).await.map(EthResponse::TxHash)
            }
            MEV_SEND_BUNDLE_METHOD => {
                let Some(Ok(bundle)) = request.take_single_param().map(|p| {
                    serde_json::from_value::<RawShareBundle>(p).inspect_err(|e| {
                        tracing::trace!(?e, body_utf8, "failed to parse mev share bundle")
                    })
                }) else {
                    ingress.user_metrics.json_rpc_parse_errors(MEV_SEND_BUNDLE_METHOD).inc();
                    return JsonRpcResponse::error(request.id, JsonRpcError::InvalidParams);
                };

                ingress
                    .user_metrics
                    .request_body_size(MEV_SEND_BUNDLE_METHOD)
                    .observe(body.len() as f64);

                ingress
                    .on_mev_share_bundle(entity, bundle, received_at)
                    .await
                    .map(EthResponse::BundleHash)
            }
            other => {
                tracing::trace!("method not supported");
                ingress.user_metrics.json_rpc_unknown_method(other.to_owned()).inc();
                return JsonRpcResponse::error(
                    request.id,
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
                JsonRpcResponse::error(request.id, error.into_jsonrpc_error())
            }
        };

        trace!(elapsed = ?received_at.instant.elapsed(), "processed json-rpc request");
        response
    }

    /// Handler for the `/readyz` endpoint. Used to check if the local builder is ready. Always
    /// returns 200 if the local builder is not configured.
    #[tracing::instrument(skip_all, name = "ingress_readyz")]
    pub async fn ready_handler(State(ingress): State<Arc<Self>>) -> Response {
        if let Some(ref url) = ingress.builder_ready_endpoint {
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
                .build()
                .unwrap();
            let url = url.join("readyz").unwrap();

            let Ok(response) = client.get(url.clone()).send().await else {
                tracing::error!(%url, "error sending readyz request");
                return Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(Body::from("not ready"))
                    .unwrap();
            };

            if response.status().is_success() {
                tracing::info!(%url, "local builder is ready");
                return Response::builder().status(StatusCode::OK).body(Body::from("OK")).unwrap();
            } else {
                tracing::error!(%url, status = %response.status(), "local builder is not ready");
                return Response::builder()
                    .status(StatusCode::SERVICE_UNAVAILABLE)
                    .body(Body::from("not ready"))
                    .unwrap();
            }
        }

        Response::builder().status(StatusCode::OK).body(Body::from("OK")).unwrap()
    }

    /// Handler for the `/infoz` endpoint. Used to get information about a peer's running proxy.
    #[tracing::instrument(skip_all, name = "ingress_infoz")]
    pub async fn info_handler(State(ingress): State<Arc<Self>>) -> Response {
        let proxy_info = PeerProxyInfo { system_api_port: ingress.system_api_port };
        let body = match serde_json::to_string(&proxy_info) {
            Ok(b) => b,
            Err(e) => {
                tracing::error!(?e, "failed to serialize peer info");
                return Response::builder()
                    .status(StatusCode::INTERNAL_SERVER_ERROR)
                    .body(Body::from("internal server error"))
                    .expect("to create error message");
            }
        };

        Response::builder()
            .status(StatusCode::OK)
            .body(Body::from(body))
            .expect("to create ok server response")
    }

    #[tracing::instrument(skip_all, name = "ingress",
        fields(
            handler = "system",
            id = %short_uuid_v4(),
            method = tracing::field::Empty,
            peer = tracing::field::Empty
        ))]
    pub async fn system_handler(
        State(ingress): State<Arc<Self>>,
        headers: HeaderMap,
        body: axum::body::Bytes,
    ) -> JsonRpcResponse<EthResponse> {
        let received_at = UtcInstant::now();
        let payload_size = body.len();

        // Before doing anything else, verify that the signature header is present.
        if headers
            .get(BUILDERNET_SIGNATURE_HEADER)
            .or_else(|| headers.get(FLASHBOTS_SIGNATURE_HEADER))
            .is_none()
        {
            tracing::error!(
                buildernet_signature_header = ?headers.get(BUILDERNET_SIGNATURE_HEADER),
                flashbots_signature_header = ?headers.get(FLASHBOTS_SIGNATURE_HEADER),
                "error verifying peer signature"
            );
            return JsonRpcResponse::error(Value::Null, JsonRpcError::Internal);
        }

        let body = match maybe_decompress(ingress.gzip_enabled, &headers, body) {
            Ok(decompressed) => decompressed,
            Err(error) => return JsonRpcResponse::error(Value::Null, error),
        };

        let mut priority = Priority::Low;
        if let Some(priority_) = maybe_buildernet_priority(&headers) {
            priority = priority_;
        } else {
            tracing::trace!("failed to retrieve priority from request, defaulting to {priority}");
        }

        let peer = 'peer: {
            let body_clone = body.clone();
            let headers_clone = headers.clone();
            if let Some(address) = ingress
                .pqueues
                .spawn_with_priority(priority, move || {
                    maybe_verify_signature(&headers_clone, &body_clone, USE_LEGACY_SIGNATURE)
                })
                .await
            {
                if ingress.flashbots_signer.is_some_and(|addr| addr == address) {
                    break 'peer "flashbots".to_string();
                }

                if let Some(peer) = ingress.forwarders.find_peer(address) {
                    break 'peer peer;
                }
            }

            tracing::error!(
                buildernet_signature_header = ?headers.get(BUILDERNET_SIGNATURE_HEADER),
                flashbots_signature_header = ?headers.get(FLASHBOTS_SIGNATURE_HEADER),
                "error verifying peer signature"
            );
            return JsonRpcResponse::error(Value::Null, JsonRpcError::Internal);
        };
        tracing::Span::current().record("peer", tracing::field::display(&peer));

        // This gets computed only if we enter in an error branch.
        let body_utf8 = || str::from_utf8(&body).unwrap_or("<invalid utf8>");

        let mut request: JsonRpcRequest<serde_json::Value> = match JsonRpcRequest::from_bytes(&body)
        {
            Ok(request) => request,
            Err(e) => {
                tracing::error!(?e, body_utf8 = body_utf8(), "failed to parse json-rpc request");
                ingress.system_metrics.json_rpc_parse_errors(UNKNOWN).inc();
                return JsonRpcResponse::error(Value::Null, e);
            }
        };
        tracing::Span::current().record("method", tracing::field::display(&request.method));

        // Record the one-way latency of the RPC call.
        let sent_at = headers.get(BUILDERNET_SENT_AT_HEADER).and_then(UtcDateTime::parse_header);
        if let Some(sent_at) = sent_at {
            ingress
                .system_metrics
                .rpc_latency_oneway(&peer, &request.method)
                .observe((received_at.utc - sent_at).as_seconds_f64());
        }

        tracing::trace!(params = ?request.params, "serving json-rpc request");

        let (raw, response) = match request.method.as_str() {
            ETH_SEND_BUNDLE_METHOD => {
                let Some(raw) = request.take_single_param() else {
                    tracing::error!("failed to parse bundle: take single param failed");
                    ingress.system_metrics.json_rpc_parse_errors(ETH_SEND_BUNDLE_METHOD).inc();
                    return JsonRpcResponse::error(request.id, JsonRpcError::InvalidParams);
                };

                let bundle = match serde_json::from_value::<RawBundle>(raw.clone()) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!(
                            ?e,
                            body_utf8 = body_utf8(),
                            "failed to parse raw bundle from system request"
                        );
                        ingress.system_metrics.json_rpc_parse_errors(ETH_SEND_BUNDLE_METHOD).inc();
                        return JsonRpcResponse::error(request.id, JsonRpcError::InvalidParams);
                    }
                };

                let bundle_hash = match bundle.metadata.bundle_hash {
                    Some(bundle_hash) => bundle_hash,
                    None => {
                        tracing::debug!("bundle hash is not set");
                        bundle.bundle_hash()
                    }
                };

                // Deduplicate bundles.
                if ingress.order_cache.contains(&bundle_hash) {
                    tracing::trace!(bundle_hash = %bundle_hash, "bundle already processed");
                    ingress.system_metrics.order_cache_hit("bundle").inc();

                    // Sample the order cache hit ratio.
                    if bundle_hash.sample(10) {
                        ingress
                            .system_metrics
                            .order_cache_hit_ratio()
                            .set(ingress.order_cache.hit_ratio() * 100.0);
                        ingress
                            .system_metrics
                            .order_cache_entry_count()
                            .set(ingress.order_cache.entry_count());
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

                ingress
                    .system_metrics
                    .rpc_request_duration(ETH_SEND_BUNDLE_METHOD, priority.as_str())
                    .observe(received_at.elapsed().as_secs_f64());

                ingress.system_metrics.txs_per_bundle().observe(bundle.txs.len() as f64);
                ingress
                    .system_metrics
                    .request_body_size(ETH_SEND_BUNDLE_METHOD)
                    .observe(body.len() as f64);

                (raw, EthResponse::BundleHash(bundle_hash))
            }
            ETH_SEND_RAW_TRANSACTION_METHOD => {
                let Some(raw) = request.take_single_param() else {
                    ingress
                        .system_metrics
                        .json_rpc_parse_errors(ETH_SEND_RAW_TRANSACTION_METHOD)
                        .inc();
                    return JsonRpcResponse::error(request.id, JsonRpcError::InvalidParams);
                };

                let Ok(tx) =
                    decode_transaction(&serde_json::from_value::<Bytes>(raw.clone()).unwrap())
                else {
                    ingress
                        .system_metrics
                        .json_rpc_parse_errors(ETH_SEND_RAW_TRANSACTION_METHOD)
                        .inc();
                    return JsonRpcResponse::error(request.id, JsonRpcError::InvalidParams);
                };

                let tx_hash = *tx.tx_hash();
                if ingress.order_cache.contains(&tx_hash) {
                    tracing::trace!(hash = %tx_hash, "transaction already processed");
                    ingress.system_metrics.order_cache_hit("transaction").inc();

                    // Sample the order cache hit ratio.
                    if tx_hash.sample(10) {
                        ingress
                            .system_metrics
                            .order_cache_hit_ratio()
                            .set(ingress.order_cache.hit_ratio() * 100.0);
                    }

                    return JsonRpcResponse::result(request.id, EthResponse::TxHash(tx_hash));
                }

                ingress.order_cache.insert(tx_hash);

                ingress
                    .system_metrics
                    .rpc_request_duration(ETH_SEND_RAW_TRANSACTION_METHOD, priority.as_str())
                    .observe(received_at.elapsed().as_secs_f64());
                ingress
                    .system_metrics
                    .request_body_size(ETH_SEND_RAW_TRANSACTION_METHOD)
                    .observe(body.len() as f64);

                (raw, EthResponse::TxHash(tx_hash))
            }
            MEV_SEND_BUNDLE_METHOD => {
                let Some(raw) = request.take_single_param() else {
                    tracing::error!("error parsing bundle: take single param failed");
                    ingress.system_metrics.json_rpc_parse_errors(MEV_SEND_BUNDLE_METHOD).inc();
                    return JsonRpcResponse::error(request.id, JsonRpcError::InvalidParams);
                };

                let bundle = match serde_json::from_value::<RawShareBundle>(raw.clone()) {
                    Ok(b) => b,
                    Err(e) => {
                        tracing::error!(?e, body = body_utf8(), "error parsing bundle");
                        ingress.system_metrics.json_rpc_parse_errors(MEV_SEND_BUNDLE_METHOD).inc();
                        return JsonRpcResponse::error(request.id, JsonRpcError::InvalidParams);
                    }
                };

                let bundle_hash = bundle.bundle_hash();
                if ingress.order_cache.contains(&bundle_hash) {
                    tracing::trace!(hash = %bundle_hash, "bundle already processed");
                    ingress.system_metrics.order_cache_hit("mev_share_bundle").inc();

                    // Sample the order cache hit ratio.
                    if bundle_hash.sample(10) {
                        ingress
                            .system_metrics
                            .order_cache_hit_ratio()
                            .set(ingress.order_cache.hit_ratio() * 100.0);
                    }

                    return JsonRpcResponse::result(
                        request.id,
                        EthResponse::BundleHash(bundle_hash),
                    );
                }

                ingress.order_cache.insert(bundle_hash);

                ingress.system_metrics.txs_per_mev_share_bundle().observe(bundle.body.len() as f64);
                ingress
                    .system_metrics
                    .rpc_request_duration(MEV_SEND_BUNDLE_METHOD, priority.as_str())
                    .observe(received_at.elapsed().as_secs_f64());
                ingress
                    .system_metrics
                    .request_body_size(MEV_SEND_BUNDLE_METHOD)
                    .observe(body.len() as f64);

                (raw, EthResponse::BundleHash(bundle_hash))
            }
            other => {
                tracing::error!("method not supported");
                ingress.system_metrics.json_rpc_unknown_method(other.to_owned()).inc();
                return JsonRpcResponse::error(
                    request.id,
                    JsonRpcError::MethodNotFound(other.to_owned()),
                );
            }
        };

        // Send request only to the local builder forwarder.
        ingress.forwarders.send_to_local(
            priority,
            &request.method,
            raw,
            response.hash(),
            received_at,
        );

        trace!(elapsed = ?received_at.instant.elapsed(), "processed json-rpc request");
        JsonRpcResponse::result(request.id, response)
    }

    /// Handles a new bundle.
    #[tracing::instrument(skip_all, name = "bundle",
        fields(
            hash = tracing::field::Empty,
            signer = tracing::field::Empty,
            priority = tracing::field::Empty,
        ))]
    async fn on_bundle(
        &self,
        entity: Entity,
        mut bundle: RawBundle,
        received_at: UtcInstant,
    ) -> Result<B256, IngressError> {
        let start = Instant::now();

        let Entity::Signer(signer) = entity else { unreachable!() };
        bundle.metadata.signing_address = Some(signer);
        let priority = self.priority_for(entity, EntityRequest::Bundle(&bundle));

        // NOTE: Before computing the bundle hash used for indexing and deduplication purposes, we
        // add two fields if they are not set: the `replacement_nonce` (if applicable) and the
        // `version`. The replacement nonce is needed to deduplicate replacement bundles
        // correctly, while the version is tech debt.

        // Set replacement nonce if it is not set and we have a replacement UUID or UUID. This is
        // needed to decode the replacement data correctly in
        // [`SystemBundle::try_from_bundle_and_signer`].
        let replacement_uuid = bundle.metadata.uuid.or(bundle.metadata.replacement_uuid);
        if replacement_uuid.is_some() && bundle.metadata.replacement_nonce.is_none() {
            let timestamp = received_at.utc.unix_timestamp_nanos() / 1000;
            bundle.metadata.replacement_nonce =
                Some(timestamp.try_into().expect("Timestamp too large"));
        }

        if bundle.metadata.version.is_none() {
            bundle.metadata.version = Some(DEFAULT_BUNDLE_VERSION.to_string());
        }

        // From now on, use THIS bundle hash for deduplication and indexing.
        let bundle_hash = bundle.bundle_hash();
        bundle.metadata.bundle_hash = Some(bundle_hash);

        tracing::Span::current().record("hash", tracing::field::display(bundle_hash));
        tracing::Span::current().record("signer", tracing::field::display(signer));
        tracing::Span::current().record("priority", tracing::field::display(priority.as_str()));

        let sample = bundle_hash.sample(10);
        if self.order_cache.contains(&bundle_hash) {
            tracing::trace!("already processed");
            self.user_metrics.order_cache_hit("bundle").inc();

            if sample {
                self.user_metrics.order_cache_hit_ratio().set(self.order_cache.hit_ratio() * 100.0);
                self.user_metrics.order_cache_entry_count().set(self.order_cache.entry_count());
            }

            return Ok(bundle_hash);
        }

        self.order_cache.insert(bundle_hash);
        let signer_cache = self.signer_cache.clone();
        let lookup = move |hash: B256| signer_cache.get(&hash);

        // Record queue capacity metrics.
        self.record_queue_capacity_metrics(priority);

        // Decode and validate the bundle.
        let decoder = self.system_bundle_decoder;

        // NOTE: If there are no transactions in the bundle, we can decode it directly without
        // spawning a task. The most computationally expensive part of decoding is related
        // to transactions.
        let metadata = SystemBundleMetadata { signer, received_at, priority };
        let bundle = if bundle.txs.is_empty() {
            // Decode normally, without spawning a task or looking up signers.
            decoder.try_decode(bundle, metadata).inspect_err(|e| {
                tracing::error!(?e, "failed to decode bundle");
                self.user_metrics.validation_errors(e.to_string()).inc();
            })?
        } else {
            self.pqueues
                .spawn_with_priority(priority, move || {
                    decoder.try_decode_with_lookup(bundle, metadata, lookup)
                })
                .await
                .inspect_err(|e| {
                    tracing::error!(?e, "failed to decode bundle");
                    self.user_metrics.validation_errors(e.to_string()).inc();
                })?
        };

        let elapsed = start.elapsed();

        match bundle.decoded_bundle.as_ref() {
            DecodedBundle::Bundle(bundle) => {
                tracing::debug!(?elapsed, "decoded new bundle");
                for tx in &bundle.txs {
                    self.signer_cache.insert(tx.hash(), tx.signer());
                }
            }
            DecodedBundle::EmptyReplacement(replacement_data) => {
                tracing::debug!(?elapsed, ?replacement_data, "decoded replacement bundle");
            }
        }

        // Sample the signer cache hit ratio.
        if sample {
            self.user_metrics.signer_cache_hit_ratio().set(self.signer_cache.hit_ratio() * 100.0);
            self.user_metrics.signer_cache_entry_count().set(self.signer_cache.entry_count());
        }

        if bundle.is_empty() {
            self.user_metrics.total_empty_bundles().inc();
        } else {
            self.user_metrics.txs_per_bundle().observe(bundle.raw_bundle.txs.len() as f64);
        }

        self.indexer_handle.index_bundle(bundle.clone());

        self.send_bundle(bundle).await
    }

    /// Handles a new mev share bundle.
    #[tracing::instrument(skip_all, name = "mev_share_bundle",
        fields(
            hash = tracing::field::Empty,
            signer = tracing::field::Empty,
            priority = tracing::field::Empty,
        ))]
    async fn on_mev_share_bundle(
        &self,
        entity: Entity,
        bundle: RawShareBundle,
        received_at: UtcInstant,
    ) -> Result<B256, IngressError> {
        let start = Instant::now();

        // Convert to system bundle.
        let Entity::Signer(signer) = entity else { unreachable!() };
        let priority = self.priority_for(entity, EntityRequest::MevShareBundle(&bundle));
        // Deduplicate bundles.
        let bundle_hash = bundle.bundle_hash();

        tracing::Span::current().record("hash", tracing::field::display(bundle_hash));
        tracing::Span::current().record("signer", tracing::field::display(signer));
        tracing::Span::current().record("priority", tracing::field::display(priority.as_str()));

        if self.order_cache.contains(&bundle_hash) {
            tracing::trace!("already processed");
            self.user_metrics.order_cache_hit("mev_share_bundle").inc();

            if bundle_hash.sample(10) {
                self.user_metrics.order_cache_hit_ratio().set(self.order_cache.hit_ratio() * 100.0);
            }

            return Ok(bundle_hash);
        }

        self.order_cache.insert(bundle_hash);

        // Decode and validate the bundle.
        let bundle = SystemMevShareBundle::try_from_bundle_and_signer(
            bundle,
            signer,
            received_at,
            priority,
        )?;
        let elapsed = start.elapsed();

        match bundle.decoded.as_ref() {
            DecodedShareBundle::New(_) => {
                tracing::debug!(?elapsed, "decoded new bundle");
            }
            DecodedShareBundle::Cancel(cancellation) => {
                tracing::debug!(?elapsed, ?cancellation, "decoded cancellation bundle");
            }
        }

        self.user_metrics.txs_per_mev_share_bundle().observe(bundle.raw.body.len() as f64);

        self.send_mev_share_bundle(priority, bundle).await
    }

    #[tracing::instrument(skip_all, name = "transaction",
        fields(
            hash = tracing::field::Empty,
            signer = tracing::field::Empty,
            priority = tracing::field::Empty,
        ))]
    async fn on_raw_transaction(
        &self,
        entity: Entity,
        transaction: EthereumTransaction,
        received_at: UtcInstant,
    ) -> Result<B256, IngressError> {
        let start = Instant::now();

        let Entity::Signer(signer) = entity else { unreachable!() };
        let tx_hash = *transaction.hash();
        let priority = self.priority_for(entity, EntityRequest::PrivateTx(&transaction));

        tracing::Span::current().record("hash", tracing::field::display(tx_hash));
        tracing::Span::current().record("signer", tracing::field::display(signer));
        tracing::Span::current().record("priority", tracing::field::display(priority.as_str()));

        // Deduplicate transactions.
        if self.order_cache.contains(&tx_hash) {
            tracing::trace!("already processed");
            self.user_metrics.order_cache_hit("transaction").inc();

            if tx_hash.sample(10) {
                self.user_metrics.order_cache_hit_ratio().set(self.order_cache.hit_ratio() * 100.0);
            }

            return Ok(tx_hash);
        }

        self.order_cache.insert(tx_hash);

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
                tracing::error!(?e, "failed to validate transaction");
                self.user_metrics.validation_errors(e.to_string()).inc();
            })?;

        // Send request to all forwarders.
        self.forwarders.broadcast_transaction(system_transaction).await;

        let elapsed = start.elapsed();
        tracing::debug!(elapsed = ?elapsed, "processed raw transaction");

        self.user_metrics
            .rpc_request_duration(ETH_SEND_RAW_TRANSACTION_METHOD, priority.as_str())
            .observe(received_at.elapsed().as_secs_f64());

        Ok(tx_hash)
    }

    async fn send_bundle(&self, bundle: SystemBundle) -> Result<B256, IngressError> {
        let bundle_hash = bundle.bundle_hash();
        let priority = bundle.metadata.priority;
        let received_at = bundle.metadata.received_at;

        // Send request to all forwarders.
        self.forwarders.broadcast_bundle(bundle).await;

        self.user_metrics
            .rpc_request_duration(ETH_SEND_BUNDLE_METHOD, priority.as_str())
            .observe(received_at.elapsed().as_secs_f64());
        Ok(bundle_hash)
    }

    async fn send_mev_share_bundle(
        &self,
        priority: Priority,
        bundle: SystemMevShareBundle,
    ) -> Result<B256, IngressError> {
        let bundle_hash = bundle.bundle_hash();
        let received_at = bundle.received_at;

        self.forwarders.broadcast_mev_share_bundle(priority, bundle).await;

        self.user_metrics
            .rpc_request_duration(MEV_SEND_BUNDLE_METHOD, priority.as_str())
            .observe(received_at.elapsed().as_secs_f64());
        Ok(bundle_hash)
    }

    #[tracing::instrument(skip_all, name = "builder_handler", fields(count = data.len()))]
    pub async fn builder_handler(
        State(ingress): State<Arc<Self>>,
        Json(data): Json<HashMap<Entity, EntityBuilderStats>>,
    ) {
        let received_at = Instant::now();
        for (entity, stats) in data {
            if let Some(mut data) = ingress.entity_data(entity) {
                data.scores.score_mut(received_at).builder_stats.extend(stats);
            }
        }
        tracing::info!("updated entity stats with builder data");
    }
}

/// Attempt to decompress the header if `content-encoding` header is set to `gzip`.
pub fn maybe_decompress(
    gzip_enabled: bool,
    headers: &HeaderMap,
    body: axum::body::Bytes,
) -> Result<Bytes, JsonRpcError> {
    if gzip_enabled && headers.get(header::CONTENT_ENCODING).is_some_and(|enc| enc == "gzip") {
        let mut decompressed = Vec::new();
        GzDecoder::new(&body[..])
            .read_to_end(&mut decompressed)
            .map_err(|_| JsonRpcError::ParseError)?;
        Ok(decompressed.into())
    } else {
        Ok(body.into())
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
