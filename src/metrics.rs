use std::time::{Duration, Instant};

use metrics::{counter, gauge, histogram};

use crate::consts::{ETH_SEND_BUNDLE_METHOD, ETH_SEND_RAW_TRANSACTION_METHOD};

pub trait IngressHandlerMetricsExt {
    const HANDLER: &str;

    fn increment_requests_received() {
        counter!("ingress_requests_received", "handler" => Self::HANDLER).increment(1);
    }

    fn increment_requests_rate_limited() {
        counter!("ingress_requests_rate_limited", "handler" => Self::HANDLER).increment(1);
    }
    fn increment_json_rpc_parse_errors() {
        counter!("ingress_json_rpc_parse_errors", "handler" => Self::HANDLER).increment(1);
    }

    fn increment_json_rpc_unknown_method() {
        counter!("ingress_json_rpc_unknown_method", "handler" => Self::HANDLER).increment(1);
    }
    fn increment_bundles_received() {
        counter!("ingress_bundles_received", "handler" => Self::HANDLER).increment(1);
    }
    fn increment_raw_transactions_received() {
        counter!("ingress_raw_transactions_received", "handler" => Self::HANDLER).increment(1);
    }
    fn record_processed_in(secs: f64) {
        histogram!("ingress_processed_in_seconds", "handler" => Self::HANDLER).record(secs);
    }
    fn record_bundle_processed_in(secs: f64) {
        histogram!("ingress_bundle_processed_in_seconds", "handler" => Self::HANDLER).record(secs);
    }
    fn record_raw_transaction_processed_in(secs: f64) {
        histogram!("ingress_raw_transaction_processed_in_seconds", "handler" => Self::HANDLER)
            .record(secs);
    }

    fn observe_raw_request(method: String, path: String, status: String, duration: Duration) {
        let labels = [
            ("handler", Self::HANDLER.to_string()),
            ("method", method),
            ("path", path),
            ("status", status),
        ];
        counter!("ingress_requests_total", &labels).increment(1);
        histogram!("ingress_request_duration_seconds", &labels).record(duration.as_millis() as f64);
    }

    fn record_method_metrics(method: &str, received_at: Instant) {
        let elapsed_secs = received_at.elapsed().as_secs_f64();
        Self::record_processed_in(elapsed_secs);
        match method {
            ETH_SEND_BUNDLE_METHOD => {
                Self::record_bundle_processed_in(elapsed_secs);
                Self::increment_bundles_received();
            }
            ETH_SEND_RAW_TRANSACTION_METHOD => {
                Self::record_raw_transaction_processed_in(elapsed_secs);
                Self::increment_raw_transactions_received();
            }
            _ => {
                Self::increment_json_rpc_unknown_method();
            }
        };
    }
}

#[derive(Clone, Debug)]
pub struct IngressUserMetrics;

impl IngressHandlerMetricsExt for IngressUserMetrics {
    const HANDLER: &'static str = "user";
}

#[derive(Clone, Debug)]
pub struct IngressSystemMetrics;

impl IngressHandlerMetricsExt for IngressSystemMetrics {
    const HANDLER: &'static str = "system";
}

#[derive(Debug, Clone)]
pub struct IngressMetrics;

#[allow(missing_debug_implementations)]
impl IngressMetrics {
    pub fn entity_count(count: usize) {
        gauge!("ingress_entity_count").set(count as f64);
    }
}

#[derive(Debug, Clone)]
pub struct BuilderHubMetrics;

#[allow(missing_debug_implementations)]
impl BuilderHubMetrics {
    pub fn increment_builderhub_peer_request_failures(err: String) {
        counter!("builderhub_peer_request_failures", "error" => err).increment(1);
    }

    pub fn builderhub_peer_count(count: usize) {
        gauge!("builderhub_peer_count").set(count as f64);
    }
}

#[derive(Debug, Clone)]
pub struct IndexerMetrics;

#[allow(missing_debug_implementations)]
impl IndexerMetrics {
    pub fn increment_bundle_indexing_failures(err: String) {
        counter!("indexer_bundle_indexing_failures", "error" => err).increment(1);
    }

    pub fn increment_bundle_receipt_indexing_failures(err: String) {
        counter!("indexer_bundle_receipt_indexing_failures", "error" => err).increment(1);
    }

    pub fn increment_clickhouse_write_failures(err: String) {
        counter!("indexer_clickhouse_writing_failures", "error" => err).increment(1);
    }

    pub fn increment_clickhouse_commit_failures(err: String) {
        counter!("indexer_clickhouse_writing_failures", "error" => err).increment(1);
    }
}
