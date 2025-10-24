use crate::{
    consts::{
        DEFAULT_CONNECTION_LIMIT_PER_HOST, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_HTTP_TIMEOUT_SECS,
        DEFAULT_POOL_IDLE_TIMEOUT_SECS,
    },
    forwarder::{spawn_forwarder, PeerHandle},
    metrics::BuilderHubMetrics,
    tasks::TaskExecutor,
    utils, DEFAULT_SYSTEM_PORT,
};
use alloy_primitives::Address;
use dashmap::DashMap;
use std::{
    convert::Infallible,
    fmt::{Debug, Display},
    future::Future,
    sync::Arc,
    time::Duration,
};

use reqwest::Certificate;
use serde::{Deserialize, Serialize};
use tracing::error;

/// A trait for a peer store.
pub trait PeerStore {
    type Error: Debug + Display;

    fn get_peers(&self) -> impl Future<Output = Result<Vec<Peer>, Self::Error>>;
}

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct InstanceData {
    /// TLS certificate of the instance in UTF-8 encoded PEM format.
    pub tls_cert: String,
}

/// The credentials of the overflow proxy for a BuilderHub peer.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct PeerCredentials {
    /// TLS certificate of the orderflow proxy in UTF-8 encoded PEM format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_cert: Option<String>,
    /// Orderflow signer public key.
    pub ecdsa_pubkey_address: Address,
}

/// Informations about a builder peer, as returned by the `api/l1-builder/v1/builders` endpoint of
/// BuilderHub.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct Peer {
    /// Builder name.
    pub name: String,
    /// Builder IP.
    pub ip: String,
    /// Builder DNS name.
    pub dns_name: String,
    /// Builder orderflow proxy configuration.
    pub orderflow_proxy: PeerCredentials,
    /// Instance data
    pub instance: InstanceData,
}

impl Peer {
    /// Get the system API URL for the builder.
    /// Mirrors Go proxy behavior: <https://github.com/flashbots/buildernet-orderflow-proxy/blob/main/proxy/confighub.go>
    pub fn system_api(&self) -> String {
        let host = if self.dns_name.is_empty() {
            if self.ip.contains(":") {
                self.ip.clone()
            } else {
                format!("{}:{}", self.ip, DEFAULT_SYSTEM_PORT)
            }
        } else {
            format!("{}:{}", self.dns_name, DEFAULT_SYSTEM_PORT)
        };

        if self.tls_certificate().is_some() {
            format!("https://{host}")
        } else {
            format!("http://{host}")
        }
    }

    /// Get the TLS certificate from the orderflow proxy credentials.
    /// If the certificate is empty (an empty string), return `None`.
    pub fn tls_certificate(&self) -> Option<Certificate> {
        if self.instance.tls_cert.is_empty() {
            None
        } else {
            // SAFETY: We expect the certificate to be valid. It's added as a root
            // certificate.
            Some(
                Certificate::from_pem(self.instance.tls_cert.as_bytes())
                    .expect("Valid certificate"),
            )
        }
    }
}

/// Errors that can occur when registering via the BuilderHub client.
#[derive(Debug, thiserror::Error)]
pub enum ClientRegisterError {
    #[error("reqwest error: {0}")]
    Reqwest(#[from] reqwest::Error),
    #[error("registration unsuccessful: {0}")]
    Unsuccessful(String),
}

/// A BuilderHub client.
#[derive(Debug)]
pub struct Client {
    inner: reqwest::Client,
    url: String,
}

impl Client {
    /// Create a new BuilderHub client with a default HTTP timeout of 2 seconds.
    pub fn new(url: String) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
            .build()
            .expect("to build reqwest client");
        Self { inner: client, url }
    }

    /// Register the given signer address with the BuilderHub peer store.
    pub async fn register(&self, signer_address: Address) -> Result<(), ClientRegisterError> {
        let endpoint =
            format!("{}/api/l1-builder/v1/register_credentials/orderflow_proxy", self.url);
        let body = PeerCredentials { tls_cert: None, ecdsa_pubkey_address: signer_address };
        let response = self.inner.post(endpoint).json(&body).send().await?;
        let status = response.status();
        if !status.is_success() {
            let e = response.text().await.unwrap_or_default();
            return Err(ClientRegisterError::Unsuccessful(e));
        }

        Ok(())
    }
}

impl PeerStore for Client {
    type Error = reqwest::Error;

    async fn get_peers(&self) -> Result<Vec<Peer>, Self::Error> {
        let endpoint = format!("{}/api/l1-builder/v1/builders", self.url);
        let response = self.inner.get(endpoint).send().await?;
        response.json().await
    }
}

#[derive(Debug, Clone)]
pub struct LocalPeerStore {
    pub(crate) builders: Arc<DashMap<String, Peer>>,
}

impl LocalPeerStore {
    pub(crate) fn new() -> Self {
        Self { builders: Arc::new(DashMap::new()) }
    }

    pub fn register(&self, signer_address: Address, port: Option<u16>) -> LocalPeerStore {
        self.builders.insert(
            signer_address.to_string(),
            Peer {
                name: signer_address.to_string(),
                ip: format!("127.0.0.1:{}", port.unwrap()),
                // Don't set the DNS name for local peer store or it will try to connect to
                // {dns_name}:5544
                dns_name: "".to_string(),
                orderflow_proxy: PeerCredentials {
                    tls_cert: None,
                    ecdsa_pubkey_address: signer_address,
                },
                instance: InstanceData { tls_cert: "".to_string() },
            },
        );

        LocalPeerStore { builders: self.builders.clone() }
    }
}

impl PeerStore for LocalPeerStore {
    type Error = Infallible;

    async fn get_peers(&self) -> Result<Vec<Peer>, Infallible> {
        Ok(self.builders.iter().map(|b| b.value().clone()).collect())
    }
}

/// Periodically updates the list of peers from the BuilderHub peer store.
pub async fn run_update_peers(
    local_signer: Address,
    peer_store: impl PeerStore,
    peers: Arc<DashMap<String, PeerHandle>>,
    disable_forwarding: bool,
    task_executor: TaskExecutor,
) {
    let delay = Duration::from_secs(30);

    loop {
        let span = tracing::info_span!("update_peers");
        let _guard = span.enter();

        let builders = match peer_store.get_peers().await {
            Ok(builders) => builders,
            Err(e) => {
                BuilderHubMetrics::increment_builderhub_peer_request_failures(e.to_string());
                tracing::error!(?e, "failed to get peers from builderhub");
                tokio::time::sleep(delay).await;
                continue;
            }
        };

        // Remove all peers that are no longer present in BuilderHub
        peers.retain(|name, handle| {
            let is_present = builders.iter().any(|b| &b.name == name);
            if !is_present {
                tracing::info!(peer = %name, info = ?handle.info, "removed from configuration");
            }
            is_present
        });

        // Update or insert builder information.
        for builder in builders {
            let peer_span = tracing::info_span!("process_peer", info = ?builder);
            let _peer_guard = peer_span.enter();

            let entry = peers.entry(builder.name.clone());
            let new_peer = match &entry {
                dashmap::Entry::Occupied(entry) => {
                    tracing::info!("received configuration update");
                    entry.get().info != builder
                }
                dashmap::Entry::Vacant(_) => {
                    tracing::info!("received new peer configuration");
                    true
                }
            };

            // Self-filter any new peers before connecting to them.
            if new_peer && builder.orderflow_proxy.ecdsa_pubkey_address != local_signer {
                // Create a new client for each peer.
                let mut client = reqwest::Client::builder()
                    .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
                    .connect_timeout(Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS))
                    .pool_idle_timeout(Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT_SECS))
                    .pool_max_idle_per_host(DEFAULT_CONNECTION_LIMIT_PER_HOST)
                    .connector_layer(utils::limit::ConnectionLimiterLayer::new(
                        DEFAULT_CONNECTION_LIMIT_PER_HOST,
                        builder.name.clone(),
                    ));

                // If the TLS certificate is present, use HTTPS and configure the client to use it.
                if let Some(ref tls_cert) = builder.tls_certificate() {
                    client = client.https_only(true).add_root_certificate(tls_cert.clone())
                }

                if disable_forwarding {
                    tracing::warn!("skipped spawning forwarder (disabled forwarding)");
                    continue;
                }

                let client = client.build().expect("Failed to build client");

                let sender = spawn_forwarder(
                    builder.name.clone(),
                    builder.system_api(),
                    client.clone(),
                    task_executor.clone(),
                )
                .expect("malformed url");

                tracing::debug!("inserting configuration");
                entry.insert(PeerHandle { info: builder, sender });
            }
        }

        BuilderHubMetrics::builderhub_peer_count(peers.len());

        tokio::time::sleep(delay).await;
    }
}
