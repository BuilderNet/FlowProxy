use crate::{
    consts::{
        DEFAULT_CONNECTION_LIMIT_PER_HOST, DEFAULT_CONNECT_TIMEOUT_MS, DEFAULT_HTTP_TIMEOUT_SECS,
        DEFAULT_POOL_IDLE_TIMEOUT_SECS,
    },
    ingress::forwarder::{spawn_forwarder, PeerHandle},
    metrics::BuilderHubMetrics,
    utils, DEFAULT_SYSTEM_PORT,
};
use alloy_primitives::Address;
use dashmap::DashMap;
use std::{convert::Infallible, fmt::Debug, future::Future, sync::Arc, time::Duration};

use rbuilder_utils::tasks::TaskExecutor;
use reqwest::Certificate;
use serde::{Deserialize, Serialize};

mod client;
pub use client::Client;

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct InstanceData {
    /// TLS certificate of the instance in UTF-8 encoded PEM format.
    pub tls_cert: String,
}

/// The credentials of the running overflow proxy of a BuilderHub peer.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct PeerCredentials {
    /// TLS certificate of the orderflow proxy in UTF-8 encoded PEM format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_cert: Option<String>,
    /// Orderflow signer public key.
    pub ecdsa_pubkey_address: Address,
}

/// A [`Peer`] is a builder inside Builderhub. This holds informations about a builder peer, as
/// returned by the `api/l1-builder/v1/builders` endpoint of BuilderHub.
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

/// Represents the interactions we can have with BuilderHub, abstracted as a general peer store to
/// allow testing implementations.
pub trait PeerStore {
    /// The error type returned when trying to get peers from the peer store.
    type Error: std::error::Error;

    /// Get the list of peers from the peer store.
    fn get_peers(&self) -> impl Future<Output = Result<Vec<Peer>, Self::Error>>;
}

impl PeerStore for client::Client {
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

/// A [`PeerUpdater`] periodically fetches the list of peers from a BuilderHub peer store,
/// updating the local list of connected peers and spawning order forwarders.
#[derive(Debug, Clone)]
pub struct PeersUpdater<P: PeerStore> {
    /// The local signer address to filter out self-connections.
    local_signer: Address,
    /// The BuilderHub peer store.
    peer_store: P,
    /// The local list of connected peers.
    peers: Arc<DashMap<String, PeerHandle>>,
    /// The task executor to spawn forwarders jobs.
    task_executor: TaskExecutor,
    /// Whether to disable spawning order forwarders to peers.
    disable_forwarding: bool,
    /// The metrics for the peer updater.
    metrics: Arc<BuilderHubMetrics>,
}

impl<P: PeerStore + Send + Sync + 'static> PeersUpdater<P> {
    /// Create a new [`PeerUpdater`].
    pub fn new(
        local_signer: Address,
        peer_store: P,
        peers: Arc<DashMap<String, PeerHandle>>,
        disable_forwarding: bool,
        task_executor: TaskExecutor,
    ) -> Self {
        Self {
            local_signer,
            peer_store,
            peers,
            disable_forwarding,
            task_executor,
            metrics: Arc::new(BuilderHubMetrics::default()),
        }
    }

    /// Run the peer updater loop.
    pub async fn run(mut self) {
        let delay = Duration::from_secs(30);

        loop {
            self.update().await;
            tokio::time::sleep(delay).await;
        }
    }

    /// Update the list of peers from the BuilderHub peer store.
    #[tracing::instrument(skip_all, name = "update_peers")]
    async fn update(&mut self) {
        let builders = match self.peer_store.get_peers().await {
            Ok(builders) => builders,
            Err(e) => {
                self.metrics.peer_request_failures(e.to_string()).inc();
                tracing::error!(?e, "failed to get peers from builderhub");
                return;
            }
        };

        // Remove all peers that are no longer present in BuilderHub
        self.peers.retain(|name, handle| {
            let is_present = builders.iter().any(|b| &b.name == name);
            if !is_present {
                tracing::info!(peer = %name, info = ?handle.info, "removed from configuration");
            }
            is_present
        });

        for builder in builders {
            self.process_peer(builder);
        }

        self.metrics.peer_count().set(self.peers.len());
    }

    /// Process a single peer, updating the local list of connected peers and spawning order
    /// forwarder.
    #[tracing::instrument(skip_all, fields(peer = ?peer))]
    fn process_peer(&mut self, peer: Peer) {
        let entry = self.peers.entry(peer.name.clone());
        let new_peer = match &entry {
            dashmap::Entry::Occupied(entry) => {
                tracing::info!("received configuration update");
                entry.get().info != peer
            }
            dashmap::Entry::Vacant(_) => {
                tracing::info!("received new peer configuration");
                true
            }
        };

        // Self-filter any new peers before connecting to them.
        if new_peer && peer.orderflow_proxy.ecdsa_pubkey_address != self.local_signer {
            // Create a new client for each peer.
            let mut client = reqwest::Client::builder()
                .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
                .connect_timeout(Duration::from_millis(DEFAULT_CONNECT_TIMEOUT_MS))
                .pool_idle_timeout(Duration::from_secs(DEFAULT_POOL_IDLE_TIMEOUT_SECS))
                .pool_max_idle_per_host(DEFAULT_CONNECTION_LIMIT_PER_HOST)
                .connector_layer(utils::limit::ConnectionLimiterLayer::new(
                    DEFAULT_CONNECTION_LIMIT_PER_HOST,
                    peer.name.clone(),
                ));

            // If the TLS certificate is present, use HTTPS and configure the client to use it.
            if let Some(ref tls_cert) = peer.tls_certificate() {
                client = client.https_only(true).add_root_certificate(tls_cert.clone())
            }

            if self.disable_forwarding {
                tracing::warn!("skipped spawning forwarder (disabled forwarding)");
                return;
            }

            let client = client.build().expect("Failed to build client");

            let sender = spawn_forwarder(
                peer.name.clone(),
                peer.system_api(),
                client.clone(),
                &self.task_executor,
            )
            .expect("malformed url");

            tracing::debug!("inserted configuration");
            entry.insert(PeerHandle { info: peer, sender });
        }
    }
}
