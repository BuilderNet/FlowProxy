use crate::{
    forwarder::{
        client::{default_http_builder, HttpClientPool, TcpTlsClientPool},
        http::spawn_http_forwarder,
        tcp::spawn_tcp_forwarder,
        ForwardingRequest, PeerHandle,
    },
    metrics::BuilderHubMetrics,
    primitives::PeerProxyInfo,
    priority, DEFAULT_SYSTEM_PORT,
};
use alloy_primitives::Address;
use dashmap::DashMap;
use msg_socket::ReqSocket;
use msg_transport::tcp_tls::{self, TcpTls};
use openssl::{
    ssl::{SslConnector, SslFiletype, SslMethod},
    x509::X509,
};
use std::{
    convert::Infallible, fmt::Debug, future::Future, net::SocketAddr, num::NonZero, path::PathBuf,
    str::FromStr, sync::Arc, time::Duration,
};

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

        if self.reqwest_tls_certificate().is_some() {
            format!("https://{host}")
        } else {
            format!("http://{host}")
        }
    }

    /// Get the TLS certificate from the orderflow proxy credentials.
    /// If the certificate is empty (an empty string), return `None`.
    pub fn reqwest_tls_certificate(&self) -> Option<Certificate> {
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

    /// Get the TLS certificate from the orderflow proxy credentials.
    /// If the certificate is empty (an empty string), return `None`.
    pub fn openssl_tls_certificate(&self) -> Option<Result<X509, openssl::error::ErrorStack>> {
        if self.instance.tls_cert.is_empty() {
            None
        } else {
            // SAFETY: We expect the certificate to be valid. It's added as a root
            // certificate.
            Some(X509::from_pem(self.instance.tls_cert.as_bytes()))
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

#[derive(Debug, Clone)]
pub struct PeersUpdaterConfig {
    /// The local signer address to filter out self-connections.
    pub local_signer: Address,
    /// Whether to disable spawning order forwarders to peers.
    pub disable_forwarding: bool,
    /// For each peer indicates the size of the client pool used to connect to it.
    pub client_pool_size: NonZero<usize>,
    /// Private key PEM file for client authentication (mTLS)
    pub private_key_pem_file: PathBuf,
    /// Certificate PEM file for client authentication (mTLS)
    pub certificate_pem_file: PathBuf,
}

/// A [`PeerUpdater`] periodically fetches the list of peers from a BuilderHub peer store,
/// updating the local list of connected peers and spawning order forwarders.
#[derive(Debug, Clone)]
pub struct PeersUpdater<P: PeerStore> {
    /// Configuration of the updater.
    config: PeersUpdaterConfig,
    /// The BuilderHub peer store.
    peer_store: P,
    /// The local list of connected peers.
    peers: Arc<DashMap<String, PeerHandle>>,
    /// The task executor to spawn forwarders jobs.
    task_executor: TaskExecutor,
    /// The metrics for the peer updater.
    metrics: Arc<BuilderHubMetrics>,
}

impl<P: PeerStore + Send + Sync + 'static> PeersUpdater<P> {
    /// Create a new [`PeerUpdater`].
    pub fn new(
        config: PeersUpdaterConfig,
        peer_store: P,
        peers: Arc<DashMap<String, PeerHandle>>,
        task_executor: TaskExecutor,
    ) -> Self {
        Self {
            peer_store,
            peers,
            config,
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
            self.process_peer(builder).await;
        }

        self.metrics.peer_count().set(self.peers.len());
    }

    /// Get information about a peer's running proxy instance, calling the `/infoz` endpoint.
    ///
    /// On any error, returns `None`.
    #[tracing::instrument(skip_all)]
    async fn proxy_info(&self, peer: &Peer) -> Option<PeerProxyInfo> {
        let http_client = match default_http_builder().build() {
            Ok(c) => c,
            Err(e) => {
                tracing::error!(?e, "critical: failed to create default http client");
                return None;
            }
        };

        // NOTE: HTTP System API is at root, so we can use it.
        let info_url = format!("{}/infoz", peer.system_api());
        let response = match http_client.get(info_url).send().await {
            Ok(r) => r,
            Err(e) => {
                tracing::warn!(
                    ?e,
                    "failed to call infoz endpoint, falling back to http client pool"
                );
                return None;
            }
        };

        let peer_info = match response.json::<PeerProxyInfo>().await {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(?e, "failed to deserialize info");
                return None;
            }
        };

        Some(peer_info)
    }

    /// Process a single peer, updating the local list of connected peers and spawning order
    /// forwarder.
    #[tracing::instrument(skip_all, fields(peer = ?peer))]
    async fn process_peer(&mut self, peer: Peer) {
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
        let is_self = peer.orderflow_proxy.ecdsa_pubkey_address == self.config.local_signer;
        if !new_peer || is_self {
            return;
        }
        if self.config.disable_forwarding {
            tracing::warn!("skipped spawning forwarder (disabled forwarding)");
            return;
        }

        let peer_sender_maybe = if let Some(proxy_info) = self.proxy_info(&peer).await {
            self.peer_tcp_sender(&peer, &proxy_info).await
        } else {
            self.peer_http_sender(&peer).await
        };

        if let Some(sender) = peer_sender_maybe {
            tracing::debug!("inserted configuration");
            entry.insert(PeerHandle { info: peer, sender });
        }
    }

    /// Create a TCP sender for the given peer.
    ///
    /// On any error, returns `None`.
    async fn peer_tcp_sender(
        &self,
        peer: &Peer,
        proxy_info: &PeerProxyInfo,
    ) -> Option<priority::channel::UnboundedSender<Arc<ForwardingRequest>>> {
        let mut sockets = Vec::with_capacity(self.config.client_pool_size.get());
        let mut config = tcp_tls::config::Client::new(peer.dns_name.clone());

        if let Some(certificate_res) = peer.openssl_tls_certificate() {
            let certificate = match certificate_res {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(?e, "failed to parse certificate");
                    return None;
                }
            };

            let connector = match tls_connector(
                &certificate,
                &self.config.private_key_pem_file,
                &self.config.certificate_pem_file,
            ) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(?e, "failed to create tls connector");
                    return None;
                }
            };

            config = config.with_ssl_connector(connector);
        }
        let socket_addr_str = format!("{}:{}", peer.ip.clone(), proxy_info.system_api_port);
        let socket_addr = match SocketAddr::from_str(&socket_addr_str) {
            Ok(a) => a,
            Err(e) => {
                tracing::error!(?e, ip = %peer.ip, "failed to parse ip and port into socket address");
                return None;
            }
        };

        for _ in 0..self.config.client_pool_size.get() {
            let transport = TcpTls::Client(tcp_tls::Client::new(config.clone()));
            let mut socket = ReqSocket::new(transport);
            if let Err(e) = socket.connect(socket_addr).await {
                tracing::error!(?e, ?socket_addr, "failed to initialize connection");
                return None;
            }
            sockets.push(socket);
        }
        let client_pool = TcpTlsClientPool::new(sockets);

        let sender =
            spawn_tcp_forwarder(peer.name.clone(), socket_addr, client_pool, &self.task_executor);

        Some(sender)
    }

    async fn peer_http_sender(
        &self,
        peer: &Peer,
    ) -> Option<priority::channel::UnboundedSender<Arc<ForwardingRequest>>> {
        // Create a client pool.
        let client_pool = HttpClientPool::new(self.config.client_pool_size, || {
            let client_builder = default_http_builder();

            // If the TLS certificate is present, use HTTPS and configure the client to use it.
            if let Some(ref tls_cert) = peer.reqwest_tls_certificate() {
                client_builder
                    .https_only(true)
                    .add_root_certificate(tls_cert.clone())
                    .build()
                    .expect("failed to build client")
            } else {
                client_builder.build().expect("failed to build client")
            }
        });

        let sender = spawn_http_forwarder(
            peer.name.clone(),
            peer.system_api(),
            client_pool.clone(),
            &self.task_executor,
        )
        .expect("malformed url");

        Some(sender)
    }
}

fn tls_connector(
    ca_certificate: &X509,
    private_key_pem_file: &PathBuf,
    certificate_pem_file: &PathBuf,
) -> Result<SslConnector, openssl::error::ErrorStack> {
    let mut builder = SslConnector::builder(SslMethod::tls())?;
    builder.set_private_key_file(private_key_pem_file, SslFiletype::PEM)?;
    builder.set_certificate_file(certificate_pem_file, SslFiletype::PEM)?;
    builder.add_client_ca(ca_certificate)?;
    Ok(builder.build())
}
