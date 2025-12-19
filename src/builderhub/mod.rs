use crate::{
    forwarder::{
        ForwardingRequest, PeerHandle,
        client::{ReqSocketIpBucketPool, TcpTransport, tcp_clients_buckets},
        tcp::spawn_tcp_forwarder,
    },
    metrics::BuilderHubMetrics,
    priority,
};
use alloy_primitives::Address;
use dashmap::DashMap;
use msg_socket::ReqSocket;
use msg_transport::{
    tcp::Tcp,
    tcp_tls::{self, TcpTls},
};
use openssl::{
    ssl::{SslConnector, SslFiletype, SslMethod},
    x509::X509,
};
use std::{
    convert::Infallible, fmt::Debug, future::Future, io, net::SocketAddr, num::NonZero,
    path::PathBuf, sync::Arc, time::Duration,
};
use tokio::{
    net::{ToSocketAddrs, lookup_host},
    sync::mpsc,
};

use rbuilder_utils::tasks::TaskExecutor;
use serde::{Deserialize, Serialize};

mod client;
pub use client::Client;

/// Default TLS ciphers for the TLS system clients. In some cases, CHACHA20 is negotiated by
/// default, which isn't as fast as AES on hardware. AES is usually accelerated by special AES-NI
/// CPU instructions present in both Intel and AMD CPUs.
///
/// <https://en.wikipedia.org/wiki/AES_instruction_set>
pub const DEFAULT_TLS_CIPHERS: &str =
    "TLS_AES_128_GCM_SHA256:TLS_AES_256_GCM_SHA384:TLS_CHACHA20_POLY1305_SHA256";

/// Default system port for proxy instances.
const DEFAULT_SYSTEM_PORT: u16 = 5544;

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
pub struct InstanceData {
    /// TLS certificate of the instance in UTF-8 encoded PEM format.
    pub tls_cert: String,
}

/// The credentials of the running overflow proxy of a BuilderHub peer.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
pub struct PeerCredentials {
    /// TLS certificate of the orderflow proxy in UTF-8 encoded PEM format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_cert: Option<String>,
    /// Orderflow signer public key.
    pub ecdsa_pubkey_address: Address,
}

/// A [`Peer`] is a builder inside Builderhub. This holds informations about a builder peer, as
/// returned by the `api/l1-builder/v1/builders` endpoint of BuilderHub.
#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize, Default)]
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
    /// Get the system API TCP socket address for the builder.
    ///
    /// Reference: <https://github.com/flashbots/buildernet-orderflow-proxy/blob/main/proxy/confighub.go>
    pub async fn system_api(&self) -> io::Result<Option<SocketAddr>> {
        // NOTE: Needed for integration tests where port is not known upfront. This is also more
        // flexible in the case some instances won't run with that default port.
        let port =
            self.ip.split(':').nth(1).and_then(|p| p.parse().ok()).unwrap_or(DEFAULT_SYSTEM_PORT);

        let host_with_port = if self.dns_name.is_empty() {
            if self.ip.contains(":") { self.ip.clone() } else { format!("{}:{}", self.ip, port) }
        } else {
            format!("{}:{}", self.dns_name, port)
        };

        Ok(lookup_host(host_with_port).await?.next())
    }

    /// Get the TLS certificate from the orderflow proxy credentials.
    /// If the certificate is empty (an empty string), return `None`.
    pub fn openssl_tls_certificate(&self) -> Option<Result<X509, openssl::error::ErrorStack>> {
        if self.instance.tls_cert.is_empty() {
            None
        } else {
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
    pub builders: Arc<DashMap<String, Peer>>,
}

impl LocalPeerStore {
    pub(crate) fn new() -> Self {
        Self { builders: Arc::new(DashMap::new()) }
    }

    pub fn register(&self, peer: Peer) -> LocalPeerStore {
        self.builders.insert(peer.orderflow_proxy.ecdsa_pubkey_address.to_string(), peer);

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
    /// Number of TCP clients to use per peer for small messages.
    pub tcp_small_clients: NonZero<usize>,
    /// Number of TCP clients to use per peer for big messages.
    pub tcp_big_clients: usize,
    /// PEM file containing both certificate and private key, used for client authentication (mTLS)
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
    /// Channel to send current peers certificates, to update TLS acceptor on the server.
    certs_tx: mpsc::Sender<Vec<X509>>,
}

impl<P: PeerStore + Send + Sync + 'static> PeersUpdater<P> {
    /// Create a new [`PeerUpdater`].
    pub fn new(
        config: PeersUpdaterConfig,
        peer_store: P,
        peers: Arc<DashMap<String, PeerHandle>>,
        task_executor: TaskExecutor,
    ) -> (Self, mpsc::Receiver<Vec<X509>>) {
        let (tx, rx) = mpsc::channel(8);
        (
            Self {
                peer_store,
                peers,
                config,
                task_executor,
                metrics: Arc::new(BuilderHubMetrics::default()),
                certs_tx: tx,
            },
            rx,
        )
    }

    /// Run the peer updater loop.
    pub async fn run(mut self, interval_s: u64) {
        let delay = Duration::from_secs(interval_s);

        loop {
            self.update().await;
            tokio::time::sleep(delay).await;
        }
    }

    /// Update the list of peers from the BuilderHub peer store.
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

        let certs = builders
            .iter()
            .filter_map(|p| {
                // Skip self.
                if p.orderflow_proxy.ecdsa_pubkey_address == self.config.local_signer {
                    return None;
                }

                let Some(c_res) = p.openssl_tls_certificate() else {
                    tracing::warn!(peer = ?p, "received peer update without certificate");
                    return None;
                };
                match c_res {
                    Ok(c) => Some(c),
                    Err(e) => {
                        tracing::warn!(?e, peer = ?p, "received invalid tls certificate");
                        None
                    }
                }
            })
            .collect::<Vec<_>>();

        tracing::debug!(
            len = certs.len(),
            builders = builders.len(),
            peers = self.peers.len(),
            "sending certificates for acceptor update"
        );

        if let Err(e) = self.certs_tx.send(certs).await {
            tracing::error!(?e, "failed to send certificates update");
        }

        for builder in builders {
            self.process_peer(builder).await;
        }

        self.metrics.peer_count().set(self.peers.len());
    }

    /// Process a single peer, updating the local list of connected peers and spawning order
    /// forwarder.
    #[tracing::instrument(skip_all, fields(peer = ?peer))]
    async fn process_peer(&mut self, peer: Peer) {
        let is_self = peer.orderflow_proxy.ecdsa_pubkey_address == self.config.local_signer;
        if is_self {
            tracing::debug!("skipped processing self peer");
            return;
        }

        match self.peers.entry(peer.name.clone()) {
            dashmap::Entry::Occupied(entry) => {
                if entry.get().info != peer {
                    tracing::info!("received configuration update");
                } else {
                    tracing::debug!("received no configuration changes, skipping");
                    return;
                }
            }
            dashmap::Entry::Vacant(_) => {
                tracing::info!("received new peer configuration");
            }
        };

        if self.config.disable_forwarding {
            tracing::warn!("skipped spawning forwarder (disabled forwarding)");
            return;
        }

        let peer_sender_maybe = self.peer_tcp_sender(&peer).await;

        if let Some(sender) = peer_sender_maybe {
            self.peers.insert(peer.name.clone(), PeerHandle { info: peer, sender });
            tracing::debug!(peers = self.peers.len(), "inserted configuration");
        }
    }

    /// Create a TCP sender for the given peer, to forwarder orderflow requests.
    ///
    /// On any error, returns `None`.
    async fn peer_tcp_sender(
        &self,
        peer: &Peer,
    ) -> Option<priority::channel::UnboundedSender<Arc<ForwardingRequest>>> {
        let tls_config = if let Some(certificate_res) = peer.openssl_tls_certificate() {
            let certificate = match certificate_res {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(?e, "failed to parse certificate");
                    return None;
                }
            };

            tracing::debug!("using tls connector");

            let connector = match tls_connector(certificate, &self.config.certificate_pem_file) {
                Ok(c) => c,
                Err(e) => {
                    tracing::error!(?e, "failed to create tls connector");
                    return None;
                }
            };

            Some(tcp_tls::config::Client::new(peer.dns_name.clone()).with_ssl_connector(connector))
        } else {
            None
        };

        let socket_addr = match peer.system_api().await {
            Ok(Some(addr)) => addr,
            Ok(None) => {
                tracing::error!("failed to resolve socket address, lookup_host returned `None`");
                return None;
            }
            Err(e) => {
                tracing::error!(?e, "failed to resolve socket address");
                return None;
            }
        };

        if let Some(config) = tls_config {
            let make_transport = || TcpTls::Client(tcp_tls::Client::new(config.clone()));
            self.peer_sender_inner(peer, socket_addr, make_transport).await
        } else {
            let make_transport = || Tcp::default();
            self.peer_sender_inner(peer, socket_addr, make_transport).await
        }
    }

    /// Inner helper to create a TCP sender for the given peer, generic over the transport (TCP or
    /// TCP+TLS).
    async fn peer_sender_inner<T: TcpTransport, S: ToSocketAddrs + Debug>(
        &self,
        peer: &Peer,
        address: S,
        make_transport: impl Fn() -> T,
    ) -> Option<priority::channel::UnboundedSender<Arc<ForwardingRequest>>> {
        let sock_addr = match lookup_host(address).await {
            Ok(mut a_iter) => {
                let Some(a) = a_iter.next() else {
                    tracing::error!("no addresses found");
                    return None;
                };
                a
            }
            Err(e) => {
                tracing::error!(?e, "failed to resolve address");
                return None;
            }
        };

        let make_socket = || {
            let transport = make_transport();
            let mut socket = ReqSocket::new(transport);
            socket.connect_sync(sock_addr);

            socket
        };

        let buckets =
            tcp_clients_buckets(self.config.tcp_small_clients.get(), self.config.tcp_big_clients);
        let client_pool = ReqSocketIpBucketPool::new(buckets, peer.name.clone(), make_socket);

        let sender =
            spawn_tcp_forwarder(peer.name.clone(), sock_addr, client_pool, &self.task_executor);

        Some(sender)
    }
}

/// Create a TLS connector with:
///
/// 1. The peer root certificate, added to the certificate store. This is to establish connections
///    with peer acting as a server.
/// 2. Private key and certificate file for client authentication (mTLS).
fn tls_connector(
    peer_root_certificate: X509,
    certificate_pem_file: &PathBuf,
) -> Result<SslConnector, openssl::error::ErrorStack> {
    let mut builder = SslConnector::builder(SslMethod::tls())?;

    builder.set_private_key_file(certificate_pem_file, SslFiletype::PEM)?;
    builder.set_certificate_file(certificate_pem_file, SslFiletype::PEM)?;

    let certificate_store = builder.cert_store_mut();
    certificate_store.add_cert(peer_root_certificate)?;

    // Explicitly set the ciphersuites
    builder.set_ciphersuites(DEFAULT_TLS_CIPHERS)?;

    Ok(builder.build())
}

#[cfg(test)]
mod tests {
    use crate::builderhub::{DEFAULT_SYSTEM_PORT, Peer};
    use tokio::net::lookup_host;

    #[tokio::test]
    async fn system_api_host_and_port_works() {
        let ip = "10.0.0.1".to_owned();
        let ip_with_port = format!("{ip}:{DEFAULT_SYSTEM_PORT}");
        let sock = ip_with_port.parse::<std::net::SocketAddr>().unwrap();

        let dns = "chainbound.io".to_owned();
        let dns_with_port = format!("{dns}:{DEFAULT_SYSTEM_PORT}");
        let sock_from_dns = lookup_host(dns_with_port).await.unwrap().next().unwrap();

        let peer = Peer { ip: ip_with_port.clone(), ..Default::default() };

        assert_eq!(peer.system_api().await.unwrap().unwrap(), sock);

        let ip = "10.0.0.1".to_owned();
        let peer = Peer { ip: ip.clone(), ..Default::default() };

        assert_eq!(peer.system_api().await.unwrap().unwrap(), sock);

        let peer = Peer { ip: ip.clone(), dns_name: dns.clone(), ..Default::default() };

        assert_eq!(peer.system_api().await.unwrap().unwrap(), sock_from_dns);
    }
}
