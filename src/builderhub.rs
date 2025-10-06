use std::{sync::Arc, time::Duration};

use dashmap::DashMap;
use reqwest::Certificate;
use revm_primitives::Address;
use serde::{Deserialize, Serialize};
use tracing::error;

use crate::DEFAULT_SYSTEM_PORT;

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct BuilderHubOrderflowProxyCredentials {
    /// TLS certificate of the orderflow proxy in UTF-8 encoded PEM format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tls_cert: Option<String>,
    /// Orderflow signer public key.
    pub ecdsa_pubkey_address: Address,
}

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct BuilderHubBuilder {
    /// Builder name.
    pub name: String,
    /// Builder IP.
    pub ip: String,
    /// Builder DNS name.
    pub dns_name: String,
    /// Builder orderflow proxy configuration.
    pub orderflow_proxy: BuilderHubOrderflowProxyCredentials,
    /// Instance data
    pub instance: BuilderHubInstanceData,
}

impl BuilderHubBuilder {
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

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct BuilderHubInstanceData {
    /// TLS certificate of the instance in UTF-8 encoded PEM format.
    pub tls_cert: String,
}

/// A trait for a peer store.
pub trait PeerStore {
    #[allow(async_fn_in_trait)]
    async fn get_peers(&self) -> eyre::Result<Vec<BuilderHubBuilder>>;
}

/// A BuilderHub client.
#[derive(Debug)]
pub struct BuilderHub {
    client: reqwest::Client,
    url: String,
}

impl BuilderHub {
    /// Create a new BuilderHub client with a default HTTP timeout of 2 seconds.
    pub fn new(url: String) -> Self {
        let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();
        Self { client, url }
    }

    pub async fn register(&self, signer_address: Address, _port: Option<u16>) -> eyre::Result<()> {
        let endpoint =
            format!("{}/api/l1-builder/v1/register_credentials/orderflow_proxy", self.url);
        let body = BuilderHubOrderflowProxyCredentials {
            tls_cert: None,
            ecdsa_pubkey_address: signer_address,
        };
        let response = self.client.post(endpoint).json(&body).send().await?;
        let status = response.status();
        if !status.is_success() {
            let error = response.text().await.unwrap_or_default();
            error!(?status, %error, "error registering with BuilderHub");
            eyre::bail!("Error registering with BuilderHub")
        }

        Ok(())
    }
}

impl PeerStore for BuilderHub {
    async fn get_peers(&self) -> eyre::Result<Vec<BuilderHubBuilder>> {
        let endpoint = format!("{}/api/l1-builder/v1/builders", self.url);
        let response = self.client.get(endpoint).send().await?;
        Ok(response.json().await?)
    }
}

#[derive(Debug, Clone)]
pub struct LocalPeerStore {
    pub(crate) builders: Arc<DashMap<String, BuilderHubBuilder>>,
}

impl LocalPeerStore {
    pub(crate) fn new() -> Self {
        Self { builders: Arc::new(DashMap::new()) }
    }

    pub fn register(&self, signer_address: Address, port: Option<u16>) -> LocalPeerStore {
        self.builders.insert(
            signer_address.to_string(),
            BuilderHubBuilder {
                name: signer_address.to_string(),
                ip: format!("127.0.0.1:{}", port.unwrap()),
                // Don't set the DNS name for local peer store or it will try to connect to
                // {dns_name}:5544
                dns_name: "".to_string(),
                orderflow_proxy: BuilderHubOrderflowProxyCredentials {
                    tls_cert: None,
                    ecdsa_pubkey_address: signer_address,
                },
                instance: BuilderHubInstanceData { tls_cert: "".to_string() },
            },
        );

        LocalPeerStore { builders: self.builders.clone() }
    }
}

impl PeerStore for LocalPeerStore {
    async fn get_peers(&self) -> eyre::Result<Vec<BuilderHubBuilder>> {
        Ok(self.builders.iter().map(|b| b.value().clone()).collect())
    }
}
