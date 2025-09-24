use std::{path::PathBuf, sync::Arc, time::Duration};

use revm_primitives::Address;
use serde::{Deserialize, Serialize};
use tracing::error;
use uuid::Uuid;

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct BuilderHubOrderflowProxyCredentials {
    /// Deprecated TLS certificate field for backward compatibility.
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

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct BuilderHubInstanceData {
    /// TLS certificate.
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
    pub(crate) db: Arc<rocksdb::DB>,
    tmp: bool,
}

impl Drop for LocalPeerStore {
    fn drop(&mut self) {
        if self.tmp {
            let path = self.db.path();

            // NOTE: If this is a shared database, this may panic if the directory is already
            // destroyed.
            let _ = std::fs::remove_dir_all(path);
        }
    }
}

#[derive(Debug, thiserror::Error)]
pub enum PeerStoreError {
    #[error(transparent)]
    Rocksdb(#[from] rocksdb::Error),
    #[error(transparent)]
    Serde(#[from] serde_json::Error),
}

impl LocalPeerStore {
    /// Create a new local peer store in the given directory. The directory will be created if it
    /// doesn't exist. If the directory is a temporary directory, the database will be destroyed
    /// when the store is dropped.
    pub(crate) fn new(path: PathBuf) -> Self {
        // Create the directory if it doesn't exist.
        if !path.exists() {
            std::fs::create_dir_all(&path).unwrap();
        }

        let tmp = path.starts_with(std::env::temp_dir());

        let db = rocksdb::DB::open_default(path).unwrap().into();
        Self { db, tmp }
    }

    /// Create a new local peer store in random, temporary directory.
    pub(crate) fn new_temp() -> Self {
        let path =
            std::env::temp_dir().join(format!("buildernet-orderflow-proxy-{}", Uuid::new_v4()));
        Self::new(path)
    }

    /// Get the path of the local peer store.
    pub(crate) fn path(&self) -> PathBuf {
        self.db.path().to_path_buf()
    }

    pub fn register(
        &self,
        signer_address: Address,
        port: Option<u16>,
    ) -> Result<(), PeerStoreError> {
        let builder = BuilderHubBuilder {
            name: signer_address.to_string(),
            ip: format!("http://127.0.0.1:{}", port.unwrap()),
            dns_name: "localhost".to_string(),
            orderflow_proxy: BuilderHubOrderflowProxyCredentials {
                tls_cert: None,
                ecdsa_pubkey_address: signer_address,
            },
            instance: BuilderHubInstanceData { tls_cert: "".to_string() },
        };

        self.db.put(signer_address.to_string(), serde_json::to_vec(&builder)?)?;

        Ok(())
    }
}

impl PeerStore for LocalPeerStore {
    async fn get_peers(&self) -> eyre::Result<Vec<BuilderHubBuilder>> {
        let mut builders = Vec::new();
        let iter = self.db.iterator(rocksdb::IteratorMode::Start);
        for result in iter {
            let (_, value) = result?;
            let builder = serde_json::from_slice(&value)?;
            builders.push(builder);
        }

        Ok(builders)
    }
}
