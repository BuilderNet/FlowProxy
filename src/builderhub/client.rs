use crate::{builderhub::PeerCredentials, consts::DEFAULT_HTTP_TIMEOUT_SECS};
use alloy_primitives::Address;
use std::{fmt::Debug, time::Duration};

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
    pub inner: reqwest::Client,
    pub url: String,
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
