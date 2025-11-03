use std::{net::SocketAddr, sync::Arc};

use alloy_primitives::Address;
use axum::{
    Json, Router,
    extract::{ConnectInfo, State},
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use dashmap::DashMap;
use serde::{Deserialize, Serialize};
use tokio::net::TcpListener;

#[tokio::main]
async fn main() {
    let system_port: u16 = std::env::var("SYSTEM_PORT")
        .expect("SYSTEM_PORT is not set")
        .parse()
        .expect("SYSTEM_PORT must be a valid port number");

    let enable_tls: bool =
        std::env::var("ENABLE_TLS").map_or(false, |v| v.parse().unwrap_or(false));

    let _ = tracing_subscriber::fmt().try_init();

    let registry = Registry::new(system_port, enable_tls);

    let router = Router::new()
        .route(
            "/api/l1-builder/v1/register_credentials/orderflow_proxy",
            post(register_credentials),
        )
        .route("/api/l1-builder/v1/builders", get(get_builders))
        .with_state(registry);

    let addr = SocketAddr::from(([0, 0, 0, 0], 3000));
    let listener = TcpListener::bind(addr).await.unwrap();
    tracing::info!(tls_enabled = enable_tls, "Listening on {}", addr);
    axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>())
        .await
        .unwrap();
}

#[tracing::instrument(skip(registry, creds))]
async fn register_credentials(
    State(registry): State<Registry>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
    Json(creds): Json<BuilderHubOrderflowProxyCredentials>,
) -> impl IntoResponse {
    tracing::info!("Registering credentials for builder: {:?}", creds.ecdsa_pubkey_address);

    let signer = creds.ecdsa_pubkey_address;

    let builder = BuilderHubBuilder {
        name: format!("{:?}", signer),
        ip: format!("{}:{}", addr.ip(), registry.system_port),
        dns_name: addr.ip().to_string(),
        orderflow_proxy: creds,
        // NOTE: Empty TLS certificate to ensure proxies use plain HTTP.
        // If TLS is enabled, proxies will use the TLS certificate.
        instance: BuilderHubInstanceData {
            tls_cert: if registry.enable_tls { String::new() } else { String::new() },
        },
    };

    registry.builders.insert(signer, builder);

    StatusCode::OK
}

#[tracing::instrument(skip(registry))]
async fn get_builders(
    State(registry): State<Registry>,
    ConnectInfo(addr): ConnectInfo<SocketAddr>,
) -> Json<Vec<BuilderHubBuilder>> {
    tracing::info!("Getting builders");
    let builders = registry
        .builders
        .iter()
        .map(|builder| builder.value().clone())
        .collect::<Vec<BuilderHubBuilder>>();

    tracing::info!("Found {} builders", builders.len());

    Json(builders)
}

#[derive(Debug, Clone)]
struct Registry {
    system_port: u16,
    enable_tls: bool,
    builders: Arc<DashMap<Address, BuilderHubBuilder>>,
}

impl Registry {
    pub fn new(system_port: u16, enable_tls: bool) -> Self {
        Self { builders: Arc::new(DashMap::new()), system_port, enable_tls }
    }
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
pub struct BuilderHubOrderflowProxyCredentials {
    /// Deprecated TLS certificate field for backward compatibility.
    pub tls_cert: Option<String>,
    /// Orderflow signer public key.
    pub ecdsa_pubkey_address: Address,
}

#[derive(PartialEq, Eq, Debug, Clone, Serialize, Deserialize)]
pub struct BuilderHubInstanceData {
    /// TLS certificate.
    pub tls_cert: String,
}
