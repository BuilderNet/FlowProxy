//! Orderflow ingress for BuilderNet.

use alloy_signer_local::PrivateKeySigner;
use axum::{
    extract::DefaultBodyLimit,
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use entity::SpamThresholds;
use eyre::Context as _;
use forwarder::{spawn_forwarder, IngressForwarders, PeerHandle};
use metrics_exporter_prometheus::PrometheusBuilder;
use metrics_util::layers::{PrefixLayer, Stack};
use reqwest::Url;
use std::{net::SocketAddr, str::FromStr as _, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tracing::{level_filters::LevelFilter, *};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt, EnvFilter};

pub mod cli;
use cli::OrderflowIngressArgs;

pub mod ingress;
use ingress::OrderflowIngress;

use crate::{builderhub::PeerStore, ingress::OrderflowIngressMetrics};

pub mod builderhub;
pub mod entity;
pub mod forwarder;
pub mod jsonrpc;
pub mod priority;
pub mod rate_limit;
pub mod types;
pub mod utils;
pub mod validation;

pub async fn run(args: OrderflowIngressArgs) -> eyre::Result<()> {
    let user_listener = TcpListener::bind(&args.user_listen_url).await?;
    let system_listener = TcpListener::bind(&args.system_listen_url).await?;
    let builder_listener = TcpListener::bind(&args.builder_listen_url).await?;
    run_with_listeners(args, user_listener, system_listener, builder_listener).await
}

pub async fn run_with_listeners(
    args: OrderflowIngressArgs,
    user_listener: TcpListener,
    system_listener: TcpListener,
    builder_listener: TcpListener,
) -> eyre::Result<()> {
    // Initialize tracing.
    let registry = tracing_subscriber::registry().with(
        EnvFilter::builder().with_default_directive(LevelFilter::INFO.into()).from_env_lossy(),
    );
    if args.log_json {
        let _ = registry.with(tracing_subscriber::fmt::layer().json()).try_init();
    } else {
        let _ = registry.with(tracing_subscriber::fmt::layer()).try_init();
    }

    if let Some(metrics_addr) = args.metrics {
        spawn_prometheus_server(SocketAddr::from_str(&metrics_addr)?)?;
    }

    let orderflow_signer = match args.orderflow_signer {
        Some(signer) => signer,
        None => {
            warn!("No orderflow signer was configured, using a random signer. Fix this by passing `--orderflow-signer <PRIVATE KEY>`");
            PrivateKeySigner::random()
        }
    };
    info!(address = %orderflow_signer.address(), "Orderflow signer configured");

    let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build()?;
    let peers = Arc::new(DashMap::<String, PeerHandle>::default());
    if let Some(builder_hub_url) = args.builder_hub_url {
        debug!(url = builder_hub_url, "Running with BuilderHub");
        let builder_hub = builderhub::BuilderHub::new(builder_hub_url);
        builder_hub.register(orderflow_signer.address(), None).await?;

        tokio::spawn({
            let peers = peers.clone();
            async move { run_update_peers(builder_hub, peers).await }
        });
    } else {
        warn!("No BuilderHub URL provided, running with local peer store");
        let local_peer_store = utils::LOCAL_PEER_STORE.clone();

        let peer_store = local_peer_store
            .register(orderflow_signer.address(), Some(system_listener.local_addr()?.port()));

        tokio::spawn({
            let peers = peers.clone();
            async move { run_update_peers(peer_store, peers).await }
        });
    }

    let builder_url = Url::from_str(&args.builder_url)?;
    let local_sender =
        spawn_forwarder(String::from("local-builder"), args.builder_url, client.clone())?;
    let forwarders = IngressForwarders::new(local_sender, peers, orderflow_signer);

    let ingress = Arc::new(OrderflowIngress {
        gzip_enabled: args.gzip_enabled,
        rate_limit_lookback_s: args.rate_limit_lookback_s,
        rate_limit_count: args.rate_limit_count,
        score_lookback_s: args.score_lookback_s,
        score_bucket_s: args.score_bucket_s,
        spam_thresholds: SpamThresholds::default(),
        pqueues: Default::default(),
        entities: DashMap::default(),
        forwarders,
        local_builder_url: Some(builder_url),
        metrics: OrderflowIngressMetrics::default(),
    });

    // Spawn a state maintenance task.
    tokio::spawn({
        let ingress = ingress.clone();
        async move {
            ingress.maintain(Duration::from_secs(60)).await;
        }
    });

    // Spawn user facing HTTP server for accepting bundles and raw transactions.
    let user_router = Router::new()
        .route("/", post(OrderflowIngress::user_handler))
        .route("/health", get(|| async { Ok::<_, ()>(()) }))
        .route("/readyz", get(OrderflowIngress::ready_handler))
        .layer(DefaultBodyLimit::max(args.max_request_size))
        .with_state(ingress.clone());
    let addr = user_listener.local_addr()?;
    info!(target: "ingress", ?addr, "Starting user ingress server");

    // Spawn system facing HTTP server for accepting bundles and raw transactions.
    let system_router = Router::new()
        .route("/", post(OrderflowIngress::system_handler))
        .route("/health", get(|| async { Ok::<_, ()>(()) }))
        .layer(DefaultBodyLimit::max(args.max_request_size))
        .with_state(ingress.clone());
    let addr = system_listener.local_addr()?;
    info!(target: "ingress", ?addr, "Starting system ingress server");

    let builder_router = Router::new()
        .route("/", post(OrderflowIngress::builder_handler))
        .route("/health", get(|| async { Ok::<_, ()>(()) }))
        .with_state(ingress);
    let addr = builder_listener.local_addr()?;
    info!(target: "ingress", ?addr, "Starting builder server");

    tokio::try_join!(
        axum::serve(user_listener, user_router),
        axum::serve(system_listener, system_router),
        axum::serve(builder_listener, builder_router)
    )?;

    Ok(())
}

async fn run_update_peers(peer_store: impl PeerStore, peers: Arc<DashMap<String, PeerHandle>>) {
    let client = reqwest::Client::builder().timeout(Duration::from_secs(2)).build().unwrap();
    let delay = Duration::from_secs(30);

    loop {
        let builders = match peer_store.get_peers().await {
            Ok(builders) => builders,
            Err(error) => {
                error!(target: "ingress::builderhub", ?error, "Error requesting builders from BuilderHub");
                tokio::time::sleep(delay).await;
                continue
            }
        };

        // Remove all peers that are no longer present in BuilderHub
        peers.retain(|name, handle| {
            let is_present = builders.iter().any(|b| &b.name == name);
            if !is_present {
                info!(target: "ingress::builderhub", peer = %name, info = ?handle.info, "Peer was removed from configuration");
            }
            is_present
        });

        // Update or insert builder information.
        for builder in builders {
            let entry = peers.entry(builder.name.clone());
            let should_spawn = match &entry {
                dashmap::Entry::Occupied(entry) => {
                    info!(target: "ingress::builderhub", peer = %builder.name, info = ?builder, "Peer configuration was updated");
                    entry.get().info != builder
                }
                dashmap::Entry::Vacant(_) => {
                    info!(target: "ingress::builderhub", peer = %builder.name, info = ?builder, "New peer configuration");
                    true
                }
            };

            if should_spawn {
                debug!(target: "ingress::builderhub", peer = %builder.name, info = ?builder, "Spawning forwarder");
                let sender =
                    spawn_forwarder(builder.name.clone(), builder.ip.clone(), client.clone())
                        .expect("malformed url");

                debug!(target: "ingress::builderhub", peer = %builder.name, info = ?builder, "Inserting peer configuration");
                entry.insert(PeerHandle { info: builder, sender });
            }
        }

        tokio::time::sleep(Duration::from_secs(30)).await;
    }
}

/// Start prometheus at provider address.
fn spawn_prometheus_server<A: Into<SocketAddr>>(address: A) -> eyre::Result<()> {
    let (recorder, exporter) = PrometheusBuilder::new().with_http_listener(address).build()?;

    // Prefix all metrics with provider prefix
    Stack::new(recorder)
        .push(PrefixLayer::new("orderflow_ingress"))
        .install()
        .wrap_err("unable to install metrics recorder")?;

    tokio::spawn(exporter);

    let collector = metrics_process::Collector::default();
    tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(1)).await;
            collector.collect();
        }
    });

    Ok(())
}
