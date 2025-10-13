//! Orderflow ingress for BuilderNet.

use crate::{
    cache::SignerCache,
    consts::{DEFAULT_CONNECTION_LIMIT_PER_HOST, DEFAULT_HTTP_TIMEOUT_SECS},
    metrics::{
        BuilderHubMetrics, IngressHandlerMetricsExt, IngressSystemMetrics, IngressUserMetrics,
    },
    runner::CliContext,
    statics::LOCAL_PEER_STORE,
    tasks::TaskExecutor,
};
use alloy_primitives::Address;
use alloy_signer_local::PrivateKeySigner;
use axum::{
    extract::{DefaultBodyLimit, Request},
    middleware::Next,
    response::Response,
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
use std::{
    net::SocketAddr,
    str::FromStr as _,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::net::TcpListener;
use tracing::{level_filters::LevelFilter, *};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt, EnvFilter};

pub mod cli;
use cli::OrderflowIngressArgs;

pub mod ingress;
use ingress::OrderflowIngress;

use crate::{builderhub::PeerStore, cache::OrderCache, indexer::Indexer};

pub mod builderhub;
mod cache;
pub mod consts;
pub mod entity;
pub mod forwarder;
pub mod indexer;
pub mod jsonrpc;
pub mod metrics;
pub mod priority;
pub mod rate_limit;
pub mod runner;
pub mod statics;
pub mod tasks;
pub mod types;
pub mod utils;
pub mod validation;

/// Default system port for proxy instances.
const DEFAULT_SYSTEM_PORT: u16 = 5544;

pub fn init_tracing(log_json: bool) {
    let registry = tracing_subscriber::registry().with(
        EnvFilter::builder().with_default_directive(LevelFilter::INFO.into()).from_env_lossy(),
    );
    if log_json {
        let _ = registry.with(tracing_subscriber::fmt::layer().json()).try_init();
    } else {
        let _ = registry.with(tracing_subscriber::fmt::layer()).try_init();
    }
}

pub async fn run(args: OrderflowIngressArgs, ctx: CliContext) -> eyre::Result<()> {
    fdlimit::raise_fd_limit()?;

    if let Some(ref metrics_addr) = args.metrics {
        spawn_prometheus_server(SocketAddr::from_str(metrics_addr)?)?;
        metrics::describe();
    }

    let user_listener = TcpListener::bind(&args.user_listen_url).await?;
    let system_listener = TcpListener::bind(&args.system_listen_url).await?;
    let builder_listener = if let Some(ref builder_listen_url) = args.builder_listen_url {
        Some(TcpListener::bind(builder_listen_url).await?)
    } else {
        None
    };
    run_with_listeners(args, user_listener, system_listener, builder_listener, ctx).await
}

pub async fn run_with_listeners(
    args: OrderflowIngressArgs,
    user_listener: TcpListener,
    system_listener: TcpListener,
    builder_listener: Option<TcpListener>,
    ctx: CliContext,
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

    let indexer_handle = Indexer::run(args.indexing, args.builder_name, ctx.task_executor.clone());

    let orderflow_signer = match args.orderflow_signer {
        Some(signer) => signer,
        None => {
            warn!("No orderflow signer was configured, using a random signer. Fix this by passing `--orderflow-signer <PRIVATE KEY>`");
            PrivateKeySigner::random()
        }
    };
    let local_signer = orderflow_signer.address();
    info!(address = %local_signer, "Orderflow signer configured");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
        .pool_max_idle_per_host(DEFAULT_CONNECTION_LIMIT_PER_HOST)
        .pool_idle_timeout(Duration::from_secs(30))
        .connector_layer(utils::limit::ConnectionLimiterLayer::new(
            DEFAULT_CONNECTION_LIMIT_PER_HOST,
            "local-builder".to_string(),
        ))
        .build()?;

    let peers = Arc::new(DashMap::<String, PeerHandle>::default());
    if let Some(builder_hub_url) = args.builder_hub_url {
        debug!(url = builder_hub_url, "Running with BuilderHub");
        let builder_hub = builderhub::BuilderHub::new(builder_hub_url);
        builder_hub.register(local_signer, None).await?;

        let peers = peers.clone();
        let task_executor = ctx.task_executor.clone();
        ctx.task_executor.spawn_critical("run_update_peers", async move {
            run_update_peers(
                local_signer,
                builder_hub,
                peers,
                args.disable_forwarding,
                task_executor,
            )
            .await
        });
    } else {
        warn!("No BuilderHub URL provided, running with local peer store");
        let local_peer_store = LOCAL_PEER_STORE.clone();

        let peer_store =
            local_peer_store.register(local_signer, Some(system_listener.local_addr()?.port()));

        let peers = peers.clone();
        let task_executor = ctx.task_executor.clone();
        ctx.task_executor.spawn_critical("local_update_peers", {
            async move {
                run_update_peers(
                    local_signer,
                    peer_store,
                    peers,
                    args.disable_forwarding,
                    task_executor.clone(),
                )
                .await
            }
        });
    }

    // Spawn forwarders
    let builder_url = args.builder_url.map(|url| Url::from_str(&url)).transpose()?;
    let forwarders = if let Some(ref builder_url) = builder_url {
        let local_sender = spawn_forwarder(
            String::from("local-builder"),
            builder_url.to_string(),
            client.clone(),
            ctx.task_executor.clone(),
        )?;

        IngressForwarders::new(local_sender, peers, orderflow_signer)
    } else {
        // No builder URL provided, so mock local forwarder.
        let (local_sender, _) = priority::pchannel::unbounded_channel();
        IngressForwarders::new(local_sender, peers, orderflow_signer)
    };

    let builder_ready_endpoint =
        args.builder_ready_endpoint.map(|url| Url::from_str(&url)).transpose()?;

    let order_cache = OrderCache::new(args.cache.order_cache_ttl, args.cache.order_cache_size);
    let signer_cache = SignerCache::new(args.cache.signer_cache_ttl, args.cache.signer_cache_size);

    let ingress = Arc::new(OrderflowIngress {
        gzip_enabled: args.gzip_enabled,
        rate_limiting_enabled: args.enable_rate_limiting,
        rate_limit_lookback_s: args.rate_limit_lookback_s,
        rate_limit_count: args.rate_limit_count,
        score_lookback_s: args.score_lookback_s,
        score_bucket_s: args.score_bucket_s,
        spam_thresholds: SpamThresholds::default(),
        flashbots_signer: args.flashbots_signer,
        pqueues: Default::default(),
        entities: DashMap::default(),
        order_cache,
        signer_cache: Arc::new(signer_cache),
        forwarders,
        local_builder_url: builder_url,
        builder_ready_endpoint,
        indexer_handle,
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
        .route("/livez", get(|| async { Ok::<_, ()>(()) }))
        .route("/readyz", get(OrderflowIngress::ready_handler))
        .layer(DefaultBodyLimit::max(args.max_request_size))
        .route_layer(axum::middleware::from_fn(track_server_metrics::<IngressUserMetrics>))
        .with_state(ingress.clone());
    let addr = user_listener.local_addr()?;
    info!(target: "ingress", ?addr, "Starting user ingress server");

    // Spawn system facing HTTP server for accepting bundles and raw transactions.
    let system_router = Router::new()
        .route("/", post(OrderflowIngress::system_handler))
        .route("/health", get(|| async { Ok::<_, ()>(()) }))
        .route("/livez", get(|| async { Ok::<_, ()>(()) }))
        .route("/readyz", get(OrderflowIngress::ready_handler))
        .layer(DefaultBodyLimit::max(args.max_request_size))
        .layer(DefaultBodyLimit::max(args.max_request_size))
        // TODO: After mTLS, we can probably take this out.
        .route_layer(axum::middleware::from_fn(track_server_metrics::<IngressSystemMetrics>))
        .with_state(ingress.clone());
    let addr = system_listener.local_addr()?;
    info!(target: "ingress", ?addr, "Starting system ingress server");

    if let Some(builder_listener) = builder_listener {
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
    } else {
        tokio::try_join!(
            axum::serve(user_listener, user_router),
            axum::serve(system_listener, system_router),
        )?;
    }

    Ok(())
}

async fn run_update_peers(
    local_signer: Address,
    peer_store: impl PeerStore,
    peers: Arc<DashMap<String, PeerHandle>>,
    disable_forwarding: bool,
    task_executor: TaskExecutor,
) {
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
        .build()
        .unwrap();
    let delay = Duration::from_secs(30);

    loop {
        let builders = match peer_store.get_peers().await {
            Ok(builders) => builders,
            Err(error) => {
                BuilderHubMetrics::increment_builderhub_peer_request_failures(error.to_string());
                error!(target: "ingress::builderhub", ?error, "Error requesting builders from BuilderHub");
                tokio::time::sleep(delay).await;
                continue;
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
            let new_peer = match &entry {
                dashmap::Entry::Occupied(entry) => {
                    info!(target: "ingress::builderhub", peer = %builder.name, info = ?builder, "Peer configuration was updated");
                    entry.get().info != builder
                }
                dashmap::Entry::Vacant(_) => {
                    info!(target: "ingress::builderhub", peer = %builder.name, info = ?builder, "New peer configuration");
                    true
                }
            };

            // Self-filter any new peers before connecting to them.
            if new_peer && builder.orderflow_proxy.ecdsa_pubkey_address != local_signer {
                let mut client = client.clone();

                debug!(target: "ingress::builderhub", peer = %builder.name, info = ?builder, "Spawning forwarder");

                // If the TLS certificate is present, use HTTPS and configure the client to use it.
                if let Some(ref tls_cert) = builder.tls_certificate() {
                    // SAFETY: We expect the certificate to be valid. It's added as a root
                    // certificate.
                    client = reqwest::Client::builder()
                        .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
                        .https_only(true)
                        .add_root_certificate(tls_cert.clone())
                        .build()
                        .expect("Valid root certificate");
                }

                if disable_forwarding {
                    warn!(target: "ingress::builderhub", peer = %builder.name, info = ?builder, "Skipped spawning forwarder (disabled forwarding)");
                    continue;
                }

                let sender = spawn_forwarder(
                    builder.name.clone(),
                    builder.system_api(),
                    client.clone(),
                    task_executor.clone(),
                )
                .expect("malformed url");

                debug!(target: "ingress::builderhub", peer = %builder.name, info = ?builder, "Inserting peer configuration");
                entry.insert(PeerHandle { info: builder, sender });
            }
        }

        BuilderHubMetrics::builderhub_peer_count(peers.len());

        tokio::time::sleep(delay).await;
    }
}

/// Start prometheus at provider address.
fn spawn_prometheus_server<A: Into<SocketAddr>>(address: A) -> eyre::Result<()> {
    let (recorder, exporter) = PrometheusBuilder::new().with_http_listener(address).build()?;

    // Prefix all metrics with provider prefix
    Stack::new(recorder)
        .push(PrefixLayer::new("orderflow_proxy"))
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

/// Middleware to track server metrics.
async fn track_server_metrics<T: IngressHandlerMetricsExt>(
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    let method = request.method().clone();

    let start = Instant::now();
    let response = next.run(request).await;
    let latency = start.elapsed();
    let status = response.status();

    T::record_http_request(&method, path, status, latency);

    response
}
