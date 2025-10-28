//! Orderflow ingress for BuilderNet.

use crate::{
    builderhub::PeersUpdater,
    cache::SignerCache,
    consts::{DEFAULT_CONNECTION_LIMIT_PER_HOST, DEFAULT_HTTP_TIMEOUT_SECS},
    metrics::{IngressHandlerMetricsExt, IngressSystemMetrics, IngressUserMetrics},
    primitives::SystemBundleDecoder,
    runner::CliContext,
    statics::LOCAL_PEER_STORE,
};
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
use ingress::forwarder::{spawn_forwarder, IngressForwarders, PeerHandle};
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
use tracing::level_filters::LevelFilter;
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt, EnvFilter};

pub mod cli;
use cli::OrderflowIngressArgs;

pub mod ingress;
use ingress::OrderflowIngress;

use crate::{cache::OrderCache, indexer::Indexer};

pub mod builderhub;
mod cache;
pub mod consts;
pub mod entity;
pub mod indexer;
pub mod jsonrpc;
pub mod metrics;
pub mod primitives;
pub mod priority;
pub mod rate_limit;
pub mod runner;
pub mod statics;
pub mod tasks;
pub mod trace;
pub mod utils;
pub mod validation;

/// Default system port for proxy instances.
const DEFAULT_SYSTEM_PORT: u16 = 5544;

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
            tracing::warn!("No orderflow signer was configured, using a random signer. Fix this by passing `--orderflow-signer <PRIVATE KEY>`");
            PrivateKeySigner::random()
        }
    };
    let local_signer = orderflow_signer.address();
    tracing::info!(address = %local_signer, "Orderflow signer configured");

    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(DEFAULT_HTTP_TIMEOUT_SECS))
        .pool_max_idle_per_host(DEFAULT_CONNECTION_LIMIT_PER_HOST)
        .connector_layer(utils::limit::ConnectionLimiterLayer::new(
            DEFAULT_CONNECTION_LIMIT_PER_HOST,
            "local-builder".to_string(),
        ))
        .build()?;

    let peers = Arc::new(DashMap::<String, PeerHandle>::default());

    if let Some(builder_hub_url) = args.builder_hub_url {
        tracing::debug!(url = builder_hub_url, "Running with BuilderHub");
        let builder_hub = builderhub::Client::new(builder_hub_url);
        builder_hub.register(local_signer).await?;

        let peer_updater = PeersUpdater::new(
            local_signer,
            builder_hub,
            peers.clone(),
            args.disable_forwarding,
            ctx.task_executor.clone(),
        );

        ctx.task_executor.spawn_critical("run_update_peers", peer_updater.run());
    } else {
        tracing::warn!("No BuilderHub URL provided, running with local peer store");
        let local_peer_store = LOCAL_PEER_STORE.clone();

        let peer_store =
            local_peer_store.register(local_signer, Some(system_listener.local_addr()?.port()));

        let peers = peers.clone();
        let peer_updater = PeersUpdater::new(
            local_signer,
            peer_store,
            peers.clone(),
            args.disable_forwarding,
            ctx.task_executor.clone(),
        );

        ctx.task_executor.spawn_critical("local_update_peers", peer_updater.run());
    }

    // Spawn forwarders
    let builder_url = args.builder_url.map(|url| Url::from_str(&url)).transpose()?;
    let forwarders = if let Some(ref builder_url) = builder_url {
        let local_sender = spawn_forwarder(
            String::from("local-builder"),
            builder_url.to_string(),
            client.clone(),
            &ctx.task_executor,
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
        system_bundle_decoder: SystemBundleDecoder { max_txs_per_bundle: args.max_txs_per_bundle },
        spam_thresholds: SpamThresholds::default(),
        flashbots_signer: args.flashbots_signer,
        pqueues: Default::default(),
        entities: DashMap::default(),
        order_cache,
        signer_cache,
        forwarders,
        local_builder_url: builder_url,
        builder_ready_endpoint,
        indexer_handle,
    });

    // Spawn a state maintenance task.
    tokio::spawn({
        let ingress = ingress.clone();
        async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                ingress.maintenance().await;
            }
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
    tracing::info!(target: "ingress", ?addr, "Starting user ingress server");

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
    tracing::info!(target: "ingress", ?addr, "Starting system ingress server");

    if let Some(builder_listener) = builder_listener {
        let builder_router = Router::new()
            .route("/", post(OrderflowIngress::builder_handler))
            .route("/health", get(|| async { Ok::<_, ()>(()) }))
            .with_state(ingress);
        let addr = builder_listener.local_addr()?;
        tracing::info!(target: "ingress", ?addr, "Starting builder server");

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
