//! Orderflow ingress for BuilderNet.

use crate::{
    builderhub::PeersUpdater,
    cache::SignerCache,
    forwarder::client::{default_http_builder, ClientPool},
    metrics::IngressMetrics,
    primitives::SystemBundleDecoder,
    priority::workers::PriorityWorkers,
    runner::CliContext,
    statics::LOCAL_PEER_STORE,
};
use alloy_signer_local::PrivateKeySigner;
use axum::{
    extract::{DefaultBodyLimit, Request, State},
    middleware::Next,
    response::Response,
    routing::{get, post},
    Router,
};
use dashmap::DashMap;
use entity::SpamThresholds;
use forwarder::{spawn_forwarder, IngressForwarders, PeerHandle};
use prometric::exporter::ExporterBuilder;
use reqwest::Url;
use std::{
    num::NonZero,
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
pub mod forwarder;
pub mod indexer;
pub mod jsonrpc;
pub mod metrics;
pub mod primitives;
pub mod priority;
pub mod rate_limit;
pub mod runner;
pub mod statics;
pub mod trace;
pub mod utils;
pub mod validation;

/// Default system port for proxy instances.
const DEFAULT_SYSTEM_PORT: u16 = 5544;

pub async fn run(args: OrderflowIngressArgs, ctx: CliContext) -> eyre::Result<()> {
    fdlimit::raise_fd_limit()?;

    if let Some(ref metrics_addr) = args.metrics {
        ExporterBuilder::new().with_address(metrics_addr).with_namespace("flowproxy").install()?;
        metrics::spawn_process_collector().await?;
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

    let client = default_http_builder("local-builder".to_string())
        .build()
        .expect("to create local-builder client");

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
            args.client_pool_size,
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
            args.client_pool_size,
            ctx.task_executor.clone(),
        );

        ctx.task_executor.spawn_critical("local_update_peers", peer_updater.run());
    }

    // Configure the priority worker pool.
    let workers = PriorityWorkers::new_with_threads(args.compute_threads);

    // Spawn forwarders
    let builder_url = args.builder_url.map(|url| Url::from_str(&url)).transpose()?;
    let forwarders = if let Some(ref builder_url) = builder_url {
        let local_sender = spawn_forwarder(
            String::from("local-builder"),
            builder_url.to_string(),
            // Use 1 client here, this is still using HTTP/1.1 with internal connection pooling.
            ClientPool::new(NonZero::new(1).unwrap(), |_idx| client.clone()),
            &ctx.task_executor,
        )?;

        IngressForwarders::new(local_sender, peers, orderflow_signer, workers.clone())
    } else {
        // No builder URL provided, so mock local forwarder.
        let (local_sender, _) = priority::channel::unbounded_channel();
        IngressForwarders::new(local_sender, peers, orderflow_signer, workers.clone())
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
        pqueues: workers,
        entities: DashMap::default(),
        order_cache,
        signer_cache,
        forwarders,
        local_builder_url: builder_url,
        builder_ready_endpoint,
        indexer_handle,
        user_metrics: IngressMetrics::builder().with_label("handler", "user").build(),
        system_metrics: IngressMetrics::builder().with_label("handler", "system").build(),
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
        .route_layer(axum::middleware::from_fn_with_state(
            Arc::new(ingress.user_metrics.clone()),
            track_server_metrics,
        ))
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
        // TODO: After mTLS, we can probably take this out.
        .route_layer(axum::middleware::from_fn_with_state(
            Arc::new(ingress.user_metrics.clone()),
            track_server_metrics,
        ))
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

/// Middleware to track server metrics.
async fn track_server_metrics(
    State(metrics): State<Arc<IngressMetrics>>,
    request: Request,
    next: Next,
) -> Response {
    let path = request.uri().path().to_string();
    let method = request.method().to_string();

    let start = Instant::now();
    let response = next.run(request).await;
    let latency = start.elapsed();
    let status = response.status().as_u16().to_string();

    metrics.http_request_duration(&method, &path, &status).observe(latency.as_secs_f64());

    response
}
