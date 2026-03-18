//! Orderflow ingress for BuilderNet.

use crate::{
    builderhub::{PeersUpdater, PeersUpdaterConfig},
    forwarder::{
        client::{default_http_builder, HttpClientPool},
        http::spawn_http_forwarder,
    },
    indexer::IndexerHandle,
    ingress::IngressSocket,
    metrics::IngressMetrics,
    priority::workers::PriorityWorkers,
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
use forwarder::{IngressForwarders, PeerHandle};
use msg_socket::RepSocket;
use msg_transport::tcp::Tcp;
use prometric::exporter::ExporterBuilder;
use rbuilder_utils::tasks::TaskExecutor;
use std::{
    net::SocketAddr,
    num::NonZero,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, select};
use tokio_util::sync::CancellationToken;
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt, EnvFilter};

pub mod cli;
use cli::OrderflowIngressArgs;

pub mod ingress;
use ingress::OrderflowIngress;

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
pub mod statics;
pub mod trace;
pub mod utils;
pub mod validation;

pub async fn run(
    args: OrderflowIngressArgs,
    task_executor: TaskExecutor,
    indexer_handle: IndexerHandle,
    cancellation_token: CancellationToken,
) -> eyre::Result<()> {
    fdlimit::raise_fd_limit()?;

    eyre::ensure!(
        args.disk_backup_size_resume_flow_threshold_mb <= args.disk_backup_size_reject_flow_threshold_mb,
        "disk-backup-size-to-resume-flow-threshold-mb ({}) must be <= disk-backup-size-reject-flow-threshold-mb ({})",
        args.disk_backup_size_resume_flow_threshold_mb,
        args.disk_backup_size_reject_flow_threshold_mb,
    );

    if let Some(ref metrics_addr) = args.metrics {
        ExporterBuilder::new().with_address(metrics_addr).with_namespace("flowproxy").install()?;
        metrics::spawn_process_collector().await?;

        // Set build info metric
        metrics::BUILD_INFO_METRICS.info(env!("CARGO_PKG_VERSION"), env!("GIT_HASH")).set(1);
    }

    let user_listener = TcpListener::bind(&args.user_listen_addr).await?;
    let mut system_listener = RepSocket::new(Tcp::default());
    system_listener.bind(args.system_listen_addr).await.expect("to bind tcp socket address");

    let builder_listener = if let Some(ref builder_listen_url) = args.builder_listen_addr {
        Some(TcpListener::bind(builder_listen_url).await?)
    } else {
        None
    };
    run_with_listeners(
        args,
        user_listener,
        system_listener,
        builder_listener,
        task_executor,
        indexer_handle,
        cancellation_token,
    )
    .await
}

/// Cancellation is a little ugly, just added enough to make it drop indexer_handle but it's a mix
/// of cancellation_token + task_executor which also has a shutdown method.
pub async fn run_with_listeners(
    args: OrderflowIngressArgs,
    user_listener: TcpListener,
    system_listener: RepSocket<Tcp, SocketAddr>,
    builder_listener: Option<TcpListener>,
    task_executor: TaskExecutor,
    indexer_handle: IndexerHandle,
    cancellation_token: CancellationToken,
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

    let config = args.ingress_config()?;

    let orderflow_signer = match args.orderflow_signer {
        Some(signer) => {
            tracing::warn!(
                "orderflow signer provided via cli, this is discouraged in production environments"
            );
            signer
        }
        None => PrivateKeySigner::random(),
    };
    let local_signer = orderflow_signer.address();
    tracing::info!(address = %local_signer, "running with orderflow signer");

    let client = default_http_builder().build().expect("to create local-builder client");

    let peers = Arc::new(DashMap::<String, PeerHandle>::default());

    let peer_update_config = PeersUpdaterConfig {
        local_signer,
        disable_forwarding: args.disable_forwarding,
        tcp_small_clients: args.tcp_small_clients,
        tcp_big_clients: args.tcp_big_clients,
        certificate_pem_file: args.certificate_pem_file,
        private_key_pem_file: args.private_key_pem_file,
    };

    if let Some(builder_hub_url) = args.builder_hub_url {
        tracing::debug!(url = builder_hub_url, "Running with BuilderHub");
        let builder_hub = builderhub::Client::new(builder_hub_url);
        builder_hub.register(local_signer).await?;

        let peer_updater = PeersUpdater::new(
            peer_update_config,
            builder_hub,
            peers.clone(),
            task_executor.clone(),
        );

        task_executor
            .spawn_critical("run_update_peers", peer_updater.run(args.peer_update_interval_s));
    } else {
        tracing::warn!("No BuilderHub URL provided, running with local peer store");
        let local_peer_store = LOCAL_PEER_STORE.clone();

        let peer_store = local_peer_store
            .register(local_signer, Some(system_listener.local_addr().expect("bound").port()));

        let peers = peers.clone();
        let peer_updater =
            PeersUpdater::new(peer_update_config, peer_store, peers.clone(), task_executor.clone());
        task_executor
            .spawn_critical("local_update_peers", peer_updater.run(args.peer_update_interval_s));
    }

    // Configure the priority worker pool.
    let workers = PriorityWorkers::new_with_threads(args.compute_threads);

    // Spawn forwarders
    let forwarders = if let Some(ref builder_url) = config.local_builder_url {
        let local_sender = spawn_http_forwarder(
            String::from("local-builder"),
            builder_url.to_string(),
            // Use 1 client here, this is still using HTTP/1.1 with internal connection pooling.
            HttpClientPool::new(NonZero::new(1).unwrap(), || client.clone()),
            &task_executor,
        )?;

        IngressForwarders::new(local_sender, peers, orderflow_signer, workers.clone())
    } else {
        // No builder URL provided, so mock local forwarder.
        let (local_sender, _) = priority::channel::unbounded_channel();
        IngressForwarders::new(local_sender, peers, orderflow_signer, workers.clone())
    };

    let ingress = Arc::new(OrderflowIngress::new(config, workers, forwarders, indexer_handle));

    // Spawn a state maintenance task.
    let cancellation_token_clone = cancellation_token.clone();
    task_executor.spawn({
        let ingress = ingress.clone();
        async move {
            loop {
                info!("starting state maintenance!!");
                select! {
                    _ = cancellation_token_clone.cancelled() => {
                        info!("Cancellation token cancelled, stopping state maintenance");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(60)) => {
                        ingress.maintenance();
                    }
                }
            }
        }
    });

    tracing::info!(addr = ?system_listener.local_addr(), "starting system tcp listener");
    let ingress_socket =
        IngressSocket::new(system_listener, ingress.clone(), task_executor.clone());
    task_executor.spawn(ingress_socket.listen(cancellation_token));

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

    if let Some(builder_listener) = builder_listener {
        let builder_router = Router::new()
            .route("/", post(OrderflowIngress::builder_handler))
            .route("/health", get(|| async { Ok::<_, ()>(()) }))
            .with_state(ingress);
        let addr = builder_listener.local_addr()?;
        tracing::info!(target: "ingress", ?addr, "Starting builder server");

        tokio::try_join!(
            axum::serve(user_listener, user_router),
            axum::serve(builder_listener, builder_router)
        )?;
    } else {
        tokio::try_join!(axum::serve(user_listener, user_router))?;
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
