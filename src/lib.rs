//! Orderflow ingress for BuilderNet.

use crate::{
    builderhub::{
        InstanceData, Peer, PeerCredentials, PeerStore, PeersUpdater, PeersUpdaterConfig,
    },
    cache::SignerCache,
    forwarder::{
        client::{HttpClientPool, default_http_builder},
        http::spawn_http_forwarder,
    },
    indexer::IndexerHandle,
    ingress::IngressSocket,
    metrics::IngressMetrics,
    primitives::{AcceptorBuilder, SslAcceptorBuilderExt, SystemBundleDecoder},
    priority::workers::PriorityWorkers,
    statics::LOCAL_PEER_STORE,
};
use alloy_signer_local::PrivateKeySigner;
use axum::{
    Router,
    extract::{DefaultBodyLimit, Request, State},
    middleware::Next,
    response::Response,
    routing::{get, post},
};
use dashmap::DashMap;
use entity::SpamThresholds;
use forwarder::{IngressForwarders, PeerHandle};
use msg_socket::RepSocket;
use msg_transport::tcp_tls::{self, TcpTls};
use prometric::exporter::ExporterBuilder;
use rbuilder_utils::tasks::TaskExecutor;
use reqwest::Url;
use std::{
    fs,
    num::NonZero,
    str::FromStr as _,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::{net::TcpListener, select};
use tokio_util::sync::CancellationToken;
use tracing::{info, level_filters::LevelFilter};
use tracing_subscriber::{EnvFilter, layer::SubscriberExt as _, util::SubscriberInitExt};

pub mod cli;
use cli::OrderflowIngressArgs;

pub mod ingress;
use ingress::OrderflowIngress;

use crate::cache::OrderCache;

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

    if let Some(ref metrics_addr) = args.metrics {
        ExporterBuilder::new().with_address(metrics_addr).with_namespace("flowproxy").install()?;
        metrics::spawn_process_collector().await?;

        // Set build info metric
        metrics::BUILD_INFO_METRICS.info(env!("CARGO_PKG_VERSION"), env!("GIT_HASH")).set(1);
    }

    let user_listener = TcpListener::bind(&args.user_listen_addr).await?;

    let builder_listener = if let Some(ref builder_listen_url) = args.builder_listen_addr {
        Some(TcpListener::bind(builder_listen_url).await?)
    } else {
        None
    };
    run_with_listeners(
        args,
        user_listener,
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
        certificate_pem_file: args.client_certificate_pem_file,
    };

    let acceptor_builder =
        Arc::new(AcceptorBuilder::new(fs::read(&args.server_certificate_pem_file)?));

    let (system_listener, certs_rx) = if let Some(url) = args.builder_hub_url {
        tracing::debug!(?url, "running with builderhub");
        let builder_hub = builderhub::Client::new(url);

        let peer_list = builder_hub.get_peers().await?;
        let certs = peer_list
            .iter()
            .filter_map(|p| p.openssl_tls_certificate().and_then(Result::ok))
            .collect::<Vec<_>>();

        let acceptor = acceptor_builder.ssl()?.add_trusted_certs(certs)?.build();
        let tls = tcp_tls::Server::new(acceptor.into());
        let mut socket = RepSocket::new(TcpTls::Server(tls));
        socket.bind(args.system_listen_addr).await.expect("to bind system listener");

        builder_hub.register(local_signer).await?;

        let (peer_updater, certs_rx) = PeersUpdater::new(
            peer_update_config,
            builder_hub,
            peers.clone(),
            task_executor.clone(),
        );

        task_executor
            .spawn_critical("run_update_peers", peer_updater.run(args.peer_update_interval_s));
        (socket, certs_rx)
    } else {
        let peer_store = LOCAL_PEER_STORE.clone();

        let peer_list = peer_store.get_peers().await?;
        let certs = peer_list
            .iter()
            .filter_map(|p| p.openssl_tls_certificate().and_then(Result::ok))
            .collect::<Vec<_>>();

        let peer_cert =
            fs::read_to_string(&peer_update_config.certificate_pem_file).unwrap_or_default();

        let acceptor = acceptor_builder.ssl()?.add_trusted_certs(certs)?.build();
        let tls = tcp_tls::Server::new(acceptor.into());
        let mut socket = RepSocket::new(TcpTls::Server(tls));
        socket.bind(args.system_listen_addr).await.expect("to bind system listener");
        let local = socket.local_addr().unwrap();

        let peer = Peer {
            name: local_signer.to_string(),
            orderflow_proxy: PeerCredentials {
                ecdsa_pubkey_address: local_signer,
                ..Default::default()
            },
            ip: format!("{}:{}", local.ip(), local.port()),
            dns_name: "localhost".to_string(),
            instance: InstanceData { tls_cert: peer_cert },
        };
        let peer_store = peer_store.register(peer);

        let (peer_updater, certs_rx) =
            PeersUpdater::new(peer_update_config, peer_store, peers.clone(), task_executor.clone());

        task_executor
            .spawn_critical("run_update_peers", peer_updater.run(args.peer_update_interval_s));
        (socket, certs_rx)
    };

    // Configure the priority worker pool.
    let workers = PriorityWorkers::new_with_threads(args.compute_threads);

    // Spawn forwarders
    let builder_url = args.builder_url.map(|url| Url::from_str(&url)).transpose()?;
    let forwarders = if let Some(ref builder_url) = builder_url {
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
        disk_max_size_to_accept_user_rpc: args.disk_max_size_to_accept_user_rpc_mb * 1024 * 1024,
        user_metrics: IngressMetrics::builder().with_label("handler", "user").build(),
        system_metrics: IngressMetrics::builder().with_label("handler", "system").build(),
    });

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
    let ingress_socket = IngressSocket::new(
        system_listener,
        certs_rx,
        ingress.clone(),
        acceptor_builder,
        task_executor.clone(),
    );
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
