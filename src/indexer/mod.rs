//! Clickhouse indexer of bundles.

use std::time::Duration;

use clickhouse::{inserter::Inserter, Client as ClickhouseClient};
use time::UtcDateTime;
use tokio::{sync::mpsc, task::JoinHandle};
use tracing::info;

use crate::{cli::ClickhouseArgs, indexer::models::BundleRow, types::SystemBundle};

mod models;

pub const BUNDLE_INDEXER_BUFFER_SIZE: usize = 4096;
pub const BUNDLE_TABLE_NAME: &str = "bundles";
const TRACING_TARGET: &str = "indexer";

pub trait BundleIndexer: Sync + Send {
    fn index_bundle(&self, bundle: SystemBundle, received_at: UtcDateTime);
}

#[derive(Debug)]
pub struct ClickhouseIndexerHandle {
    bundle_tx: mpsc::Sender<(SystemBundle, UtcDateTime)>,
}

struct ClickhouseIndexer {
    #[allow(dead_code)]
    pub client: ClickhouseClient,
    pub bundle_rx: mpsc::Receiver<(SystemBundle, UtcDateTime)>,
    pub bundle_inserter: Inserter<BundleRow>,
}

impl ClickhouseIndexer {
    async fn run(mut self) {
        while let Some((bundle, received_at)) = self.bundle_rx.recv().await {
            let bundle_row = (bundle, received_at).into();

            if let Err(e) = self.bundle_inserter.write(&bundle_row) {
                tracing::error!(target: TRACING_TARGET,
                    ?e,
                    bundle_hash = bundle_row.hash,
                    "failed to write bundle to clickhouse inserter"
                )
            }

            // TODO: current clickhouse code doesn't let me know if this calls `force_commit` or
            // not. It kinda sucks. I should fork it and make a PR eventually.
            //
            // TODO: implement a file-based backup in case this call fails due to connection
            // timeouts or whatever.
            let _ = self.bundle_inserter.commit().await;
        }

        tracing::error!(target: TRACING_TARGET, "bundle tx channel closed, indexer will stop running");
        if let Err(e) = self.bundle_inserter.end().await {
            tracing::error!(target: TRACING_TARGET, ?e, "failed to write end insertion to indexer");
        }
    }
}

impl BundleIndexer for ClickhouseIndexerHandle {
    fn index_bundle(&self, bundle: SystemBundle, received_at: UtcDateTime) {
        if let Err(e) = self.bundle_tx.try_send((bundle, received_at)) {
            tracing::error!(?e, "failed to send bundle to index");
        }
    }
}

#[derive(Debug)]
pub struct MockIndexer {
    pub bundle_rx: mpsc::Receiver<(SystemBundle, UtcDateTime)>,
}

pub fn spawn_indexer(args: Option<ClickhouseArgs>) -> (ClickhouseIndexerHandle, JoinHandle<()>) {
    let (tx, rx) = mpsc::channel(BUNDLE_INDEXER_BUFFER_SIZE);
    let handle = ClickhouseIndexerHandle { bundle_tx: tx };

    let Some(args) = args else {
        info!("Running with mocked indexer");
        let mut indexer = MockIndexer { bundle_rx: rx };
        let task_handle =
            tokio::task::spawn(
                async move { while let Some(_b) = indexer.bundle_rx.recv().await {} },
            );
        return (handle, task_handle);
    };

    info!(host = %args.host, "Running with clickhouse indexer");

    let client = ClickhouseClient::default()
        .with_url(args.host)
        .with_database(args.database)
        .with_user(args.username)
        .with_password(args.password);

    // TODO: configure the inserter, example: <https://github.com/ClickHouse/clickhouse-rs/blob/2e57c2602eb24c032964405ce58687a5f4147d81/examples/inserter.rs#L43-L59>
    let bundle_inserter = client
        .inserter::<BundleRow>(BUNDLE_TABLE_NAME)
        .expect("in 0.13.3, this never returns Err")
        .with_period(Some(Duration::from_secs(4))) // Dump every 4s
        .with_period_bias(0.1); // 12±(0.1*12)

    let indexer = ClickhouseIndexer { bundle_rx: rx, client, bundle_inserter };
    let task_handle = tokio::spawn(indexer.run());

    (handle, task_handle)
}
