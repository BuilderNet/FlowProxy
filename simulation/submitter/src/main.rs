use std::{
    collections::VecDeque,
    path::PathBuf,
    time::{Duration, SystemTime},
};

use alloy_primitives::keccak256;
use alloy_signer::SignerSync;
use alloy_signer_local::PrivateKeySigner;
use arrow::array::*;
use clap::Parser;
use futures::TryStreamExt;
use parquet::arrow::ParquetRecordBatchStreamBuilder;

mod models;
#[path = "parquet.rs"]
mod parq;
use parq::*;

use models::BundleRow;
use rbuilder_primitives::serialize::RawBundle;
use reqwest::{
    Client, Method, Request, Url,
    header::{self, HeaderValue},
};
use serde_json::json;
use tokio::{sync::mpsc, task::JoinHandle};

#[derive(Debug, Parser)]
struct Args {
    /// The URL to submit bundles to.
    #[clap(long)]
    url: Url,
    /// The number of signers to use.
    #[clap(long, default_value = "1024")]
    num_signers: usize,
    /// The number of requests per second per signer.
    #[clap(long, default_value = "3")]
    rps: usize,
    /// The path to the Parquet bundle transcript.
    #[clap(long)]
    path: PathBuf,
    #[clap(long, default_value = "1.0")]
    scale: f64,
}

#[derive(Debug, Clone)]
struct BundleWithMetadata {
    bundle: RawBundle,
    timestamp: i64,
}

impl BundleWithMetadata {
    /// Normalize the timestamp to the current time with scaling.
    fn normalized(&self, offset: i64, scale: f64) -> i64 {
        let scaled_timestamp = (self.timestamp as f64 / scale) as i64;
        scaled_timestamp + offset
    }
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    let args = Args::parse();

    println!("Opening Parquet file {}...", args.path.display());
    let file = tokio::fs::File::open(args.path.clone()).await?;

    let builder = ParquetRecordBatchStreamBuilder::new(file).await?;

    let num_rows = builder.metadata().file_metadata().num_rows();
    println!("Number of rows: {}", num_rows);

    let mut stream = builder.with_batch_size(1024).build()?;

    let (tx, rx) = mpsc::channel(12);

    let replayer = BundleReplayer::new(rx, &args);
    let handle = replayer.spawn().await;

    while let Some(batch) = stream.try_next().await? {
        let mut bundles = Vec::with_capacity(batch.num_rows());
        // Convert each row in the batch to a BundleRow
        for row_idx in 0..batch.num_rows() {
            let bundle_row = convert_row_to_bundle(&batch, row_idx)?;
            let timestamp = bundle_row.time;
            let raw_bundle = RawBundle::from(bundle_row);

            // NOTE: We assume that the data is sorted by timestamp
            bundles.push(BundleWithMetadata { bundle: raw_bundle, timestamp });
        }

        tx.send(bundles).await?;
    }

    // Close the sender to signal completion
    drop(tx);

    // Wait for the replayer to finish processing
    handle.await?;

    Ok(())
}

struct BundleReplayer {
    /// The inbox channel that receives batched bundles.
    inbox: mpsc::Receiver<Vec<BundleWithMetadata>>,

    /// The queue of bundles to be processed.
    queue: VecDeque<BundleWithMetadata>,

    /// The offset between the current timestamp and the first timestamp in the batch.
    offset: Option<i64>,

    /// The replay speed scale factor (e.g., 2.0 for 2x speed).
    scale: f64,

    /// The ticker that will trigger the processing of the queue.
    ticker: tokio::time::Interval,

    /// Stats ticker to print stats every second.
    stats_ticker: tokio::time::Interval,
    /// Counter for the number of bundles processed in the last second.
    ctr: usize,

    submitter: Submitter,
}

fn unix_micros() -> i64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap().as_micros() as i64
}

impl BundleReplayer {
    pub fn new(inbox: mpsc::Receiver<Vec<BundleWithMetadata>>, args: &Args) -> Self {
        Self {
            inbox,
            offset: None,
            scale: args.scale,
            queue: VecDeque::with_capacity(1024),
            ticker: tokio::time::interval(Duration::from_micros(5)),
            stats_ticker: tokio::time::interval_at(
                tokio::time::Instant::now() + Duration::from_millis(1000),
                Duration::from_millis(1000),
            ),
            ctr: 0,
            submitter: Submitter::new(args.num_signers, args.rps, args.url.clone()),
        }
    }

    pub async fn spawn(mut self) -> JoinHandle<()> {
        tokio::spawn(async move {
            let mut done = false;
            loop {
                tokio::select! {
                    // Check if the next bundle should be processed with scaling
                    _ = self.ticker.tick() => {
                        if let Some(next) = self.queue.front() {
                            // Check if the next bundle should be processed
                            if let Some(offset) = self.offset {
                                if next.normalized(offset, self.scale) <= unix_micros() {
                                    let bundle = self.queue.pop_front().unwrap();
                                    self.on_bundle(bundle).await;
                                }
                            }
                        } else if done {
                            break;
                        }
                    }
                    // Print stats
                    _ = self.stats_ticker.tick() => {
                        println!("Stats: (bundle rate: {}/sec  queue size: {})", self.ctr, self.queue.len());
                        self.ctr = 0;
                    }
                    // Read buffered
                    result = self.inbox.recv(), if self.queue.len() <= 8192 => {
                        match result {
                            Some(bundles) => {
                                if self.offset.is_none() {
                                    // Apply scaling to the first timestamp
                                    let scaled_first_timestamp = (bundles[0].timestamp as f64 / self.scale) as i64;
                                    let offset = unix_micros() - scaled_first_timestamp;
                                    println!("Setting offset to {offset} => {:?} (scale: {}x)", Duration::from_micros(offset as u64), self.scale);
                                    self.offset = Some(offset);
                                }

                                self.queue.extend(bundles);
                            }
                            None => {
                                done = true;
                            }
                        }
                    }
                }
            }
        })
    }

    async fn on_bundle(&mut self, bundle: BundleWithMetadata) {
        self.submitter.submit(bundle.bundle);
        self.ctr += 1;
    }
}

struct Submitter {
    /// The signers to use.
    signers: Vec<PrivateKeySigner>,
    /// The index of the currently active signer.
    idx: usize,
    /// Requests per second per signer.
    rps: usize,
    /// The counter for the number of requests sent by the currently active signer.
    ctr: usize,

    sender: flume::Sender<Request>,
    url: Url,
}

impl Submitter {
    pub fn new(num_signers: usize, rps: usize, url: Url) -> Self {
        let signers = (0..num_signers).map(|_| PrivateKeySigner::random()).collect();
        // Use this as a work-stealing queue. Any item is received exactly once.
        let (tx, queue) = flume::unbounded();

        // Spawn the submitter HTTP clients
        let client = Client::new();

        for _ in 0..32 {
            let client = client.clone();
            let queue: flume::Receiver<Request> = queue.clone();
            tokio::spawn(async move {
                loop {
                    let Ok(request) = queue.recv_async().await else {
                        eprintln!("Worker shutting down");
                        break;
                    };

                    let response = client.execute(request).await.unwrap();
                    let status = response.status();
                    if !status.is_success() {
                        eprintln!("Failed to submit bundle: {}", response.text().await.unwrap());
                    }
                }
            });
        }

        Self { signers, idx: 0, rps, ctr: 0, sender: tx, url }
    }

    pub fn submit(&mut self, bundle: RawBundle) {
        let mut request = Request::new(Method::POST, self.url.clone());
        let body = json!({
            "id": 0,
            "jsonrpc": "2.0",
            "method": "eth_sendBundle",
            "params": [bundle],
        });

        let body = serde_json::to_vec(&body).unwrap();
        let sig_header = self.sign(&body).unwrap();

        request.body_mut().replace(body.into());
        let headers = request.headers_mut();
        headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/json"));
        headers.insert("x-flashbots-signature", sig_header);
        self.sender.send(request).unwrap();
    }

    fn sign(&mut self, body: &[u8]) -> eyre::Result<HeaderValue> {
        let sighash = format!("{:?}", keccak256(body));

        if self.ctr >= self.rps {
            self.idx = (self.idx + 1) % self.signers.len();
            self.ctr = 0;
        }

        let signer = &self.signers[self.idx];

        let signature = signer.sign_message_sync(sighash.as_bytes())?;
        let header = format!("{:?}:{}", signer.address(), signature);
        self.ctr += 1;

        Ok(HeaderValue::from_str(&header)?)
    }
}

fn convert_row_to_bundle(
    batch: &arrow::record_batch::RecordBatch,
    row_idx: usize,
) -> eyre::Result<BundleRow> {
    let schema = batch.schema();

    // Helper function to get column by name
    let get_column = |name: &str| -> eyre::Result<&dyn Array> {
        let col_idx = schema
            .fields()
            .iter()
            .position(|f| f.name() == name)
            .ok_or_else(|| eyre::eyre!("Column '{}' not found", name))?;
        Ok(batch.column(col_idx).as_ref())
    };

    // Extract data from each column
    // Note: We'll need to handle the problematic UTF-8 columns carefully

    // For now, let's create a minimal BundleRow with the fields we can safely extract
    let bundle_row = BundleRow {
        time: extract_i64(get_column("time")?, row_idx)?,
        block_number: extract_optional_u64(get_column("block_number")?, row_idx)?,
        min_timestamp: extract_optional_u64(get_column("min_timestamp")?, row_idx)?,
        max_timestamp: extract_optional_u64(get_column("max_timestamp")?, row_idx)?,

        // Extract transaction hashes (FixedSizeBinary(32) list)
        transactions_hash: extract_hash_list(get_column("transactions.hash")?, row_idx)?,

        // Extract from addresses (FixedSizeBinary(20) list)
        transactions_from: extract_address_list(get_column("transactions.from")?, row_idx)?,

        // Extract nonces (UInt64 list)
        transactions_nonce: extract_u64_list(get_column("transactions.nonce")?, row_idx)?,

        // Extract signature r values (FixedSizeBinary(32) list)
        transactions_r: extract_u256_list(get_column("transactions.r")?, row_idx)?,

        // Extract signature s values (FixedSizeBinary(32) list)
        transactions_s: extract_u256_list(get_column("transactions.s")?, row_idx)?,

        // Extract signature v values (UInt8 list)
        transactions_v: extract_u8_list(get_column("transactions.v")?, row_idx)?,

        // Extract to addresses (FixedSizeBinary(20) list, nullable)
        transactions_to: extract_optional_address_list(get_column("transactions.to")?, row_idx)?,

        // Extract gas values (UInt64 list)
        transactions_gas: extract_u64_list(get_column("transactions.gas")?, row_idx)?,

        // Extract transaction types (UInt8 list)
        transactions_type: extract_u8_list(get_column("transactions.type")?, row_idx)?,

        // Extract transaction input (Binary list)
        transactions_input: extract_binary_list(get_column("transactions.input")?, row_idx)?,

        // Extract transaction values (FixedSizeBinary(32) list)
        transactions_value: extract_u256_list(get_column("transactions.value")?, row_idx)?,

        // Extract gas prices (FixedSizeBinary(16) list, nullable)
        transactions_gas_price: extract_optional_u128_list(
            get_column("transactions.gasPrice")?,
            row_idx,
        )?,

        // Extract max fee per gas (FixedSizeBinary(16) list, nullable)
        transactions_max_fee_per_gas: extract_optional_u128_list(
            get_column("transactions.maxFeePerGas")?,
            row_idx,
        )?,

        // Extract max priority fee per gas (FixedSizeBinary(16) list, nullable)
        transactions_max_priority_fee_per_gas: extract_optional_u128_list(
            get_column("transactions.maxPriorityFeePerGas")?,
            row_idx,
        )?,

        // Extract access list and authorization list (Binary lists, nullable)
        transactions_access_list: extract_optional_binary_list(
            get_column("transactions.accessList")?,
            row_idx,
        )?,
        transactions_authorization_list: extract_optional_binary_list(
            get_column("transactions.authorizationList")?,
            row_idx,
        )?,

        // Extract hash lists
        reverting_tx_hashes: extract_hash_list(get_column("reverting_tx_hashes")?, row_idx)?,
        dropping_tx_hashes: extract_hash_list(get_column("dropping_tx_hashes")?, row_idx)?,
        refund_tx_hashes: extract_hash_list(get_column("refund_tx_hashes")?, row_idx)?,

        // Extract optional string fields (these should work)
        replacement_uuid: extract_optional_string(get_column("replacement_uuid")?, row_idx)?,

        // Extract optional numeric fields
        refund_percent: extract_optional_u8(get_column("refund_percent")?, row_idx)?,

        // Extract optional addresses
        refund_recipient: extract_optional_address(get_column("refund_recipient")?, row_idx)?,
        signer_address: extract_optional_address(get_column("signer_address")?, row_idx)?,
        refund_identity: extract_optional_address(get_column("refund_identity")?, row_idx)?,
    };

    Ok(bundle_row)
}
