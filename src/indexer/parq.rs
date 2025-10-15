//! Indexing functionality powered by Parquet files and Apache Arrow.
//!
//! Incoming orders are buffered into Arrow and then flushed every few seconds into a Parquet file.

use arrow::{
    array::{
        ArrayBuilder, FixedSizeBinaryBuilder, RecordBatch, StringBuilder,
        TimestampMicrosecondBuilder, UInt32Builder, UInt8Builder,
    },
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::Result as ArrowResult,
};
use parquet::{arrow::ArrowWriter, file::properties::WriterPropertiesBuilder};
use tokio::{sync::mpsc, time::Instant};

use std::{
    fs::{File, OpenOptions},
    io::{self},
    sync::{Arc, LazyLock},
    time::Duration,
};

use crate::{
    cli::ParquetArgs,
    indexer::{BuilderName, OrderReceivers, TARGET},
    metrics::IndexerMetrics,
    primitives::{BundleReceipt, Sampler},
    tasks::TaskExecutor,
};

/// The Arrow schema for bundle receipts.
static BUNDLE_RECEIPTS_PARQUET_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    const NULLABLE: bool = true;
    Schema::new(vec![
        // The bundle hash.
        Field::new("bundle_hash", DataType::FixedSizeBinary(32), !NULLABLE),
        // The time the bundle has been sent at, as present in the JSON-RPC header.
        Field::new("sent_at", DataType::Timestamp(TimeUnit::Microsecond, None), NULLABLE),
        // The time the local operator has received the payload.
        Field::new("received_at", DataType::Timestamp(TimeUnit::Microsecond, None), !NULLABLE),
        // This local operator.
        Field::new("dst_builder_name", DataType::Utf8, !NULLABLE),
        // The name of the operator which sent us the payload.
        Field::new("src_builder_name", DataType::Utf8, NULLABLE),
        // The payload size. `UInt32` allows max 4GB size.
        Field::new("payload_size", DataType::UInt32, !NULLABLE),
        // The priority of the bundle.
        Field::new("priority", DataType::UInt8, !NULLABLE),
    ])
});

/// The abstraction over a [`parquet::arrow::ArrowWriter`] that allows appending bundle receipts,
/// buffering them into Arrow arrays and flushing them to Parquet files every few seconds.
///
/// It ensures that when dropped, the writer is flushed and closed.
struct BundleReceiptWriter {
    pub bundle_hash: FixedSizeBinaryBuilder,
    pub sent_at: TimestampMicrosecondBuilder,
    pub received_at: TimestampMicrosecondBuilder,
    pub dst_builder_name: StringBuilder,
    pub src_builder_name: StringBuilder,
    pub payload_size: UInt32Builder,
    pub priority: UInt8Builder,

    pub builder_name: BuilderName,

    /// The inner Parquet Arrow writer. It is wrapped in an `Option` to allow taking it in the
    /// `Drop` implementation. It is guaranteed to be `Some` during the lifetime of the struct.
    pub writer: Option<ArrowWriter<File>>,
}

/// Ensure that when dropped, the writer is flushed and closed.
impl Drop for BundleReceiptWriter {
    fn drop(&mut self) {
        if let Err(e) = self.flush() {
            eprintln!("failed to flush bundle receipt writer while being dropped: {e}");
        }

        let writer = std::mem::take(&mut self.writer).expect("some");
        if let Err(e) = writer.close() {
            eprintln!("failed to close bundle arrow writer while being dropped: {e}");
        }
    }
}

impl BundleReceiptWriter {
    fn new(writer: ArrowWriter<File>, builder_name: BuilderName) -> Self {
        BundleReceiptWriter {
            bundle_hash: FixedSizeBinaryBuilder::new(32),
            sent_at: TimestampMicrosecondBuilder::new(),
            received_at: TimestampMicrosecondBuilder::new(),
            dst_builder_name: StringBuilder::new(),
            src_builder_name: StringBuilder::new(),
            payload_size: UInt32Builder::new(),
            priority: UInt8Builder::new(),

            writer: writer.into(),
            builder_name,
        }
    }

    /// Append a new bundle receipt to the internal Arrow buffer.
    fn append(&mut self, receipt: BundleReceipt) {
        self.bundle_hash.append_value(receipt.bundle_hash).expect("bundle hash is always 32 bytes");
        if let Some(sent_at) = receipt.sent_at {
            self.sent_at.append_value((sent_at.unix_timestamp_nanos() / 1_000) as i64);
        } else {
            self.sent_at.append_null();
        }
        self.received_at.append_value((receipt.received_at.unix_timestamp_nanos() / 1_000) as i64);
        self.dst_builder_name.append_value(self.builder_name.clone());
        self.src_builder_name.append_value(receipt.src_builder_name);
        self.payload_size.append_value(receipt.payload_size);
        self.priority.append_value(receipt.priority as u8);
    }

    /// Flush the internal Arrow buffer to the Parquet file.
    ///
    /// NOTE: if it fails, data is lost, as the internal buffers are cleared.
    fn flush(&mut self) -> ArrowResult<()> {
        if self.bundle_hash.len() == 0 {
            return Ok(());
        }

        let bundle_hash = self.bundle_hash.finish();
        let sent_at = self.sent_at.finish();
        let received_at = self.received_at.finish();
        let dst_builder_name = self.dst_builder_name.finish();
        let src_builder_name = self.src_builder_name.finish();
        let payload_size = self.payload_size.finish();
        let priority = self.priority.finish();

        let record_batch = RecordBatch::try_new(
            Arc::new(BUNDLE_RECEIPTS_PARQUET_SCHEMA.clone()),
            vec![
                Arc::new(bundle_hash),
                Arc::new(sent_at),
                Arc::new(received_at),
                Arc::new(dst_builder_name),
                Arc::new(src_builder_name),
                Arc::new(payload_size),
                Arc::new(priority),
            ],
        )?;

        // Write and flush to Parquet file immediately.
        let writer = self.writer.as_mut().expect("not dropped");
        writer.write(&record_batch)?;
        writer.flush()?;

        tracing::debug!(target: TARGET, rows = record_batch.num_rows(), "Flushed bundle receipt writer to disk");

        Ok(())
    }
}

/// A namespace struct for spawning a Parquet indexer.
pub(crate) struct ParquetIndexer;

impl ParquetIndexer {
    pub(crate) fn run(
        parquet_args: ParquetArgs,
        builder_name: BuilderName,
        receivers: OrderReceivers,
        task_executor: TaskExecutor,
    ) -> io::Result<()> {
        let OrderReceivers { mut bundle_rx, bundle_receipt_rx } = receivers;

        let parquet_file = OpenOptions::new().create(true).append(true).open(
            parquet_args.bundle_receipts_file_path.expect("bundle receipts file path is set"),
        )?;

        let writer_properties = WriterPropertiesBuilder::default()
            .set_max_row_group_size(100_000)
            .set_data_page_size_limit(1024 * 1024)
            .set_dictionary_enabled(true)
            .build();

        let writer = ArrowWriter::try_new(
            parquet_file,
            Arc::new(BUNDLE_RECEIPTS_PARQUET_SCHEMA.clone()),
            Some(writer_properties),
        )?;

        let receipts_writer = BundleReceiptWriter::new(writer, builder_name.clone());
        let mut runner = ParquetRunner { rx: bundle_receipt_rx, receipt_writer: receipts_writer };

        task_executor.spawn(async move { while let Some(_b) = bundle_rx.recv().await {} });
        task_executor.spawn_with_graceful_shutdown_signal(|mut shutdown| async move {
            tokio::select! {
                _ = runner.run_loop() => {
                    // runner finished (channel closed, etc.)
                    tracing::info!(target: TARGET, "Runner exited");
                }
                guard = &mut shutdown => {
                    tracing::info!(target: TARGET, "Received shutdown, performing cleanup");
                    drop(runner);
                    drop(guard);
                    return;
                }
            }

            // If we exit the loop (runner finished first), still wait for shutdown
            let guard = shutdown.await;
            drop(guard);
        });

        Ok(())
    }
}

struct ParquetRunner {
    rx: mpsc::Receiver<BundleReceipt>,
    receipt_writer: BundleReceiptWriter,
}

impl ParquetRunner {
    async fn run_loop(&mut self) {
        let mut sampler = Sampler::default()
            .with_sample_size(self.rx.capacity() / 2)
            .with_interval(Duration::from_secs(4));

        let start = Instant::now();
        let mut interval = tokio::time::interval_at(start, Duration::from_secs(4));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Either append or flush if the interval ticks.
        loop {
            tokio::select! {
                maybe_receipt = self.rx.recv() => {
                    sampler.sample(|| {
                        IndexerMetrics::set_clickhouse_queue_size(self.rx.len(), "bundle_receipt");
                    });

                    let Some(receipt) = maybe_receipt else {
                        tracing::error!(target: TARGET, "Bundle receipt channel closed, shutting down Parquet indexer");
                        break;
                    };

                    tracing::trace!(target: TARGET, hash = %receipt.bundle_hash, "Received bundle receipt to index");
                    self.receipt_writer.append(receipt);
                },

                _ = interval.tick() => {
                    tracing::debug!(target: TARGET, "Flushing Parquet writer");

                    if let Err(e) = self.receipt_writer.flush() {
                        tracing::error!(target: TARGET, ?e, "Failed to flush Parquet writer");
                    }
                },

            }
        }
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::{Address, B256, U32};
    use time::UtcDateTime;

    // Uncomment to enable logging during tests.
    // use tracing::level_filters::LevelFilter;
    // use tracing_subscriber::{layer::SubscriberExt as _, util::SubscriberInitExt as _, EnvFilter};

    use crate::{
        cli::ParquetArgs,
        indexer::{self, parq::ParquetIndexer},
        primitives::BundleReceipt,
        priority::Priority,
        tasks::TaskManager,
        utils::testutils::Random,
    };

    use std::{fs::File, io, time::Duration};

    use arrow::{
        array::{
            Array as _, FixedSizeBinaryArray, StringArray, TimestampMicrosecondArray, UInt32Array,
            UInt8Array,
        },
        record_batch::RecordBatch,
    };
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    impl Random for BundleReceipt {
        fn random<R: rand::Rng>(rng: &mut R) -> Self {
            // Ensure microsecond precision only.
            let mut sent_at = UtcDateTime::now();
            let micros = sent_at.microsecond();
            sent_at = sent_at.replace_microsecond(micros).unwrap();

            let mut received_at = UtcDateTime::now();
            let micros = received_at.microsecond();
            received_at = received_at.replace_microsecond(micros).unwrap();

            Self {
                bundle_hash: B256::random_with(rng),
                sent_at: Some(sent_at),
                received_at,
                src_builder_name: Address::random_with(rng).to_string(),
                dst_builder_name: Some(Address::random_with(rng).to_string()),
                payload_size: U32::random_with(rng).to(),
                priority: Priority::Medium,
            }
        }
    }

    /// Read all BundleReceipts from a parquet file.
    fn read_bundle_receipts(path: &str) -> io::Result<Vec<BundleReceipt>> {
        let file = File::open(path)?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file).unwrap();
        let reader = builder.build().unwrap();

        let receipts = reader.flat_map(|b| from_record_batch(&b.unwrap())).collect::<Vec<_>>();

        Ok(receipts)
    }

    /// Convert an Arrow RecordBatch into BundleReceipt structs.
    fn from_record_batch(batch: &RecordBatch) -> Vec<BundleReceipt> {
        let mut out = Vec::with_capacity(batch.num_rows());

        let bundle_hash: FixedSizeBinaryArray = batch.column(0).to_data().into();
        let sent_at: TimestampMicrosecondArray = batch.column(1).to_data().into();
        let received_at: TimestampMicrosecondArray = batch.column(2).to_data().into();
        let dst_builder_name: StringArray = batch.column(3).to_data().into();
        let src_builder_name: StringArray = batch.column(4).to_data().into();
        let payload_size: UInt32Array = batch.column(5).to_data().into();
        let priority: UInt8Array = batch.column(6).to_data().into();

        for i in 0..batch.num_rows() {
            let bundle_hash_bytes = bundle_hash.value(i);
            let mut fixed = [0u8; 32];
            fixed.copy_from_slice(bundle_hash_bytes);
            let bundle_hash = B256::from(fixed);

            let sent_at = if sent_at.is_null(i) {
                None
            } else {
                let micros = sent_at.value(i);
                Some(UtcDateTime::from_unix_timestamp_nanos((micros * 1_000) as i128).unwrap())
            };

            let received_at = {
                let micros = received_at.value(i);
                UtcDateTime::from_unix_timestamp_nanos((micros * 1_000) as i128).unwrap()
            };

            let src_builder_name = src_builder_name.value(i).to_string();
            let dst_builder_name = dst_builder_name.value(i).to_string();

            let payload_size = payload_size.value(i);
            let priority = match priority.value(i) {
                0 => Priority::High,
                1 => Priority::Medium,
                2 => Priority::Low,
                x => panic!("unsupport priority level: {x}"),
            };

            out.push(BundleReceipt {
                bundle_hash,
                sent_at,
                received_at,
                src_builder_name,
                dst_builder_name: Some(dst_builder_name),
                payload_size,
                priority,
            });
        }

        out
    }

    /// An E2E of the parquet indexer, which spins up the indexer, sends it some bundle receipts,
    /// and then reads back the parquet file to ensure the data is correct.
    #[test]
    fn indexer_parquet_file_works() {
        let rt = tokio::runtime::Builder::new_multi_thread().enable_all().build().unwrap();

        // Uncomment to toggle logs.
        // let registry = tracing_subscriber::registry().with(
        //     EnvFilter::builder().with_default_directive(LevelFilter::DEBUG.into()).
        // from_env_lossy(), );
        // let _ = registry.with(tracing_subscriber::fmt::layer()).try_init();

        // 1. --- Setup.

        let parquet_tempfile = tempfile::Builder::new().suffix(".parquet").tempfile().unwrap();
        let path = parquet_tempfile.path().to_path_buf();

        tracing::debug!(?path, "Created tempfile");

        let args = ParquetArgs { bundle_receipts_file_path: Some(path.clone()) };

        let (senders, receivers) = indexer::OrderSenders::new();

        let task_manager = TaskManager::new(rt.handle().clone());
        let task_executor = task_manager.executor();
        ParquetIndexer::run(args, "buildernet_dst".to_string(), receivers, task_executor).unwrap();

        // 2. --- Spam.

        let mut rng = rand::rng();
        let example_bundle_receipts = (0..8192)
            .map(|_| {
                let mut r = BundleReceipt::random(&mut rng);
                r.dst_builder_name = Some("buildernet_dst".to_string());
                r
            })
            .collect::<Vec<_>>();

        rt.block_on(async {
            for r in &example_bundle_receipts {
                senders.bundle_receipt_tx.try_send(r.clone()).unwrap();
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        });

        // Cancel the indexer task.
        assert!(
            task_manager.graceful_shutdown_with_timeout(Duration::from_secs(5)),
            "shutdown hit timeout"
        );

        // 3. --- Check.

        let receipts = read_bundle_receipts(path.to_str().unwrap()).unwrap();

        // Less noisy errors.
        let equal = example_bundle_receipts == receipts;
        if !equal {
            assert_eq!(example_bundle_receipts.len(), receipts.len());
            for (expected, got) in example_bundle_receipts.iter().zip(&receipts) {
                assert_eq!(expected, got, "expected != got");
            }
        }

        // Ensure parquet tempfile is dropped at the very end of the test and not before.
        drop(parquet_tempfile);
    }
}
