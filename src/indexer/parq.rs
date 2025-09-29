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
    indexer::{BuilderName, OrderIndexerTasks, OrderReceivers, TRACING_TARGET},
    types::BundleReceipt,
};

/// The Arrow schema for bundle receipts.
static BUNDLE_RECEIPTS_PARQUET_SCHEMA: LazyLock<Schema> = LazyLock::new(|| {
    const NULLABLE: bool = true;
    Schema::new(vec![
        // The bundle hash.
        Field::new("bundle_hash", DataType::FixedSizeBinary(32), !NULLABLE),
        // The time the bundle has been sent at, as present in the JSON-RPC header.
        Field::new(
            "sent_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            NULLABLE,
        ),
        // The time the local operator has received the payload.
        Field::new(
            "received_at",
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            !NULLABLE,
        ),
        // This local operator.
        Field::new("dst_builder_name", DataType::Utf8, !NULLABLE),
        // The name of the operator which sent us the payload.
        Field::new("src_builder_name", DataType::Utf8, !NULLABLE),
        // The payload size. `UInt32` allows max 4GB size.
        Field::new("payload_size", DataType::UInt32, !NULLABLE),
        // The priority of the bundle.
        Field::new("priority", DataType::UInt8, !NULLABLE),
    ])
});

/// The abstraction over a [`parquet::arrow::ArrowWriter`] that allows appending bundle receipts,
/// buffering them into Arrow arrays and flushing them to Parquet files every few seconds.
struct BundleReceiptWriter {
    pub bundle_hash: FixedSizeBinaryBuilder,
    pub sent_at: TimestampMicrosecondBuilder,
    pub received_at: TimestampMicrosecondBuilder,
    pub dst_builder_name: StringBuilder,
    pub src_builder_name: StringBuilder,
    pub payload_size: UInt32Builder,
    pub priority: UInt8Builder,

    /// The inner Parquet writer that support Arrow datatypes.
    pub writer: ArrowWriter<File>,
    pub builder_name: BuilderName,
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

            writer,
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
        self.src_builder_name.append_value(receipt.src_builder_name.clone());
        self.payload_size.append_value(receipt.payload_size);
        self.priority.append_value(receipt.priority as u8);
    }

    /// Flush the internal Arrow buffer to the Parquet file.
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
        self.writer.write(&record_batch)?;
        self.writer.flush()?;

        Ok(())
    }

    /// Close the Parquet writer, flushing any remaining data.
    fn close(mut self) -> ArrowResult<()> {
        self.flush()?;
        self.writer.close()?;
        Ok(())
    }
}

/// A namespace struct for spawning a Parquet indexer.
pub(crate) struct ParquetIndexer;

impl ParquetIndexer {
    pub(crate) fn spawn(
        parquet_args: ParquetArgs,
        builder_name: BuilderName,
        receivers: OrderReceivers,
    ) -> io::Result<OrderIndexerTasks> {
        let OrderReceivers { mut bundle_rx, bundle_receipt_rx, mut transaction_rx } = receivers;

        let parquet_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(parquet_args.bundle_receipts_file_path)?;

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

        let bundle_indexer_task =
            tokio::task::spawn(async move { while let Some(_b) = bundle_rx.recv().await {} });
        let bundle_receipt_indexer_task =
            tokio::spawn(run_indexer(bundle_receipt_rx, receipts_writer));
        let transaction_indexer_task =
            tokio::task::spawn(async move { while let Some(_t) = transaction_rx.recv().await {} });

        let tasks = OrderIndexerTasks {
            bundle_indexer_task,
            bundle_receipt_indexer_task,
            transaction_indexer_task,
        };

        Ok(tasks)
    }
}

/// Run the indexer of the specified type until the receiving channel is closed.
async fn run_indexer(
    mut rx: mpsc::Receiver<BundleReceipt>,
    mut receipt_writer: BundleReceiptWriter,
) {
    let start = Instant::now();
    let mut interval = tokio::time::interval_at(start, Duration::from_secs(4));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    // Either append or flush if the interval ticks.
    loop {
        tokio::select! {
            maybe_receipt = rx.recv() => {
                let Some(receipt) = maybe_receipt else {
                    break;
                };

                tracing::trace!(target: TRACING_TARGET, hash = %receipt.bundle_hash, "Received bundle receipt to index");
                receipt_writer.append(receipt);
            },

            _ = interval.tick() => {
                tracing::debug!(target: TRACING_TARGET, "Flushing Parquet writer");

                if let Err(e) = receipt_writer.flush() {
                    tracing::error!(target: TRACING_TARGET, ?e, "Failed to flush Parquet writer");
                }
            }

        }
    }

    tracing::error!(target: TRACING_TARGET, "Bundle receipt channel closed, shutting down Parquet indexer");
    if let Err(e) = receipt_writer.close() {
        tracing::error!(target: TRACING_TARGET, ?e, "Failed to close Parquet writer");
    }
}

#[cfg(test)]
mod tests {
    use alloy_primitives::B256;
    use time::UtcDateTime;

    use crate::{
        cli::ParquetArgs,
        indexer::{self, parq::ParquetIndexer},
        priority::Priority,
        types::BundleReceipt,
    };

    use std::sync::Arc;
    use std::{fs::File, io};

    use arrow::record_batch::RecordBatch;
    use parquet::file::reader::SerializedFileReader;

    /// Read all BundleReceipts from a parquet file.
    // pub fn read_bundle_receipts(path: &str) -> io::Result<Vec<BundleReceipt>> {
    //     let file = File::open(path)?;
    //     let parquet_reader = SerializedFileReader::new(file)?;
    //     let mut arrow_reader = ArrowReader::new(Arc::new(parquet_reader));
    //
    //     let mut receipts = Vec::new();
    //
    //     // Batch size = how many rows you pull at once
    //     let record_batch_reader = arrow_reader.get_record_reader(1024)?;
    //     for batch in record_batch_reader {
    //         let batch = batch?;
    //         receipts.extend(from_record_batch(&batch));
    //     }
    //
    //     Ok(receipts)
    // }

    /// Convert an Arrow RecordBatch into BundleReceipt structs.
    // fn from_record_batch(batch: &RecordBatch) -> Vec<BundleReceipt> {
    //     let mut out = Vec::with_capacity(batch.num_rows());
    //
    //     let bundle_hash = as_fixed_size_binary_array(batch.column(0));
    //     let sent_at = as_timestamp_microsecond_array(batch.column(1));
    //     let received_at = as_timestamp_microsecond_array(batch.column(2));
    //     let dst_builder_name = as_string_array(batch.column(3));
    //     let src_builder_name = as_string_array(batch.column(4));
    //     let payload_size = as_uint32_array(batch.column(5));
    //     let priority = as_uint8_array(batch.column(6));
    //
    //     for i in 0..batch.num_rows() {
    //         let bundle_hash_bytes = bundle_hash.value(i);
    //         let mut fixed = [0u8; 32];
    //         fixed.copy_from_slice(bundle_hash_bytes);
    //         let bundle_hash = B256::from(fixed);
    //
    //         let sent_at = if sent_at.is_null(i) {
    //             None
    //         } else {
    //             let micros = sent_at.value(i);
    //             Some(UtcDateTime::from_unix_timestamp_nanos(micros * 1_000).unwrap())
    //         };
    //
    //         let received_at = {
    //             let micros = received_at.value(i);
    //             UtcDateTime::from_unix_timestamp_nanos(micros * 1_000).unwrap()
    //         };
    //
    //         let src_builder_name = src_builder_name.value(i).to_string();
    //
    //         let payload_size = payload_size.value(i);
    //         let priority = match priority.value(i) {
    //             0 => Priority::Low,
    //             1 => Priority::Medium,
    //             2 => Priority::High,
    //             x => panic!("unsupport priority level: {x}"),
    //         };
    //
    //         out.push(BundleReceipt {
    //             bundle_hash,
    //             sent_at,
    //             received_at,
    //             src_builder_name,
    //             payload_size,
    //             priority,
    //         });
    //     }
    //
    //     out
    // }

    #[test]
    pub fn asdf() {
        let parquet_tempfile = tempfile::Builder::new().suffix(".parquet").tempfile().unwrap();

        let args = ParquetArgs { bundle_receipts_file_path: parquet_tempfile.path().to_path_buf() };

        let example_bundle_receipt = BundleReceipt {
            bundle_hash: B256::ZERO,
            sent_at: Some(UtcDateTime::now()),
            received_at: UtcDateTime::now(),
            src_builder_name: "buildernet_src".to_string(),
            payload_size: 0,
            priority: Priority::High,
        };

        let (senders, receivers) = indexer::OrderSenders::new();
        let _ = ParquetIndexer::spawn(args, "buildernet_dst".to_string(), receivers).unwrap();

        senders.bundle_receipt_tx.try_send(example_bundle_receipt).unwrap();
    }
}
