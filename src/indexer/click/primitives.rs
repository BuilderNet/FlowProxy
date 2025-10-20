use alloy_primitives::B256;
use clickhouse::{Row, RowWrite};
use serde::{de::DeserializeOwned, Serialize};

use crate::{
    indexer::click::models::{BundleReceiptRow, BundleRow},
    primitives::{BundleReceipt, SystemBundle},
};

pub(crate) trait ClickhouseRowExt:
    Row + RowWrite + Serialize + DeserializeOwned + Sync + Send + 'static
{
    /// The type of such row, e.g. "bundles" or "transactions". For informational purposes.
    const ORDER: &'static str;

    /// An identifier of such row.
    fn hash(&self) -> B256;

    /// Internal function that takes the inner row types and extracts the reference needed for
    /// Clickhouse inserter functions like `Inserter::write`. While a default implementation is not
    /// provided, it should suffice to simply return `row`.
    fn to_row_ref(row: &Self) -> &<Self as Row>::Value<'_>;
}

impl ClickhouseRowExt for BundleRow {
    const ORDER: &'static str = "bundle";

    fn hash(&self) -> B256 {
        self.hash
    }

    fn to_row_ref(row: &Self) -> &<Self as Row>::Value<'_> {
        row
    }
}

impl ClickhouseRowExt for BundleReceiptRow {
    const ORDER: &'static str = "bundle_receipt";

    fn hash(&self) -> B256 {
        self.bundle_hash
    }

    fn to_row_ref(row: &Self) -> &<Self as Row>::Value<'_> {
        row
    }
}

/// An high-level order type that can be indexed in clickhouse.
pub(crate) trait ClickhouseIndexableOrder: Sized {
    /// The associated inner row type that can be serialized into Clickhouse data.
    type ClickhouseRowType: ClickhouseRowExt;

    /// The type of such order, e.g. "bundles" or "transactions". For informational purposes.
    const ORDER: &'static str;

    /// An identifier of such order.
    fn hash(&self) -> B256;

    /// Converts such order into the associated Clickhouse row type.
    fn to_row(self, builder_name: String) -> Self::ClickhouseRowType;
}

impl ClickhouseIndexableOrder for SystemBundle {
    type ClickhouseRowType = BundleRow;

    const ORDER: &'static str = <BundleRow as ClickhouseRowExt>::ORDER;

    fn hash(&self) -> B256 {
        self.bundle_hash
    }

    fn to_row(self, builder_name: String) -> Self::ClickhouseRowType {
        (self, builder_name).into()
    }
}

impl ClickhouseIndexableOrder for BundleReceipt {
    type ClickhouseRowType = BundleReceiptRow;

    const ORDER: &'static str = <BundleReceiptRow as ClickhouseRowExt>::ORDER;

    fn hash(&self) -> B256 {
        self.bundle_hash
    }

    fn to_row(self, builder_name: String) -> Self::ClickhouseRowType {
        (self, builder_name).into()
    }
}
