use alloy_primitives::B256;
use clickhouse::{Row, RowWrite};
use serde::Serialize;

use crate::{
    indexer::{models::BundleRow, BuilderName},
    types::SystemBundle,
};

/// An high-level order type that can be indexed in clickhouse.
pub(crate) trait ClickhouseIndexableOrder: Sized {
    /// The associated inner row type that can be serialized into Clickhouse data.
    type ClickhouseRowType: Row + RowWrite + Serialize + From<(Self, BuilderName)>;

    /// The type of such order, e.g. "bundles" or "transactions". For informational purposes.
    const ORDER_TYPE: &'static str;

    /// An identifier of such order.
    fn hash(&self) -> B256;

    /// Internal function that takes the inner row types and extracts the reference needed for
    /// Clickhouse inserter functions like `Inserter::write`. While a default implementation is not
    /// provided, it should suffice to simply return `row`.
    fn to_row_ref(row: &Self::ClickhouseRowType) -> &<Self::ClickhouseRowType as Row>::Value<'_>;
}

impl ClickhouseIndexableOrder for SystemBundle {
    type ClickhouseRowType = BundleRow;

    const ORDER_TYPE: &'static str = "bundle";

    fn hash(&self) -> B256 {
        self.raw_bundle_hash
    }

    fn to_row_ref(row: &Self::ClickhouseRowType) -> &<Self::ClickhouseRowType as Row>::Value<'_> {
        row
    }
}
