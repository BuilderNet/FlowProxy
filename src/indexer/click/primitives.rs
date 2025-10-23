use alloy_primitives::B256;
use clickhouse::Row;
use rbuilder_utils::clickhouse::backup::primitives::{ClickhouseIndexableOrder, ClickhouseRowExt};

use crate::{
    indexer::click::models::{BundleReceiptRow, BundleRow},
    primitives::{BundleReceipt, SystemBundle},
};

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
