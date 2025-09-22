use alloy_eips::eip2718::EIP4844_TX_TYPE_ID;
use alloy_primitives::Bytes;
use alloy_rlp::{Buf as _, Header};
use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use crate::{builderhub::LocalPeerStore, validation::MAINNET_CHAIN_ID};

/// An artificial timestamp used for duration clamping.
static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// Clamp the instant to duration bucket since the start time.
pub fn clamp_to_duration_bucket(time: Instant, duration: Duration) -> Instant {
    let full_durations =
        (time.duration_since(*START).as_secs_f64() / duration.as_secs_f64()).floor();

    // Convert that back to a Duration.
    let clamped_duration = Duration::from_secs_f64(full_durations * duration.as_secs_f64());

    // Add that Duration to the start time to get the clamped time.
    *START + clamped_duration
}

pub static LOCAL_PEER_STORE: LazyLock<LocalPeerStore<()>> = LazyLock::new(LocalPeerStore::new);

pub fn looks_like_canonical_blob_tx(raw_tx: &Bytes) -> bool {
    // For full check we could call TransactionSigned::decode_enveloped and fully try to decode it
    // is way more expensive. We expect EIP4844_TX_TYPE_ID + rlp(chainId = 01,.....)
    let mut tx_slice = raw_tx.as_ref();
    if let Some(tx_type) = tx_slice.first() {
        if *tx_type == EIP4844_TX_TYPE_ID {
            tx_slice.advance(1);
            if let Ok(outer_header) = Header::decode(&mut tx_slice) {
                if outer_header.list {
                    if let Some(chain_id) = tx_slice.first() {
                        return (*chain_id as u64) == MAINNET_CHAIN_ID;
                    }
                }
            }
        }
    }
    false
}
