use std::{
    sync::LazyLock,
    time::{Duration, Instant},
};

use crate::builderhub::LocalPeerStore;

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
