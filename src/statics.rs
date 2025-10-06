//! Module containing static variables used throughout the application.

use std::{sync::LazyLock, time::Instant};

use crate::builderhub::LocalPeerStore;

/// An artificial timestamp used for duration clamping.
pub static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// A local peer store to use in case we're running without BuilderHub.
pub static LOCAL_PEER_STORE: LazyLock<LocalPeerStore> = LazyLock::new(LocalPeerStore::new);
