//! Module containing static variables used throughout the application.

use std::{
    sync::{LazyLock, RwLock},
    time::Instant,
};

use tokio_util::sync::CancellationToken;

use crate::{builderhub::LocalPeerStore, types::LongLivedTask};

/// A global shutdown token that can be used to signal shutdown to long-lived tasks.
pub static SHUTDOWN_TOKEN: LazyLock<CancellationToken> = LazyLock::new(CancellationToken::new);

/// The collection of long-lived tasks that need graceful shutdown and cleanup of resources.
pub static TASKS: LazyLock<RwLock<Vec<LongLivedTask>>> = LazyLock::new(|| RwLock::new(Vec::new()));

/// An artificial timestamp used for duration clamping.
pub static START: LazyLock<Instant> = LazyLock::new(Instant::now);

/// A local peer store to use in case we're running without BuilderHub.
pub static LOCAL_PEER_STORE: LazyLock<LocalPeerStore> = LazyLock::new(LocalPeerStore::new);
