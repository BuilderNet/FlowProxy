/// Header name for BuilderNet signature.
pub const BUILDERNET_SIGNATURE_HEADER: &str = "X-BuilderNet-Signature";

/// Header name for Flashbots signature.
/// NOTE: this header is used for backwards compatibility.
pub const FLASHBOTS_SIGNATURE_HEADER: &str = "X-Flashbots-Signature";

/// Header name for flashbots priority.
pub const BUILDERNET_PRIORITY_HEADER: &str = "X-BuilderNet-Priority";

/// Header name for BuilderNet sent at timestamp (in Unix microseconds).
pub const BUILDERNET_SENT_AT_HEADER: &str = "X-BuilderNet-SentAtUs";

/// Header name for XFF header.
pub const XFF_HEADER: &str = "X-Forwarded-For";

pub const UNKNOWN: &str = "unknown";

/// JSON-RPC method name for sending bundles.
pub const ETH_SEND_BUNDLE_METHOD: &str = "eth_sendBundle";

/// JSON-RPC method name for sending raw transactions.
pub const ETH_SEND_RAW_TRANSACTION_METHOD: &str = "eth_sendRawTransaction";

/// JSON-RPC method name for sending MEV Share bundles.
pub const MEV_SEND_BUNDLE_METHOD: &str = "mev_sendBundle";

/// Whether to use the legacy signature verification.
pub const USE_LEGACY_SIGNATURE: bool = true;

/// The threshold for a big request size in bytes.
pub const BIG_REQUEST_SIZE_THRESHOLD_KB: usize = 50_000; // 50 KB

/// The default bundle version.
pub const DEFAULT_BUNDLE_VERSION: &str = "v2";
