/// Header name for flashbots signature.
pub const FLASHBOTS_SIGNATURE_HEADER: &str = "X-Flashbots-Signature";

/// Header name for flashbots priority.
pub const BUILDERNET_PRIORITY_HEADER: &str = "X-BuilderNet-Priority";

/// Header name for BuilderNet sent at timestamp (in Unix microseconds).
pub const BUILDERNET_SENT_AT_HEADER: &str = "X-BuilderNet-SentAtUs";

/// Header name for XFF header.
pub const XFF_HEADER: &str = "X-Forwarded-For";

/// JSON-RPC method name for sending bundles.
pub const ETH_SEND_BUNDLE_METHOD: &str = "eth_sendBundle";

/// JSON-RPC method name for sending raw transactions.
pub const ETH_SEND_RAW_TRANSACTION_METHOD: &str = "eth_sendRawTransaction";

/// Whether to use the legacy signature verification.
pub const USE_LEGACY_SIGNATURE: bool = true;
