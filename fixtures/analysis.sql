-- ===================================
-- Constants / utility functions
-- ===================================
WITH
    -- Take bytes and convert to hex with '0x' prefix
    (x -> concat('0x', lower(hex(x)))) AS hex0x,
    -- Time window for analysis.
    toDateTime64('2025-10-30 08:22:00', 6, 'UTC') AS t_since,
    toDateTime64('2025-10-30 09:15:00', 6, 'UTC') AS t_until,

-- ===================================
-- Common reusable subqueries
-- ===================================
    bundle_receipts AS (
        SELECT
            hex0x(double_bundle_hash) AS double_bundle_hash,
            *,
        FROM buildernet.bundle_receipts_wo_bundle_hash
        WHERE received_at >= t_since AND received_at <= t_until
    ),
    occurrences AS (
        SELECT
            double_bundle_hash,
            count(*) AS occurrences
        FROM bundle_receipts
        GROUP BY double_bundle_hash
        ORDER BY occurrences ASC
    ),
    dropped_bundles AS (
        SELECT * FROM occurrences WHERE occurrences < 5
    ),
    src_dst_quantiles AS (
      SELECT
          src_builder_name,
          dst_builder_name,
          quantileExact(0.5)(received_at - sent_at) AS p50_latency_sec,
          quantileExact(0.9)(received_at - sent_at) AS p90_latency_sec,
          quantileExact(0.99)(received_at - sent_at) AS p99_latency_sec,
          quantileExact(0.999)(received_at - sent_at) AS p999_latency_sec,
          count() AS observations
      FROM bundle_receipts
      WHERE sent_at IS NOT NULL
      GROUP BY
          src_builder_name,
          dst_builder_name
      ORDER BY
          p99_latency_sec DESC
    )

-- ===================================
-- Final query
-- ===================================
SELECT * FROM src_dst_quantiles;

