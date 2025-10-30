-- ===================================
-- Constants / utility functions
-- ===================================
WITH
    -- Utility: convert bytes to 0x-prefixed lowercase hex
    (x -> concat('0x', lower(hex(x)))) AS hex0x,
    -- Time window for analysis
    toDateTime64('2025-10-30 08:40:00', 6, 'UTC') AS t_since,
    toDateTime64('2025-10-30 09:30:00', 6, 'UTC') AS t_until,

-- ===================================
-- Common reusable subqueries
-- ===================================

    ------------ BUNDLE QUERIES --------------

    -- Get bundle receipts within the specified time window
    bundle_receipts AS (
        SELECT
            hex0x(double_bundle_hash) AS double_bundle_hash,
            *
        FROM buildernet.bundle_receipts_wo_bundle_hash
        WHERE received_at >= t_since AND received_at <= t_until
    ),

    ------------ METADATA QUERIES ------------

    -- Get all unique builders in the dataset
    builders AS (
        SELECT groupUniqArray(dst_builder_name) AS dsts
        FROM bundle_receipts
    ),

    -- Count total number of unique builders (scalar)
    builders_count AS (
        SELECT length(dsts) AS total_builders FROM builders
    ),

    ------------ BUNDLE VISIBILITY ------------

    -- For each bundle, get the list of builders that have seen it
    bundle_seen_by AS (
        SELECT
            double_bundle_hash,
            groupUniqArray(dst_builder_name) AS seen_dsts
        FROM bundle_receipts
        GROUP BY double_bundle_hash
    ),

    -- For each bundle, determine which builders have not seen it
    bundle_not_seen_by AS (
        SELECT
            double_bundle_hash,
            arrayFilter(x -> NOT has(seen_dsts, x), (SELECT dsts FROM builders)) as missing_dsts
        FROM bundle_seen_by
        WHERE length(missing_dsts) > 0
    ),

    ------------ OCCURRENCE COUNTS ------------

    -- Count occurrences of each bundle receipt, grouped by double_bundle_hash
    bundle_occurrences AS (
        SELECT
            double_bundle_hash,
            count() AS occurrences
        FROM bundle_receipts
        GROUP BY double_bundle_hash
        ORDER BY occurrences ASC
    ),

    ------------ LOST BUNDLES QUERIES ---------

    -- A detailed list of bundles that were not seen by all builders
    lost_bundles_detailed AS (
        SELECT *
        FROM bundle_occurrences
        WHERE occurrences < (SELECT total_builders FROM builders_count)
    ),

    -- Get a summary of lost bundles by counting how many bundles were missed by how many builders
    lost_bundles AS (
        SELECT
            count() AS observations,
            occurrences
        FROM lost_bundles_detailed
        GROUP BY occurrences
        ORDER BY occurrences ASC
    ),

    -- Rank builders by the number of bundles they missed
    lost_bundles_by_dst AS (
        SELECT
            arrayJoin(missing_dsts) AS dst_builder_name,
            count() AS missed_bundle_count
        FROM bundle_not_seen_by
        GROUP BY dst_builder_name
        ORDER BY missed_bundle_count DESC
    ),

    ------------ LATENCY QUERIES -------------

    -- Calculate latency quantiles between source and destination builders
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
        GROUP BY src_builder_name, dst_builder_name
        ORDER BY p99_latency_sec DESC
    )

-- ===================================
-- Final query
-- ===================================
SELECT *
FROM lost_bundles_by_dst
