WITH
-- ===================================
-- Constants / utility functions
-- ===================================

    -- Utility: convert bytes to 0x-prefixed lowercase hex.
    (x -> concat('0x', lower(hex(x)))) AS hex0x,
    -- Time window for analysis
    toDateTime64('2025-10-30 00:00:00', 6, 'UTC') AS t_since,
    toDateTime64('2025-10-30 06:00:00', 6, 'UTC') AS t_until,

-- ===================================
-- Common reusable subqueries
-- ===================================

    ------------ BUNDLE QUERIES --------------

    -- Get bundle receipts within the specified time window.
    bundle_receipts AS (
        SELECT
            hex0x(double_bundle_hash) AS double_bundle_hash,
            sent_at,
            received_at,
            replaceAll(dst_builder_name, '-', '_') AS dst_builder_name,
            src_builder_name,
            payload_size,
            priority
        FROM buildernet.bundle_receipts_wo_bundle_hash
        WHERE received_at >= t_since AND received_at <= t_until
            -- Skip test builders
            AND dst_builder_name != 'buildernet_flashbots_mkosi_test_1'
    ),

    ------------ METADATA QUERIES ------------

    -- Get all unique builders in the dataset.
    builders AS (
        SELECT groupUniqArray(dst_builder_name) AS dsts
        FROM bundle_receipts
    ),

    -- Count total number of unique builders (scalar).
    builders_count AS (
        SELECT length(dsts) AS total_builders FROM builders
    ),

    ------------ BUNDLE VISIBILITY ------------

    -- For each bundle, get the list of builders that have seen it, along with their sources.
    bundle_seen_by AS (
        SELECT
            double_bundle_hash,
            groupUniqArray(src_builder_name) AS src_builders,  -- multiple possible sources
            groupUniqArray(dst_builder_name) AS seen_dsts
        FROM bundle_receipts
        GROUP BY double_bundle_hash
    ),

    ------------ OCCURRENCE COUNTS ------------

    -- Count occurrences of each bundle receipt, grouped by double_bundle_hash.
    bundle_occurrences AS (
        SELECT
            double_bundle_hash,
            count() AS occurrences
        FROM bundle_receipts
        GROUP BY double_bundle_hash
        ORDER BY occurrences ASC
    ),

    ------------ LOST BUNDLES QUERIES ---------

    -- For each bundle, determine which builders have not seen it.
    --
    -- This filters the list of bundles to those that have been missed by at least one builder.
    lost_bundles_detailed AS (
        SELECT
            double_bundle_hash,
            src_builders,
            -- We have to exclude source builders since they won't see their own bundles.
            arrayFilter(
                x -> (NOT has(src_builders, x)) AND (NOT has(seen_dsts, x)),
                (SELECT dsts FROM builders)
            ) AS missing_dsts,
            length(missing_dsts) AS missed_builders
        FROM bundle_seen_by
        WHERE missed_builders > 0
    ),

    -- Get a summary of lost bundles by counting how many bundles were missed by how many builders.
    lost_bundles AS (
        SELECT
            missed_builders,
            count() AS observations
        FROM lost_bundles_detailed
        GROUP BY missed_builders
        ORDER BY missed_builders ASC
    ),

    -- Rank builders by the number of bundles they missed.
    lost_bundles_by_dst AS (
        SELECT
            dst_builder_name,
            count(*) AS missed_bundle_count
        FROM (
            -- explode every missing_dsts array so each element = one "miss event"
            SELECT
                double_bundle_hash,
                arrayJoin(missing_dsts) AS dst_builder_name
            FROM lost_bundles_detailed
        )
        GROUP BY dst_builder_name
        ORDER BY missed_bundle_count DESC
    ),

    -------- sanity checks ---------

    -- Should match
    lost_bundles_count AS (SELECT sum(observations * missed_builders) FROM lost_bundles),
    lost_bundles_by_dst_count AS (SELECT sum(missed_bundle_count) FROM lost_bundles_by_dst),

    ------------ LATENCY QUERIES -------------

    -- Calculate latency quantiles between source and destination builders.
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
FROM lost_bundles;
