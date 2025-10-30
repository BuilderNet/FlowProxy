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

    bundle_unique_count AS (
        SELECT count(DISTINCT double_bundle_hash) AS unique_bundles
        FROM bundle_receipts
    ),

    bundle_duplicates AS (
        select
            double_bundle_hash,
            occurrences
        FROM (SELECT
              double_bundle_hash,
              count() AS occurrences
            FROM bundle_receipts
            GROUP by double_bundle_hash)
        WHERE occurrences > 5 -- TODO: do not hardcode this.
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
            -- We may more than one source builder for a given bundle.
            groupUniqArray(src_builder_name) AS src_builders,
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

    -- Count occurrences of bundle receipts by source-destination builder pairs.
    -- NOTE: duplicates will inflate these counts.
    bundle_occurrences_by_link AS (
        SELECT
            src_builder_name,
            dst_builder_name,
            count() AS occurrences
        FROM bundle_receipts
        GROUP BY src_builder_name, dst_builder_name
        ORDER BY occurrences DESC
    ),

    ------------ LOST BUNDLES QUERIES ---------

    -- For each bundle hash, determine which builders have not seen it, along with their sources.
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
            -- If we had more than one source builder for a bundle, it means
            -- multiple observations of bundle loss.
            sum(length(src_builders)) AS observations,
            (SELECT unique_bundles FROM bundle_unique_count) AS total_unique_bundles,
            concat(toString(round(100 * observations / total_unique_bundles, 2)), '%') AS percent_of_total_bundles
        FROM lost_bundles_detailed
        GROUP BY missed_builders
        ORDER BY missed_builders ASC
    ),

    -- Rank links by the number of bundles they missed.
    lost_bundles_by_link AS (
        WITH loss_events AS (
            -- For each lost bundle, explode the missing_dsts and src_builders to get individual loss events.
            SELECT
                arrayJoin(src_builders) AS src_builder_name,
                arrayJoin(missing_dsts) AS dst_builder_name
            FROM lost_bundles_detailed
        ), loss_events_by_link AS (
            -- Aggregate loss events by source-destination builder pairs.
            SELECT
                src_builder_name,
                dst_builder_name,
                count() AS missed_bundle_count
            FROM loss_events
            GROUP BY src_builder_name, dst_builder_name
        )
        SELECT
            l.src_builder_name AS src_builder_name,
            l.dst_builder_name AS dst_builder_name,
            missed_bundle_count,
            bobl.occurrences AS total_bundles_sent_between_link,
            concat(toString(round(100 * missed_bundle_count / total_bundles_sent_between_link, 2)), '%') AS percent_of_total_bundles
        FROM loss_events_by_link AS l
        JOIN bundle_occurrences_by_link bobl
             ON l.src_builder_name = bobl.src_builder_name
            AND l.dst_builder_name = bobl.dst_builder_name
        ORDER BY missed_bundle_count DESC
    ),

    -------- sanity checks ---------

    -- Should match
    lost_bundles_count AS (SELECT sum(observations * missed_builders) FROM lost_bundles),
    lost_bundles_by_link_count AS (SELECT sum(missed_bundle_count) FROM lost_bundles_by_link),

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
