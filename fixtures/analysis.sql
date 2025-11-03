WITH
-- ===================================
-- Constants / utility functions
-- ===================================

    -- Utility: convert bytes to 0x-prefixed lowercase hex.
    (x -> concat('0x', lower(hex(x)))) AS hex0x,
    -- Time window for analysis
    toDateTime64('2025-11-02 23:09:00', 6, 'UTC') AS t_since,
    toDateTime64('2025-11-03 00:09:00', 6, 'UTC') AS t_until,

    -- Slot time for reference. `base_offset` is just an old slot to compute offsets from.
    12 as slot_time,
    toUnixTimestamp('2025-06-14 10:39:35') AS base_offset,
    (x -> mod((toUnixTimestamp(x) - toUnixTimestamp(base_offset)), slot_time)) as to_time_bucket,

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
            priority,
            to_time_bucket(sent_at) AS sent_at_second_in_slot
        FROM buildernet.bundle_receipts_wo_bundle_hash
        WHERE received_at >= t_since AND received_at <= t_until
    ),

    bundle_receipts_count AS (
        SELECT count() AS total_receipts FROM bundle_receipts
    ),

    -- NOTE: given we don't track self-receipts, if a sender completely fails
    -- to send a bundle to anyone, it won't be captured in this dataset.
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

    -- Count occurrences of bundle receipts by source builder.
    -- NOTE: The same bundle may be sent multiple times between the same pair, increasing the count.
    bundle_occurrences_by_src AS (
        SELECT
            src_builder_name,
            count() AS occurrences,
            (SELECT total_receipts FROM bundle_receipts_count) AS total_receipts,
            concat(toString(round(100 * occurrences / total_receipts, 2)), '%') AS percent_of_total_receipts
        FROM bundle_receipts
        GROUP BY src_builder_name
        ORDER BY occurrences DESC
    ),

    -- Count occurrences of bundle receipts by source-destination builder pairs.
    -- NOTE: The same bundle may be sent multiple times between the same pair, increasing the count.
    bundle_occurrences_by_link AS (
        SELECT
            src_builder_name,
            dst_builder_name,
            count() AS occurrences,
            (SELECT total_receipts FROM bundle_receipts_count) AS total_receipts,
            concat(toString(round(100 * occurrences / total_receipts, 2)), '%') AS percent_of_total_receipts
        FROM bundle_receipts
        GROUP BY src_builder_name, dst_builder_name
        ORDER BY occurrences DESC
    ),

    ------------ LOST BUNDLES QUERIES ---------

    -- For each bundle hash, determine which builders have not seen it, along with their sources.
    --
    -- This filters the list of bundles to those that have been missed by at least one builder.
    -- NOTE: because we don't track self-receipts, it means every bundle here
    -- has been seen by at least one builder.
    lost_bundles_srcs_dsts AS (
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

    -- A detailed, flattened view of lost bundles with their source and missing destination builders.
    -- NOTE: This doesn't represent every attempt to send a lost bundle, just the combinations of
    -- source and missing destination builders.
    lost_bundles_links_flattened AS (
        SELECT
            double_bundle_hash,
            arrayJoin(src_builders) AS src_builder_name,
            arrayJoin(missing_dsts) AS dst_builder_name
        FROM lost_bundles_srcs_dsts
    ),

    -- Detailed view of lost bundles with metadata from bundle_receipts.
    -- This represents _every_ attempt to send a certain lost bundle from a source to a missing destination.
    lost_bundles_detailed AS (
        -- 1️⃣ First, gather per-(bundle, source) metadata from bundle_receipts
        WITH bundle_meta AS (
            SELECT
                double_bundle_hash,
                src_builder_name,
                -- Gather every sent_at time for this (bundle, source) pair
                groupUniqArray(sent_at) AS sent_ats,       
                -- We assume these fields are consistent across sends
                any(payload_size) AS payload_size,
                any(priority) AS priority
            FROM bundle_receipts
            GROUP BY double_bundle_hash, src_builder_name
        )
        -- 2️⃣ Now, join lost bundles with that metadata
        SELECT
            lbd.double_bundle_hash,
            lbd.src_builder_name,
            lbd.dst_builder_name,
            -- Explode multiple sent_ats, since we want to count multiple attempts.
            arrayJoin(bm.sent_ats) AS sent_at,
            bm.payload_size,
            bm.priority,
            to_time_bucket(sent_at) AS sent_at_second_in_slot
        FROM lost_bundles_links_flattened AS lbd
        INNER JOIN bundle_meta bm USING (double_bundle_hash, src_builder_name)
    ),

    -- Get a summary of lost bundles by counting how many bundles were missed by how many builders.
    -- FIX: this is okay but doesn't take into account multiple attempts for the same bundle yet.
    lost_bundles_by_builder_count_summary AS (
        SELECT
            missed_builders,
            -- If we had more than one source builder for a bundle, it means
            -- multiple observations of bundle loss.
            sum(length(src_builders)) AS observations,
            (SELECT unique_bundles FROM bundle_unique_count) AS total_unique_bundles,
            concat(toString(round(100 * observations / total_unique_bundles, 2)), '%') AS percent_of_total_bundles
        FROM lost_bundles_srcs_dsts
        GROUP BY missed_builders
        ORDER BY missed_builders ASC
    ),

    -- Rank links by the number of bundles they missed.
    lost_bundles_by_link AS (
        WITH loss_events_by_link AS (
            -- Aggregate loss events by source-destination builder pairs.
            SELECT
                src_builder_name,
                dst_builder_name,
                count() AS missed_bundle_count
            FROM lost_bundles_detailed
            GROUP BY src_builder_name, dst_builder_name
        )
        SELECT
            l.src_builder_name AS src_builder_name,
            l.dst_builder_name AS dst_builder_name,
            missed_bundle_count,
            (bobl.occurrences + missed_bundle_count) AS expected_bundles_sent_between_link,
            concat(toString(round(100 * missed_bundle_count / expected_bundles_sent_between_link, 2)), '%') AS percent_of_total_bundles
        FROM loss_events_by_link AS l
        JOIN bundle_occurrences_by_link bobl
             ON l.src_builder_name = bobl.src_builder_name
            AND l.dst_builder_name = bobl.dst_builder_name
        ORDER BY missed_bundle_count DESC
    ),

    ------------ EXTENDED RECEIPTS ------------

    -- Combine real receipts with synthetic missing receipts for lost bundles.
    bundle_receipts_extended AS (
        -- 1️⃣ Real receipts
        SELECT
            double_bundle_hash,
            sent_at,
            received_at,
            dst_builder_name,
            src_builder_name,
            payload_size,
            priority,
            sent_at_second_in_slot,
            0 AS is_lost
        FROM bundle_receipts

        UNION ALL

        SELECT
            double_bundle_hash,
            sent_at,
            NULL AS received_at, -- no receipt recorded
            dst_builder_name,
            src_builder_name,
            payload_size,
            priority,
            sent_at_second_in_slot,
            1 AS is_lost
        FROM lost_bundles_detailed
    ),
    
    -- Aggregate extended receipts over slot seconds.
    bundle_receipts_extended_over_slot AS (
        SELECT
            sent_at_second_in_slot,
            count() AS total_receipts
        FROM bundle_receipts_extended
        GROUP BY sent_at_second_in_slot
    ),

    -- Aggregate lost bundles over slot seconds.
    lost_bundles_over_slot AS (
        SELECT
            sent_at_second_in_slot,
            count() AS lost_bundles
        FROM lost_bundles_detailed
        GROUP BY sent_at_second_in_slot
    ),

    -- Combine lost bundles with total receipts to get percentage lost over slot seconds.
    lost_bundles_over_slot_percentage AS (
        SELECT
            lbsos.sent_at_second_in_slot,
            lbsos.lost_bundles,
            breos.total_receipts,
            concat(toString(round(100 * lbsos.lost_bundles / breos.total_receipts, 2)), '%') AS percent_lost_bundles
        FROM lost_bundles_over_slot AS lbsos
        INNER JOIN bundle_receipts_extended_over_slot AS breos
            USING (sent_at_second_in_slot)
    ),

    -------- sanity checks ---------

    -- Should match
    lost_bundles_detailed_count AS (SELECT count() FROM lost_bundles_detailed),
    -- lost_bundles_by_builder_count_summary_count AS (SELECT sum(observations * missed_builders) FROM lost_bundles_by_builder_count_summary),
    lost_bundles_by_link_count AS (SELECT sum(missed_bundle_count) FROM lost_bundles_by_link),
    lost_bundles_over_slot_count AS (SELECT sum(lost_bundles) FROM lost_bundles_over_slot),

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
            round(corr(toFloat64(payload_size), toFloat64(received_at - sent_at)), 2) AS corr_payload_size,
            count() AS observations
        FROM bundle_receipts
        WHERE sent_at IS NOT NULL
        GROUP BY src_builder_name, dst_builder_name
        ORDER BY p99_latency_sec DESC
    ),

    latency_over_slot AS (
        WITH total_receipts AS (
            SELECT count() AS total FROM bundle_receipts
        ),
        latency_events AS (
            SELECT
                sent_at_second_in_slot,
                received_at - sent_at AS latency,
                payload_size
            FROM bundle_receipts
        )
        SELECT
            sent_at_second_in_slot,
            quantileExact(0.5)(latency) AS p50_latency_sec,
            quantileExact(0.9)(latency) AS p90_latency_sec,
            quantileExact(0.99)(latency) AS p99_latency_sec,
            quantileExact(0.999)(latency) AS p999_latency_sec,
            round(corr(toFloat64(payload_size), toFloat64(latency)), 2) AS corr_payload_latency,
            count() AS observations,
            (SELECT total from total_receipts) AS total_receipts,
            concat(toString(round(100 * observations / total_receipts, 2)), '%') AS percent_of_total_receipts
        FROM latency_events
        INNER JOIN lost_bundles_over_slot_percentage lbosp USING (sent_at_second_in_slot)
        GROUP BY sent_at_second_in_slot
        ORDER BY p99_latency_sec DESC
    ),

    -- Calculate latency quantiles and bundles lost distributed over slot seconds.
    stats_over_slot AS (
        SELECT
            sent_at_second_in_slot,
            p50_latency_sec,
            p90_latency_sec,
            p99_latency_sec,
            p999_latency_sec,
            corr_payload_latency,
            observations,
            total_receipts,
            percent_of_total_receipts,
            lbosp.lost_bundles AS lost_bundles,
            lbosp.percent_lost_bundles AS percent_lost_bundles
            FROM latency_over_slot
        INNER JOIN lost_bundles_over_slot_percentage lbosp USING (sent_at_second_in_slot)
        ORDER BY sent_at_second_in_slot ASC
    )

-- ===================================
-- Final query
-- ===================================

SELECT *
FROM stats_over_slot
