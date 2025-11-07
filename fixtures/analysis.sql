WITH
-- ===================================
-- Constants / utility functions
-- ===================================

    -- Utility: convert bytes to 0x-prefixed lowercase hex.
    (x -> concat('0x', lower(hex(x)))) AS hex0x,
    -- Time window for analysis
    '2025-11-02 23:39:00' AS t_since_str,
    '2025-11-02 23:45:00' AS t_until_str,
    toDateTime64(t_since_str, 6, 'UTC') AS t_since,
    toDateTime64(t_until_str, 6, 'UTC') AS t_until,

    -- Slot time for reference. `base_offset` is just an old slot to compute offsets from.
    12 as slot_time,
    toUnixTimestamp('2025-06-14 10:39:35') AS base_offset,
    (x -> mod((toUnixTimestamp(x) - toUnixTimestamp(base_offset)), slot_time)) as to_time_bucket,

    -- Region patterns to make analysis only on specific regions easier.
    '(?i)europe|mkosi_test_1' AS europe_pattern,
    'eastus' AS us_pattern,
    '.*' AS all_regions_pattern,
    -- Choose here the pattern to match against builder names.
    -- This closure is used in `bundles` and `bundle_receipts` to filter all input data by region.
    (x -> match(x, all_regions_pattern)) AS is_region_match,

-- ===================================
-- Common reusable subqueries
-- ===================================

    ------------ BUNDLE QUERIES --------------

    -- Get bundles within the specified time window.
    bundles AS (
        SELECT
            hex0x(double_bundle_hash) AS double_bundle_hash,
            hex0x(signer_address) AS signer_hash,
            replaceAll(builder_name, '-', '_') AS builder_name,
            received_at
        FROM buildernet.bundles_v2_double_hash_from_date(query_time=t_since_str)
        WHERE t_since <= received_at AND received_at <= t_until AND
            is_region_match(builder_name)
    ),

    -- Count total number of unique bundles (scalar).
    bundles_unique_count AS (
        SELECT count(DISTINCT double_bundle_hash) AS count FROM bundles
    ),

    ------------ BUNDLE RECEIPTS QUERIES ------------

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
        FROM buildernet.bundle_receipts_wo_bundle_hash_from_date(query_time=t_since_str)
        WHERE t_since <= received_at AND received_at <= t_until AND
            is_region_match(src_builder_name) AND is_region_match(dst_builder_name)
    ),

    -- Create synthetic self-receipts for each bundle.
    bundle_receipts_self AS (
        SELECT
            double_bundle_hash,
            received_at AS sent_at, -- self-sent at received_at time
            received_at, -- self-received at received_at time
            builder_name AS dst_builder_name,
            builder_name AS src_builder_name,
            NULL AS payload_size,
            NULL AS priority,
            to_time_bucket(received_at) AS sent_at_second_in_slot
        FROM bundles b
    ),

    -- Combine real receipts with synthetic self-receipts for all bundles.
    -- This is useful to determine lost bundles, as every bundle should at least have a self-receipt.
    bundle_receipts_with_self AS (
        SELECT * FROM bundle_receipts

        UNION ALL

        SELECT * FROM bundle_receipts_self
    ),


    -- Group per-(bundle, source) data from bundle_receipts_with_self
    bundle_receipts_send_attempts AS (
        SELECT
            double_bundle_hash,
            src_builder_name,
            -- Every sent_at time for this (bundle, source) pair represents an attempt.
            groupUniqArray(sent_at) AS sent_ats,       
            -- In case of self-receipts, these two fields will be NULL, but we assume they are consistent across other receipts, 
            -- when available.
            anyIf(payload_size, payload_size IS NOT NULL) AS payload_size,
            anyIf(priority, priority IS NOT NULL) AS priority
        FROM bundle_receipts_with_self
        GROUP BY double_bundle_hash, src_builder_name
    ),

    -- Count total number of bundle receipts (scalar).
    bundle_receipts_count AS (
        SELECT count() AS total_receipts FROM bundle_receipts
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

    ------------ SIGNER QUERIES ----------------
    
    -- Rank signers by the number of unique bundles they have signed.
    signer_rank AS (
        SELECT
            signer_hash,
            countDistinct(double_bundle_hash) AS unique_bundles,
            concat(toString(round(100 * unique_bundles / (SELECT count FROM bundles_unique_count), 2)), '%') AS percent_of_total_unique_bundles
        FROM bundles
        GROUP BY signer_hash
        ORDER BY unique_bundles DESC
    ),

    -- Calculate the number of duplicates sent by signers. A duplicate is
    -- defined as a bundle that has been sent more than once by a certain signer
    -- to the same builder. If we do otherwise, we would be counting legitimate
    -- multiple sends to different builders as duplicates, which is a common
    -- searcher strategy.
    signer_rank_duplicates_detailed AS (
        SELECT
            signer_hash,
            double_bundle_hash,
            count(double_bundle_hash) AS bundles_sent,
            -- The first occurrence is not a duplicate, so we subtract 1.
            bundles_sent - 1 AS duplicates
        FROM bundles
        GROUP BY signer_hash, builder_name, double_bundle_hash
    ),

    -- Rank signers by the amount of duplicates sent. See definition of duplicates above for details.
    signer_rank_duplicates AS (
        SELECT
            signer_hash,
            sum(duplicates) AS total_duplicates_sent,
            sum(bundles_sent) AS total_bundles_sent,
            concat(toString(round(100 * total_duplicates_sent / total_bundles_sent, 2)), '%') AS percent_duplicates_sent
        FROM signer_rank_duplicates_detailed
        GROUP BY signer_hash
        ORDER BY total_duplicates_sent DESC
    ),

    -- Combine signer rank with duplicates info.
    signer_rank_combined AS (
        SELECT
            sr.signer_hash,
            sr.unique_bundles,
            sr.percent_of_total_unique_bundles,
            srd.total_duplicates_sent AS duplicates_sent,
            srd.total_bundles_sent AS total_bundles_sent,
            srd.percent_duplicates_sent AS percent_duplicates_sent
        FROM signer_rank AS sr
        LEFT SEMI JOIN signer_rank_duplicates AS srd USING (signer_hash)
        ORDER BY sr.unique_bundles DESC
    ),
    
    -- For each signer, get the distribution of unique bundles sent to each builder.
    signer_distribution_raw AS (
        SELECT
            signer_hash,
            builder_name,
            countDistinct(double_bundle_hash) AS unique_bundles_sent_to_builder
        FROM bundles
        GROUP BY signer_hash, builder_name
    ),

    -- Rank signer by unique bundles they have signed, and show distribution across builders.
    -- Useful to understand signer preferences of builders.
    signer_distribution AS (
        SELECT
            sdr.signer_hash,
            sdr.builder_name,
            sdr.unique_bundles_sent_to_builder,
            sr.unique_bundles AS total_unique_bundles_sent,
            concat(toString(round(100 * sdr.unique_bundles_sent_to_builder / sr.unique_bundles, 2)), '%') AS percent_of_unique_bundles_sent_to_builder
        FROM signer_distribution_raw AS sdr
        LEFT SEMI JOIN signer_rank AS sr USING (signer_hash)
        ORDER BY sr.unique_bundles DESC, sdr.signer_hash, sdr.unique_bundles_sent_to_builder DESC
    ),

    ------------ BUNDLE VISIBILITY ------------

    -- For each bundle, get the list of builders that have seen it, along with their sources.
    -- NOTE: This contains self-receipts, so every bundle will have at least one seen_dst (its source).
    bundle_seen_by AS (
        SELECT
            double_bundle_hash,
            -- We may more than one source builder for a given bundle.
            groupUniqArray(src_builder_name) AS src_builders,
            groupUniqArray(dst_builder_name) AS seen_dsts
        FROM bundle_receipts_with_self
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
    
    -- WARN: lost bundles queries within a single region may report incorrect results, due to
    -- searchers sending to multiple builders in different regions at the same time but being
    -- colocated with only one of them.
    -- Example: suppose a searcher in US sends bundle to both a builder in EU and US. Then it might happen
    -- that a another peer in EU receives the bundle from a US peer first. In that case, we would be counting
    -- incorrectly a lost receipt.

    -- Bundle hashes that have been missed by at least one builder.
    lost_bundles AS (
        WITH grouped AS (
          SELECT
              double_bundle_hash,
              count() AS occurrences
          FROM bundle_receipts_with_self
          GROUP BY double_bundle_hash
        ) SELECT
            double_bundle_hash,
            occurrences,
            (SELECT total_builders FROM builders_count) AS total_builders,
            total_builders - occurrences AS missed_builders
        FROM grouped
        WHERE missed_builders > 0
    ),

    -- For each bundle hash, determine which builders have not seen it, along with their sources.
    lost_bundles_srcs_dsts AS (
        SELECT
            double_bundle_hash,
            src_builders,
            arrayFilter(
                x -> NOT has(seen_dsts, x),
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
        SELECT
            lbd.double_bundle_hash,
            lbd.src_builder_name,
            lbd.dst_builder_name,
            -- Explode multiple sent_ats, since we want to count multiple attempts.
            arrayJoin(brsa.sent_ats) AS sent_at,
            brsa.payload_size,
            brsa.priority,
            to_time_bucket(sent_at) AS sent_at_second_in_slot
        FROM lost_bundles_links_flattened AS lbd
        LEFT SEMI JOIN bundle_receipts_send_attempts brsa USING (double_bundle_hash, src_builder_name)
    ),

    -- Count lost bundles between each source-destination builder pair.
    lost_bundles_between_link_count AS (
        SELECT
            src_builder_name,
            dst_builder_name,
            count() AS missed_bundle_count
        FROM lost_bundles_detailed
        GROUP BY src_builder_name, dst_builder_name
    ),

    -- Rank links by the number of bundles they missed.
    lost_bundles_by_link AS (
        SELECT
            l.src_builder_name AS src_builder_name,
            l.dst_builder_name AS dst_builder_name,
            missed_bundle_count,
            (bobl.occurrences + missed_bundle_count) AS expected_bundles_sent_between_link,
            concat(toString(round(100 * missed_bundle_count / expected_bundles_sent_between_link, 2)), '%') AS percent_of_total_bundles
        FROM lost_bundles_between_link_count AS l
        LEFT SEMI JOIN bundle_occurrences_by_link bobl USING (src_builder_name, dst_builder_name)
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
