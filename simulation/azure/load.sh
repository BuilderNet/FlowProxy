#!/usr/bin/env bash

# Generate a 1MB file of random data
dd if=/dev/urandom of=upload.bin bs=1M count=1 > /dev/null 2>&1

REGION=${REGION:-us}

N=${N:-50}
FILE=upload.bin
DEBUG=${DEBUG:-0}

RELAYS=( "https://${REGION}.titanrelay.xyz" "https://relay-builders-${REGION}.ultrasound.money" "https://bloxroute.max-profit.blxrbdn.com" "https://bloxroute.regulated.blxrbdn.com" )

for r in "${RELAYS[@]}"; do
  echo ""
  echo "Load testing relay: $r"
  echo ""

    args=()
    for i in $(seq 1 $N); do
    # Don't use -o /dev/null - it causes throughput issues with --next
    # Instead, use a marker and grep out the stats
    args+=(
        -X POST
        -D /dev/stderr
        -H 'content-type: application/json'
        --data-binary @"$FILE"
        -w "\n__STATS__ %{http_code} %{size_upload} %{time_total} %{num_connects}\n"
        "$r/eth/v1/builder/blinded_blocks"
    )
    if [ "$i" -lt "$N" ]; then
        args+=( --next )
    fi
    done

    # Redirect stderr to /dev/null to hide headers, keep only the stats output
    output=$(curl --http1.1 -sS "${args[@]}" 2>/dev/null)
    
    if [ "$DEBUG" = "1" ]; then
        echo "Raw output (first 20 lines):"
        echo "$output" | head -20
        echo "---"
    fi
    
    # Extract stats lines using the marker
    echo "$output" \
    | grep -E '^__STATS__ [0-9]{3} [0-9]+ [0-9]+\.[0-9]+ [0-9]+' \
    | sed 's/__STATS__ //' \
    | awk '
    {
        status[$1]++;
        bytes += $2;
        time += $3;
        connects += $4;
        count++;
    }
    END {
        if (count > 0 && time > 0) {
            printf "requests=%d\n", count;
            printf "status_codes: ";
            for (code in status) printf "%s=%d ", code, status[code];
            printf "\n";
            printf "tcp_connections=%d\n", connects;
            printf "total_uploaded=%d bytes (%.2f MB)\n", bytes, bytes/1048576;
            printf "total_time=%.3f s\n", time;
            printf "aggregate_throughput=%.2f Mbps\n", (bytes*8/time/1000000);
            printf "avg_time_per_request=%.3f s\n", time/count;
        } else {
            printf "No successful requests\n";
        }
    }' || {
        echo "Failed to test relay $r"
        continue
    }
done

echo ""
echo "Load test complete!"
