#!/usr/bin/env bash
set -euo pipefail

IMAGE="shadow-buildernet"
CONTAINER_NAME="shadow-buildernet"
RUN_ARGS="--shm-size=1024g --security-opt=seccomp=unconfined --cpuset-cpus=0-7 --cap-add=PERFMON"

# Helper: UTC datetime
datetime_utc() {
  date -u +"%Y-%m-%d-%H-%M-%S"
}

help() {
  cat <<EOF
Available commands:

  help                Show this message
  build               Build the docker image
  run [scenario]      Run a shadow simulation (default: buildernet.yaml)
  get-results         Copy results out of the container
  logs <process>      View logs of given shadow process
  tail <process>      Tail logs of given shadow process
  clean-container     Remove the simulation container
  clean-image         Remove the built docker image
  clean-all           Remove both container and image
  process             Process the latest results parquet with clickhouse
EOF
}

build() {
  docker build -t "$IMAGE" -f Dockerfile ..
}

run() {
  local scenario="buildernet.yaml"
  local profile=false

  # Parse flags
  while [[ $# -gt 0 ]]; do
    case "$1" in
    --profile)
      profile=true
      shift
      ;;
    *)
      scenario="$1"
      shift
      ;;
    esac
  done

  if [[ "$profile" == true ]]; then
    echo "Profiling enabled"

    # Check sysctl values for profiling
    kptr_restrict=$(sysctl -n kernel.kptr_restrict 2>/dev/null || echo "unknown")
    perf_paranoid=$(sysctl -n kernel.perf_event_paranoid 2>/dev/null || echo "unknown")

    if [[ "$kptr_restrict" != "0" ]] || [[ "$perf_paranoid" != "1" ]]; then
      echo "WARNING: Profiling requires specific sysctl settings."
      echo "Current values: kernel.kptr_restrict=$kptr_restrict, kernel.perf_event_paranoid=$perf_paranoid"
      echo "Required values: kernel.kptr_restrict=0, kernel.perf_event_paranoid=1"
      echo ""
      echo "Please run the following commands:"
      echo "  sudo sysctl kernel.kptr_restrict=0"
      echo "  sudo sysctl kernel.perf_event_paranoid=1"
      exit 1
    fi
  else
    echo "Profiling disabled"
  fi

  ./$(basename "$0") clean-container || true

  if [[ "$profile" == true ]]; then
    docker run $RUN_ARGS --name "$CONTAINER_NAME" -v ./scenarios:/root/scenarios:ro -it "$IMAGE" \
      /bin/bash -c "
        ./shadow --template-directory /root/testdata/ scenarios/${scenario} | while IFS= read -r line; do
          if [[ \$line =~ ru_utime=([0-9.]+)\ minutes.*ru_stime=([0-9.]+)\ minutes ]]; then
            echo \"Real user time: \${BASH_REMATCH[1]} minutes\"
            echo \"Simulated time: \${BASH_REMATCH[2]} minutes\"
          fi
        done &

        # Wait a bit for processes to start
        sleep 5

        echo 'Generating flamegraphs for running processes...'
        for pid in \$(pgrep -f flowproxy); do
          proxyName=\$(ps -p \$pid -o args= | grep -oP 'proxy[0-9]+' | head -n1 || true)
          if [[ -n \"\$proxyName\" ]]; then
            echo -n
            echo \"Generating flamegraph for \$proxyName (PID \$pid)\"
            mkdir -p /root/svg
            (mkdir -p \"/tmp/flamegraph_\${proxyName}\" && cd \"/tmp/flamegraph_\${proxyName}\" && /root/flamegraph -o \"/root/svg/\${proxyName}.svg\" --pid \$pid --no-inline -F 99) &
          fi
        done

        echo \"Waiting for flamegraph generation to complete...\"
        wait
      "
  else
    docker run $RUN_ARGS --name "$CONTAINER_NAME" -v ./scenarios:/root/scenarios:ro -it "$IMAGE" \
      /bin/bash -c "./shadow --template-directory /root/testdata/ scenarios/${scenario} | while IFS= read -r line; do
        if [[ \$line =~ ru_utime=([0-9.]+)\ minutes.*ru_stime=([0-9.]+)\ minutes ]]; then
          echo \"Real user time: \${BASH_REMATCH[1]} minutes\"
          echo \"Simulated time: \${BASH_REMATCH[2]} minutes\"
        fi
      done"
  fi

  ./$(basename "$0") get-results
}

get-results() {
  mkdir -p results
  local timestamp
  timestamp=$(datetime_utc)
  local runtime
  runtime=$(grep stop_time scenarios/buildernet.yaml | cut -d ":" -f 2 | xargs)
  local scale
  scale=$(awk -v RS=' ' '/--scale/ { getline; print; exit }' scenarios/buildernet.yaml | xargs)

  docker cp "$CONTAINER_NAME":/root/shadow.data/hosts/proxy2/bundle_receipts_proxy2.parquet \
    "./results/bundle_receipts_${timestamp}_runtime-${runtime}_scale-${scale}.parquet"

  docker cp "$CONTAINER_NAME":/root/shadow.data/hosts/proxy1/eth0.pcap \
    "./results/proxy1_eth0_${timestamp}_runtime-${runtime}_scale-${scale}.pcap"

  docker cp "$CONTAINER_NAME":/root/shadow.data/hosts/proxy2/eth0.pcap \
    "./results/proxy2_eth0_${timestamp}_runtime-${runtime}_scale-${scale}.pcap"

  echo "Generating pcap summary CSV for proxy1"
  tshark -r "./results/proxy1_eth0_${timestamp}_runtime-${runtime}_scale-${scale}.pcap" \
    -T fields -E header=y -E separator=\; \
    -e frame.time -e ip.src -e ip.dst -e frame.len \
    >"./results/proxy1_eth0_${timestamp}_runtime-${runtime}_scale-${scale}_summary.csv"

  echo "Generating pcap summary CSV for proxy2"
  tshark -r "./results/proxy2_eth0_${timestamp}_runtime-${runtime}_scale-${scale}.pcap" \
    -T fields -E header=y -E separator=\; \
    -e frame.time -e ip.src -e ip.dst -e frame.len \
    >"./results/proxy2_eth0_${timestamp}_runtime-${runtime}_scale-${scale}_summary.csv"

  # Copy flamegraph SVGs with timestamped names
  docker cp "$CONTAINER_NAME:/root/svg/" ./tmp 2>/dev/null || true

  for svg in ./tmp/*.svg; do
    [ -f "$svg" ] || continue
    svg_basename=$(basename "$svg")
    svg_name="${svg_basename%.svg}"
    echo "Copying $svg to ./results/${svg_name}_${timestamp}_runtime-${runtime}_scale-${scale}.svg"
    cp "$svg" "./results/${svg_name}_${timestamp}_runtime-${runtime}_scale-${scale}.svg"
  done

  rm -rf ./tmp
}

logs() {
  local process="$1"
  docker exec "$CONTAINER_NAME" bash -c "cat shadow.data/hosts/${process}/*.1000.stdout" | less -R
}

tail-logs() {
  local process="$1"
  docker exec "$CONTAINER_NAME" bash -c "tail -f shadow.data/hosts/${process}/*.1000.stdout"
}

clean-container() {
  docker rm -f "$CONTAINER_NAME" || true
}

clean-image() {
  docker rmi -f "$IMAGE" || true
}

clean-all() {
  clean-container
  clean-image
}

process-results() {
  local latest_parquet
  latest_parquet=$(ls -t results/*.parquet 2>/dev/null | head -n1 || true)
  if [[ -z "$latest_parquet" ]]; then
    echo "No parquet results found in results/"
    exit 1
  fi

  local proxy1_csv
  proxy1_csv=$(ls -t results/*proxy1*.csv 2>/dev/null | head -n1 || true)
  local proxy2_csv
  proxy2_csv=$(ls -t results/*proxy2*.csv 2>/dev/null | head -n1 || true)

  if [[ -z "$proxy1_csv" ]] || [[ -z "$proxy2_csv" ]]; then
    echo "Missing CSV results (need both proxy1 and proxy2 in results/)"
    exit 1
  fi

  echo "Processing $latest_parquet, $proxy1_csv and $proxy2_csv..."

  local count_query="SELECT count(bundle_hash) FROM file('$latest_parquet', Parquet)"

  local query="WITH data AS (
        SELECT toUnixTimestamp64Micro(received_at) - toUnixTimestamp64Micro(sent_at) AS diff_us, payload_size
        FROM file('$latest_parquet', Parquet)
    ) SELECT
        avg(diff_us) AS avg_us,
        quantileExact(0.5)(diff_us) AS p50_us,
        quantileExact(0.9)(diff_us) AS p90_us,
        quantileExact(0.99)(diff_us) AS p99_us,
        quantileExact(0.999)(diff_us) AS p999_us,
        min(diff_us) AS min_us,
        max(diff_us) AS max_us,
        corr(diff_us, payload_size) AS corr_tp,
        avg(payload_size) AS avg_size,
        quantileExact(0.5)(payload_size) AS p50_size,
        quantileExact(0.9)(payload_size) AS p90_size,
        quantileExact(0.99)(payload_size) AS p99_size
    FROM data"

  echo "Number of rows:"
  ./clickhouse local --no-system-tables --output-format=PrettyCompact -q "$count_query"

  echo
  echo "Aggregated statistics:"
  ./clickhouse local --no-system-tables --output-format=PrettyCompact -q "$query"

  local csv_query="SET format_csv_delimiter=';';
  WITH combined_data AS
    (
        SELECT * FROM file('$proxy1_csv', CSVWithNames)
        UNION ALL
        SELECT * FROM file('$proxy2_csv', CSVWithNames)
    ),
    filtered_data AS
    (
        SELECT
            ip.src AS src,
            ip.dst AS dst,
            toStartOfSecond(parseDateTime64BestEffortOrNull(replaceRegexpAll(frame.time, '\\sCET$', ''), 9, 'Europe/Brussels')) AS ts,
            frame.len AS bytes
        FROM combined_data
        WHERE src NOT IN ('10.0.0.1', '10.0.0.2') AND dst NOT IN ('10.0.0.1', '10.0.0.2')
    ),
    upload_stats AS
    (
        SELECT
            src AS host,
            sum(bytes) / 1e6 AS upload_total_MB,
            ((sum(bytes) * 8) / 1e6) / greatest(1, dateDiff('second', min(ts), max(ts)) + 1) AS upload_avg_Mbps,
            (max(bytes) * 8) / 1e6 AS upload_peak_Mbps
        FROM (
            SELECT src, ts, sum(bytes) AS bytes
            FROM filtered_data
            GROUP BY src, ts
        )
        GROUP BY src
    ),
    download_stats AS
    (
        SELECT
            dst AS host,
            sum(bytes) / 1e6 AS download_total_MB,
            ((sum(bytes) * 8) / 1e6) / greatest(1, dateDiff('second', min(ts), max(ts)) + 1) AS download_avg_Mbps,
            (max(bytes) * 8) / 1e6 AS download_peak_Mbps
        FROM (
            SELECT dst, ts, sum(bytes) AS bytes
            FROM filtered_data
            GROUP BY dst, ts
        )
        GROUP BY dst
    )
  SELECT
    COALESCE(u.host, d.host) AS host,
    COALESCE(u.upload_total_MB, 0) AS upload_total_MB,
    COALESCE(u.upload_avg_Mbps, 0) AS upload_avg_Mbps,
    COALESCE(u.upload_peak_Mbps, 0) AS upload_peak_Mbps,
    COALESCE(d.download_total_MB, 0) AS download_total_MB,
    COALESCE(d.download_avg_Mbps, 0) AS download_avg_Mbps,
    COALESCE(d.download_peak_Mbps, 0) AS download_peak_Mbps
  FROM upload_stats u
  FULL OUTER JOIN download_stats d ON u.host = d.host
  ORDER BY host
  "

  echo
  echo "Bandwidth usage data:"
  ./clickhouse local --no-system-tables --output-format=PrettyCompact -q "$csv_query"
}

# Dispatch
cmd="${1:-help}"
shift || true

case "$cmd" in
help) help ;;
build) build ;;
run) run "$@" ;;
get-results) get-results ;;
logs) logs "$@" ;;
tail) tail-logs "$@" ;;
clean-container) clean-container ;;
clean-image) clean-image ;;
clean-all) clean-all ;;
process) process-results ;;
*)
  echo "Unknown command: $cmd"
  help
  exit 1
  ;;
esac
