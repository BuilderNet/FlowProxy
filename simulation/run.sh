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
  local scenario="${1:-buildernet.yaml}"
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
        ./shadow --template-directory /root/testdata/ scenarios/${scenario} &
        SHADOW_PID=\$!

        # Wait a bit for processes to start
        sleep 5

        echo 'Generating flamegraphs for running processes...'
        for pid in \$(pgrep -f buildernet-orderflow-proxy); do
          proxyName=\$(ps -p \$pid -o args= | grep -oP 'proxy[0-9]+' || true)
          if [[ -n \"\$proxyName\" ]]; then
            echo -n
            echo \"Generating flamegraph for \$proxyName (PID \$pid)\"
            (cd /tmp && mkdir -p flamegraph_\${proxyName} && cd flamegraph_\${proxyName} && /root/flamegraph -o /root/\${proxyName}.svg --pid \$pid --no-inline -F 99) &
          fi
        done

        wait \$SHADOW_PID

        echo \"Waiting for flamegraph generation to complete...\"
        wait
      "
  else
    docker run $RUN_ARGS --name "$CONTAINER_NAME" -v ./scenarios:/root/scenarios:ro -it "$IMAGE" \
      /bin/bash -c "./shadow --template-directory /root/testdata/ scenarios/${scenario}"
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

  docker cp "$CONTAINER_NAME":/root/shadow.data/hosts/proxy2/eth0.pcap \
    "./results/proxy2_eth0_${timestamp}_runtime-${runtime}_scale-${scale}.pcap"

  tshark -r "./results/proxy2_eth0_${timestamp}_runtime-${runtime}_scale-${scale}.pcap" \
    -T fields -E header=y -E separator=\; \
    -e frame.time -e ip.src -e ip.dst -e frame.len \
    >"./results/proxy2_eth0_${timestamp}_runtime-${runtime}_scale-${scale}_summary.csv"

  # Copy flamegraph SVGs with timestamped names
  for svg in $(docker exec "$CONTAINER_NAME" ls /root/*.svg 2>/dev/null || true); do
    svg_basename=$(basename "$svg")
    svg_name="${svg_basename%.svg}"
    docker cp "$CONTAINER_NAME:$svg" \
      "./results/${svg_name}_${timestamp}_runtime-${runtime}_scale-${scale}.svg"
  done
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
