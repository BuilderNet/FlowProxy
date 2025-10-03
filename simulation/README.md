# Buildernet Orderflow Proxy Simulation

This directory contains a discrete-event network simulation for testing the Buildernet orderflow proxy at scale using [Shadow](https://shadow.github.io/).

> [!IMPORTANT]
> This only works on Linux x86_64 machines.

## Requirements
- Docker
- Clickhouse local: install with `curl https://clickhouse.com/ | sh`
- tshark: install with `apt-get install tshark`

## Quick Start

```bash
# Build the Docker image (includes Shadow, proxy, and test tools)
./sim.sh build

# Run the simulation
./sim.sh run

# Run with profiling (generates flamegraphs of the proxy processes)
./sim.sh run --profile

# Process latest results with ClickHouse
./sim.sh process
```

## What is Shadow?

[Shadow](https://shadow.github.io/) is a discrete-event network simulator that runs real applications (including our orderflow proxy) in a controlled, deterministic environment. Key features:

- **Deterministic**: Same seed produces identical results for reproducible testing
- **Scalable**: Simulates network conditions and multiple processes efficiently
- **Real binaries**: Runs actual compiled applications, not mocks
- **Configurable network**: Define latency, bandwidth, and topology via YAML

Shadow intercepts system calls and simulates network I/O, allowing us to test distributed systems behavior without real network infrastructure.

## How It Works

### Architecture

The simulation runs inside a Docker container with these components:

1. **Shadow** - The discrete-event simulator
2. **Buildernet Orderflow Proxy** - Two instances (`proxy1`, `proxy2`) in different network zones
3. **Mock Hub** - Simulates the builder hub
4. **Submitter** - Generates test traffic from parquet data

### Simulation Flow

1. Shadow starts and initializes the virtual network topology (defined in [network-graph-2-zones.yaml](./scenarios/network-graph-2-zones.yaml))
2. Mock hub starts at t=0s
3. Proxy instances start at t=1s and connect to the hub
4. Submitter starts at t=35s (after proxies initialize) and sends bundles to proxy1
5. Proxies forward bundles between each other and to builders
6. Simulation runs for configured duration (default 5 minutes simulated time)
7. Results are collected: parquet files, PCAP network traces, and optional flamegraphs

### Network Topology

The default scenario uses a 2-zone network:
- **Zone 0**: proxy1, hub, submitter (simulates one datacenter/region)
- **Zone 1**: proxy2 (simulates remote datacenter/region)

Latencies are configured based on real Azure inter-zone measurements.

## Configuration

### Scenario Files

Scenarios are defined in `scenarios/*.yaml`:

- [buildernet.yaml](./scenarios/buildernet.yaml) - Main simulation configuration
- [network-graph-2-zones.yaml](./scenarios/network-graph-2-zones.yaml) - Network topology definition

Key configuration options in `buildernet.yaml`:

```yaml
general:
  stop_time: 5m          # Simulated duration
  seed: 1                # Randomness seed for determinism

experimental:
  use_cpu_pinning: true  # Pin to CPU core for performance
  use_new_tcp: true      # Use Rust TCP implementation
```

### Scaling Traffic

Adjust the `--scale` parameter in the submitter args:

```yaml
processes:
  - path: /root/submitter
    args: --url http://proxy1:9754 --path ../../../testdata/testdata.parquet --scale 5
```

Higher scale = faster bundle submission rate.

## Profiling

When running with `--profile` flag:

1. **Flamegraphs generated** for each proxy process using `perf` and `flamegraph`

2. **Results include** timestamped SVG flamegraphs showing CPU profiles

To set required kernel parameters:
```bash
sudo sysctl kernel.kptr_restrict=0
sudo sysctl kernel.perf_event_paranoid=1
```

Example output of flamegraphs:
- https://chainbound-public.s3.us-east-1.amazonaws.com/proxy1_2025-10-02-14-48-04_runtime-5m_scale-5.svg
- https://chainbound-public.s3.us-east-1.amazonaws.com/proxy2_2025-10-02-14-48-04_runtime-5m_scale-5.svg

## Output & Results

Results are saved to `results/` with timestamps:

- `bundle_receipts_<timestamp>_runtime-<time>_scale-<scale>.parquet` - Bundle receipt data
- `proxy1_eth0_<timestamp>_runtime-<time>_scale-<scale>.pcap` - Network packet capture
- `proxy2_eth0_<timestamp>_runtime-<time>_scale-<scale>.pcap` - Network packet capture
- `proxy1_eth0_<timestamp>_runtime-<time>_scale-<scale>_summary.csv` - PCAP summary (via tshark)
- `proxy2_eth0_<timestamp>_runtime-<time>_scale-<scale>_summary.csv` - PCAP summary (via tshark)
- `proxy1_<timestamp>_runtime-<time>_scale-<scale>.svg` - Flamegraph (if profiling)
- `proxy2_<timestamp>_runtime-<time>_scale-<scale>.svg` - Flamegraph (if profiling)

### Analyzing Results

Use the `process` command to analyze parquet data with ClickHouse:

```bash
./sim.sh process
```

Example output:
```
Processing results/bundle_receipts_2025-10-03-08-57-50_runtime-5m_scale-5.parquet, results/proxy1_eth0_2025-10-03-08-57-50_runtime-5m_scale-5_summary.csv and results/proxy2_eth0_2025-10-03-08-57-50_runtime-5m_scale-5_summary.csv...
Number of rows:
   ┌─count(bundle_hash)─┐
1. │             177985 │
   └────────────────────┘

Aggregated statistics:
   ┌─────────────avg_us─┬─p50_us─┬─p90_us─┬─p99_us─┬─p999_us─┬─min_us─┬─max_us─┬────────────corr_tp─┬─────────avg_size─┬─p50_size─┬─p90_size─┬─p99_size─┐
1. │ 44447.021855774365 │  44004 │  44008 │  48994 │  132006 │  44003 │ 220050 │ 0.1461330024788131 │ 6894.85476304183 │     2966 │    19376 │    40390 │
   └────────────────────┴────────┴────────┴────────┴─────────┴────────┴────────┴────────────────────┴──────────────────┴──────────┴──────────┴──────────┘

Bandwidth usage data:
   ┌─host─────┬─upload_total_MB─┬────upload_avg_Mbps─┬─upload_peak_Mbps─┬─download_total_MB─┬──download_avg_Mbps─┬─download_peak_Mbps─┐
1. │ 10.0.0.3 │     2692.634864 │ 104.56834423300971 │       930.272048 │          127.1639 │  4.938403883495146 │          23.797824 │
2. │ 10.0.0.4 │        127.1639 │  4.938403883495146 │        23.797824 │       2692.634864 │ 104.56834423300971 │         930.272048 │
   └──────────┴─────────────────┴────────────────────┴──────────────────┴───────────────────┴────────────────────┴────────────────────┘
```

## Available Commands

```bash
./sim.sh help                # Show help
./sim.sh build               # Build Docker image
./sim.sh run [scenario]      # Run simulation (default: buildernet.yaml)
./sim.sh run --profile       # Run with CPU profiling
./sim.sh get-results         # Extract results from container
./sim.sh logs <process>      # View process logs (e.g., proxy1, proxy2)
./sim.sh tail <process>      # Tail process logs
./sim.sh clean-container     # Remove simulation container
./sim.sh clean-image         # Remove Docker image
./sim.sh clean-all           # Remove both
./sim.sh process             # Analyze latest results with ClickHouse
```

## Troubleshooting

**Container exits immediately**: Check logs with `docker logs shadow-buildernet`

**Profiling fails**: Ensure kernel parameters are set correctly (see Profiling section)

**No results generated**: Check that simulation duration (`stop_time`) is sufficient for your test data

## Development

### Adding More Proxies

Edit `scenarios/buildernet.yaml` to add more proxy hosts and adjust network topology accordingly.

### Custom Test Data

Replace `simulation/testdata/testdata.parquet` with your own bundle data. Update `stop_time` to accommodate the data's time range (also in accordance with the submitter's `--scale` parameter).

### Modifying Network Conditions

Edit `scenarios/network-graph-2-zones.yaml` to change latencies, bandwidth limits, or add packet loss.
