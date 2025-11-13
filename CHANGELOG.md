# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

## [v1.2.1] - 2025-11-13

### Changed

- Add more buckets to histogram metrics.
- Performance improvements for bundle processing.

## [v1.2.0] - 2025-11-12

### Changed

- Global metrics prefix from `orderflow_proxy` to `flowproxy_`.
- Histograms are now actual histograms, not summaries.
- Percentages are now integers from 0 to 100, instead of floats from 0.0 to 1.0.
- Increase default order cache capacity to 1,048,576 entries (32 MiB).
- Add `--io-threads` (`IO_THREADS`) and `--compute-threads` (`COMPUTE_THREADS`) CLI flags, defaults to 4 and 2 respectively.
- Override dashes to underscore in value provided to CLI flag `--builder-name`.
- Switch to HTTP/2 for system API (incoming and outgoing connections):
    - Add `--http.client-pool-size` (`CLIENT_POOL_SIZE`) CLI flag, defaults to 8. HTTP/2 streams will be multiplexed over these clients.

