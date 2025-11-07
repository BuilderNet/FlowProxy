# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed
- Global metrics prefix from `orderflow_proxy` to `flowproxy_`.
- Histograms are now actual histograms, not summaries.
- Percentages are now integers from 0 to 100, instead of floats from 0.0 to 1.0.
- Increase default order cache capacity to 1,048,576 entries (32 MiB).
- Add `--io-threads` (`IO_THREADS`) and `--compute-threads` (`COMPUTE_THREADS`) CLI flags, defaults to 4 and 2 respectively.