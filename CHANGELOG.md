# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Changed
- Global metrics prefix from `orderflow_proxy` to `flowproxy_`.
- Histograms are now actual histograms, not summaries.
- Percentages are now integers from 0 to 100, instead of floats from 0.0 to 1.0.