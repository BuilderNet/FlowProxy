# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## v1.3.0 - 2025-11-27

This PR introduces messaging on system API using request/reply TCP (+ TLS)
sockets from the `msg-socket/msg-transport` library. TLS is done via the
`openssl` crate. Binary encoding via `bitcode` has been introduced when
communicating using this transport on bundle order types.

Lastly, it also introduces support for mTLS, allowing to provide client
certificate files via CLI arguments.

Backwards compatibility is completely preserved, with HTTP support becoming
officially deprecated, but still available.

Detailed changes:

- Add support for TCP (+TLS) request/reply sockets using the
  [msg-socket](https://github.com/chainbound/msg-rs/tree/v0.1.3/msg-socket) and
  [`msg-transport`](https://github.com/chainbound/msg-rs/tree/v0.1.3/msg-transport)
  crates.
- TCP-only communication uses `bitcode` binary encoding for bundle order types.
- Add support for mutual TLS (mTLS) authentication when using TCP sockets.
- The following CLI flags were added:
  - `--system-listen-addr-tcp` (`SYSTEM_LISTEN_ADDR_TCP`): IP socket address for
    the TCP-only system API listener.
  - `--private-key-pem-file` (`PRIVATE_KEY_PEM_FILE`), optional: Path to the PEM
    file containing the private key for client authentication (mTLS).
  - `--client-cert-pem-file` (`CLIENT_CERT_PEM_FILE`), optional: Path to the PEM
    file containing the client certificate for client authentication (mTLS).
  - `--peer-update-interval-s` (`PEER_UPDATE_INTERVAL_S`): Interval in seconds
    for updating the list of peers from the BuilderHub. It defaults to 30 seconds.
  - `--tcp.small-clients` (`TCP_SMALL_CLIENTS`): Number of TCP clients
    to use per peer for small messages (<32 KiB). It defaults to 4.
  - `--tcp.big-clients` (`TCP_BIG_CLIENTS`): Number of
    TCP clients to use per peer for big messages (>=32 KiB). It defaults to 2.
- The following CLI flags were modified, but the corresponding environment variables
  remain the same:
  - `--user-listen-url` -> `--user-listen-addr` (`USER_LISTEN_ADDR`)
  - `--system-listen-url` -> `--system-listen-addr-http` (`SYSTEM_LISTEN_ADDR`)
  - `--builder-listen-url` -> `--builder-listen-addr` (`BUILDERHUB_ADDR`)
  - `--client.pool-size` -> `--http.client-pool-size` (`CLIENT_POOL_SIZE`)
- All histogram metrics have been migrated to summaries for better accuracy.
  Existing dashboards relying on these metrics should be updated accordingly, by
  looking at the exported `quantile` field instead of the "bucket" family of
  fields.

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
- Add `--io-threads` (`IO_THREADS`) and `--compute-threads` (`COMPUTE_THREADS`)
  CLI flags, defaults to 4 and 2 respectively.
- Override dashes to underscore in value provided to CLI flag `--builder-name`.
- Switch to HTTP/2 for system API (incoming and outgoing connections):
  - Add `--http.client-pool-size` (`CLIENT_POOL_SIZE`) CLI flag, defaults to 8.
    HTTP/2 streams will be multiplexed over these clients.
