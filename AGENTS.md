# Repository Guidelines

This guide helps contributors deliver changes to `buildernet-orderflow-proxy` safely and predictably.

## Project Structure & Module Organization
- The entrypoint binary lives in `src/main.rs` and delegates to the library crate in `src/lib.rs`.
- Domain modules are located under `src/`:
  - `ingress/`: Handles HTTP+JSON-RPC requests.
  - `priority/`: Scores queues.
  - `forwarder.rs`: Relays bundles.
  - `rate_limit.rs`: Implements throttling logic.
- Shared helpers are in `types.rs` and `utils.rs`.
- Integration tests are under `tests/`, with reusable fixtures in `tests/common/`.
- Place new code in the closest domain module and expose it through `lib.rs`.
## Build, Test, and Development Commands
- `cargo build` compiles the binary with the default dev profile.
- `cargo run -- --help` prints the CLI flags defined in `src/cli.rs` and is the fastest smoke-test.
- `cargo fmt --all` applies the formatting rules in `rustfmt.toml`.
- `cargo clippy --all-targets --all-features -D warnings` enforces lints configured in `Cargo.toml`.
- `cargo test` executes unit and integration tests; add `-- --nocapture` when debugging async failures.

## Coding Style & Naming Conventions
Rust code should stay within 100 columns; `rustfmt` is authoritative and will reorder imports at crate granularity. Use snake_case for modules/functions and UpperCamelCase for types; reserve SCREAMING_SNAKE_CASE for constants. Prefer structured errors (`thiserror`, `eyre`) over panics, and bubble fallible calls with `?`. When adding public APIs, gate re-exports in `lib.rs` and document behavior with concise rustdoc comments.

## Testing Guidelines
Async flows rely on `tokio::test`, so keep helpers in `tests/common` to avoid duplication. Write integration tests alongside existing `ingress.rs` and `network.rs` files, naming functions with the behavior under test (e.g. `ingress_rejects_invalid_signature`). Run `cargo test --features ...` if you introduce optional features. New behavior must include either a targeted unit test in the owning module or an integration scenario covering success and error paths.

## Commit & Pull Request Guidelines
Follow the short, imperative style seen in history (`fix package name`); prefix with scope if it aids triage (e.g. `ingress:`). Each PR should explain motivation, outline the approach, and call out risky areas. Link to BuilderNet issues when available and attach logs or screenshots for user-facing changes. Confirm CI-ready state by running build, fmt, clippy, and tests before requesting review.

## Configuration & Runtime Notes
The proxy is configured through CLI flags defined in `OrderflowIngressArgs`; document any new flag, default, or environment interaction. Keep defaults safe for local testing (loopback listeners, disabled BuilderHub) and update `--help` text whenever behavior shifts.
