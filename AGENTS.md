# Repository Guidelines

This guide helps contributors deliver changes to `buildernet-orderflow-proxy-v2` safely and predictably.

## Project Structure & Module Organization

### Entry Points & Core Structure
- The entrypoint binary lives in `src/main.rs` and delegates to the library crate in `src/lib.rs`.
- CLI arguments are defined in `src/cli.rs` using the `clap` crate.
- The `src/runner/` module handles application lifecycle and HTTP server setup.

### Domain Modules
Core domain logic is organized under `src/`:
- **`ingress/`**: HTTP handlers for three endpoints:
  - `user.rs`: Public API for bundle/transaction submission
  - `system.rs`: Internal API for system operations
  - `builder.rs`: Builder stats endpoint
  - Handles validation, entity scoring, rate limiting, and queuing
- **`forwarder.rs`**: Relays bundles to local builder via HTTP and peer proxies via BuilderHub
- **`priority/`**: Multi-level priority queue system (High/Medium/Low)
  - `mod.rs`: PriorityQueues implementation
  - `pchannel.rs`: Custom priority channels with backoff strategies
- **`entity.rs`**: Entity tracking, scoring, and spam detection logic
- **`rate_limit.rs`**: Per-entity rate limiting with sliding windows
- **`validation.rs`**: EVM transaction validation (intrinsic gas, initcode, chain ID, EIP-4844/7702)
- **`builderhub.rs`**: Peer discovery and management via BuilderHub integration
- **`indexer/`**: Data persistence layer
  - `click/`: ClickHouse indexer with async batching
  - `parq.rs`: Parquet file-based storage
- **`cache.rs`**: Order and signer caching with TTL-based LRU eviction
- **`metrics.rs`**: Prometheus metrics for observability
- **`primitives/`**: Bundle types and encoding utilities
- **`jsonrpc.rs`**: JSON-RPC protocol handling
- **`tasks/`**: Task execution and graceful shutdown coordination

### Supporting Code
- **`src/utils.rs`**: Shared helper functions
- **`benches/`**: Criterion performance benchmarks
- **`fixtures/`**: SQL schema files for ClickHouse tables
- **`simulation/`**: Load testing harness with Docker Compose
- **`tests/`**: Integration tests with reusable fixtures in `tests/common/`

### Guidelines for New Code
- Place new code in the closest domain module and expose it through `lib.rs`.
- For cross-cutting concerns, consider adding to `utils.rs` or creating a new focused module.
- Keep modules focused and cohesive; split large modules into submodules when they exceed ~500 lines.
## Build, Test, and Development Commands

### Building
- `cargo build` compiles the binary with the default dev profile.
- `cargo build --release` produces an optimized build with LTO enabled.
- `just build-reproducible` creates a verifiable production build (uses the `reproducible` profile).
- `cargo run -- --help` prints the CLI flags defined in `src/cli.rs` and is the fastest smoke-test.

### Testing
- `just test` runs the test suite using nextest (faster parallel test runner).
- `cargo test` executes unit and integration tests; add `-- --nocapture` when debugging async failures.
- `cargo bench` runs Criterion benchmarks (validation, signature verification, etc.).
- Integration tests use `testcontainers` for ClickHouse and require Docker to be running.

### Code Quality
- `just fmt` or `cargo +nightly fmt --all` applies formatting rules from `rustfmt.toml`.
- `just clippy` or `cargo clippy --all-targets --all-features -- -D warnings` enforces lints.
- All linting configuration lives in `Cargo.toml` under `[lints.clippy]`.

### Database Operations
- `just provision-db` creates the ClickHouse database and tables from `fixtures/`.
- `just reset-db` drops and recreates tables (destructive operation).
- `just extract-data <FILE>` exports data to Parquet format.

### Justfile Commands
Run `just --list` to see all available commands. The justfile automates common workflows and should be the preferred method for development tasks.

## Coding Style & Naming Conventions
- Keep Rust code within 100 columns (enforced by rustfmt).
- Use `rustfmt` for formatting; it is authoritative and will reorder imports at crate granularity.
- Use `snake_case` for modules and functions, `UpperCamelCase` for types, and reserve `SCREAMING_SNAKE_CASE` for constants.
- Prefer structured errors (`thiserror`, `eyre`) over panics, and bubble fallible calls with `?`.
- When adding public APIs, gate re-exports in `lib.rs`.
- Document public APIs with concise rustdoc comments (`///` for public items).
- Use `tracing::instrument` for function-level observability on hot paths and error scenarios.
- Prefer immutable data structures; use `Arc` for shared ownership across async tasks.
- Keep async functions small and focused; extract complex logic into sync helper functions when possible.

## Testing Guidelines

### Test Organization
- Unit tests live in the same file as the code they test, in a `#[cfg(test)]` module.
- Integration tests are in the `tests/` directory, organized by feature area.
- Shared test utilities and fixtures belong in `tests/common/`.
- Async flows rely on `#[tokio::test]`, so ensure all async test helpers are properly propagated.

### Naming Conventions
Name test functions with the behavior under test, following the pattern:
- `<module>_<scenario>_<expected_outcome>`
- Examples: `ingress_rejects_invalid_signature`, `forwarder_retries_on_timeout`, `entity_scores_spam_bundle_low`

### Test Coverage Requirements
New behavior must include either:
1. A targeted unit test in the owning module covering the happy path and key edge cases, OR
2. An integration test scenario covering success and error paths

### Running Tests
- `just test` uses nextest for parallel execution (recommended).
- `cargo test` for standard test runner.
- `cargo test -- --nocapture` when debugging async failures or inspecting tracing output.
- Run `cargo test --features ...` if you introduce optional features.

### Integration Tests
- Integration tests may use `testcontainers` to spin up ClickHouse for indexer tests.
- Ensure Docker is running before executing integration tests.
- Clean up resources in test teardown to avoid port conflicts and leaked containers.

### Performance Tests
- Benchmark critical paths using Criterion in `benches/`.
- Focus on hot paths: validation, signature verification, entity scoring, serialization.
- Run `cargo bench` to execute all benchmarks and generate performance reports.

## Commit & Pull Request Guidelines

### Commit Message Style
- Follow the short, imperative style seen in history (e.g., `fix package name`, `add builder metrics`).
- Prefix with scope if it aids triage: `ingress:`, `forwarder:`, `entity:`, `validation:`, `chore:`, `fix:`, `feat:`.
- Examples:
  - `feat(forwarder): add retry logic for peer connections`
  - `fix(ingress): validate max txs per bundle`
  - `chore: bump version to 1.1.3`

### Pull Request Requirements
Each PR should include:
1. **Motivation**: Why is this change needed? What problem does it solve?
2. **Approach**: High-level overview of the implementation strategy.
3. **Risky Areas**: Call out potential edge cases, performance impacts, or breaking changes.
4. **Testing**: Describe how the change was tested (unit tests, integration tests, manual testing).
5. **Issue Links**: Reference BuilderNet issues when available (e.g., `Fixes #123`).
6. **Evidence**: Attach logs, metrics screenshots, or before/after comparisons for user-facing changes.

### CI Readiness Checklist
Before requesting review, ensure:
- [ ] `cargo build` succeeds
- [ ] `just fmt` produces no changes
- [ ] `just clippy` passes with no warnings
- [ ] `just test` passes all tests
- [ ] Integration tests pass if touching ingress, indexer, or forwarder

### Review Process
- Address reviewer feedback promptly.
- Keep commits focused; avoid mixing refactoring with feature work.
- Rebase on `main` before merging to maintain a clean history.

## Configuration & Runtime Notes

### CLI Configuration
The proxy is configured through CLI flags defined in `src/cli.rs` (`OrderflowIngressArgs` struct):
- **Required**: `--user-listen-url`, `--system-listen-url`, `--builder-name`
- **Optional**: `--builder-url`, `--builder-hub-url`, `--orderflow-signer`, `--metrics`, etc.
- All flags support environment variable overrides (e.g., `USER_LISTEN_ADDR`, `SYSTEM_LISTEN_ADDR`).

### Configuration Guidelines
When adding new configuration:
1. Document the flag in the struct's `#[arg(...)]` annotation with clear help text.
2. Choose safe defaults for local testing (loopback listeners, disabled external services).
3. Update `--help` text to reflect behavior changes.
4. Add validation logic in `src/runner/` if the flag affects startup behavior.
5. Update this document's Configuration section if the change is significant.

### Runtime Behavior
- The proxy uses Tokio's multi-threaded runtime for async operations.
- Jemalloc is the default allocator for better memory performance.
- Graceful shutdown is coordinated through `src/tasks/` using `TaskExecutor` and `TaskManager`.
- HTTP servers bind to configured addresses and serve JSON-RPC requests via Axum.

### Observability
- **Logs**: Structured logging via `tracing` (text or JSON format with `--log.json`).
- **Metrics**: Prometheus metrics exposed at `--metrics` endpoint (default: disabled).
- **Tracing**: Use `RUST_LOG` environment variable for log level control (e.g., `RUST_LOG=info,flowlink=debug`).

### Performance Considerations
- Rate limiting is disabled by default; enable with `--enable-rate-limiting`.
- Gzip compression is disabled by default; enable with `--http.enable-gzip`.
- Indexing is optional; ClickHouse/Parquet indexers only run if configured.
- Entity caches (order, signer) use LRU eviction with configurable TTLs.
- Priority queues have configurable capacities and backoff strategies.

## Architecture & Design Patterns

### Data Flow Overview
1. **Ingress** receives bundles/transactions via JSON-RPC over HTTP
2. **Validation** checks transaction intrinsics, signatures, and bundle structure
3. **Entity Scoring** computes priority based on sender reputation and bundle characteristics
4. **Priority Queues** buffer bundles by priority level (High/Medium/Low)
5. **Forwarder** distributes bundles to local builder and peer proxies
6. **Indexer** persists bundles and receipts for analytics (optional)

### Common Patterns

#### Error Handling
- Use `eyre::Result` for most fallible operations.
- Use `thiserror` for domain-specific error types with context.
- Log errors at appropriate levels: `error!` for unexpected failures, `warn!` for recoverable issues.
- Return structured errors from public APIs; log internal errors with context.

#### Async Patterns
- Spawn long-running tasks using `TaskExecutor` and `TaskManager` for graceful shutdown.
- Use `tokio::select!` for concurrent operations with cancellation support.
- Prefer bounded channels (`tokio::sync::mpsc`) over unbounded to prevent memory growth.
- Use `Arc<RwLock>` or `Arc<Mutex>` for shared mutable state; prefer immutable sharing when possible.

#### Observability Patterns
- Instrument public functions with `#[tracing::instrument]` for automatic span creation.
- Use structured fields in log messages: `tracing::info!(entity = %addr, "processing bundle")`.
- Emit metrics for critical operations: request counts, latencies, queue depths, error rates.
- Add custom metrics in `src/metrics.rs` using the `metrics` crate.

#### Testing Patterns
- Mock external dependencies using trait abstractions (see `PeerStore` trait in `builderhub.rs`).
- Use builder patterns for complex test fixtures.
- Leverage `proptest` for property-based testing of validation logic.
- Use `testcontainers` for integration tests requiring external services.

### Key Abstractions

- **`Entity`**: Represents a sender identified by signing address; tracks reputation and spam detection.
- **`Priority`**: Enum for bundle priority levels (High, Medium, Low).
- **`PriorityQueues`**: Multi-level queue system with per-level capacities and backoff.
- **`Indexer`**: Trait for persisting bundles/receipts (implementations: ClickHouse, Parquet).
- **`PeerStore`**: Trait for peer discovery (implementations: BuilderHub HTTP, local fallback).

### Dependencies & Ecosystem

Key dependencies and their roles:
- **`alloy`**: Ethereum primitives (transactions, signatures, addresses)
- **`rbuilder-primitives`**: BuilderNet-specific bundle types
- **`axum`**: HTTP framework for JSON-RPC APIs
- **`tokio`**: Async runtime
- **`tracing`**: Structured logging and observability
- **`metrics`**: Prometheus-compatible metrics collection
- **`clickhouse`**: ClickHouse client for indexing
- **`parquet`**: Parquet file format for indexing
- **`moka`**: Async-aware LRU caching
- **`criterion`**: Performance benchmarking

### Performance Hotspots
Monitor and optimize these areas:
1. **Signature Verification**: Uses `alloy` for ECDSA recovery; consider batching.
2. **Entity Scoring**: Computed per bundle; cache aggressively.
3. **Validation**: Runs on every transaction; keep logic fast and allocate minimally.
4. **Serialization**: JSON-RPC parsing and encoding; consider message size limits.
5. **Queue Operations**: High contention on shared queues; use per-level locks.

### Security Considerations
- Validate all user inputs before processing (see `validation.rs`).
- Rate limit by entity to prevent spam and DoS attacks.
- Reject bundles from entities with low scores (spam detection).
- Use TLS for peer communication (BuilderHub integration).
- Sign bundles when forwarding to peers for authentication.
- Never log private keys or sensitive configuration values.
