# display a help message about available commands
default:
  @just --list --unsorted

clippy:
  cargo clippy --all-targets --all-features -- -D warnings

fmt:
  rustup toolchain install nightly-2025-09-21 --component rustfmt > /dev/null 2>&1 && \
  cd {{justfile_directory()}} && \
  cargo +nightly-2025-09-21 fmt

test:
  cargo nextest run --lib --bins --no-tests=warn --retries 3
