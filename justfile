set shell := ["bash", "-euo", "pipefail", "-c"]

# Build, run, and deploy the examples
mod examples

# The published crates
published := "-p partyline -p partyline-client -p partyline-dioxus -p partyline-worker"

# Crates that only build natively
native_only := "--exclude partyline-e2e --exclude orders-tail"

export WRANGLER_SEND_METRICS := "false"

[private]
default:
    @just --list --list-submodules

# Run every Rust check: formatting, lints, tests, and docs
check: fmt-check clippy test doc

# Format the code
fmt:
    cargo fmt --all

# Check the formatting
fmt-check:
    cargo fmt --all --check

# Lint the workspace natively and for wasm32
clippy:
    cargo clippy --locked --workspace --all-targets -- -D warnings
    cargo clippy --locked --workspace {{ native_only }} --target wasm32-unknown-unknown -- -D warnings

# Run the native tests: layers 1-3 and the hook tests
test:
    cargo test --locked

# Build the docs of the published crates, failing on warnings
doc:
    RUSTDOCFLAGS="-D warnings" cargo doc --locked --no-deps {{ published }}

# Check the dependencies for advisories, licenses, and sources
deny:
    cargo deny --locked check

# Build the published crates natively and for wasm32. Run it on the minimum supported Rust version, as docs/testing.md shows.
msrv:
    cargo check --locked {{ published }}
    cargo check --locked --target wasm32-unknown-unknown {{ published }}

# Check the relative links and anchors in the Markdown files. Links to other sites are not checked.
links:
    lychee --offline --no-progress --include-fragments --exclude-path target --exclude-path .devenv --exclude-path .claude .

# Build the Worker the end-to-end tests use for hooks only tests need
fixture:
    cd e2e/fixture && worker-build --release

# Run every end-to-end test: against the orders and chat examples and the fixture under wrangler dev
e2e: (examples::e2e "orders" "8790") (examples::e2e "chat" "8792") e2e-fixture

# Run the end-to-end tests against the fixture under wrangler dev
e2e-fixture: fixture
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/wrangler-dev.sh
    serve e2e/fixture 8791
    PARTYLINE_E2E_FIXTURE_URL=http://localhost:8791 \
        cargo test --locked -p partyline-e2e --test fixture -- --test-threads=1

# Run the browser transport tests in a headless browser against the orders example under wrangler dev. Set CHROMEDRIVER to pick the driver.
browser: (examples::build "orders")
    #!/usr/bin/env bash
    set -euo pipefail
    source scripts/wrangler-dev.sh
    serve examples/orders/worker 8790
    CARGO_TARGET_WASM32_UNKNOWN_UNKNOWN_RUNNER=wasm-bindgen-test-runner \
    PARTYLINE_TEST_URL=http://localhost:8790 \
        cargo test --locked -p partyline-client --target wasm32-unknown-unknown --features web-wake --test browser
