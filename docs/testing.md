# Testing

partyline is tested in five layers. Layers 1 to 4 run in CI. Layer 5 and the hibernation check are manual.

| Layer | What it proves | Where | Runtime needed |
| --- | --- | --- | --- |
| 1. State machine | Backoff, timers, dedupe, gap handling, terminal codes | `crates/partyline/src/client.rs` | None |
| 2. Loopback | Client and server logic agree under faults | `crates/partyline/tests/loopback.rs` | None |
| 3. Transport | Each socket type maps open, message, close, and error correctly; the driver resumes | `crates/partyline-client/tests/` | A local WebSocket server; headless Chrome for wasm |
| 4. End to end | The Durable Object, the SQLite log, and the Worker helpers work in workerd | `e2e/` | `wrangler dev` |
| 5. Phone matrix | Real mobile browsers recover as designed | Manual | The deployed demo and two phones |

The Dioxus hooks are tested in a real `VirtualDom` against a local server in `crates/partyline-dioxus/tests/hooks.rs`.

## Run the tests

The tests run with [just](https://just.systems), in `devenv shell`, which provides every tool. CI runs the same recipes.

```shell
# Layers 1-3 (native) and the hook tests, with formatting, lints, and docs
just check

# Layer 3 (browser), in headless Chrome against the orders example under wrangler dev
just browser

# Layer 4, against the orders and chat examples and the e2e fixture under wrangler dev
just e2e

# Layer 4, against one of them
just examples e2e orders
just examples e2e chat
just e2e-fixture
```

`just browser` needs Chrome and a matching `chromedriver`. Set `CHROMEDRIVER` to the driver's path if it is not on the `PATH`.
`just e2e` and `just browser` build the examples they need first, and run `wrangler dev` on ports 8790 to 8792. `just examples e2e` takes the port as a second argument and defaults to 8790.
They start `wrangler dev` with [`scripts/wrangler-dev.sh`](../scripts/wrangler-dev.sh), which waits until the Worker answers and stops it when the tests end.

### In CI

[CI](../.github/workflows/ci.yaml) runs the same recipes in parallel jobs:

- One job runs `just check` and `just links`.
- Each example builds in its own job. The orders and chat jobs also run `just examples e2e`, and the orders job runs `just browser`.
- One job runs `just e2e-fixture`.

CI also runs these checks:

| Check | What it proves |
| --- | --- |
| `just examples build <example>` | Every example builds |
| `just links` | Every relative link and anchor in the Markdown files resolves. Links to other sites are not checked. It uses [lychee](https://lychee.cli.rs) |

CI does not check the minimum supported Rust version. To check that the four published crates build on Rust 1.91, natively and for wasm32, run:

```shell
devenv --option languages.rust.version:string 1.91.0 shell -- just msrv
```

`just --list --list-submodules` lists every recipe.

Run an example locally with `just examples dev orders`. See [Run the examples locally](how-to/run-examples-locally.md).
Deploy one with `just examples deploy poll`.

## Layer 2 carries the main guarantee

The harness connects the client state machine to the server logic with `MemLog` through an in-memory pipe.
The pipe can drop the connection at any frame, lose a single frame, duplicate frames, delay them, and fail connect attempts.
A property test publishes a random event sequence under random faults and checks one invariant:
in Log mode the app sees every event exactly once and in order, or it sees `Reset`.

The server side of the harness calls the same `server::handshake` and `server::publish` functions as the Workers hub.

## Manual hibernation check

Layer 4 cannot force hibernation locally. The hub is safe by construction, because it holds no state in memory. Confirm it once on a deployed Worker:

1. [Deploy the demo](../examples/poll/README.md#deploy-the-demo).
2. Open the phone view on one device. Wait until the badge shows "Live".
3. Leave the socket idle for at least 5 minutes. The Durable Object hibernates after about 10 seconds without events.
4. Vote from another device.
5. Confirm that the first device shows the vote within a second.
6. In the Cloudflare dashboard, open the Durable Object metrics for `ActivityChannel` and `PollObject`. Confirm that the billed duration during the idle period is near zero.

## Layer 5: phone matrix

Run on iOS Safari and Android Chrome against the deployed demo. In each case, vote from a second device while the phone under test is away.

| Case | Steps |
| --- | --- |
| Background, 1 minute | Switch to another app for 1 minute, then return |
| Background, 10 minutes | Switch to another app for 10 minutes, then return |
| Screen locked | Lock the screen for 1 minute, then unlock |
| Airplane mode | Turn on airplane mode for 30 seconds, then turn it off |
| Network switch | Switch from Wi-Fi to mobile data, or back |

A case passes when the phone shows the correct tally and the missed votes in the feed within a few seconds of returning, with no manual refresh.

Record each run:

| Date | Device | Browser | Case | Result | Notes |
| --- | --- | --- | --- | --- | --- |
| | | | | | |
