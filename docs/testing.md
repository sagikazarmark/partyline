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

```shell
# Layers 1-3 (native) and the hook tests
cargo test

# Layer 3 (browser), in headless Chromium against the orders example under wrangler dev
dagger check examples:browser

# Layer 4, against the orders and chat examples and the e2e fixture under wrangler dev
dagger check examples:end-to-end
```

Both run in containers with Dagger, so they need only Docker and the Dagger CLI.
`dagger check examples` also builds every example, and runs these checks:

| Check | What it proves |
| --- | --- |
| `examples:end-to-end-oldest-compatibility-date` | Layer 4 passes with every Worker at compatibility date 2025-04-01, as well as at the date the examples pin. Hibernation, the auto-response, and close handling depend on compatibility flags |
| `examples:msrv` | The four published crates build on Rust 1.91, natively and for wasm32 |
| `examples:links` | Every relative link and anchor in the Markdown files resolves. Links to other sites are not checked |

`dagger check -l` lists every check.

Run an example locally with `dagger call examples orders service up --ports 8787:8787`.
Deploy one with `dagger call examples poll deploy --account-id <id> --api-token env://CLOUDFLARE_API_TOKEN`.

The link check uses [lychee](https://lychee.cli.rs). To run it without Dagger:

```shell
lychee --offline --include-fragments --exclude-path target --exclude-path .devenv --exclude-path .claude .
```

## Layer 2 carries the main guarantee

The harness connects the client state machine to the server logic with `MemLog` through an in-memory pipe.
The pipe can drop the connection at any frame, lose a single frame, duplicate frames, delay them, and fail connect attempts.
A property test publishes a random event sequence under random faults and checks one invariant:
in Log mode the app sees every event exactly once and in order, or it sees `Reset`.

The server side of the harness calls the same `server::handshake` and `server::publish` functions as the Workers hub.

## Manual hibernation check

Layer 4 cannot force hibernation locally. The hub is safe by construction, because it holds no state in memory. Confirm it once on a deployed Worker:

1. Deploy the demo.
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
