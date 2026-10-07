# M0 spike notes

M0 existed to remove the unknowns in the Rust bindings for Durable Objects before the design was fixed.
Instead of a throwaway Worker, the questions were answered against `worker` 0.8.7 with the real hub, the orders example, and the demo, running in workerd under `wrangler dev` 4.148.

## Checklist

| Question | Result | Evidence |
| --- | --- | --- |
| `state.storage().sql().exec(..)` works for inserts, range reads, and deletes, and is synchronous from Rust | Yes. `SqlStorage::exec` is synchronous and returns a cursor | `SqlLog`; the end-to-end resume, reset, and snapshot tests |
| `worker` exposes the WebSocket auto-response | Yes: `State::set_websocket_auto_response` with `WebSocketRequestResponsePair` | `Hub::new` sets `ping`/`pong`; the end-to-end tests receive `pong` |
| `accept_websocket_with_tags` and tag-filtered socket lookup work as in JavaScript | Yes: `accept_websocket_with_tags`, `get_websockets_with_tag` | `close_by_tag_closes_only_that_users_sockets` |
| The Worker can route an upgrade next to its other routes | Yes, with `worker::Router` | The orders example and the demo |
| ... including through an axum router on Workers | Yes, with `Connect::forward_http` (feature `http`). The WebSocket survives the `http` conversions in the response extensions | The e2e fixture routes through axum: `e2e/tests/fixture.rs` |
| Frames sent on the server socket before the 101 response arrive first | Yes | Every end-to-end test reads `Hello` and the replay first |
| `#[durable_object]` expands correctly when a `macro_rules!` macro in another crate emits it | Yes, once `wasm_bindgen` is in scope (see below) | The demo's `ActivityChannel` and the e2e fixture's `TickChannel` are built with `channel_object!` and run under `wrangler dev` |
| Publish the `0.0.0` placeholders | **Open.** Needs the crate owner's crates.io token | See [Releasing](how-to/release.md) |
| A phone receives a broadcast after the Durable Object has hibernated | **Open.** Hibernation cannot be forced locally | The manual check in [Testing](testing.md#manual-hibernation-check) |

## Awkward `worker` APIs and how partyline handles them

| API | Problem | Handling |
| --- | --- | --- |
| `DurableObject` default handlers | `alarm`, `websocket_message`, `websocket_close`, and `websocket_error` default to `unimplemented!()`, which aborts the instance | `channel_object!` always emits all of them, including `websocket_error` |
| `SqlStorageValue::Integer` | Values pass through a JavaScript number, so integers above 2^53 lose precision | Epochs are drawn from `[1, 2^53)`. `SqlLog` refuses larger values |
| `ScheduledTime::from(i64)` | The integer is an offset from now, not a timestamp | The hub builds a `js_sys::Date` for an absolute alarm time |
| `State::set_websocket_auto_response` | Panics on a JavaScript error | Called once per instance with constant strings |
| No `transactionSync` binding | A multi-statement transaction cannot be opened from Rust | The runtime commits all writes made without an intervening `await` atomically. `SqlLog::append` inserts before it moves the head, so a failed insert leaves the head unchanged |
| `worker` macros | `#[durable_object]` emits absolute `::worker::` paths. With `wasm-bindgen` 0.2.129 its expansion also names `wasm_bindgen` unqualified, which fails unless `wasm_bindgen` is in scope | The app crate depends on `worker` under that name. `channel_object!` expands inside an anonymous `const` that imports `::worker::wasm_bindgen`. A hand-written object needs `use worker::*;` or an explicit import |
| axum on Workers | Handlers must be `Send`, and `worker` types are not | `#[worker::send]` on handlers. axum's `Path` extractor percent-decodes the ID |

The fallbacks from the risk table were not needed: the log uses SQLite, and heartbeats use the auto-response. `Hub::on_message` still answers `ping` in case the runtime delivers it.

## Changes to the plan

The implementation follows the plan, with these changes. Each one came from a problem found while building.

| Area | Plan | Implementation | Why |
| --- | --- | --- | --- |
| `Log` trait | Infallible methods | Each method returns `Result<_, Self::Error>`. `MemLog` uses `Infallible` | SQL calls can fail, and a panic aborts the Worker instance |
| Client timers | Four timer IDs | Five: the fifth, `Stable`, resets the attempt counter after 10 s open | The state machine reads no clock, so "open for 10 s" needs a timer |
| 4401 handling | "A status the driver uses to refresh the token" | `Status::Unauthorized { retry_in }`, and the token provider receives `TokenRequest { refresh: true }` | The provider needs to know when to skip a cached token |
| Latest mode without a cursor | "Live events only" | The hub sends the current value | Each Latest event is the full value. Without it, `use_channel_latest` shows nothing until the next publish |
| Channel reset | Not in the hub routes | `POST /reset` and `Publisher::reset`: wipe the log, new epoch, close sockets with 1012 | The demo's "Reset demo" needs an epoch change. Closing the sockets makes clients reconnect and receive `Reset` |
| Rejecting an upgrade | Not specified | `partyline_worker::reject(code, reason)` | Authorization in the route handler needs a way to send 4401 and 4403 |
| `use_channel_reducer` | `refetch` returns the state | `refetch` returns the state and the head it was read at. Events that arrive during the refetch are buffered and applied after that head | Without the head, events during a refetch are lost or applied twice |
| Client `rng` | `impl FnMut() -> f64 + 'static` | Also `Send` | The native driver future is `Send`, so it can run on a multi-threaded tokio runtime |
| Layout | The demo lives in `demo/` | `examples/poll`, next to `examples/orders` | Every runnable app lives in `examples/`. The poll is still the public demo |
| Test hooks | Not specified | Test-only behavior (magic tokens, close-by-tag and reset routes) lives in the `e2e/fixture` Worker, not in the examples | Keeps `examples/orders` minimal and keeps those routes off public deployments |

## Tooling notes

- `dx` 0.7.9 resolves the workspace's `default-members` relative to the current directory and panics in a member directory. Run `dx bundle --package <name>` from the workspace root.
- `wrangler dev` does not pick up assets that change after it starts. Restart it after a web build.
- `wasm-bindgen-test-runner` must match the `wasm-bindgen` version in `Cargo.lock`.
