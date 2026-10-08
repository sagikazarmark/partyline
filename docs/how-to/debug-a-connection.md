# How to debug a connection

This guide shows where to look when a client does not connect, keeps reconnecting, or stops.
Start with the client's status. It usually names the problem.

## 1. Read the status

Show the status somewhere while you debug. Print it with `{:?}`: it carries the close code and the attempt number.

```rust
let channel = use_channel::<Orders>(options, on_message);
rsx! { pre { "{channel.status():?}" } }
```

| Status | Meaning | Look at |
| --- | --- | --- |
| `Connecting` for a long time | The socket does not open, or `Hello` does not arrive within 10 s | The route, step 3 |
| `Waiting { attempt, last_code, .. }` with a growing `attempt` | Each attempt fails. `last_code` is the close code of the last one, `None` if the socket never opened | The close code, step 2 |
| `Unauthorized { .. }` | The server closed with 4401 | Your token provider and token check |
| `Stopped { reason: Closed(code) }` | The server closed with a terminal code | The close code, step 2 |
| `Stopped { reason: Incompatible { seq } }` | The client cannot decode event `seq`: the Worker is newer than the client | [Evolve event types](evolve-event-types.md). Prompt a reload |
| `Stopped { reason: InvalidUrl }` | The base URL is not `http`, `https`, `ws`, or `wss`, or `BaseUrl::SameOrigin` was used outside a browser | `PartylineProvider` or `ConnectOptions` |
| `Stopped { reason: App }` | The app stopped the client, or the component unmounted | Your component tree |
| `Open`, but no events | The socket is fine. Events go to another channel ID, or nothing publishes | The channel ID, step 3 |

## 2. Map the close code

| Code | Sent by | Usual cause |
| --- | --- | --- |
| 1006 | The browser | The connection dropped, or the upgrade never reached the hub |
| 1011 | The hub | A storage error during the handshake, or a failed send. See the Worker logs |
| 1012 | The hub | The channel was reset with `Publisher::reset`. The client reconnects and gets `Reset` |
| 4400 | The hub, or your route | A malformed connect URL, or your route rejected the ID |
| 4401 | Your route | Token missing or expired |
| 4403 | Your route, or `close_tagged` | Not allowed, or the user signed out |
| 4426 | The hub | The client speaks another protocol version |

The [protocol reference](../protocol.md#close-codes) lists which codes the client retries.

## 3. Check the route

The client connects to `/partyline/{channel}/{id}`, with the channel's `NAME` and the percent-encoded ID.

- **The Worker must run before the assets.** With `[assets]` and `not_found_handling = "single-page-application"`, a request the Worker does not handle returns `index.html`, and the upgrade fails with 1006. Set `run_worker_first = ["/api/*", "/partyline/*"]`.
- **The ID must be decoded once.** Decode the route parameter with `decode_segment`, or use `Connect::from_path`. axum's `Path` decodes for you. An ID decoded twice, or not at all, names another Durable Object: the socket opens, but publishes go elsewhere.
- **The request must be an upgrade.** `Connect::forward` answers anything else with HTTP 426.

Open the browser's developer tools, Network tab, filter by "WS". The connect request shows the URL and the status. The Messages view shows the frames: `hello`, `event`, `reset`, and the `ping`/`pong` heartbeat every 25 s.

## 4. Read the Worker logs

```shell
npx wrangler tail <worker-name>
```

Locally, `wrangler dev` prints the same logs. The hub logs its own errors with `console_error!` and the prefix `partyline:`:

| Log line starts with | Meaning |
| --- | --- |
| `partyline: creating tables failed` | The class does not use the SQLite storage backend. Add it to `new_sqlite_classes` in a migration |
| `partyline: auto-response failed` | The heartbeat auto-response could not be set. Heartbeats still work, but wake the Durable Object |
| `partyline: handshake failed` | Reading the log failed. The client gets 1011 and retries |
| `partyline: handshake send failed` | The socket closed during the handshake. The client retries |
| `partyline: dropping socket tag` | A tag broke the rules. `Connect` checks tags, so this means a request reached the hub another way |

A failed publish shows up in your Worker as an error from `Publisher::publish`, such as `partyline hub returned 413: event is 70000 bytes, more than the limit of 65536`. See [Limits](../explanation/limits.md).

## 5. Turn on client tracing

The `tracing` feature of `partyline-client` logs every input and output of the client state machine at trace level, and transport errors at debug level.

```shell
cargo add partyline-client --features tracing
```

A Dioxus app depends on `partyline-client` through `partyline-dioxus`. Add it directly with the feature, at the same version; Cargo merges the features.

Then install a `tracing` subscriber. In a Dioxus app, `dioxus::logger::init(dioxus::logger::tracing::Level::TRACE)` sends the logs to the browser console.
On native, use `tracing-subscriber` with a filter such as `partyline_client=trace`.

A typical failing sequence reads: `Start`, `Connect { cursor }`, `Closed { code: Some(4401) }`, `Status(Unauthorized { .. })`, then a new `Connect` after the delay.

## Related

- [Glossary](../explanation/glossary.md)
- [Testing: the manual hibernation check](../testing.md#manual-hibernation-check)
