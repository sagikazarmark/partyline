# partyline-client

[![crates.io](https://img.shields.io/crates/v/partyline-client?style=flat-square)](https://crates.io/crates/partyline-client)
[![docs.rs](https://img.shields.io/docsrs/partyline-client?style=flat-square)](https://docs.rs/partyline-client)

> partyline is inspired by [PartyKit](https://www.partykit.io/) but is not a port. It is not affiliated with Cloudflare or PartyKit, and is not wire-compatible with PartyServer or partysocket.

The client of [partyline](https://github.com/sagikazarmark/partyline). It connects the core state machine to a real socket and real timers.

It is runtime-agnostic: `connect` returns a driver future and never spawns, so Dioxus, tokio, or `wasm-bindgen-futures` can run it.

| Target | Transport | Built on |
| --- | --- | --- |
| `wasm32` | `BrowserSocket` | `gloo-net`. Same-origin cookies are sent with the handshake |
| Native | `NativeSocket` | `tokio-tungstenite` with `rustls` and the Mozilla roots. No system OpenSSL. Needs a tokio runtime |

## Usage

```rust
let (handle, mut events, driver) = partyline_client::connect::<Orders>(ConnectOptions {
    since: Some(head),
    ..ConnectOptions::new(BaseUrl::SameOrigin, order_id.clone())
});

spawn(driver); // the caller chooses the executor

while let Some(msg) = events.next().await {
    match msg {
        ClientEvent::Event { event, .. } => apply(event),
        ClientEvent::Reset => refetch().await,
        ClientEvent::Status(status) => show(status),
    }
}

handle.wake(); // connect now, or probe an open socket
handle.stop(); // close with 1000 and end the driver
```

## Status

| Status | Meaning |
| --- | --- |
| `Idle` | Not started |
| `Connecting` | Opening a socket and waiting for `Hello` |
| `Open` | Connected and receiving events |
| `Waiting { retry_in, attempt, last_code }` | Waiting `retry_in` before connect attempt `attempt`. `last_code` is the close code of the failed connection |
| `Unauthorized { retry_in, attempt }` | The server rejected the token. The next connect asks the token provider for a fresh one |
| `Stopped { reason }` | Stopped for good. The driver has ended |

`StopReason` says why the client stopped:

| Reason | Meaning |
| --- | --- |
| `App` | The app called `stop`, or dropped every handle |
| `Closed(code)` | The server closed with a terminal code, such as 4403 |
| `Incompatible { seq }` | The server sent an event this build cannot decode. The app is outdated: ask the user to reload. The client does not reconnect, because the server would replay the same event |
| `InvalidUrl` | The base URL could not be resolved |

## Authentication

Set a `TokenProvider`. The driver calls it before every connect and puts the result in the `token` query parameter, because browsers cannot set headers on a WebSocket.
After a 4401 close, the provider receives `TokenRequest { refresh: true }`.
See [how to authenticate with Clerk](https://github.com/sagikazarmark/partyline/blob/main/docs/how-to/authenticate-with-clerk.md).

## Features

| Feature | Effect |
| --- | --- |
| `web-wake` | Wakes the client on `visibilitychange` and `online` in the browser |

Native targets have no wake source. The heartbeat finds a dead socket within 35 s with the default settings.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
