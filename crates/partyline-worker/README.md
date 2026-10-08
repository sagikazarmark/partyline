# partyline-worker

[![crates.io](https://img.shields.io/crates/v/partyline-worker?style=flat-square)](https://crates.io/crates/partyline-worker)
[![docs.rs](https://img.shields.io/docsrs/partyline-worker?style=flat-square)](https://docs.rs/partyline-worker)

> partyline is inspired by [PartyKit](https://www.partykit.io/) but is not a port. It is not affiliated with Cloudflare or PartyKit, and is not wire-compatible with PartyServer or partysocket.

The Cloudflare Durable Object hub of [partyline](https://github.com/sagikazarmark/partyline).
Each channel ID is one Durable Object that owns the sockets and the event log.

- **`Hub`**: embed it in your own Durable Object, or generate the object with `channel_object!`.
- **`Connect`**: forward a client's WebSocket upgrade from the Worker.
- **`Publisher`**: publish, read the head, close sockets by tag, and reset a channel.
- **`reject`**: refuse an upgrade with a close code, such as 4401 or 4403.

The Durable Object class must use the SQLite storage backend.
The hub holds no state in memory and answers heartbeats with the runtime's auto-response, so the Durable Object can hibernate with sockets open.

## Generate the Durable Object

```rust
partyline_worker::channel_object! {
    /// Durable Object for the orders channel.
    pub struct OrderChannel: Orders;
}

partyline_worker::channel_object! {
    pub struct ActivityChannel: Activity {
        config = HubConfig::default().retain_events(500);
    }
}
```

The struct name is the `class_name` in the wrangler configuration.
`#[durable_object]` emits absolute `::worker::` paths, so your crate must depend on `worker` under that name.

## Embed the hub by hand

An object that owns its own state or routes embeds the hub as a field and delegates each handler.
See the `Hub` documentation for the full form, and [`examples/orders`](https://github.com/sagikazarmark/partyline/tree/main/examples/orders) for an object that applies each event to its own table before it publishes.

## Use it from the Worker

Route parameters usually arrive percent-encoded, because the client encodes the channel ID in the connect path. Decode the ID with `decode_segment` before you pass it to `Connect` or `Publisher`, so both name the same Durable Object. axum's `Path` extractor decodes for you.

```rust
// Forward a client upgrade. Authorize first.
let response = Connect::<Orders>::new(&order_id)
    .tag(&user_id)
    .forward(&env, "ORDER_CHANNEL", req)
    .await?;

// Publish from any code path.
let cursor = Publisher::<Orders>::new(&env, "ORDER_CHANNEL")?
    .publish(&order_id, &OrderEvent::StatusChanged { status })
    .await?;

// Close a user's sockets on sign-out. 4403 stops the client.
Publisher::<Orders>::new(&env, "ORDER_CHANNEL")?
    .close_tagged(&order_id, &user_id, close::FORBIDDEN)
    .await?;
```

## Defaults

| Setting | Default |
| --- | --- |
| Retention, Log mode | 1,000 events or 24 hours, whichever is smaller |
| Retention, Latest mode | 1 event |
| Target event size | Under 16 KB. Send IDs and let the client fetch large payloads |

`Connect::forward` removes the `token` query parameter before it forwards the upgrade, so tokens stay out of the Durable Object's request logs.
One Durable Object accepts at most 32,768 WebSocket connections.
A socket carries at most 10 tags of at most 256 characters each.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
