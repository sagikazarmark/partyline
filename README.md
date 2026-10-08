# partyline

[![GitHub Workflow Status](https://img.shields.io/github/actions/workflow/status/sagikazarmark/partyline/ci.yaml?style=flat-square)](https://github.com/sagikazarmark/partyline/actions/workflows/ci.yaml)
[![OpenSSF Scorecard](https://api.securityscorecards.dev/projects/github.com/sagikazarmark/partyline/badge?style=flat-square)](https://securityscorecards.dev/viewer/?uri=github.com/sagikazarmark/partyline)
[![crates.io](https://img.shields.io/crates/v/partyline?style=flat-square)](https://crates.io/crates/partyline)
[![docs.rs](https://img.shields.io/docsrs/partyline?style=flat-square)](https://docs.rs/partyline)

> partyline is inspired by [PartyKit](https://www.partykit.io/) but is not a port. It is not affiliated with Cloudflare or PartyKit, and is not wire-compatible with PartyServer or partysocket.

**Sequenced, resumable events from a Cloudflare Durable Object to Dioxus clients over WebSockets.**
A client that loses its connection reconnects with its last cursor and receives exactly the events it missed.

**Live demo:** a live poll that an audience opens on their phones. See [`examples/poll`](examples/poll).

## Features

- **Shared types.** You define each event type once and use it in the Durable Object and in the client.
- **Resume.** Delivery is at least once, with a per-channel sequence number. The client removes duplicates.
- **Two channel modes.** Log mode keeps an event log and replays it. Latest mode keeps only the most recent value.
- **Free when idle.** The hub keeps nothing in memory, so the Durable Object can hibernate with sockets open.
- **Web and native.** The client runs on Dioxus web (wasm), desktop, and mobile.
- **Testable offline.** All reconnect and resume logic is a pure state machine.

## Crates

| Crate | Use it in | Contents |
| --- | --- | --- |
| [`partyline`](crates/partyline) | Both sides | Protocol types, the sans-IO client state machine, replay rules, a loopback test harness |
| [`partyline-worker`](crates/partyline-worker) | The Worker | The Durable Object `Hub`, the `channel_object!` macro, `Connect` and `Publisher` |
| [`partyline-client`](crates/partyline-client) | The client | WebSocket transports for web and native, the runtime-agnostic driver |
| [`partyline-dioxus`](crates/partyline-dioxus) | The client | `use_channel`, `use_channel_latest`, `use_channel_reducer`, `PartylineProvider` |

## Quick start

This is a summary of [`examples/orders`](examples/orders), simplified.

**1. Define the channel** in a crate that the Worker and the client share:

```rust
use partyline::{Channel, Mode};

pub struct Orders;

impl Channel for Orders {
    const NAME: &'static str = "orders";
    const MODE: Mode = Mode::Log;
    type Event = OrderEvent; // Serialize + Deserialize + Clone
}
```

**2. Add the Durable Object** to the Worker. One order is one Durable Object. It owns the order and embeds the channel's hub, so it applies and publishes each event in one turn:

```rust
#[durable_object]
pub struct OrderChannel {
    hub: Hub<Orders>,
}

impl DurableObject for OrderChannel {
    fn new(state: State, _env: Env) -> Self {
        Self { hub: Hub::new(state, HubConfig::default()) }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        match req.path().as_str() {
            // The order and the channel head it was read at, in one turn.
            "/order" => Response::from_json(&OrderSnapshot {
                order: self.load()?,
                head: self.hub.head()?,
            }),
            // WebSocket upgrades and the hub's own routes. `Publisher::publish` lands here:
            // the closure applies each event in the same turn as the hub stores and sends it.
            _ => self.hub.fetch_with(req, |event| self.apply(event)).await,
        }
    }

    // websocket_message, websocket_close, websocket_error, and alarm delegate to the hub.
    partyline_worker::hub_handlers!(hub);
}
```

The class must use the SQLite storage backend:

```toml
# wrangler.toml
[[durable_objects.bindings]]
name = "ORDER_CHANNEL"
class_name = "OrderChannel"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["OrderChannel"]
```

A channel with no state of its own needs no hand-written object: `channel_object! { pub struct ActivityChannel: Activity; }` generates it.
[`examples/poll`](examples/poll) uses both forms.

**3. Route upgrades and publish** from the Worker:

```rust
// GET /partyline/orders/{id}. Match the path and decode the ID, authorize, then forward.
let Some(Ok(connect)) = Connect::<Orders>::from_path(&req.path()) else {
    return reject(close::BAD_REQUEST, "invalid id");
};
connect.forward(&env, "ORDER_CHANNEL", req).await

// From any code path:
Publisher::<Orders>::new(&env, "ORDER_CHANNEL")?
    .publish(&order_id, &OrderEvent::StatusChanged { status })
    .await?;
```

**4. Load, then subscribe** in a Dioxus component. The page loads the order and its head with one request, then subscribes from that head, so no event between the two is lost:

```rust
#[component]
fn OrderStatus(id: String, initial: Order, head: Cursor) -> Element {
    let mut order = use_signal(|| initial);
    let channel = use_channel::<Orders>(
        ChannelOptions::new(id.clone()).since(head),
        move |msg| match msg {
            ChannelMessage::Event(event) => order.write().apply(&event),
            ChannelMessage::Reset => { /* refetch the order */ }
        },
    );
    // Render `order` and `channel.status()`.
}
```

## Documentation

The [documentation index](docs/README.md) lists every page. Good places to start:

- [Tutorial: your first channel](docs/tutorial/first-channel.md)
- [How partyline works](docs/explanation/how-it-works.md)
- [Choosing a mode](docs/explanation/choosing-a-mode.md)
- [Protocol reference](docs/protocol.md)
- [How to authenticate connections with Clerk](docs/how-to/authenticate-with-clerk.md)

For contributors: [Testing](docs/testing.md) and the design records: the [implementation plan](docs/design/plan.md) and the [M0 spike notes](docs/design/m0-spike.md).

## Development

The tools come from [devenv](https://devenv.sh): run the commands below in `devenv shell`. CI runs the same [just](https://just.systems) recipes.

```shell
just check                 # formatting, lints, layers 1-3 (native), and docs
just e2e                   # layer 4, against the examples and the fixture under wrangler dev
just examples e2e orders   # layer 4, against one example
just browser               # layer 3 (browser), in headless Chrome
just examples dev orders   # run an example on http://localhost:8787
just                       # list every recipe
```

The minimum supported Rust version is 1.91, the higher of what `dioxus` and `worker` require. CI does not check it; see [Testing](docs/testing.md) to check it locally.

The four crates share one version and are released together with `cargo release`, under one `v{version}` tag.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
