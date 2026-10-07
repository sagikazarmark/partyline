# partyline

[![crates.io](https://img.shields.io/crates/v/partyline?style=flat-square)](https://crates.io/crates/partyline)
[![docs.rs](https://img.shields.io/docsrs/partyline?style=flat-square)](https://docs.rs/partyline)

> partyline is inspired by [PartyKit](https://www.partykit.io/) but is not a port. It is not affiliated with Cloudflare or PartyKit, and is not wire-compatible with PartyServer or partysocket.

The core of [partyline](https://github.com/sagikazarmark/partyline): everything that needs no I/O.
Every other partyline crate depends on it. It compiles for `wasm32-unknown-unknown` and native targets.

| Module | Contents |
| --- | --- |
| `frame` | `ServerFrame`, `Cursor`, `Mode`, connect parameters, close codes |
| `codec` | JSON encoding |
| `client` | The sans-IO client state machine: backoff, heartbeat, dedupe, gap detection |
| `server` | The replay decision, the retention policy, the `Log` trait, and the in-memory `MemLog` |
| `testing` | A loopback harness that runs the client against the server logic over a faulty pipe |

## Define a channel

Define each channel once, in a crate that the Worker and the client share:

```rust
use partyline::{Channel, Mode};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub enum OrderEvent {
    StatusChanged { status: String },
}

pub struct Orders;

impl Channel for Orders {
    const NAME: &'static str = "orders";
    const MODE: Mode = Mode::Log;
    type Event = OrderEvent;
}
```

Use `Mode::Latest` when a client that missed updates only needs the final value.
Use `Mode::Log` only where a missed intermediate event is a bug.

## Test with the loopback harness

```rust
use partyline::testing::{Faults, Loopback};

let mut lb = Loopback::<Orders>::new(seed, Faults::default());
lb.start();
lb.publish(&event);
lb.advance_ms(1_000);
lb.heal();
lb.settle();
assert_eq!(lb.client().cursor(), Some(lb.head()));
```

The [protocol reference](https://github.com/sagikazarmark/partyline/blob/main/docs/protocol.md) describes the wire format.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
