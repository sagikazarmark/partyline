# partyline-dioxus

[![crates.io](https://img.shields.io/crates/v/partyline-dioxus?style=flat-square)](https://crates.io/crates/partyline-dioxus)
[![docs.rs](https://img.shields.io/docsrs/partyline-dioxus?style=flat-square)](https://docs.rs/partyline-dioxus)

> partyline is inspired by [PartyKit](https://www.partykit.io/) but is not a port. It is not affiliated with Cloudflare or PartyKit, and is not wire-compatible with PartyServer or partysocket.

Dioxus 0.7 hooks for [partyline](https://github.com/sagikazarmark/partyline). The hooks own the driver's lifetime, expose status and cursor as signals, and open nothing during server-side rendering.

| Hook | Use it for | Returns |
| --- | --- | --- |
| `use_channel::<C>(options, on_message)` | Any channel. Full control over events and resets | `UseChannel`: `status()`, `cursor()`, `wake()`, `reconnect()` |
| `use_channel_latest::<C>(options)` | Latest-mode channels | `ReadSignal<Option<C::Event>>` and the handle |
| `use_channel_reducer::<C, S>(options, initial, reduce, refetch)` | Log-mode channels that fold events into state | `ReadSignal<S>` and the handle |

```rust
#[component]
fn OrderStatus(id: String, initial: Order, head: Cursor) -> Element {
    let mut order = use_signal(|| initial);

    let channel = use_channel::<Orders>(
        ChannelOptions::new(id.clone()).since(head),
        move |msg| match msg {
            ChannelMessage::Event(event) => order.write().apply(&event),
            ChannelMessage::Reset => { /* refetch the order, then order.set(..) */ }
        },
    );

    rsx! {
        if channel.status() != Status::Open {
            span { class: "badge", "Reconnecting" }
        }
        OrderView { order }
    }
}
```

`PartylineProvider` puts the base URL, the token provider, and the client config in context.
On web the base URL defaults to the page origin, so most apps configure nothing.

## Lifecycle

- **Start.** The driver starts from an effect. Effects do not run during server-side rendering.
- **Stop.** On unmount the hook closes the socket with 1000.
- **Change.** When the channel ID changes, the hook stops the old driver and starts a new one from the new `since`. When the effective base URL or client config changes, or after `reconnect()`, it starts a new one from the current cursor. A new handler or token provider does not restart the driver: the latest one is called.
- **Disable.** `ChannelOptions::enabled(false)` stops the driver, keeps the cursor, and sets the status to `Stopped { reason: StopReason::App }`. Enabling again resumes from the cursor.
- **Wake.** On wasm, the client wakes on `visibilitychange`, `online`, `pageshow` from the back-forward cache, `resume`, and network changes.

A client that receives an event it cannot decode stops with `Stopped { reason: StopReason::Incompatible { .. } }` instead of reconnecting. The app is older than the server: ask the user to reload.

## Reducer

`use_channel_reducer` folds events into state, and refetches the state after a reset:

```rust
let (order, channel) = use_channel_reducer::<Orders, Order, _, _, _>(
    ChannelOptions::new(id.clone()).since(head),
    move || initial.clone(),                      // the state at `since`, again after an ID change
    |order, event| order.apply(&event),
    move || fetch_order(id.clone()),               // Result<(Order, Cursor), impl Display>
);
```

Events that arrive during a refetch are buffered and applied to the fresh state when they are newer than its head. A failed refetch is logged with `tracing` and retried after 1 s, doubling up to 30 s. A change of channel ID resets the state with `initial` and discards a refetch in flight.

Two components that subscribe to the same channel open two sockets in 0.1.

## License

Licensed under either of

- Apache License, Version 2.0 ([LICENSE-APACHE](LICENSE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0>)
- MIT license ([LICENSE-MIT](LICENSE-MIT) or <http://opensource.org/licenses/MIT>)

at your option.

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be
dual licensed as above, without any additional terms or conditions.
