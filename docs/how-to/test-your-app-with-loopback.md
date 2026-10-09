# How to test your app with the loopback harness

This guide shows how to test your own client logic against partyline's real client and server code, with dropped connections and duplicate frames, in a plain `cargo test`.

`partyline::testing::Loopback` connects the client state machine to the server logic over an in-memory pipe, in virtual time.
It needs no socket, no browser, and no Workers runtime. partyline's own property test uses it.

## What it covers

| Covered | Not covered |
| --- | --- |
| Reconnect, backoff, and resume from the cursor | The Durable Object and its SQLite storage |
| Dedupe, gap detection, and `Reset` | Real sockets and timers |
| Retention and the replay decision | The Dioxus hooks |
| Your code that folds events into state | Authorization in your Worker |

Use it to test the code that turns events into state: a reducer, a `Reset` handler, a projection.

## 1. Add the dependency

`partyline` is already a dependency of your shared crate. Add `proptest` for property tests if you want them:

```shell
cargo add --dev proptest
```

## 2. Write a test

```rust
use partyline::testing::{Faults, Loopback, Observed};
use orders_shared::{Order, OrderEvent, OrderStatus, Orders};

#[test]
fn the_order_matches_after_a_bad_connection() {
    let events = [
        OrderEvent::StatusChanged { status: OrderStatus::Preparing },
        OrderEvent::NoteAdded { note: "No onions".into() },
        OrderEvent::StatusChanged { status: OrderStatus::Ready },
    ];

    // A hostile pipe: drops, lost and duplicate frames, delays, failed connects.
    let mut lb = Loopback::<Orders>::new(7, Faults::default());
    lb.start();
    for event in &events {
        lb.publish(event);
        lb.advance_ms(700); // virtual time
    }
    lb.heal();   // stop injecting faults
    lb.settle(); // run until the client is idle and caught up

    // Fold what the app saw, as the app would.
    let mut order = Order::default();
    for observed in lb.observed() {
        match observed {
            Observed::Event { event, .. } => order.apply(event),
            Observed::Reset { .. } => order = Order::default(), // and refetch
            Observed::Status(_) => {}
        }
    }

    let mut expected = Order::default();
    events.iter().for_each(|e| expected.apply(e));
    assert_eq!(order, expected);
    assert_eq!(lb.client().cursor(), Some(lb.head()));
}
```

The seed makes a run repeatable. A failing seed fails the same way every time.

## 3. Turn it into a property test

Run the same check over many seeds and random event sequences:

```rust
use proptest::prelude::*;

proptest! {
    #[test]
    fn delivery_survives_faults(
        seed in any::<u64>(),
        notes in proptest::collection::vec(".{0,8}", 1..50),
    ) {
        let mut lb = Loopback::<Orders>::new(seed, Faults::default());
        lb.start();
        for note in &notes {
            lb.publish(&OrderEvent::NoteAdded { note: note.clone() });
            lb.advance_ms(300);
        }
        lb.heal();
        lb.settle();
        // Every event exactly once and in order, or a Reset.
        partyline::testing::check_log_delivery(0, lb.observed()).map_err(TestCaseError::fail)?;
    }
}
```

`check_log_delivery` checks partyline's own guarantee. Add your own assertions on the folded state next to it.

## Useful controls

| Method | Effect |
| --- | --- |
| `Loopback::new(seed, faults)` | A client that starts at the server's head |
| `Loopback::with_options(seed, faults, retention, config, since)` | Every setting explicit, such as a small retention to force `Reset` |
| `Faults::NONE`, `Faults::default()` | A perfect pipe, or a moderately hostile one. Set the fields for your own mix |
| `publish(&event)` | Publish on the server side |
| `advance_ms(ms)`, `advance(duration)` | Move virtual time. Timers fire, frames arrive |
| `disconnect()`, `server_close(code)` | Drop the connection, or close it with a code such as 4403 |
| `reset_server()` | Wipe the log with a new epoch, like `Publisher::reset` |
| `wake()`, `stop()` | The app's wake and stop |
| `heal()`, `settle()` | Stop faults, then run until idle |
| `observed()`, `events()`, `stats()` | What the app saw, the events alone, and pipe statistics |

## Related

- [Testing](../testing.md): how partyline tests itself
- The `testing` module on [docs.rs](https://docs.rs/partyline/latest/partyline/testing/)
