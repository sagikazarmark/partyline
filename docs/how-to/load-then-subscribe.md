# How to load, then subscribe

This guide shows how to load a page's initial state over HTTP and subscribe to its channel without losing or repeating an event in between.
[`examples/orders`](../../examples/orders) uses this pattern.

## The problem

A page that loads state and then opens a socket has a gap. An event published after the load and before the socket opens is in neither.
A page that opens the socket first and loads after has the opposite problem: an event can be applied twice.

The fix: the HTTP response returns the state **and the channel head it was read at**. The client connects with `since` set to that head, so it receives every event after the state, and none before it.

## 1. Return the state with its head

Read both in one Durable Object turn when the object owns the state. Nothing can publish between the two reads, because a Durable Object runs one request at a time and the two reads do not await.

```rust
// In the Durable Object that embeds the hub.
async fn fetch(&self, req: Request) -> Result<Response> {
    match req.path().as_str() {
        "/order" => Response::from_json(&OrderSnapshot {
            order: self.load()?,
            head: self.hub.head()?,
        }),
        _ => self.hub.fetch(req).await,
    }
}
```

When the state lives elsewhere, read the head **first**, then the state. The client then receives every event after the head. Some of them may already be in the state, so applying them must be safe to repeat. The poll example reads the activity head this way:

```rust
let head = Publisher::<PollActivity>::new(&env, ACTIVITY_BINDING)?.head(&id).await?;
let tally = load_tally(&env, &id).await?;
Response::from_json(&PollSnapshot { tally, activity_head: head, /* .. */ })
```

## 2. Subscribe from the head

Pass the head to `ChannelOptions::since`:

```rust
#[component]
fn OrderView(id: String, initial: Order, head: Cursor) -> Element {
    let mut order = use_signal(|| initial);
    let channel = use_channel::<Orders>(
        ChannelOptions::new(id.clone()).since(head),
        move |message| match message {
            ChannelMessage::Event(event) => order.write().apply(&event),
            ChannelMessage::Reset => { /* load again, then order.set(..) */ }
        },
    );
    // ..
}
```

The hub replays every retained event after `head`, then sends live events. The client drops any event at or before its cursor.

## 3. Handle Reset

The client receives `Reset` when it cannot resume: the channel was reset, its cursor is older than the retained events, or the replay is larger than the hub's replay budget.
Discard the local state and load it again.

`use_channel_reducer` does this for you. It buffers the events that arrive during the reload, and applies only those after the head the reload returns:

```rust
let (order, channel) = use_channel_reducer::<Orders, _, _, _, _>(
    ChannelOptions::new(id.clone()).since(head),
    move || initial.clone(),
    |order, event| order.apply(&event),
    move || {
        let id = id.clone();
        async move {
            let snapshot = fetch_order(&id).await?;
            Ok::<_, gloo_net::Error>((snapshot.order, snapshot.head))
        }
    },
);
```

The reload returns `Result<(S, Cursor), E>`, where `E` implements `Display`. A failed reload is retried. When the channel ID changes, the hook calls `initial` again and starts over.

## Latest channels

A Latest channel needs none of this. The hub sends the current value on every connect when the client is behind, so `use_channel_latest` shows the value without an HTTP load.

## Related

- [Protocol: loading state without a race](../protocol.md#loading-state-without-a-race)
- [Choosing a mode](../explanation/choosing-a-mode.md)
