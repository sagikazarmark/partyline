# Choosing a mode

Each channel has one of two modes. The mode is part of the channel definition, so the Worker and the client always agree on it.

```rust
impl Channel for Orders {
    const NAME: &'static str = "orders";
    const MODE: Mode = Mode::Log; // or Mode::Latest
    type Event = OrderEvent;
}
```

## The short rule

Use **Latest** when a client that missed updates only needs the final value.
Use **Log** only where a missed intermediate event is a bug.

## Log

Every event matters. The hub keeps a window of events and replays the gap on reconnect.

- **Events are changes.** "Status changed to Ready", "Note added".
- **The client folds them into state.** `use_channel` or `use_channel_reducer`.
- **The client must load state first.** It connects with the head returned with that state. See [Load, then subscribe](../how-to/load-then-subscribe.md).
- **`Reset` happens.** When the cursor is older than the retained window, or the replay is too large, or the channel was reset. The client refetches.
- **A gap is an error.** A jump in `seq` makes the client reconnect from its cursor.

Default retention: 1,000 events or 24 hours, whichever is smaller.

## Latest

Each event is the full current value. The hub keeps only the most recent one.

- **Events are snapshots.** "The tally is 12, 7, 3".
- **The client needs no initial load.** The hub sends the current value on connect when the client is behind. `use_channel_latest` shows it at once.
- **Gaps are expected.** A client that missed ten updates gets the latest one.
- **`Reset` never happens.** A Latest client that sees a new epoch adopts it, and accepts the value that follows.

After `Publisher::reset` on a Latest channel, the log is empty. Clients reconnect and show nothing new until the app publishes a value. Publish a fresh value right after the reset. The poll example publishes the zero tally.

## Comparison

| | Log | Latest |
| --- | --- | --- |
| Event | A change | The whole value |
| Retained | A window of events | One event |
| On reconnect | The missed events, or `Reset` | The latest value |
| Initial load over HTTP | Needed | Not needed |
| Dioxus hook | `use_channel`, `use_channel_reducer` | `use_channel_latest` |
| Event size | Small changes | The whole value, so keep it small |
| Example | An order's status and notes, a chat, an activity feed | A vote tally, a progress bar, a presence count |

## Both in one app

The poll demo uses both. The tally is Latest: a phone that was locked needs the current counts, not every vote.
The activity feed is Log: each vote is a line in the feed, and a missed line would be visible.

Two channels with different modes can share an ID, such as the poll ID. They are separate Durable Objects.

## Related

- [How partyline works](how-it-works.md)
- [Limits](limits.md)
- [Protocol: what follows Hello](../protocol.md#what-follows-hello)
