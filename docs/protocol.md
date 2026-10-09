# Protocol reference

Version 1 of the partyline wire protocol. The `partyline` crate implements it.

The server sends three frame types. The client resumes by putting its cursor in the connect URL.
Frames are JSON text frames.

## Connect

```
GET wss://{host}/partyline/{channel}/{id}?v=1&cursor={epoch}.{seq}&token={token}
```

| Parameter | Required | Meaning |
| --- | --- | --- |
| `v` | Yes | The protocol version. Must be `1` |
| `cursor` | No | Resume after this cursor. Without it, the client receives live events only |
| `token` | No | A short-lived session token, for the app's own authorization. `Connect::forward` removes it before the request reaches the Durable Object |

`channel` and `id` are percent-encoded path segments. Unknown parameters are ignored.

### Cursor

A cursor is `(epoch, seq)`, written `{epoch}.{seq}` in the URL and in JSON.

- **`seq`** is the sequence number of the last event the client received. Sequence numbers start at 1. `0` means "before the first event".
- **`epoch`** is a random number in `[1, 2^53)` that the hub writes when it creates its log. If the log is wiped, the epoch changes, and old cursors no longer match new sequence numbers.

## Frames

```rust
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ServerFrame<E> {
    Hello { v: u8, mode: Mode, head: Cursor },
    Event { seq: u64, event: E },
    Reset { head: Cursor },
}

#[serde(rename_all = "snake_case")]
pub enum Mode { Log, Latest }
```

```json
{"t":"hello","v":1,"mode":"log","head":"4503599627370495.12"}
{"t":"event","seq":13,"event":{"type":"status_changed","status":"ready"}}
{"t":"reset","head":"4503599627370495.13"}
```

| Frame | Sent when | Client action |
| --- | --- | --- |
| `Hello` | Once, first, right after the upgrade | Record the mode and head. Mark the connection open |
| `Event` | On replay and on every publish | Deliver it if `seq` is newer than the cursor, then advance the cursor |
| `Reset` | The cursor cannot be resumed | Tell the app to refetch, then continue from `head` |

Frames never deny unknown fields. A later version can add a field, such as a snapshot on `Reset`, without breaking 0.1 clients.
A client skips a well-formed frame whose `t` it does not know, so a later version can also add frame types. A malformed frame of a known type is a protocol error: the client closes the socket and reconnects.

An `Event` frame whose `event` the client cannot decode is different: the server runs a newer event type than the client. Reconnecting would replay the same event, so the Rust client stops with `StopReason::Incompatible` and the app prompts a reload. See [Evolve event types](how-to/evolve-event-types.md).

The only client-to-server message is the heartbeat: the text `ping`. The server answers `pong`.

## What follows Hello

| Mode | Cursor | Server sends |
| --- | --- | --- |
| Log | None | Nothing. Live events start at the head |
| Log | Equal to the head | Nothing |
| Log | Same epoch, behind the head, every missed event retained, within the replay budget | Every event after the cursor |
| Log | Same epoch, behind the head, an event trimmed or missing | `Reset` |
| Log | Same epoch, behind the head, the replay larger than the replay budget | `Reset` |
| Log | Another epoch, or ahead of the head | `Reset` |
| Latest | Equal to the head, or the channel is empty | Nothing |
| Latest | Anything else, including none | The latest event |

A Latest channel never sends `Reset`. A Latest client that sees a new epoch in `Hello` adopts it, and accepts the latest event that follows.

## Channel modes

- **Log.** Every event matters. The hub retains a window of events and replays the gap on reconnect. A client treats a jump in `seq` as a protocol error: it closes the socket and reconnects from its cursor.
- **Latest.** Each event is the full current value. The hub keeps one event. Gaps are expected.

## Delivery rules

- **Ordering.** A Durable Object is single-threaded, so sequence number assignment and fan-out are atomic per publish.
- **At least once.** The client drops any event whose `seq` is not newer than its cursor.
- **No interleaving.** The hub accepts the socket, reads the backlog, and sends it without awaiting. No publish can land between the replay and live events.
- **Durable before visible.** The Durable Object output gate holds outgoing messages until the storage write is confirmed.
- **One audience per channel.** Every event goes to every socket. To target one user, use one channel per user.

## Close codes

| Code | Meaning | Client reconnects |
| --- | --- | --- |
| 1000 | Normal close by either side | No |
| 1006, 1011, 1012, 1013 | Network loss, server error, channel reset, or overload | Yes, with backoff |
| 4400 | Malformed connect request | No |
| 4401 | Token missing or expired | Yes, after asking the token provider for a fresh token |
| 4403 | Not allowed on this channel | No |
| 4426 | Protocol version not supported | No |

A server that rejects an upgrade accepts the socket and closes it with the code at once, because browsers cannot read the HTTP status of a failed upgrade.

The server never sends the reserved codes 1005, 1006, and 1015. Close reasons are at most 123 bytes. A client must not depend on the reason text.

## Client timing

| Setting | Default |
| --- | --- |
| Reconnect backoff | Exponential with full jitter: base 500 ms, factor 2, cap 30 s |
| Attempt counter reset | After a connection stays open for 10 s |
| Connect timeout (socket open and `Hello`) | 10 s |
| Heartbeat interval / timeout | 25 s / 10 s |
| Heartbeat timeout after a wake | 3 s |

## Retention

| Mode | Default |
| --- | --- |
| Log | 1,000 events or 24 hours, whichever is smaller |
| Latest | 1 event |

A cursor older than the retained window gets `Reset`, and the refetch that follows is always correct.

| Limit | Default |
| --- | --- |
| Event size, encoded | 64 KiB. A larger publish is rejected |
| Replay budget | 8 MiB of events per connect. A larger replay becomes `Reset` |

See [Limits](explanation/limits.md) for every limit.

## Loading state without a race

The HTTP response that loads the initial state also returns the channel head it was read at. The client connects with that head as its cursor.
Read the head and the state in one Durable Object turn when you can. Otherwise read the head first: the client then receives every event after it.

## Writing a client in another language

The protocol is small enough to implement in any language with a WebSocket client and a JSON parser.
The Rust client in `partyline::client` is a sans-IO state machine, and is the reference.
A client that follows these rules gets the same delivery guarantee.

### State

Keep two values per subscription, in memory:

- **The cursor**: `(epoch, seq)` of the last event delivered, or none. Start from the head your app loaded with its initial state.
- **The mode**: from `Hello`. The channel definition also fixes it, so you can check that they agree.

### Connect

1. Build the URL: `wss://{host}/partyline/{channel}/{id}?v=1`, with `channel` and `id` percent-encoded as path segments. Encode every byte except `A-Z a-z 0-9 - _ . ~`, including `/`.
2. Add `&cursor={epoch}.{seq}` if you have a cursor, and `&token={token}` if your app uses tokens.
3. Open the socket. Start a 10 s timer for the socket to open and `Hello` to arrive. On timeout, close and retry.

### Handle frames

Parse each text frame as JSON and switch on `t`:

| `t` | Do this |
| --- | --- |
| `hello` | If `v` is not 1, close and stop. Record `mode`. In Log mode without a cursor, set the cursor to `head`. In Latest mode, if you have no cursor, or its epoch differs from `head.epoch`, set the cursor to `(head.epoch, 0)`. Mark the connection open |
| `event` | If `seq` is at or below the cursor's `seq`, drop it. Log mode: if `seq` is cursor + 1, deliver it and set the cursor; if it is further ahead, an event is missing: close and reconnect from the cursor. Latest mode: deliver it and set the cursor |
| `reset` | Tell the app to discard its state and refetch. Set the cursor to `head` |
| anything else | Ignore the frame |

The text `pong` is the heartbeat answer, not JSON. Treat it as activity.

If `event` does not decode as your event type, the server is newer than your client. Stop and ask the user to reload or update, instead of reconnecting.

### Heartbeat

After 25 s with no frame, send the text `ping`. If nothing arrives within 10 s, the socket is dead: close it and reconnect.
When the app returns to the foreground or the network comes back, send `ping` at once and wait 3 s.

### Reconnect

On close, read the close code:

| Code | Do this |
| --- | --- |
| 1000, 4400, 4403, 4426 | Stop. Do not reconnect |
| 4401 | Get a fresh token from your app, skipping any cache, then reconnect after the backoff delay |
| Anything else, or no code | Reconnect after the backoff delay, with the cursor |

Backoff: wait a uniform random time in `[0, min(30 s, 500 ms × 2^attempt))`. Reset `attempt` to 0 once a connection has stayed open for 10 s, not when it opens: a server that accepts and then closes must not cause a tight loop.
A wake while waiting connects at once.

### Check your client

Run it against `wrangler dev` with the [orders example](../examples/orders) and the end-to-end fixture in [`e2e/fixture`](../e2e/fixture).
The fixture keeps at most 5 events for at most 3 seconds, so `Reset` is easy to trigger, and it can close sockets by tag and reset the channel.
