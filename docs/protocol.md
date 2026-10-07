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
| `token` | No | A short-lived session token, for the app's own authorization |

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

The only client-to-server message is the heartbeat: the text `ping`. The server answers `pong`.

## What follows Hello

| Mode | Cursor | Server sends |
| --- | --- | --- |
| Log | None | Nothing. Live events start at the head |
| Log | Equal to the head | Nothing |
| Log | Same epoch, behind the head, every missed event retained | Every event after the cursor |
| Log | Same epoch, behind the head, an event trimmed | `Reset` |
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

Retention is the only limit on a replay. A cursor older than the retained window gets `Reset`, and the refetch that follows is always correct.

## Loading state without a race

The HTTP response that loads the initial state also returns the channel head it was read at. The client connects with that head as its cursor.
Read the head and the state in one Durable Object turn when you can. Otherwise read the head first: the client then receives every event after it.
