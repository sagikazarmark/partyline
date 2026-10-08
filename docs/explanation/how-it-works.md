# How partyline works

partyline sends events one way: from a Cloudflare Durable Object to clients, over WebSockets.
Each event gets a sequence number. A client that loses its connection reconnects with the last number it saw, and receives exactly the events it missed.

## The parts

Each app is one Rust Worker. It serves the app, the API, and the Durable Object classes, so the socket is same-origin: cookies work and no CORS setup is needed.

Each channel ID is one Durable Object. The object owns the channel's sockets and its event log. The Worker only authorizes, forwards upgrades, and publishes.

| Crate | Runs in | Role |
| --- | --- | --- |
| `partyline` | Both sides | The protocol types, the client state machine, the replay rules. No I/O |
| `partyline-worker` | The Worker | `Hub` in the Durable Object, `Connect` and `Publisher` in the Worker |
| `partyline-client` | The client | Real sockets and timers around the state machine, for web and native |
| `partyline-dioxus` | The client | Hooks that own the client's lifetime and expose status as signals |

The channel definition and its event type live in a crate that both sides compile, so the two sides cannot drift apart.

## The three flows

### 1. Connect

1. The client opens a WebSocket to `/partyline/{channel}/{id}?v=1&cursor={epoch}.{seq}`.
2. The Worker's route handler authorizes the request and calls `Connect::forward`. It can add tags, such as the user ID.
3. The hub accepts the socket and sends `Hello` with the channel's mode and head.
4. In the same turn, without awaiting, the hub sends what the client missed: every retained event after the cursor, the latest value, or `Reset`.

Step 4 runs without awaiting, so no publish can land between the replay and the live events.

### 2. Publish

1. Backend code calls `Publisher::publish(id, &event)`, or the Durable Object calls `hub.publish(&event)` itself.
2. The hub checks the event's size and type, appends it to its SQLite log at `head + 1`, and trims the log to the retention window.
3. The hub encodes the `Event` frame once and sends it to every socket.

A Durable Object runs one request at a time, so sequence numbers and fan-out are atomic per publish.
The output gate holds the frames until the storage write is confirmed, so no client sees an event that could be lost.

### 3. Recover

1. The connection drops: the phone locks, the network changes, the Worker restarts.
2. The client waits with exponential backoff and full jitter: 500 ms, then up to 30 s.
3. It reconnects with its cursor. The hub replays every event after it, or sends `Reset` if it cannot.
4. On `Reset`, the app refetches its state and the client continues from the new head.

A wake (the page becomes visible, the network comes back, the page returns from the back-forward cache) resets the backoff and connects at once.
An open socket is probed with a heartbeat: the client sends `ping` after 25 s of silence and expects `pong` within 10 s.

## The cursor

A cursor is `(epoch, seq)`.

- `seq` is the sequence number of the last event the client received.
- `epoch` is a random number the hub writes when it creates its log. If the log is wiped, the epoch changes, and old cursors no longer match.

The cursor lives in memory for the life of the page or app. It is not saved across reloads.
The app supplies the first cursor: the head returned with the initial state. See [Load, then subscribe](../how-to/load-then-subscribe.md).

## Delivery guarantee

In Log mode, the app sees every event exactly once and in order, or it sees `Reset`.

Delivery on the wire is at least once. The client drops any event at or behind its cursor, and treats a jump ahead as a protocol error: it closes the socket and resumes from its cursor.

The guarantee is checked by a property test that runs the real client and server logic over a pipe that drops, duplicates, loses, and delays frames. See [Testing](../testing.md).

## One-way by design

The socket carries server events and the heartbeat only. Clients write through HTTP or server functions.
This keeps the hub small, lets the Durable Object sleep between publishes, and puts all authorization in normal HTTP handlers.

## Related

- [Protocol reference](../protocol.md)
- [Choosing a mode](choosing-a-mode.md)
- [Hibernation and cost](hibernation-and-cost.md)
- [Implementation plan](../design/plan.md), the original design record
