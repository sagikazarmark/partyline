# partyline: implementation plan

Oct 7, 2026 · @Mark

## Summary

partyline is four Rust crates that push sequenced, resumable events from a Cloudflare Durable Object to Dioxus clients over WebSockets. A client that loses its connection reconnects with its last sequence number and receives exactly the events it missed.

It is inspired by PartyKit but is not a port. It shares no API and no wire format with PartyServer or partysocket.

### Goals

- **Shared types.** One event type, defined once, used by the Durable Object and the client.
- **Resume.** At-least-once delivery with a per-channel sequence number, deduplicated on the client.
- **Two channel modes.** An event log with replay, and a state snapshot where only the latest value matters.
- **Free when idle.** The hub keeps nothing in memory, so the Durable Object can hibernate with sockets open.
- **Web and native.** The client runs on Dioxus web (wasm) and on desktop and mobile (native).
- **Testable offline.** All reconnect and resume logic is a pure state machine with no browser or Workers runtime needed.

### Non-goals for v1

- **Client-to-server application messages.** Clients write through HTTP or server functions. The socket carries server events and control frames only.
- **Authentication.** partyline exposes a hook for a token and leaves verification to the app.
- **A backend outside Workers.** A hub for regular servers (tokio, axum) is deferred. Version 1 targets Cloudflare Workers only.
- **Sharding.** One channel maps to one Durable Object. The mapping stays pluggable for later.
- **Closed-app delivery.** Web Push is a separate problem.
- **Presence, CRDTs, or a generic WebSocket framework.**

## Architecture

Each channel is one Durable Object that owns the sockets and the event log. The Worker only authenticates, forwards upgrades, and publishes.

Each app is one Rust Worker. It serves the app, the API, and the Durable Object classes, so the socket is same-origin: cookies work and no CORS setup is needed.

&#91;embedded content: partyline architecture · 4 crates, 3 flows\]

The numbered arrows are the three flows below. The shared `partyline` crate is compiled into both sides, so the event types cannot drift apart.

1. **Connect.** The client opens a WebSocket to `/partyline/{channel}/{id}` with its cursor. The Worker authorizes the request and forwards it. The hub accepts the socket and sends `Hello`.
2. **Publish.** Backend code calls `Publisher::publish`. The hub appends the event to its SQLite log, assigns the next sequence number, and sends it to every socket.
3. **Recover.** The phone loses the connection. The client backs off, reconnects with its last cursor, and receives the events it missed or a `Reset`.

## Wire protocol

The server sends three frame types, and the client resumes by putting its cursor in the connect URL. Version 1 uses JSON text frames.

### Connect

```
GET wss://{host}/partyline/{channel}/{id}?v=1&cursor={epoch}.{seq}
```

- `cursor` absent: the client gets live events only, starting at the current head.
- `cursor` present: the server replays every retained event after it, or sends `Reset`.
- `token` is an optional third parameter that carries a short-lived session token.

A cursor is `(epoch, seq)`. The epoch is a random number the hub writes when it creates its log. If the Durable Object's storage is ever wiped, the epoch changes and old cursors are rejected instead of silently matching new sequence numbers.

### Frames

```rust
#[derive(Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ServerFrame<E> {
    /// First frame on every connection.
    Hello { v: u8, mode: Mode, head: Cursor },
    /// One event. In Log mode, seq increases by exactly 1.
    Event { seq: u64, event: E },
    /// The cursor cannot be resumed. Discard local state and refetch.
    Reset { head: Cursor },
}

pub enum Mode { Log, Latest }
```

| Frame | Sent when | Client action |
| --- | --- | --- |
| `Hello` | Once, right after the upgrade | Record mode and head, mark the connection open |
| `Event` | On replay and on every publish | Deliver if `seq` is newer than the cursor, then advance the cursor |
| `Reset` | Epoch mismatch, or cursor older than retention | Tell the app to refetch, then continue from `head` |

The only client-to-server message is the heartbeat: the text `ping`, answered with `pong`.

Frame types never use `deny_unknown_fields`. A later version can then add a field, such as a snapshot on `Reset`, without breaking 0.1 clients. `Reset` carries no snapshot in 0.1.

### Channel modes

- **Log.** Every event matters. The hub retains a window of events and replays the gap on reconnect. The client treats a jump in `seq` as a protocol error and reconnects.
- **Latest.** Each event is the full current value. The hub keeps one row and sends it on connect when the cursor is behind. Gaps are expected, and `Reset` is never needed.

This replaces the separate `Snapshot` frame from the earlier sketch. A Latest channel is a Log channel with a retention of one and gap checking turned off.

**Choosing a mode.** Use Latest when a client that missed updates only needs the final value. Use Log only where a missed intermediate event is a bug.

### Delivery rules

- **Ordering.** A Durable Object is single-threaded, so `seq` assignment and fan-out are atomic per publish.
- **At-least-once.** The client drops any event whose `seq` is not newer than its cursor.
- **No interleaving.** The hub reads the backlog and sends it without awaiting. No publish can land between replay and live.
- **Durable before visible.** The Durable Object output gate holds outgoing messages until the storage write is confirmed.
- **One audience per channel.** Every event goes to every socket. To target one user, use one channel per user.

### Close codes

| Code | Meaning | Client reconnects |
| --- | --- | --- |
| 1000 | Normal close by either side | No |
| 1006, 1011, 1012, 1013 | Network loss or server restart | Yes, with backoff |
| 4400 | Malformed connect request | No |
| 4401 | Token missing or expired | Yes, after asking the token provider again |
| 4403 | Not allowed on this channel | No |
| 4426 | Protocol version not supported | No |

### Defaults

| Setting | Default |
| --- | --- |
| Retention, Log mode | 1,000 events or 24 hours, whichever is smaller |
| Target event size | Under 16 KB; send IDs and let the client fetch large payloads |
| Heartbeat interval / timeout | 25 s / 10 s |

Retention is the only limit on a replay. A cursor older than the retained window gets `Reset`, and the refetch that follows is always correct.

## Crate: partyline

The core crate holds everything that needs no I/O: the protocol types, the client state machine, and the server's replay rules. It compiles for `wasm32-unknown-unknown` and native, and every other crate depends on it.

Dependencies: `serde`, `serde_json`, `thiserror`. No `tokio`, no `web-sys`, no `worker`.

### Channel definition

An app defines each channel once, in a crate shared by its Worker and its client.

```rust
pub trait Channel: 'static {
    /// URL path segment, e.g. "orders".
    const NAME: &'static str;
    const MODE: Mode;
    type Event: Serialize + DeserializeOwned + Clone + 'static;
}

// In the app's shared crate:
pub struct Orders;
impl Channel for Orders {
    const NAME: &'static str = "orders";
    const MODE: Mode = Mode::Log;
    type Event = OrderEvent;
}
```

### Modules

| Module | Contents |
| --- | --- |
| `frame` | `ServerFrame`, `Cursor` (with `Display` and `FromStr` as `epoch.seq`), `Mode`, close-code constants |
| `codec` | JSON encoding only. The `Codec` trait is private in 0.1 and becomes public when a second codec exists |
| `client` | The sans-IO client state machine |
| `server` | Replay decision, retention policy, and the `Log` trait, with an in-memory `MemLog` |
| `testing` | Loopback harness that wires `client` to `server` over a faulty in-memory pipe |

### Client state machine

The state machine never reads a clock, opens a socket, or sleeps. The driver feeds it inputs and performs the outputs.

```rust
pub enum Input {
    Start,
    Opened,
    Frame(Message),
    Closed { code: Option<u16> },
    Timer(TimerId),   // a timer requested earlier has fired
    Wake,             // app became visible, or the network came back
    Stop,
}

pub enum Output<E> {
    Connect { cursor: Option<Cursor> },
    Send(Message),    // heartbeat ping
    Close,
    SetTimer { id: TimerId, after: Duration },
    Event { seq: u64, event: E },
    Reset,
    Status(Status),
}

pub enum Status {
    Idle,
    Connecting,
    Open,
    Waiting { retry_in: Duration },
    Stopped { code: Option<u16> },
}

impl<C: Channel> Client<C> {
    pub fn new(config: ClientConfig, since: Option<Cursor>, rng: impl FnMut() -> f64 + 'static) -> Self;
    pub fn handle(&mut self, input: Input, out: &mut Vec<Output<C::Event>>);
    pub fn cursor(&self) -> Option<Cursor>;
}
```

Behaviour to implement:

- **Backoff.** Exponential with full jitter: base 500 ms, factor 2, cap 30 s. The random source is injected so tests are deterministic.
- **Stable reset.** The attempt counter resets only after a connection stays open for 10 s. A server that accepts and then closes cannot cause a tight loop.
- **Timers.** Four timer IDs: reconnect, connect timeout, heartbeat send, heartbeat timeout.
- **Wake.** While waiting, connect at once. While open, send a ping with a short timeout to detect a dead socket after the phone resumes.
- **Dedupe and gaps.** Drop events at or behind the cursor. In Log mode, a jump ahead closes the socket and reconnects from the cursor.
- **Terminal codes.** 4400, 4403, and 4426 move to `Stopped`. 4401 emits a status the driver uses to refresh the token.

### Cursor ownership

The cursor lives in memory and survives reconnects for the life of the page or app. It is not persisted across reloads.

The app supplies the starting cursor. The HTTP response that loads the initial state also returns the channel head it was read at, and the client connects with `since` set to that head. This closes the race between loading state and subscribing.

### Server logic

```rust
pub trait Log {
    fn head(&self) -> Cursor;
    fn oldest(&self) -> Option<u64>;
    fn append(&mut self, body: &[u8], now_ms: u64) -> u64;
    fn range(&self, after: u64) -> Vec<(u64, Vec<u8>)>;
    fn trim(&mut self, policy: &Retention, now_ms: u64);
}

pub enum OnConnect { LiveOnly, Replay { after: u64 }, SendLatest, Reset }

pub fn on_connect(mode: Mode, since: Option<Cursor>, log: &impl Log) -> OnConnect;
```

There is no separate replay cap. Retention bounds the log, so it also bounds a replay.

Keeping this in the core crate means the Workers backend is a thin adapter. A backend for regular servers is deferred. When an app needs one, it can reuse these rules without a protocol change.

## Crate: partyline-worker

This crate adapts the core server logic to a Durable Object. It provides a `Hub` you embed in your own Durable Object, plus two helpers for the Worker: one to forward upgrades and one to publish.

Dependencies: `partyline`, `worker`. It needs a Durable Object class with the SQLite storage backend.

### Embedding the hub

Rust has no base classes, so the hub is a field and each handler delegates to it. Signatures follow `worker` 0.x and must be checked in the spike.

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
        self.hub.fetch(req).await
    }

    async fn websocket_message(&self, ws: WebSocket, msg: WebSocketIncomingMessage) -> Result<()> {
        self.hub.on_message(ws, msg).await
    }

    async fn websocket_close(&self, ws: WebSocket, code: usize, reason: String, clean: bool) -> Result<()> {
        self.hub.on_close(ws, code, reason, clean).await
    }

    async fn alarm(&self) -> Result<Response> {
        self.hub.on_alarm().await
    }
}
```

### Optional macro: channel\_object!

A `macro_rules!` macro generates the struct and all five delegating handlers, and the manual form above stays fully supported. A proc macro is not needed for v1.

```rust
partyline_worker::channel_object! {
    /// Durable Object for the orders channel.
    pub struct OrderChannel: Orders;
}

// With configuration:
partyline_worker::channel_object! {
    pub struct ActivityChannel: Activity {
        config = HubConfig::default().retain_events(500);
    }
}
```

The struct name is the `class_name` in the wrangler configuration.

**Why code generation at all.** A Durable Object class must be a concrete struct exported by name, so a generic `ChannelObject<C>` cannot be used. The orphan rule also forbids a blanket `impl DurableObject` for app types. A macro is the only way to remove the boilerplate.

**What the macro buys.**

- **No missed handler.** A forgotten `alarm` means age-based trimming never runs, and nothing fails visibly. The macro always emits all five.
- **A buffer against `worker` changes.** When the `DurableObject` trait changes, `partyline-worker` ships a new version and app code stays the same.

**Why `macro_rules!` and not a proc macro.**

|  | `macro_rules!` | Proc macro |
| --- | --- | --- |
| Extra crate | None | A fifth crate, `partyline-macros` |
| Build cost | None | `syn` and `quote` in every app build |
| Exit path | The expansion is the documented manual code. Copy it and edit | Same, but harder to read from `cargo expand` |
| Needed for v1 | Covers the hub-only object | Only needed to inspect or rewrite app code |

**When to embed by hand.** An object that owns its own state or routes embeds the hub as a field and calls `hub.publish(&event)` directly. The demo has one object of each kind.

**When a proc macro becomes worth it.** If apps often need the hub inside an object with a custom `fetch`, an attribute that adds only the handlers the app did not write would help. Add it then, as an opt-in feature, with the same expansion.

**Requirement.** The `worker` macro emits absolute `::worker::` paths, so the app crate must depend on `worker` under that name. M0 confirms that `#[durable_object]` expands correctly when a `macro_rules!` macro in another crate emits it.

### Worker-side helpers

```rust
// 1. Forward a client upgrade. The route handler authorizes first, then calls this.
let response = Connect::<Orders>::new(&order_id)
    .tag(&user_id)                             // optional, from the app's auth layer
    .forward(&env, "ORDER_CHANNEL", req)
    .await?;

// 2. Publish from any backend code path.
let cursor = Publisher::<Orders>::new(&env, "ORDER_CHANNEL")?
    .publish(&order_id, &OrderEvent::StatusChanged { status })
    .await?;

// 3. Read the head, to return alongside initial state.
let head = Publisher::<Orders>::new(&env, "ORDER_CHANNEL")?.head(&order_id).await?;
```

`Connect` is a builder and one async function, not a router integration. It takes the request type the Worker already has: `worker::Request`, or `http::Request` when the `http` feature of `worker` is on. An axum handler on Workers calls it like any other function, so partyline depends on no web framework.

The route handler owns path parsing, authorization, and tags. `Connect` refuses any request that is not a WebSocket upgrade.

`Publisher` calls the Durable Object through its stub with an internal `POST /publish` request. A Durable Object is reachable only through a Worker binding, and `Connect::forward` passes on upgrade requests only, so the publish route is never exposed to clients.

### Authentication

The app verifies the session in its own route handler or middleware, before `Connect::forward`. partyline contains no auth code and no ticket system.

- **Verify at upgrade only.** A socket can outlive the token that opened it. This is accepted for 0.1.
- **Tag by user.** The handler passes the user ID with `.tag()`, and the hub stores it as a socket tag.
- **Close on sign-out.** The app closes that user's sockets by tag. The close code is 4403, so the client does not reconnect.
- **No auth.** Apps without auth call `Connect::forward` directly. A public channel is safe to expose because the socket is read-only.

```rust
Publisher::<Orders>::new(&env, "ORDER_CHANNEL")?
    .close_tagged(&order_id, &user_id, close::FORBIDDEN)
    .await?;
```

The close call is per channel. An app closes the channels it knows the user has open. Any other socket ends at its next reconnect, when verification fails.

With Clerk, the handler reads the session token from the `token` parameter, and falls back to the session cookie when the parameter is absent.

### Hub routes

| Request | Action |
| --- | --- |
| `GET` with `Upgrade: websocket` | Accept the socket, send `Hello`, then replay, send latest, or reset |
| `POST /publish` | Append, trim, fan out, return the new cursor |
| `GET /head` | Return the current cursor |
| `POST /close` | Close sockets with a given tag and close code |

### Storage schema

```sql
CREATE TABLE IF NOT EXISTS partyline_meta (
  id    INTEGER PRIMARY KEY CHECK (id = 1),
  epoch INTEGER NOT NULL,
  head  INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS partyline_log (
  seq  INTEGER PRIMARY KEY,
  ts   INTEGER NOT NULL,   -- milliseconds since the Unix epoch
  body BLOB    NOT NULL    -- the encoded event
);
```

Both modes use the same tables. Latest mode trims to one row on every publish. `SqlLog` implements the core `Log` trait over `state.storage().sql()`.

### Publish path

1. Encode the event once.
2. In one synchronous transaction, insert the row at `head + 1` and update `head`.
3. Trim by count. Age-based trimming runs from an alarm, so idle channels are trimmed too.
4. Build the `Event` frame text once and send it to every socket from `get_websockets()`.
5. Close any socket whose send fails. Its client will resume from its cursor.

### Connect path

1. Parse `v` and `cursor`. Reject with 4400 or 4426 if invalid.
2. Create the `WebSocketPair` and call `accept_websocket_with_tags`, using tags the Worker passed in a header.
3. Send `Hello`.
4. Call the core `on_connect` and act on its result.
5. Return the 101 response.

Steps 3 and 4 run without awaiting. This is what guarantees that no publish lands between replay and live.

### Hibernation rules

- The hub holds only `State` and its config. It caches nothing, including `head`.
- Heartbeats use the runtime's WebSocket auto-response for `ping` and `pong`, so they do not wake the Durable Object.
- Per-socket data, if any, goes in the socket attachment, which is limited to 2,048 bytes.
- The constructor runs only the two `CREATE TABLE IF NOT EXISTS` statements.

### Limits to respect

One Durable Object accepts at most [32,768 WebSocket connections](https://developers.cloudflare.com/durable-objects/api/state/). A channel that could exceed this needs sharding, which is out of scope for v1.

## Crate: partyline-client

This crate connects the core state machine to a real socket and real timers. It is runtime-agnostic: it returns a driver future and never spawns, so Dioxus, tokio, or `wasm-bindgen-futures` can run it.

### Transports

```rust
pub trait Transport {
    type Conn: Stream<Item = Result<Message, TransportError>>
        + Sink<Message, Error = TransportError>
        + Unpin;

    fn connect(&self, url: &str) -> impl Future<Output = Result<Self::Conn, TransportError>>;
}
```

| Target | Type | Built on | Notes |
| --- | --- | --- | --- |
| `wasm32` | `BrowserSocket` | `gloo-net` | Same-origin cookies are sent automatically on the handshake |
| Native | `NativeSocket` | `tokio-tungstenite` with `rustls` | No system OpenSSL, which keeps Android and iOS builds simple |

The choice is made with `cfg(target_arch = "wasm32")`. Timers use one cross-target timer crate so the driver loop has a single implementation.

### Public API

```rust
let (handle, events, driver) = partyline_client::connect::<Orders>(ConnectOptions {
    base_url: BaseUrl::SameOrigin,          // or BaseUrl::Explicit(url) on native
    id: order_id.clone(),
    since: Some(head),
    token: None,                            // Option<TokenProvider>
    config: ClientConfig::default(),
});

spawn(driver);                              // caller chooses the executor

while let Some(msg) = events.next().await {
    match msg {
        ClientEvent::Event { event, .. } => apply(event),
        ClientEvent::Reset => refetch().await,
        ClientEvent::Status(status) => show(status),
    }
}

handle.wake();   // connect now, or probe an open socket
handle.stop();   // close with 1000 and end the driver
```

### Driver loop

The driver is one `select` over three sources: the socket, the pending timers, and the handle's command channel. Each wake-up becomes one `Input`, and each `Output` is performed in order. It contains no protocol decisions.

### Authentication hook

- **Token provider.** An async function called before every connect. Its result goes in the `token` query parameter, because browsers cannot set headers on a WebSocket.
- **With Clerk.** The provider calls `getToken()`. The session token is short-lived, so it does the job of a ticket, and partyline ships no ticket system.
- **Same-origin cookie.** The browser also sends the session cookie with the upgrade. After time in the background that cookie can be stale, which is why apps with auth set a provider.
- **No auth.** Apps without auth set no provider.
- **Docs.** A how-to guide covers the Clerk wiring. The crates contain no Clerk code.

### Wake sources

Behind the `web-wake` feature, the crate listens for `visibilitychange` and `online` and calls `handle.wake()`. These are browser APIs, so they live here and not in the Dioxus crate.

Native targets have no wake source in v1. A dead socket is found by the heartbeat, at most 35 s later with the default settings.

## Crate: partyline-dioxus

This crate is a thin set of hooks over `partyline-client`. It owns the driver's lifetime, exposes status as a signal, and stays inert during server-side rendering. It targets Dioxus 0.7.

### Hooks

| Hook | Use it for | Returns |
| --- | --- | --- |
| `use_channel::<C>(options, on_message)` | Any channel. Full control over events and resets | `UseChannel`: `status()`, `cursor()`, `wake()`, `reconnect()` |
| `use_channel_latest::<C>(options)` | Latest-mode channels | `ReadSignal<Option<C::Event>>` plus the same handle |
| `use_channel_reducer::<C, S>(options, initial, reduce, refetch)` | Log-mode channels that fold events into state | `ReadSignal<S>` plus the same handle |

```rust
#[component]
fn OrderStatus(id: String, initial: Order, head: Cursor) -> Element {
    let mut order = use_signal(|| initial);

    let channel = use_channel::<Orders>(
        ChannelOptions::new(id.clone()).since(head),
        move |msg| match msg {
            ChannelMessage::Event(event) => order.write().apply(event),
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

### Provider

`PartylineProvider` puts shared settings in context: the base URL, the token provider, and the client config. On web the base URL defaults to the page origin, so most apps configure nothing.

### Lifecycle rules

- **Start.** The driver starts from an effect, which does not run during server-side rendering. On the server the hook returns `Status::Idle` and opens nothing.
- **Stop.** On unmount the hook calls `handle.stop()`, which closes the socket with 1000.
- **Change.** When the channel ID in `options` changes, the hook stops the old driver and starts a new one.
- **Wake.** The hook enables the `web-wake` feature of the client crate on wasm.

### Deferred to 0.2

Two components that subscribe to the same channel open two sockets in v1. Sharing one connection per channel ID through the provider, with a reference count, is the first follow-up.

### Example app

`examples/orders` holds a Worker with the Durable Object, a Dioxus web client, and the shared channel crate. It is the fixture for end-to-end tests and the source for the README quick start.

## Demo: live poll

The demo is a live poll that an audience opens on their phones, and it shows every partyline feature in about two minutes. It is deployed at a public URL and is the target of the phone matrix.

It lives in `demo/`. `examples/orders` stays the smallest possible fixture for the quick start and the end-to-end tests.

### Screens

- **Presenter view.** The question, a QR code to the join link, live tally bars, the activity feed, and a "Reset demo" button.
- **Phone view.** Answer buttons, the live tally, a status badge with the current cursor, the activity feed, and a "Go offline" switch.

### Channels

The demo uses both channel modes and both ways to build the Durable Object.

| Channel | Mode | Event | Durable Object | Built with |
| --- | --- | --- | --- | --- |
| `PollTally` | Latest | The full tally: one count per option | `PollObject` | Manual embedding. The object owns the vote counts and publishes through its own hub |
| `PollActivity` | Log | `VoteCast`, `PollReset` | `ActivityChannel` | `channel_object!`. The Worker publishes through `Publisher` |

A vote travels over HTTP, which shows the one-way design:

1. The phone sends `POST /api/polls/{id}/vote`.
2. The Worker calls `PollObject`, which increments the count in its own table and publishes the new tally.
3. The Worker publishes `VoteCast` to `ActivityChannel`.
4. Every connected screen receives both events.

### What each element proves

| Demo element | Capability shown |
| --- | --- |
| Bars move on every screen after a vote | Fan-out, Latest mode |
| The activity feed lists votes in order | Log mode, ordering |
| "Go offline", then back | Resume. The missed votes arrive, with a count of replayed events |
| Status badge and cursor readout | Status and cursor as signals |
| Lock the phone, vote elsewhere, unlock | Wake and heartbeat |
| "Reset demo" | Epoch change and the `Reset` path |
| First vote after an idle night | Hibernation |

The "Go offline" switch needs no extra API. It unmounts the subscribed component and keeps its cursor in a signal. Switching back mounts the component again with `since` set to that cursor. The replay count is the difference between the two sequence numbers.

### Demo script

1. Open the presenter view on a laptop.
2. Scan the QR code with two phones.
3. Vote on the first phone. The bars move on all three screens.
4. Switch the second phone to offline. Vote several times on the first.
5. Switch the second phone back. Its feed fills with the missed votes and shows the replay count.
6. Lock the second phone for a minute, vote on the first, then unlock. The tally is correct within seconds.
7. Press "Reset demo". Both phones receive `Reset` and reload the poll.

### Stack and hosting

- **One Rust Worker.** It serves the Dioxus web build as static assets, the vote API, and both Durable Object classes.
- **Guards for a public URL.** A rate limit on the vote route, a small retention window, and a daily reset from an alarm.
- **Hosting.** It runs on a custom subdomain, so zone-level rate limiting and firewall rules can apply.

### Place in the plan

The demo is built in M4 and replaces the example app as the phone-matrix target. It is also where the manual hibernation check runs. M6 deploys it publicly and links it from the top of the README.

## Workspace, tooling, and testing

One Cargo workspace holds the four crates, each versioned and released on its own. `partyline-dioxus` and `partyline-worker` track fast-moving 0.x dependencies, and independent versions keep that churn out of the core.

### Layout

```
partyline/
├── Cargo.toml                 # workspace, shared lints, workspace.dependencies
├── crates/
│   ├── partyline/             # protocol, codec, client state machine, server logic
│   ├── partyline-client/      # transports and driver
│   ├── partyline-dioxus/      # hooks and provider
│   └── partyline-worker/      # Durable Object hub, channel_object! macro, Worker helpers
├── examples/
│   └── orders/                # smallest fixture: quick start and end-to-end tests
│       ├── shared/            # Channel impl and event types
│       ├── worker/            # Worker + Durable Object, wrangler config
│       └── web/               # Dioxus client
├── demo/                      # live poll, deployed publicly
│   ├── shared/                # PollTally and PollActivity channels
│   ├── worker/                # vote API, PollObject, ActivityChannel
│   └── web/                   # presenter and phone views
├── e2e/                       # tests that run against wrangler dev
└── docs/                      # protocol reference, how-to guides
```

### Dependency direction

`partyline-dioxus` depends on `partyline-client`, which depends on `partyline`. `partyline-worker` depends on `partyline` only. The client and worker crates never depend on each other.

### CI checks

| Check | Command or tool | Applies to |
| --- | --- | --- |
| Unit and property tests | `cargo test` | `partyline`, `partyline-client` |
| wasm build | `cargo check --target wasm32-unknown-unknown` | All four crates |
| Native build | `cargo check` | `partyline`, `partyline-client`, `partyline-dioxus` |
| Browser transport tests | `wasm-bindgen-test` in headless Chrome | `partyline-client` |
| Lints | `cargo fmt --check`, `cargo clippy -D warnings` | Workspace |
| Dependency policy | `cargo deny` | Workspace |
| Docs | `cargo doc` with `missing_docs` as a warning, Vale, lychee | Workspace and `docs/` |
| End to end | `wrangler dev` plus the native test client | `examples/orders` |

Docs follow the shared documentation standards: STE-lite prose, one README per crate, rustdoc for reference, and how-to guides in `docs/`.

The minimum Rust version is the higher of what `dioxus` and `worker` require. It is declared as `rust-version` in the workspace, and CI builds on it.

### Test layers

| Layer | What it proves | Runtime needed |
| --- | --- | --- |
| 1. State machine | Backoff, timers, dedupe, gap handling, terminal codes | None |
| 2. Loopback | Client and server logic agree under faults | None |
| 3. Transport | Each socket type maps open, message, close, and error correctly | Local WebSocket server; headless Chrome for wasm |
| 4. End to end | The Durable Object, SQLite log, and Worker helpers work in workerd | `wrangler dev` |
| 5. Phone matrix | Real mobile browsers recover as designed | The deployed demo and two phones |

**Layer 2 carries the main guarantee.** The harness connects the client state machine to the server logic with `MemLog` through an in-memory pipe that can drop the connection at any frame, duplicate frames, and delay them. A property test publishes a random event sequence under random faults and checks one invariant: in Log mode the app sees every event exactly once and in order, or it sees `Reset`.

**Layer 4 cannot force hibernation locally.** The hub is safe by construction because it holds no memory state. One manual check on a deployed Worker confirms it: leave a socket idle for several minutes, publish, and confirm delivery and near-zero billed duration.

**Layer 5 matrix.** Run on iOS Safari and Android Chrome: tab in the background for 1 minute and for 10 minutes, screen locked, airplane mode toggled, and a switch between Wi-Fi and mobile data. Each case passes when the app shows the correct state within a few seconds of returning, with no manual refresh.

## Milestones

Seven milestones take partyline from a throwaway spike to a 0.1.0 release, and the first real app integrates before anything is published. M2 and M3 depend only on M1 and can run in either order.

| Milestone | Scope | Exit criterion |
| --- | --- | --- |
| **M0 Spike** | A throwaway Worker with a hibernating Durable Object in Rust: SQLite storage, tags, auto-response. A Dioxus web page that connects with `gloo-net`. Reserve the four crate names and the repository. | A phone receives a broadcast after the Durable Object has hibernated. Every missing or awkward `worker` API is written down with its fallback. |
| **M1 Core** | Frames, cursor, codec, client state machine, server logic, `MemLog`, loopback harness. | The loopback property test passes, and the crate builds for wasm32. |
| **M2 Worker** | `Hub`, `SqlLog`, publish, connect, replay, trim, alarm, `Connect` and `Publisher` helpers, the channel\_object! macro, the example Worker. | The end-to-end resume test passes against `wrangler dev` with a scripted native socket. |
| **M3 Client** | Both transports, the driver, the token provider, the web wake listeners. | Transport tests pass on native and in headless Chrome. The end-to-end test now uses the real client. |
| **M4 Dioxus** | The three hooks, the provider, the server-rendering guard, the example web app, the live poll demo. | The demo passes the phone matrix. |
| **M5 Dogfood** | Integrate partyline into the first real app. Change the API wherever the integration is awkward. | The app runs on partyline in production, and no API change is still open. |
| **M6 Release** | README per crate, rustdoc, protocol reference, one how-to guide, CI, release automation, the public demo deployment. | Version 0.1.0 of all four crates is on crates.io, and the demo is live at a public URL. |

### M0 checklist

The spike exists to remove the unknowns that could change the design.

- [ ] `state.storage().sql().exec(..)` works for inserts, range reads, and deletes, and is synchronous from Rust.
- [ ] The WebSocket auto-response is exposed by `worker`. Fallback: answer `ping` in `websocket_message`, which wakes the Durable Object.
- [ ] `accept_websocket_with_tags` and tag-filtered socket lookup work as in the JavaScript API.
- [ ] The Worker can route an upgrade request to the Durable Object next to the app's other routes, including through an axum router on Workers.
- [ ] Sending on the server socket before returning the 101 response delivers those frames first.
- [ ] `#[durable_object]` expands correctly when a `macro_rules!` macro in another crate emits it.
- [ ] Publish the `0.0.0` placeholders: `partyline`, `partyline-client`, `partyline-dioxus`, `partyline-worker`.

## Risks and open questions

The largest risk is a gap in the Rust bindings for Durable Objects, and M0 exists to find it before any design is fixed. The other risks are about scope and upstream churn.

### Risks

| Risk | Effect | Response |
| --- | --- | --- |
| `worker` lacks an API the hub needs | Hibernation or the SQLite log does not work as designed | M0 spike. Fallbacks: key-value storage with ordered keys for the log, and handling `ping` in the message handler |
| The Worker's router cannot pass upgrades through cleanly | `Connect::forward` needs a different shape | Check in M0 against the real app's router before writing the helper |
| `dioxus` and `worker` release breaking 0.x versions | Frequent major bumps | Separate crates with independent versions. Keep both adapters thin |
| Dioxus adds reconnect to its own WebSocket hook | Part of the client overlaps | Resume and the Durable Object backend remain the reason to use partyline |
| A channel outgrows one Durable Object | Connections are refused past the per-object limit | Keep the channel-to-object mapping pluggable. Add sharding when a real channel needs it |
| No wake signal on native mobile | Dead sockets are found only by the heartbeat | Accept for v1. Add platform lifecycle hooks when Dioxus exposes them |
| The name is mistaken for a Cloudflare package | Wrong expectations about compatibility | State "not affiliated, not wire-compatible" at the top of every README |
| Small audience | Little outside testing | Build for your own apps first. Do not add features without a consumer |

### Decisions

These were the open questions. Each one is now decided and applied to the sections above.

| Question | Decision |
| --- | --- |
| First consumer and channel mode | Start with the phone app that prompted this plan. Use Latest when a client that missed updates only needs the final value. Use Log only where a missed intermediate event is a bug |
| Worker topology | One Rust Worker per app, with the Durable Objects in it. Same origin keeps cookies working and avoids CORS |
| Authentication | No ticket system in partyline, and no Clerk code in the crates. With Clerk, call `getToken()` before each connect and send the result as `token`. Verify at upgrade only, tag each socket with the user ID, and close by tag on sign-out. Apps without auth skip authorization |
| Retention defaults | Keep 1,000 events and 24 hours. Remove the separate replay cap, because an event beyond the cap can never be replayed |
| `Reset` with a snapshot | Not in 0.1. Do not deny unknown fields on frames, so a later field stays compatible |
| Codec | JSON only in 0.1. Keep the `Codec` trait private until a second codec exists |
| Minimum Rust version | The higher of what `dioxus` and `worker` require, tested in CI |
| Demo hosting | A custom subdomain, so zone-level rate limiting and firewall rules can apply |

Still open:

- [ ] License.
- [ ] Is the `partyline` repository name free on GitHub under the intended owner?

