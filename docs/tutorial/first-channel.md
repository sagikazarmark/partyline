# Your first channel

In this tutorial you build a shoutbox: a page where anyone can post a short message, and every open tab shows it at once.
You start from an empty directory and finish with a deployed Worker.

You will write three crates:

| Crate | Contents |
| --- | --- |
| `shared` | The channel and its event type, used by both sides |
| `worker` | The Worker: the Durable Object, the connect route, and the post route |
| `web` | The Dioxus web client |

It takes about 30 minutes. [`examples/orders`](../../examples/orders) is a finished app of the same shape.

## Before you start

You need:

- Rust 1.91 or later, with the `wasm32-unknown-unknown` target: `rustup target add wasm32-unknown-unknown`.
- `worker-build`: `cargo install worker-build`.
- The Dioxus CLI 0.7: `cargo install dioxus-cli`.
- Node.js, for `npx wrangler`.
- A Cloudflare account, for the last step only.

## 1. Create the workspace

```shell
mkdir shoutbox && cd shoutbox
cargo new --lib shared --name shoutbox-shared
cargo new --lib worker --name shoutbox-worker
cargo new web --name shoutbox-web
```

Put a workspace manifest at the root. `dx` and `worker-build` both work from a workspace.

```toml
# Cargo.toml
[workspace]
resolver = "3"
members = ["shared", "worker", "web"]
```

## 2. Define the channel

A channel definition names the channel, picks its mode, and sets its event type.
Both the Worker and the client compile it, so the two sides cannot disagree about the event type.

```shell
cargo add --package shoutbox-shared partyline
cargo add --package shoutbox-shared serde --features derive
```

```rust
// shared/src/lib.rs
use partyline::{Channel, Mode};
use serde::{Deserialize, Serialize};

/// The shoutbox channel. One channel per room ID.
pub struct Shouts;

impl Channel for Shouts {
    const NAME: &'static str = "shouts";
    const MODE: Mode = Mode::Log;
    type Event = Shout;
}

/// One message.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Shout {
    pub text: String,
}

/// The Durable Object binding name in wrangler.toml.
pub const BINDING: &str = "SHOUT_CHANNEL";
```

`Mode::Log` keeps a window of events, and a client that reconnects receives every one it missed.
See [Choosing a mode](../explanation/choosing-a-mode.md) for the other mode, `Latest`.

## 3. Write the Worker

```shell
cargo add --package shoutbox-worker partyline-worker worker serde_json
cargo add --package shoutbox-worker shoutbox-shared --path shared
```

A Worker is a `cdylib`. Add this to `worker/Cargo.toml`:

```toml
[lib]
crate-type = ["cdylib", "rlib"]
```

The Worker has two jobs: forward WebSocket upgrades to the channel's Durable Object, and publish each new shout.

```rust
// worker/src/lib.rs
use partyline_worker::{Connect, HubConfig, Publisher, close, decode_segment, reject};
use shoutbox_shared::{BINDING, Shout, Shouts};
use worker::*;

partyline_worker::channel_object! {
    /// The Durable Object for one room. It keeps the last 100 shouts.
    pub struct ShoutChannel: Shouts {
        config = HubConfig::default().retain_events(100);
    }
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        // The client connects to /partyline/shouts/{room}.
        .get_async("/partyline/shouts/:room", |req, ctx| async move {
            let Some(Ok(connect)) = Connect::<Shouts>::from_path(&req.path()) else {
                return reject(close::BAD_REQUEST, "invalid room");
            };
            connect.forward(&ctx.env, BINDING, req).await
        })
        .post_async("/api/rooms/:room/shouts", |mut req, ctx| async move {
            let Some(room) = ctx.param("room").and_then(|room| decode_segment(room)) else {
                return Response::error("invalid room", 400);
            };
            let shout: Shout = match req.json().await {
                Ok(shout) => shout,
                Err(e) => return Response::error(e.to_string(), 400),
            };
            let head = Publisher::<Shouts>::new(&ctx.env, BINDING)?
                .publish(&room, &shout)
                .await?;
            Response::from_json(&head)
        })
        .run(req, env)
        .await
}
```

`channel_object!` generates the whole Durable Object: a struct with a `Hub` field and every handler delegated to it.
`Connect::from_path` matches `/partyline/shouts/{room}` and decodes the room ID.
`Publisher::publish` stores the event, gives it the next sequence number, and sends it to every open socket.

A real app authorizes the request before `forward`. See [Authenticate with Clerk](../how-to/authenticate-with-clerk.md).

## 4. Configure wrangler

```toml
# worker/wrangler.toml
name = "shoutbox"
main = "build/index.js"
compatibility_date = "2026-09-01"

[build]
command = "worker-build --release"

[[durable_objects.bindings]]
name = "SHOUT_CHANNEL"
class_name = "ShoutChannel"

# The hub stores its log in SQLite, so the class must use the SQLite storage backend.
[[migrations]]
tag = "v1"
new_sqlite_classes = ["ShoutChannel"]

# The web client. The Worker runs first for the API and the socket.
[assets]
directory = "./public"
binding = "ASSETS"
not_found_handling = "single-page-application"
run_worker_first = ["/api/*", "/partyline/*"]
```

The `class_name` is the struct name from `channel_object!`.
Add `build/`, `public/`, and `.wrangler/` to `worker/.gitignore`.

## 5. Write the web client

```shell
cargo add --package shoutbox-web dioxus --features web
cargo add --package shoutbox-web partyline-dioxus
cargo add --package shoutbox-web gloo-net --features http,json
cargo add --package shoutbox-web shoutbox-shared --path shared
```

```rust
// web/src/main.rs
use dioxus::prelude::*;
use partyline_dioxus::{ChannelMessage, ChannelOptions, PartylineProvider, Status, use_channel};
use shoutbox_shared::{Shout, Shouts};

const ROOM: &str = "lobby";

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    rsx! {
        PartylineProvider {
            Shoutbox {}
        }
    }
}

#[component]
fn Shoutbox() -> Element {
    let mut shouts = use_signal(Vec::<Shout>::new);
    let mut draft = use_signal(String::new);

    let channel = use_channel::<Shouts>(ChannelOptions::new(ROOM), move |message| match message {
        ChannelMessage::Event(shout) => shouts.write().push(shout),
        // The client fell too far behind to resume. Start the list over.
        ChannelMessage::Reset => shouts.write().clear(),
    });

    let post = move |e: FormEvent| {
        e.prevent_default();
        let text = draft.read().trim().to_owned();
        if text.is_empty() {
            return;
        }
        draft.set(String::new());
        spawn(async move {
            let _ = gloo_net::http::Request::post(&format!("/api/rooms/{ROOM}/shouts"))
                .json(&Shout { text })
                .unwrap()
                .send()
                .await;
        });
    };

    rsx! {
        p { if channel.status() == Status::Open { "Live" } else { "Connecting…" } }
        form { onsubmit: post,
            input { value: "{draft}", oninput: move |e| draft.set(e.value()) }
            button { r#type: "submit", "Shout" }
        }
        ul {
            for (i, shout) in shouts.read().iter().enumerate() {
                li { key: "{i}", "{shout.text}" }
            }
        }
    }
}
```

`PartylineProvider` puts the shared settings in context. On the web, the server is the page's own origin, so you configure nothing.
`use_channel` opens the socket when the component mounts and closes it when it unmounts.
It calls the closure once per event, in order, and the client drops duplicates.

`ChannelOptions::new(ROOM)` without `since` receives live events only, so a new tab starts with an empty list.
To show the shouts posted before the page opened, load them with the head and pass `since`: see [Load, then subscribe](../how-to/load-then-subscribe.md).

## 6. Run it locally

Build the web client into the Worker's assets, then start the Worker:

```shell
# From the workspace root. dx fails inside a member directory.
dx bundle --package shoutbox-web --platform web --release
mkdir -p worker/public
cp -r target/dx/shoutbox-web/release/web/public/. worker/public/

cd worker
npx wrangler dev
```

Open <http://localhost:8787/> in two tabs. Post in one. The shout appears in both.

Now stop `wrangler dev` with Ctrl-C, and start it again. Both tabs show "Connecting…", then "Live" again, without a reload.
That is the client's reconnect with backoff. It reconnects with its cursor, so a shout posted while a tab was away arrives when the tab is back.

`wrangler dev` does not pick up new assets while it runs. Restart it after each web build.

## 7. Deploy

```shell
cd worker
npx wrangler login
npx wrangler deploy
```

`wrangler deploy` runs `worker-build`, uploads the Worker and the assets, and applies the `v1` migration.
It prints the URL, such as `https://shoutbox.<your-subdomain>.workers.dev`. Open it on your phone and on your laptop.

## What you built

- A channel defined once and compiled into both sides.
- A Durable Object per room that stores the last 100 shouts and holds every socket. It hibernates between shouts, so an idle room costs almost nothing. See [Hibernation and cost](../explanation/hibernation-and-cost.md).
- A client that reconnects with backoff and resumes from its cursor.

## Next steps

- [Load, then subscribe](../how-to/load-then-subscribe.md): show the history on page load.
- [Authenticate with Clerk](../how-to/authenticate-with-clerk.md): let only signed-in users connect.
- [Deploy to Cloudflare](../how-to/deploy-to-cloudflare.md): a custom domain and rate limits.
- [How partyline works](../explanation/how-it-works.md).
