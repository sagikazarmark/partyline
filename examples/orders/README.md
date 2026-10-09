# Orders example

The smallest partyline app: follow an order's status and notes live. It is the source of the README quick start and the fixture for the end-to-end tests.

| Directory | Contents |
| --- | --- |
| `shared/` | The `Orders` channel and the event types |
| `worker/` | The Worker and the `OrderChannel` Durable Object, with its wrangler configuration |
| `web/` | The Dioxus web client |
| `tail/` | A native command-line client that prints the order's events and status changes |

`OrderChannel` embeds the hub by hand. It owns the order state, so it applies each event and publishes it in one turn, and it reads a snapshot and its head in one turn.

## Run it

In `devenv shell`, one command builds and runs everything:

```shell
just examples dev orders
```

Or by hand. Requirements: `worker-build` (`cargo install worker-build`), Node.js, and the Dioxus CLI.

```shell
# From the workspace root: build the web client into the Worker's assets.
dx bundle --package orders-web --platform web --release
mkdir -p examples/orders/worker/public
cp -r target/dx/orders-web/release/web/public/. examples/orders/worker/public/

cd examples/orders/worker
worker-build --release
npx wrangler dev
```

Open <http://localhost:8787/> in two tabs, and change the status in one.

To follow the same order from a terminal, run the native client next to it:

```shell
cargo run -p orders-tail -- http://localhost:8787 demo
```

The connect route forwards every upgrade. A real app authorizes the request there first: see [how to authenticate with Clerk](../../docs/how-to/authenticate-with-clerk.md).

## Styles

The web client uses [Tailwind CSS](https://tailwindcss.com/) v4 through the built-in support in the Dioxus CLI, like the [poll example](../poll/README.md#styles).
`dx` compiles `web/tailwind.css` to `web/assets/tailwind.css` on every build. The compiled file is committed, so `cargo check` and CI work without `dx`.
After you change classes, run a `dx` build and commit the updated `web/assets/tailwind.css`.
