# How to run the examples locally

This guide shows two ways to run an example on your machine: one `just` command, or the tools by hand.

| Example | Open |
| --- | --- |
| `orders` | <http://localhost:8787/> in two tabs |
| `chat` | <http://localhost:8787/> in two browsers |
| `poll` | <http://localhost:8787/present> and <http://localhost:8787/> |

## With just: one command

You need [devenv](https://devenv.sh). It provides Rust, `dx`, `worker-build`, wrangler, and [just](https://just.systems). From the workspace root:

```shell
devenv shell
just examples dev orders
```

Replace `orders` with `chat` or `poll`. The command builds the Worker and the web client, then runs `wrangler dev` on port 8787 until you stop it with Ctrl-C.
Pass another port as a second argument: `just examples dev chat 8788`.

For `chat`, the command copies `.dev.vars.example` to `.dev.vars` if `.dev.vars` does not exist, so the Worker has a development `SESSION_SECRET`.

`just examples dev-all` runs all three at once, on ports 8787 (orders), 8788 (chat), and 8789 (poll).
To list every recipe:

```shell
just --list examples
```

## By hand

You need the Dioxus CLI (`dx`), `worker-build` (`cargo install worker-build`), and Node.js.

```shell
# From the workspace root. dx fails inside a member directory.
dx bundle --package orders-web --platform web --release
mkdir -p examples/orders/worker/public
cp -r target/dx/orders-web/release/web/public/. examples/orders/worker/public/

cd examples/orders/worker
worker-build --release
npx wrangler dev
```

For another example, replace `orders` in the paths and in the package name.
For `chat`, first copy `examples/chat/worker/.dev.vars.example` to `.dev.vars`.

`wrangler dev` does not pick up new assets while it runs: restart it after each web build.

## Follow an order from the terminal

With the orders example running, the native client in [`examples/orders/tail`](../../examples/orders/tail) prints each event and status change:

```shell
cargo run -p orders-tail -- http://localhost:8787 demo
```

`demo` is the order the web client shows. Change its status in the browser and watch the terminal.

## Run the end-to-end tests

```shell
# Against the orders and chat examples and the e2e fixture
just e2e

# Against one example
just examples e2e orders
```

The tests run their Workers under `wrangler dev` on ports 8790 to 8792 and stop them when they end, so they do not clash with `just examples dev` on ports 8787 to 8789.

See [Testing](../testing.md) for every test layer.

## Related

- [Deploy to Cloudflare](deploy-to-cloudflare.md)
- [Examples](../../examples/README.md)
