# How to run the examples locally

This guide shows two ways to run an example on your machine: one Dagger command, or the tools by hand.

| Example | Open |
| --- | --- |
| `orders` | <http://localhost:8787/> in two tabs |
| `chat` | <http://localhost:8787/> in two browsers |
| `poll` | <http://localhost:8787/present> and <http://localhost:8787/> |

## With Dagger: one command

You need Docker and the [Dagger CLI](https://docs.dagger.io/install). Nothing else: Rust, `dx`, `worker-build`, and wrangler run in containers.

```shell
dagger call examples orders service up --ports 8787:8787
```

Replace `orders` with `chat` or `poll`. The command builds the Worker and the web client, then runs `wrangler dev` on port 8787 until you stop it with Ctrl-C.
The first run takes several minutes. Later runs reuse the build cache.

The chat example gets a development `SESSION_SECRET` from the Dagger module, so it needs no `.dev.vars`.

To run an example at another Workers compatibility date:

```shell
dagger call examples --compatibility-date 2025-04-01 orders service up --ports 8787:8787
```

To list everything the module can do:

```shell
dagger functions examples
```

## By hand

You need the Dioxus CLI (`dx`), `worker-build` (`cargo install worker-build`), and Node.js.

```shell
# From the workspace root. dx fails inside a member directory.
dx bundle --package orders-web --platform web --release
mkdir -p examples/orders/worker/public
cp -r target/dx/orders-web/release/web/public/. examples/orders/worker/public/

cd examples/orders/worker
npx wrangler dev
```

For another example, replace `orders` in the three paths and in the package name.
For `chat`, first copy `examples/chat/worker/.dev.vars.example` to `.dev.vars`.

`wrangler dev` runs `worker-build` itself, from the `[build]` section of `wrangler.toml`.
It does not pick up new assets while it runs: restart it after each web build.

## Follow an order from the terminal

With the orders example running, the native client in [`examples/orders/tail`](../../examples/orders/tail) prints each event and status change:

```shell
cargo run -p orders-tail -- http://localhost:8787 demo
```

`demo` is the order the web client shows. Change its status in the browser and watch the terminal.

## Run the end-to-end tests

```shell
dagger check examples:end-to-end
```

See [Testing](../testing.md) for every test layer.

## Related

- [Deploy to Cloudflare](deploy-to-cloudflare.md)
- [Examples](../../examples/README.md)
