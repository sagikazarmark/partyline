# partyline documentation

partyline pushes sequenced, resumable events from a Cloudflare Durable Object to Dioxus clients over WebSockets.
A client that loses its connection reconnects with its last cursor and receives exactly the events it missed.

The documentation has four parts. Start with the tutorial if partyline is new to you.

## Tutorial

| Page | You will |
| --- | --- |
| [Your first channel](tutorial/first-channel.md) | Build a counter app from an empty directory: a shared crate, a Worker, a Dioxus web client. Run it locally, then deploy it |

## How-to guides

Each guide solves one task. They assume you have finished the tutorial or read [`examples/orders`](../examples/orders).

| Guide | Task |
| --- | --- |
| [Load, then subscribe](how-to/load-then-subscribe.md) | Load initial state and subscribe without losing an event in between |
| [Authenticate with Clerk](how-to/authenticate-with-clerk.md) | Let only signed-in users open a socket, and close their sockets on sign-out |
| [Target one user](how-to/target-one-user.md) | Send events to one user only |
| [Share the alarm](how-to/share-the-alarm.md) | Use the Durable Object's alarm for your own work next to the hub |
| [Use with axum](how-to/use-with-axum.md) | Route upgrades and publish from an axum router on Workers |
| [Evolve event types](how-to/evolve-event-types.md) | Change an event type without breaking clients that are still open |
| [Test your app with the loopback harness](how-to/test-your-app-with-loopback.md) | Test your client logic under dropped connections, offline |
| [Debug a connection](how-to/debug-a-connection.md) | Find out why a client does not connect, or stops |
| [Deploy to Cloudflare](how-to/deploy-to-cloudflare.md) | Deploy a partyline Worker: migrations, secrets, a custom domain, rate limits |

## Explanation

| Page | Topic |
| --- | --- |
| [How partyline works](explanation/how-it-works.md) | The three flows: connect, publish, recover |
| [Choosing a mode](explanation/choosing-a-mode.md) | Log or Latest |
| [Hibernation and cost](explanation/hibernation-and-cost.md) | Why an idle channel costs almost nothing |
| [Limits](explanation/limits.md) | Every size, count, and time limit, and what happens past it |
| [Glossary](explanation/glossary.md) | The terms these pages use |

## Reference

| Page | Contents |
| --- | --- |
| [Protocol](protocol.md) | The wire protocol: connect URL, frames, close codes, timing. Includes how to write a client in another language |
| API documentation on docs.rs | [`partyline`](https://docs.rs/partyline), [`partyline-worker`](https://docs.rs/partyline-worker), [`partyline-client`](https://docs.rs/partyline-client), [`partyline-dioxus`](https://docs.rs/partyline-dioxus) |
| [Changelog](../CHANGELOG.md) | Changes in each release |

## For contributors

| Page | Contents |
| --- | --- |
| [Testing](testing.md) | The five test layers, the manual hibernation check, and the phone matrix |
| [Implementation plan](design/plan.md) | The design record partyline was built from |
| [M0 spike notes](design/m0-spike.md) | What the `worker` API spike found, and every change to the plan |
