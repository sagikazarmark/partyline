# Examples

| Example | What it is |
| --- | --- |
| [`orders`](orders) | The smallest partyline app: follow an order's status and notes live. The README quick start is a summary of it. Start here |
| [`chat`](chat) | Authentication: a stand-in sign-in, short-lived access tokens, the 4401 refresh path, and sign-out with 4403. Read it before you add auth to your app |
| [`poll`](poll) | The live poll demo: a presenter view and a phone view, both channel modes, and both ways to build a Durable Object. It is deployed as the public demo |

Each example has three crates: `shared` (the channels and event types), `worker` (the Worker and its Durable Objects), and `web` (the Dioxus client).

The end-to-end tests run against `orders`, against `chat`, and against a separate test Worker in [`e2e/fixture`](../e2e/fixture), which holds the hooks that only tests need.
