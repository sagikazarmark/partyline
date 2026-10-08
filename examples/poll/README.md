# Live poll demo

A live poll that an audience opens on their phones. It shows every partyline feature in about two minutes.

| View | URL | Contents |
| --- | --- | --- |
| Presenter | `/present` | The question, a QR code to the join link, live tally bars, the activity feed, and "Reset demo" |
| Phone | `/` | Answer buttons, the live tally, a status badge with the cursor, the activity feed, and "Go offline" |

## Channels

| Channel | Mode | Event | Durable Object | Built with |
| --- | --- | --- | --- | --- |
| `PollTally` | Latest | The full tally | `PollObject` | Manual embedding. The object owns the vote counts and publishes through its own hub |
| `PollActivity` | Log | `VoteCast`, `PollReset` | `ActivityChannel` | `channel_object!`. The Worker publishes through `Publisher` |

A vote travels over HTTP: the phone sends `POST /api/polls/{id}/vote`, `PollObject` counts it and publishes the tally, and the Worker publishes `VoteCast` to `ActivityChannel`.

## Demo script

1. Open the presenter view on a laptop.
2. Scan the QR code with two phones.
3. Vote on the first phone. The bars move on all three screens.
4. Switch the second phone to offline. Vote several times on the first.
5. Switch the second phone back. Its feed fills with the missed votes and shows the replay count.
6. Lock the second phone for a minute, vote on the first, then unlock. The tally is correct within seconds.
7. Press "Reset demo". Both phones receive `Reset` and start the feed over.

| Demo element | Capability shown |
| --- | --- |
| Bars move on every screen after a vote | Fan-out, Latest mode |
| The activity feed lists votes in order | Log mode, ordering |
| "Go offline", then back | Resume, with a count of replayed events |
| Status badge and cursor readout | Status and cursor as signals |
| Lock the phone, vote elsewhere, unlock | Wake and heartbeat |
| "Reset demo" | Epoch change and the `Reset` path |
| First vote after an idle night | Hibernation |

## Guards for a public URL

- A rate limit on the vote route: 20 votes per client IP per 10 seconds.
- A small retention window: 200 activity events.
- A daily reset at midnight UTC, from `PollObject`'s alarm.
- A custom subdomain, so zone-level rate limiting and firewall rules apply.

## Run it

Requirements: the Dioxus CLI (`dx`), `worker-build` (`cargo install worker-build`), and Node.js.

```shell
# From the workspace root. `dx` fails inside a member directory.
dx bundle --package poll-web --platform web --release
mkdir -p examples/poll/worker/public
cp -r target/dx/poll-web/release/web/public/. examples/poll/worker/public/
cd examples/poll/worker
npx wrangler dev
```

Restart `wrangler dev` after each web build: it does not pick up new assets while it runs.

Open <http://localhost:8787/present> and <http://localhost:8787/>. See [Deploy the demo](#deploy-the-demo) to deploy it.

## Deploy the demo

This example is the public demo. Deploy it after a release, so it runs the released code.

### What it uses

Taken from [`examples/poll/worker/wrangler.toml`](worker/wrangler.toml):

| Resource | Configuration | Setup needed |
| --- | --- | --- |
| Durable Objects | `PollObject` and `ActivityChannel`, both in `new_sqlite_classes` of migration `v1` | None. `wrangler deploy` applies the migration |
| Rate limiting | `VOTE_LIMITER`: 20 votes per client IP per 10 seconds | None. The binding is created on deploy |
| Static assets | The web client in `./public` | Build it first, below |
| Custom domain | Commented out in `routes` | Set your domain, below |

The demo needs no secrets, no KV namespace, and no D1 database.

### Steps

1. Set the domain. In `examples/poll/worker/wrangler.toml`, uncomment the `routes` line and set your subdomain. The zone must be in the Cloudflare account you deploy to. Do not commit your domain if the repository is public and the domain is private.

   ```toml
   routes = [{ pattern = "partyline.example.com", custom_domain = true }]
   ```

2. Deploy with Dagger, which builds everything in containers:

   ```shell
   dagger call examples poll deploy \
     --account-id <account-id> \
     --api-token env://CLOUDFLARE_API_TOKEN
   ```

   The API token needs the "Edit Cloudflare Workers" permissions, and "Zone: DNS: Edit" for the custom domain.

   Or by hand, from the workspace root:

   ```shell
   dx bundle --package poll-web --platform web --release
   mkdir -p examples/poll/worker/public
   cp -r target/dx/poll-web/release/web/public/. examples/poll/worker/public/
   cd examples/poll/worker
   npx wrangler deploy
   ```

3. Open `/present` on the domain and vote from a phone.
4. After a deploy that changes the hub, run the [manual hibernation check](../../docs/testing.md#manual-hibernation-check) and the [phone matrix](../../docs/testing.md#layer-5-phone-matrix).

The demo resets itself every day at midnight UTC, from `PollObject`'s alarm. Press "Reset demo" in the presenter view to reset it at once.
## Styles

The web client uses [Tailwind CSS](https://tailwindcss.com/) v4 through the built-in support in the Dioxus CLI.
`dx` compiles `web/tailwind.css` to `web/assets/tailwind.css` on every build, scanning `web/src` for class names, and the app links the result with `asset!`.

The compiled file is committed, because `asset!` checks at compile time that the file exists, so `cargo check` and CI work without `dx`.
After you change classes, run a `dx` build and commit the updated `web/assets/tailwind.css`.

The theme follows the system setting: every component has `dark:` variants.
