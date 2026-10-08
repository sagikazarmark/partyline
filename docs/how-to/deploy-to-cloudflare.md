# How to deploy a partyline Worker to Cloudflare

This guide covers what a partyline Worker needs in production: the Durable Object migrations, secrets, a custom domain, and rate limits.
[`examples/poll`](../../examples/poll) uses all of them. Its [wrangler.toml](../../examples/poll/worker/wrangler.toml) is a working reference.

## Deploy

From the Worker's directory, after you build the web client into `public/`:

```shell
npx wrangler login
npx wrangler deploy
```

`wrangler deploy` runs the `[build]` command (`worker-build --release`), uploads the Worker and the assets, and applies new migrations.

To deploy an example from a container, without local tools, use Dagger:

```shell
dagger call examples poll deploy --account-id <account-id> --api-token env://CLOUDFLARE_API_TOKEN
```

## Durable Object migrations

The hub stores its log in SQLite, so every class that embeds a hub must use the SQLite storage backend.
Declare it in the migration that creates the class:

```toml
[[durable_objects.bindings]]
name = "POLL_OBJECT"
class_name = "PollObject"

[[migrations]]
tag = "v1"
new_sqlite_classes = ["PollObject", "ActivityChannel"]
```

Rules for later changes:

- **Never edit an applied migration.** Add a new one with a new tag, such as `v2`.
- **A new class** goes in `new_sqlite_classes` of the new migration.
- **A renamed class** goes in `renamed_classes = [{ from = "Old", to = "New" }]`. Without it, the old objects and their logs are lost to the new name.
- **A class created without SQLite** cannot switch to it. A hub in it logs `partyline: creating tables failed`. Create a new SQLite class and move to it.
- **The class name is the struct name**, from `#[durable_object]` or `channel_object!`.

## Secrets

Values like signing keys are secrets, not `[vars]`. Set them per Worker:

```shell
npx wrangler secret put SESSION_SECRET
```

For `wrangler dev`, put them in `.dev.vars` next to `wrangler.toml`, and keep that file out of git. The chat example ships a `.dev.vars.example`.
Read a secret in the Worker with `env.secret("SESSION_SECRET")?.to_string()`.

partyline itself needs no secret.

## A custom domain

Run a public deployment on a custom domain. Zone-level rate limiting and firewall rules then apply to it.

```toml
routes = [{ pattern = "partyline.example.com", custom_domain = true }]
```

The zone must be in the same Cloudflare account. `wrangler deploy` creates the DNS record and the certificate.
The client connects to the page's own origin by default, so the web client needs no change.

## Rate limits

A public write route needs a rate limit. The poll example limits votes per client IP with the Workers rate limiting binding:

```toml
[[ratelimits]]
name = "VOTE_LIMITER"
namespace_id = "1001"
simple = { limit = 20, period = 10 }
```

```rust
let limiter = ctx.env.rate_limiter("VOTE_LIMITER")?;
let ip = req.headers().get("cf-connecting-ip")?.unwrap_or_default();
if !limiter.limit(ip).await?.success {
    return Response::error("Too many votes", 429);
}
```

`period` is 10 or 60 seconds. The limit is per Cloudflare location and is approximate, so treat it as abuse protection, not as an exact quota.
The socket itself is read-only, so connects need no rate limit for correctness. Use zone-level rules if you need one.

## Assets and routing

The Worker serves the Dioxus build as static assets, so the socket is same-origin: cookies work and CORS is not needed.

```toml
[assets]
directory = "./public"
binding = "ASSETS"
not_found_handling = "single-page-application"
run_worker_first = ["/api/*", "/partyline/*"]
```

Without `/partyline/*` in `run_worker_first`, upgrades get `index.html` and fail.

## Compatibility date

The examples pin `compatibility_date = "2026-09-01"`. CI also runs the end-to-end tests at 2025-04-01, the oldest date partyline is tested with.
Hibernation, the WebSocket auto-response, and close handling depend on compatibility flags, so test before you move an existing Worker to a much newer date.

## Before going public

- [ ] Every hub class is in `new_sqlite_classes`.
- [ ] Secrets are set with `wrangler secret put`.
- [ ] Write routes have a rate limit.
- [ ] The connect route authorizes, or the channel is meant to be public.
- [ ] Retention suits the audience: `HubConfig::retain_events`, `retain_for`. See [Limits](../explanation/limits.md).
- [ ] Test-only routes are not deployed.

## Related

- [Deploy the demo](../../examples/poll/README.md#deploy-the-demo)
- [Debug a connection](debug-a-connection.md)
- [Hibernation and cost](../explanation/hibernation-and-cost.md)
