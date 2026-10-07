# Chat example

A chat room that only signed-in users can open. It shows authentication end to end: a token provider on the client, token checks at upgrade, the 4401 refresh path, and sign-out closing a user's sockets with 4403.

| Directory | Contents |
| --- | --- |
| `shared/` | The `Chat` channel and the API types |
| `worker/` | The Worker, the token signing, and the `ChatChannel` Durable Object |
| `web/` | The Dioxus web client |

The sign-in is a stand-in for an identity provider: anyone can sign in as any name. Everything after sign-in works as it does with a real provider. To use Clerk, see [how to authenticate with Clerk](../../docs/how-to/authenticate-with-clerk.md).

## How it works

| Step | Client | Worker | Clerk equivalent |
| --- | --- | --- | --- |
| Sign in | `POST /api/login` with a name | Sets a signed session cookie for 8 hours, and returns an access token | Clerk's sign-in and session |
| Get a token | The token provider calls `GET /api/token` | Checks the session cookie, and returns an access token for 60 seconds | `getToken()` |
| Connect | Sends the token in the `token` query parameter | Checks the token, then forwards with `Connect::tag(user)` | Verify the JWT in the route |
| Token expired | Receives 4401, and calls the provider with `refresh: true` | Rejects the token with 4401 | `getToken({ skipCache: true })` |
| Sign out | `POST /api/logout` | Clears the cookie, and closes the user's sockets with 4403 | Clerk's sign-out |

The token provider returns its cached token until the server rejects it, like Clerk's `getToken()`.
The Worker accepts only the access token at upgrade, not the session cookie, so every connect goes through the token provider.

The two token kinds have the same format: base64url JSON claims and an HMAC-SHA256 signature, keyed with `SESSION_SECRET`.
A `kind` claim keeps the session cookie from being used as an access token.

The channel log is the message history: `ChatChannel` keeps the last 50 messages.
The page reads the channel head, and connects with `since` set 50 messages before it, so the server replays the history.

## Demo script

1. Sign in as Alice in one browser window, and as Bob in a private window. A session cookie belongs to one browser, so two tabs in the same browser are the same user.
2. Send messages from both. Each message shows the author from the session, not from the request body.
3. Wait one minute, then press "Reconnect" in Alice's window. The server rejects the expired token with 4401, the badge shows "Token rejected", and the client gets a fresh token and connects.
4. Open a second Alice tab, then press "Sign out" in the first. The second tab drops to the sign-in form. Bob stays connected.

## Run it

Requirements: the Dioxus CLI (`dx`), `worker-build` (`cargo install worker-build`), and Node.js.

```shell
# From the workspace root. `dx` fails inside a member directory.
dx bundle --package chat-web --platform web --release
mkdir -p examples/chat/worker/public
cp -r target/dx/chat-web/release/web/public/. examples/chat/worker/public/
cd examples/chat/worker
cp .dev.vars.example .dev.vars
npx wrangler dev
```

Restart `wrangler dev` after each web build: it does not pick up new assets while it runs.

Open <http://localhost:8787/>.

To deploy, set a random secret of at least 32 characters with `npx wrangler secret put SESSION_SECRET`. Do not deploy the stand-in sign-in.

## Limits of the stand-in

- Anyone can sign in as any name, and two people with the same name are the same user.
- Sign-out clears the cookie in that browser, but the session token stays valid until it expires. A real identity provider revokes the session on the server.
- The token is checked at upgrade only. A socket can outlive the token that opened it.

## Styles

The web client uses [Tailwind CSS](https://tailwindcss.com/) v4 through the built-in support in the Dioxus CLI, like the [poll example](../poll/README.md#styles).
`dx` compiles `web/tailwind.css` to `web/assets/tailwind.css` on every build. The compiled file is committed, so `cargo check` and CI work without `dx`.
After you change classes, run a `dx` build and commit the updated `web/assets/tailwind.css`.
