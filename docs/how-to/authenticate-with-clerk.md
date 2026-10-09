# How to authenticate connections with Clerk

This guide shows how to let only signed-in users open a partyline socket, with Clerk as the identity provider.
partyline contains no Clerk code and no ticket system. You verify the session in your own route handler.
The [chat example](../../examples/chat) runs the same flow with a stand-in sign-in in place of Clerk.

## How it works

1. Before every connect, the client calls its token provider. The provider calls Clerk's `getToken()`.
2. The client sends the token in the `token` query parameter. Browsers cannot set headers on a WebSocket.
3. The Worker verifies the token in the route handler, then calls `Connect::forward` with the user ID as a tag. `Connect::forward` removes the `token` parameter before the request reaches the Durable Object, so tokens stay out of its logs.
4. On sign-out, the Worker closes that user's sockets by tag with 4403, so the client does not reconnect.

The Clerk session token is short-lived, so it does the job of a one-time ticket.
The token is verified at upgrade only. A socket can outlive the token that opened it, and this is accepted in 0.1.

## 1. Provide the token on the client

Get the token from your Clerk integration and wrap it in a `TokenProvider`.
When the server rejects a token with 4401, the provider receives `refresh: true`. Skip Clerk's token cache then.

```rust
use partyline_dioxus::{PartylineProvider, TokenProvider, TokenRequest};

let token = TokenProvider::new(move |request: TokenRequest| async move {
    // Replace with your Clerk binding, for example a call to `Clerk.session.getToken()`.
    clerk_get_token(request.refresh).await.ok()
});

rsx! {
    PartylineProvider { token: Some(token),
        App {}
    }
}
```

A new token provider, for example after the user switches accounts, takes effect at the next connect. The hooks do not restart for it. To use it at once, call `reconnect()` on the handle the hook returns.

The browser also sends the session cookie with the upgrade, because the socket is same-origin.
After time in the background that cookie can be stale, which is why apps with auth set a provider.

## 2. Verify the token in the Worker

Read the token from the `token` parameter. Fall back to the session cookie when the parameter is absent.
Reject a missing or expired token with 4401, and a user without access to the channel with 4403.

```rust
use partyline_worker::{Connect, close, decode_segment, reject};

router.get_async("/partyline/orders/:id", |req, ctx| async move {
    // Route parameters arrive percent-encoded. Decode, so `Connect` and `Publisher` agree.
    let Some(id) = ctx.param("id").and_then(|id| decode_segment(id)) else {
        return reject(close::BAD_REQUEST, "invalid id");
    };
    let token = query_param(&req, "token")?.or_else(|| session_cookie(&req));

    // Your Clerk verification: check the JWT signature against Clerk's JWKS,
    // and the `exp`, `nbf`, and `azp` claims.
    let Some(user_id) = verify_clerk_token(token.as_deref(), &ctx.env).await? else {
        return reject(close::UNAUTHORIZED, "token missing or expired");
    };
    if !can_read_order(&user_id, &id, &ctx.env).await? {
        return reject(close::FORBIDDEN, "not allowed");
    }

    Connect::<Orders>::new(id)
        .tag(user_id)
        .forward(&ctx.env, "ORDER_CHANNEL", req)
        .await
})
```

`reject` accepts the socket and closes it at once, because browsers cannot read the HTTP status of a failed upgrade.

## 3. Close sockets on sign-out

When a user signs out, close their sockets on each channel you know they have open:

```rust
Publisher::<Orders>::new(&env, "ORDER_CHANNEL")?
    .close_tagged(&order_id, &user_id, close::FORBIDDEN)
    .await?;
```

Any other socket ends at its next reconnect, when verification fails.

## Apps without auth

Set no token provider, and call `Connect::forward` without a check.
A public channel is safe to expose, because the socket is read-only.
