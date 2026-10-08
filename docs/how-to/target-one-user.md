# How to send events to one user

This guide shows how to send events that only one user may see, such as notifications.

Every event on a channel goes to every socket on that channel. There is no per-socket filter, because filtering would wake the Durable Object for each socket and keep it from hibernating.
To target one user, give each user their own channel.

## 1. Define a per-user channel

```rust
pub struct Inbox;

impl Channel for Inbox {
    const NAME: &'static str = "inbox";
    const MODE: Mode = Mode::Log;
    type Event = Notification;
}
```

The channel ID is the user ID. Each user is one Durable Object.

## 2. Allow only that user to connect

Authorize the upgrade in the route handler. The user may open only the channel whose ID is their own user ID.
Tag the socket with the user ID, so you can close it later.

```rust
router.get_async("/partyline/inbox/:id", |req, ctx| async move {
    let Some(id) = ctx.param("id").and_then(|id| decode_segment(id)) else {
        return reject(close::BAD_REQUEST, "invalid id");
    };
    let Some(user_id) = verify_session(&req, &ctx.env).await? else {
        return reject(close::UNAUTHORIZED, "token missing or expired");
    };
    if id != user_id {
        return reject(close::FORBIDDEN, "not your inbox");
    }
    Connect::<Inbox>::new(id)
        .tag(&user_id)
        .forward(&ctx.env, "INBOX_CHANNEL", req)
        .await
});
```

`verify_session` stands for your own code. See [Authenticate with Clerk](authenticate-with-clerk.md) for a full handler.

## 3. Publish to the user

```rust
Publisher::<Inbox>::new(&env, "INBOX_CHANNEL")?
    .publish(&user_id, &Notification::OrderShipped { order_id })
    .await?;
```

## 4. Close the user's sockets

On sign-out, or when access is revoked, close the user's sockets by tag. 4403 tells the client not to reconnect.

```rust
Publisher::<Inbox>::new(&env, "INBOX_CHANNEL")?
    .close_tagged(&user_id, &user_id, close::FORBIDDEN)
    .await?;
```

The first argument is the channel ID, the second the tag.
`close_tagged` works on one channel. Close the user's sockets on every channel you know they have open. Any other socket ends at its next reconnect, when authorization fails.

## Tags

A socket carries at most 10 tags. Each tag must be non-empty and at most 256 characters. `Connect::forward` returns an error for a tag that breaks these rules.
Tags can hold any Unicode text. `Connect` percent-encodes them on the way to the Durable Object.

Tags are useful on shared channels too: tag each socket with the user ID, and `close_tagged` removes one user from a shared channel.

## Related

- [Limits](../explanation/limits.md)
- [Glossary: tag](../explanation/glossary.md#tag)
