# How to evolve event types

This guide shows how to change a channel's event type without breaking clients that are already open.

## Why this needs care

The event type lives in the shared crate, and the Worker and the client compile the same version of it. A deploy changes that.
After you deploy a new Worker, browser tabs and native apps still run the old client, for minutes or for weeks.
The hub also stores events: a client that resumes receives events published by the old Worker and by the new one.

So for a while, an old client receives events from a new Worker, and a new client replays events from the old one.

## What the client does with an event it cannot decode

When a client cannot decode an event, it stops with `Status::Stopped { reason: StopReason::Incompatible { seq } }`.
It does not reconnect: the replay would deliver the same event again, forever.

Show a reload prompt when you see it:

```rust
let outdated = matches!(
    channel.status(),
    Status::Stopped { reason: StopReason::Incompatible { .. } }
);
rsx! {
    if outdated {
        p { "A new version is available. " a { href: "", "Reload" } }
    }
}
```

A reload loads the new client, which decodes the new events and resumes.

## Safe changes

The events are JSON. serde decides what an old client accepts.

| Change | Old client | New client reading old events | Safe |
| --- | --- | --- | --- |
| Add an optional field with `#[serde(default)]` | Ignores the field | Uses the default | Yes |
| Add a required field | Ignores the field | Fails on old events in the log | No. Make it optional |
| Remove a field that has `#[serde(default)]` in old clients | Uses the default | Unaffected | Yes |
| Remove a required field | Fails | Unaffected | No. Make it optional first, in an earlier release |
| Rename a field or a variant | Fails | Fails on old events | No. Never rename. Add a new one, use `#[serde(alias)]` to read the old name |
| Add an enum variant | Fails, unless it has a catch-all variant | Unaffected | Only in two steps, below |
| Change a field's type | Usually fails | Usually fails | No. Add a new field |

partyline never denies unknown fields in its own frames. Do not use `#[serde(deny_unknown_fields)]` on events.

## Add an enum variant

An old client fails on a variant it does not know. Do one of these:

**Option 1: deploy the clients first.** Release a client that knows the new variant, wait until most users run it, then deploy the Worker that publishes it.
This works for native apps with forced updates, and for web apps where a reload happens often.

**Option 2: add a catch-all variant once.** With an internally tagged enum, `#[serde(other)]` on a unit variant catches every unknown tag:

```rust
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OrderEvent {
    StatusChanged { status: OrderStatus },
    NoteAdded { note: String },
    /// A variant this client does not know. Ignore it.
    #[serde(other)]
    Unknown,
}
```

Clients that have `Unknown` decode any new variant as `Unknown` and keep running. Ship it before the first new variant: it protects every later one.
Your reducer must ignore `Unknown`, and the Worker must never publish it.

`#[serde(other)]` works with internally tagged (`tag = "type"`) and adjacently tagged enums, not with the default externally tagged form.

**Option 3: accept the reload.** If neither fits, deploy the Worker and let old clients stop with `Incompatible` and prompt a reload. This is correct, but every open tab sees the prompt.

## Change the meaning of an event

If old and new events cannot be read the same way, start a new channel instead: a new `NAME`, such as `orders-v2`, and a new Durable Object binding.
Old clients stay on the old channel until they reload. Publish to both while old clients exist.

## Reset as a last resort

`Publisher::reset` wipes a channel's log and starts a new epoch. Clients that resume get `Reset` and refetch, so no client replays old events.
This removes the "new client reads old events" half of the problem, but not the "old client reads new events" half.

## Checklist

- [ ] New fields are optional, with `#[serde(default)]`.
- [ ] No renames. No type changes.
- [ ] Enums have a `#[serde(other)]` catch-all, or clients ship before the Worker.
- [ ] The app shows a reload prompt on `StopReason::Incompatible`.

## Related

- [Protocol: frames](../protocol.md#frames)
- [Debug a connection](debug-a-connection.md)
