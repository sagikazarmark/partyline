# How to share the alarm with the hub

This guide shows how to run your own scheduled work in a Durable Object that embeds a hub.
[`examples/poll`](../../examples/poll) does this for its daily reset.

## The problem

A Durable Object has one alarm. The hub uses it in Log mode to trim events older than the retention age, so idle channels are trimmed too.
If your object calls `storage.set_alarm` directly, it can overwrite the hub's alarm, or the hub can overwrite yours. Then one of the two never runs.

## The rule

Both sides schedule with `Hub::schedule_alarm(at_ms)`. It sets the alarm to the **earlier** of the current alarm and `at_ms`, so neither side pushes the other's time back.
The alarm can then fire for either side, so each side checks its own due time when it fires.

## 1. Store your due time

The alarm holds only one time, so store your own due time in the object's storage. The poll example keeps it in a one-row table:

```rust
fn reset_at(&self) -> Result<Option<u64>> { /* SELECT reset_at FROM poll_schedule */ }
fn set_reset_at(&self, at: Option<u64>) -> Result<()> { /* INSERT or DELETE */ }
```

## 2. Schedule with the hub

```rust
async fn schedule_daily_reset(&self) -> Result<()> {
    let at = match self.reset_at()? {
        Some(at) => at,
        None => {
            let next_midnight = (Date::now().as_millis() / DAY_MS + 1) * DAY_MS;
            self.set_reset_at(Some(next_midnight))?;
            next_midnight
        }
    };
    self.hub.schedule_alarm(at).await
}
```

`at` is in milliseconds since the Unix epoch.

## 3. Handle the alarm

Generate the WebSocket handlers with `hub_websocket_handlers!`, and write `alarm` yourself:

```rust
impl DurableObject for PollObject {
    // new, fetch ..

    partyline_worker::hub_websocket_handlers!(hub);

    async fn alarm(&self) -> Result<Response> {
        // Always first: the hub trims if that is due, and schedules its next trim.
        self.hub.on_alarm().await?;

        // The alarm is shared, so it can fire before your work is due.
        if let Some(at) = self.reset_at()? {
            if Date::now().as_millis() >= at {
                self.set_reset_at(None)?;
                self.daily_reset().await?;
            } else {
                // Not due yet. Schedule your time again: the earlier time wins.
                self.hub.schedule_alarm(at).await?;
            }
        }
        Response::ok("")
    }
}
```

The order matters:

1. `self.hub.on_alarm()` runs on every alarm. It trims events past the retention age and re-arms the hub's next trim, with the same earliest-wins rule. It does nothing when nothing has expired.
2. Check your own due time. The alarm may have fired for the hub.
3. Do your work if it is due. Otherwise schedule your time again with `schedule_alarm`, because the alarm that just fired is gone.

## Objects without their own alarm

Use `hub_handlers!(hub)`. It generates the three WebSocket handlers and an `alarm` that calls `self.hub.on_alarm()`.
`channel_object!` does the same.

## Related

- [Limits: retention](../explanation/limits.md#retention)
- The `Hub` documentation on [docs.rs](https://docs.rs/partyline-worker)
