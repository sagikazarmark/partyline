# Limits

This page lists every limit that applies to a partyline channel, where it comes from, and what happens past it.

## Per channel

| Limit | Value | Set by | Past the limit |
| --- | --- | --- | --- |
| WebSocket connections | 32,768 per Durable Object | Cloudflare | The upgrade fails. A channel that could grow this large needs sharding, which partyline does not provide |
| Event size | 64 KiB encoded, by default | `HubConfig::max_event_bytes` | The hub answers HTTP 413, and `Publisher::publish` and `Hub::publish` return an error. Nothing is stored or sent |
| Replay size | 8 MiB of events per reconnect, by default | `HubConfig::max_replay_bytes` | The client gets `Reset` instead of the replay, and refetches |
| Retention, Log mode | 1,000 events or 24 hours, whichever is smaller | `HubConfig::retain_events`, `HubConfig::retain_for` | Older events are trimmed. A client whose cursor is older gets `Reset` |
| Retention, Latest mode | 1 event | Fixed | Not applicable |

Keep events well under the size limit. Send IDs and let the client fetch large payloads over HTTP.
A replay is built in one Durable Object turn, and the object has 128 MB of memory, so the replay budget protects the object as much as the client.

### Retention

Retention bounds the log. Count-based trimming runs on every publish. Age-based trimming runs from the Durable Object's alarm, so idle channels are trimmed too.
Trimming only ever removes the oldest events. The retained events are always a contiguous range ending at the head.

A replay also checks that the retained range has no gap after the client's cursor. If it has one, for example after storage trouble, the client gets `Reset`. A refetch is always correct.

```rust
partyline_worker::channel_object! {
    pub struct ActivityChannel: Activity {
        config = HubConfig::default()
            .retain_events(200)
            .retain_for(Some(Duration::from_secs(60 * 60)))
            .max_event_bytes(16 * 1024);
    }
}
```

`retain_for(None)` removes the age limit.

## Per socket

| Limit | Value | Set by | Past the limit |
| --- | --- | --- | --- |
| Tags | At most 10 per socket | Cloudflare | `Connect::forward` returns an error |
| Tag length | 1 to 256 characters | Cloudflare | `Connect::forward` returns an error |
| Attachment | 2,048 bytes | Cloudflare | The hub uses no attachment. Your own per-socket data must fit |
| Close reason | 123 bytes | The WebSocket protocol | The hub and `reject` cut longer reasons at a character boundary |

Tags can hold any Unicode text. `Connect` percent-encodes them on the way to the Durable Object.

## Close codes the hub does not send

The runtime reports 1005, 1006, and 1015 when a client goes away without a close frame. These codes are reserved and cannot be sent, so the hub does not echo them back.

## Client timing

| Setting | Default | Field of `ClientConfig` |
| --- | --- | --- |
| First reconnect delay | 500 ms, before jitter | `backoff_base` |
| Backoff factor | 2 | `backoff_factor` |
| Longest reconnect delay | 30 s, before jitter | `backoff_cap` |
| Open time before the attempt counter resets | 10 s | `stable_after` |
| Connect timeout: socket open and `Hello` | 10 s | `connect_timeout` |
| Heartbeat interval | 25 s of silence | `heartbeat_interval` |
| Heartbeat timeout | 10 s | `heartbeat_timeout` |
| Heartbeat timeout after a wake | 3 s | `wake_timeout` |

With the defaults, a client finds a dead socket within 35 s without a wake signal. Native targets have no wake source, so this is their worst case.

## Durable Object limits that matter

| Limit | Value |
| --- | --- |
| Memory per object | 128 MB |
| Alarms per object | 1. Share it with the hub: see [Share the alarm](../how-to/share-the-alarm.md) |
| Storage per object, SQLite backend | See Cloudflare's [limits](https://developers.cloudflare.com/durable-objects/platform/limits/) |

For every value set by Cloudflare, Cloudflare's [limits page](https://developers.cloudflare.com/durable-objects/platform/limits/) is the source. Check it if a value here looks out of date.

## Related

- [Choosing a mode](choosing-a-mode.md)
- [Protocol: retention](../protocol.md#retention)
