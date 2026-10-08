# Hibernation and cost

A Durable Object that holds open WebSockets can hibernate: the runtime removes it from memory and keeps the sockets open.
partyline is built so that a channel hibernates whenever nothing is published. An idle channel with connected clients then costs almost nothing.

## How hibernation works

Without hibernation, a Durable Object stays in memory as long as any socket is open, and you pay for that time.

With the WebSocket hibernation API, the runtime holds the sockets. The object is evicted after about 10 seconds without activity.
When something happens, such as a publish or an alarm, the runtime creates the object again, runs its constructor, and calls the handler.
The clients notice nothing.

## What partyline does to allow it

| Rule | Why |
| --- | --- |
| The hub holds only `State` and its config. It caches nothing, not even the head | An evicted object loses its memory. State in memory would be wrong after a wake |
| The log is in SQLite | Storage survives eviction |
| Heartbeats use the runtime's auto-response: the client sends `ping`, the runtime answers `pong` | A heartbeat answered in code would wake the object every 25 s per socket |
| The socket is one-way | No client message wakes the object |
| Tags live on the socket, set at accept | Close-by-tag needs no in-memory map |
| The constructor runs only two `CREATE TABLE IF NOT EXISTS` statements | A wake is cheap |

The same rules apply to your code in an object that embeds a hub: keep state in storage, not in fields.

## What costs money

| Activity | Wakes the object | Billed |
| --- | --- | --- |
| An idle socket | No | No duration |
| A heartbeat | No | No duration |
| A publish | Yes | One request, plus a short duration, plus the storage write |
| A connect | Yes | One request, plus the replay |
| An alarm for age-based trimming | Yes | One alarm, at most once per expiry |

See Cloudflare's [Durable Objects pricing](https://developers.cloudflare.com/durable-objects/platform/pricing/) for the current rates.
WebSocket messages sent to clients are not billed as requests.

## When an object does not hibernate

- A request or a handler is still running.
- An `await` is pending, such as an outgoing `fetch`.
- The object holds sockets accepted with the non-hibernation API. partyline always uses the hibernation API.

## Checking it

Hibernation cannot be forced in `wrangler dev`. The [manual hibernation check](../testing.md#manual-hibernation-check) confirms it on a deployed Worker: leave a socket idle for several minutes, publish, and confirm delivery and near-zero billed duration.

## Scale

One Durable Object accepts at most 32,768 WebSocket connections. Hibernation does not change this limit.
A channel that could exceed it needs sharding, which partyline does not provide. See [Limits](limits.md).

## Related

- [How partyline works](how-it-works.md)
- Cloudflare's [WebSocket hibernation](https://developers.cloudflare.com/durable-objects/best-practices/websockets/) documentation
