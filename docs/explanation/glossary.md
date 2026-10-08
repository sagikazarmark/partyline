# Glossary

The terms these pages use, in alphabetical order.

## Channel

A stream of events of one type. A channel definition is a type that implements `Channel`: a `NAME`, a `MODE`, and an `Event` type. It lives in a crate that the Worker and the client share.

## Channel ID

The ID of one instance of a channel, such as an order ID. Each channel ID is one Durable Object. The client connects to `/partyline/{channel}/{id}`.

## Cursor

The position of a client in a channel: `(epoch, seq)`, written `{epoch}.{seq}`. A client reconnects with its cursor and receives every event after it.

## Durable Object

A Cloudflare Workers object with a single thread, its own storage, and a unique ID. partyline runs one per channel ID. See Cloudflare's [documentation](https://developers.cloudflare.com/durable-objects/).

## Epoch

A random number the hub writes when it creates its log. A reset or a storage wipe creates a new epoch, so old cursors stop matching.

## Event

One value the hub sends to every socket on a channel, with a sequence number. Its type is the channel's `Event` type.

## Head

The cursor of the last event published on a channel. A new client that loads state with the head and connects from it misses nothing.

## Hibernation

The runtime removes an idle Durable Object from memory and keeps its WebSockets open. See [Hibernation and cost](hibernation-and-cost.md).

## Hub

The part of a Durable Object that owns a channel's sockets and event log: `partyline_worker::Hub`. Embed it in your own object, or generate the object with `channel_object!`.

## Latest mode

A channel mode in which each event is the whole current value, and the hub keeps only the last one. See [Choosing a mode](choosing-a-mode.md).

## Log mode

A channel mode in which every event matters, and the hub keeps a window of events to replay. See [Choosing a mode](choosing-a-mode.md).

## Publish

Store an event in a channel's log, give it the next sequence number, and send it to every socket. `Publisher::publish` from the Worker, `Hub::publish` from the Durable Object.

## Replay

The events the hub sends right after `Hello` to a client that reconnects with a cursor behind the head.

## Reset

A frame that tells the client its cursor cannot be resumed. The app discards its state and refetches. Also `Publisher::reset`, which wipes a channel's log and starts a new epoch.

## Retention

How many events, and for how long, a Log channel keeps for replay. See [Limits](limits.md#retention).

## Sequence number

The position of an event in its channel, `seq`. It starts at 1 and increases by exactly 1 per publish.

## Status

The client's connection state: `Idle`, `Connecting`, `Open`, `Waiting`, `Unauthorized`, or `Stopped`. The Dioxus hooks expose it as a signal.

## Tag

A string attached to a socket when it is accepted, such as a user ID. `Publisher::close_tagged` closes the sockets with a tag. See [Target one user](../how-to/target-one-user.md).

## Token provider

An async function the client calls before every connect. Its result goes in the `token` query parameter. See [Authenticate with Clerk](../how-to/authenticate-with-clerk.md).

## Wake

A signal that the client may have been away: the page became visible, the network came back, or the page returned from the back-forward cache. A wake resets the backoff and connects at once, or probes an open socket.

## Worker

The Cloudflare Worker that serves the app, authorizes connects, forwards upgrades, and publishes. One Rust Worker per app holds the Durable Object classes too.
