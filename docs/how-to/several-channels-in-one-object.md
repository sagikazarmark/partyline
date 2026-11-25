# How to hold several channels in one object

This guide shows how to keep several channels in one Durable Object, so the object can change its own state and publish to all of them in one turn.

By default each channel ID is its own object. That is awkward for an object whose state has more than one audience.
Take a game: one object owns the game, every player sees a shared market feed, and each player also has a private view of their portfolio. Every event on a channel goes to every socket on it, so each private view needs a channel of its own.
With one object per channel, the game publishes each view to another object after every command. The publish can fail after the game's state was saved, and two quick publishes can arrive out of order. Holding the views in the game's object avoids both: the state change and every event commit together.

## 1. Define the channels

Define each channel type as usual, in the crate that the Worker and the client share:

```rust
pub struct Market;

impl Channel for Market {
    const NAME: &'static str = "market";
    const MODE: Mode = Mode::Log;
    type Event = MarketEvent;
}

pub struct Seat;

impl Channel for Seat {
    const NAME: &'static str = "seat";
    const MODE: Mode = Mode::Latest;
    type Event = SeatView;
}
```

The game holds one market channel and one seat channel per player. The market's ID is the game ID, and each seat's ID is the player's seat ID.

## 2. Embed one hub per channel type

A hub serves every channel of its type in the object. Create the first hub with the object's state, and the others from it with `Hub::sibling`:

```rust
#[durable_object]
pub struct Game {
    market: Hub<Market>,
    seats: Hub<Seat>,
}

impl DurableObject for Game {
    fn new(state: State, _env: Env) -> Self {
        let market = Hub::new(state, HubConfig::default());
        let seats = market.sibling(HubConfig::default());
        Self { market, seats }
    }

    // fetch, below.

    // The WebSocket handlers, and an alarm that runs `on_alarm` on both hubs.
    partyline_worker::hub_handlers!(market, seats);
}
```

The hubs share the object's one alarm. An object with its own alarm calls `on_alarm` on every hub: see [Share the alarm](share-the-alarm.md#objects-with-several-hubs).

## 3. Route each request to its hub

`Connect` and `Publisher` name the channel in every request to the object. `Hub::handles` tells whether a request is for one of a hub's channels. Ask the hubs first, then match the object's own paths:

```rust
async fn fetch(&self, req: Request) -> Result<Response> {
    if self.seats.handles(&req) {
        return self.seats.fetch(req).await;
    }
    if self.market.handles(&req) {
        return self.market.fetch(req).await;
    }
    match req.path().as_str() {
        "/command" => self.command(req).await,
        _ => self.market.fetch(req).await,
    }
}
```

The order matters. The hubs' internal routes are `/publish`, `/head`, `/close`, and `/reset`. An object route on one of those paths, matched before the hubs, would catch the hub's request, and for example close a tag across the object instead of on one channel.

A request that names no channel goes to the last line, where the market hub serves it as its own channel, whose ID is the object's name. That only happens when the app sends a request without `Connect` or `Publisher`.

`fetch` passes on events that `Publisher::publish` sends from the Worker. To apply them to the object's state as they arrive, use `fetch_with_channel`, which also passes the channel ID: `self.seats.fetch_with_channel(req, |seat_id, view| self.store_view(seat_id, view))`.

## 4. Address the object from the Worker

`Connect::object` and `Publisher::object` name the object separately from the channel ID. Authorize the upgrade, then forward it to the game's object:

```rust
// GET /partyline/seat/{seat_id}
let Some(Ok(connect)) = Connect::<Seat>::from_path(&req.path()) else {
    return reject(close::BAD_REQUEST, "invalid id");
};
let Some(seat) = verify_seat(&req, &env).await? else {
    return reject(close::UNAUTHORIZED, "not signed in");
};
connect
    .object(&seat.game_id)
    .tag(&seat.id)
    .forward(&env, "GAME", req)
    .await
```

`verify_seat` stands for your own code. The market channel's ID is the game ID, which is also the object's name, so `Connect::<Market>` needs no `object`.

The client connects to `/partyline/seat/{seat_id}` as usual. It does not know which object holds the channel.

Always forward upgrades with `Connect`. The hub trusts the `x-partyline-channel` header to name the socket's channel, and `Connect` replaces any value the client sent. A forward written by hand would pass the client's header on, so a client could join another player's view.

## 5. Publish in the same turn as the state change

`Hub::channel` returns a handle on one channel. Change the state, then publish to every channel it affects, with no `await` in between:

```rust
async fn command(&self, req: Request) -> Result<Response> {
    let command: Command = req.json().await?;
    // From here to the last publish, nothing awaits.
    let outcome = self.apply(&command)?;
    // The market's ID is the object's name, so it is the market hub's own channel.
    self.market.publish_now(&outcome.tick)?;
    for (seat_id, view) in &outcome.views {
        self.seats.channel(seat_id).publish_now(view)?;
    }
    // Schedule age-based trimming once, after the last publish. Each hub trims its own
    // channels, so call it on every hub with an age limit. The seats are Latest, and keep
    // only their last value.
    self.market.schedule_trim().await?;
    Response::ok("")
}
```

The Durable Object commits the state change and every event together, and its output gate holds the sent events until the writes are stored.

### Inside a block that can roll back

If the object commits inside `transactionSync`, don't send from inside the block. A rollback would leave clients holding an event that no longer exists.
Store each event with `append` inside the block, and send it after the commit:

```rust
let pending = transaction_sync(|| {
    let outcome = self.apply(&command)?;
    let mut pending = vec![self.market.channel(&game_id).append(&outcome.tick)?];
    for (seat_id, view) in &outcome.views {
        pending.push(self.seats.channel(seat_id).append(view)?);
    }
    Ok(pending)
})?;
for event in pending {
    event.send();
}
```

`transaction_sync` stands for your own binding of the storage's `transactionSync`, which `worker` does not provide, and `game_id` for the game's ID.
An appended event that is never sent is not lost, because it is stored. A Log client sees the gap at the next event and resumes from its cursor. A Latest client gets the value with the next event, or when it next connects.

Send right after the commit, before the next `await`. While the object awaits, other requests run, and one could reset a channel. `send` then sends nothing, because the reset deleted the event with the old epoch.

## 6. Remove a player, and delete the game

`Hub::close_tagged` closes the sockets with a tag on every channel in the object, of every type. Tag each socket with the seat ID, as in step 4, to remove a player from the market and their view at once:

```rust
self.market.close_tagged(&seat_id, close::FORBIDDEN);
```

`ChannelHandle::close_tagged` and `Publisher::close_tagged` close the sockets on one channel only.

When the game ends and the object deletes its own data, `Hub::drop_tables` deletes every channel in the object in the same synchronous block. It closes their sockets with 1000, so clients do not reconnect. It leaves the alarm set: if nothing else uses it, delete it after the block with `storage.delete_alarm()`.

## Upgrading from partyline 0.4

partyline 0.4 kept one channel per object, in tables of its own. 0.5 changes these:

- **Storage.** The first hub created in an object moves the 0.4 channel into the new tables, as its own channel. Clients resume from their cursors without `Reset`. In an object with several hubs, create the hub of the channel type the object held in 0.4 with `Hub::new`, and the others with `Hub::sibling`.
- **Tags.** The hub uses one of a socket's 10 tags for its channel, so a socket carries at most 9 app tags. Tags that start with `partyline:` are reserved.
- **Sockets.** Sockets that 0.4 accepted have no channel tag, so 0.5 would not send them events. The deploy restarts the object, which disconnects them, and the clients reconnect and resume.
- **`SqlLog::new`** takes the channel name and ID.

The client protocol is unchanged, so clients need no update.

## Limits

Every channel in the object shares its 32,768 sockets, its 128 MB of memory, its storage, and its throughput. See [Limits: several channels in one object](../explanation/limits.md#several-channels-in-one-object).

## Related

- [Send events to one user](target-one-user.md)
- [Load, then subscribe](load-then-subscribe.md)
- [How partyline works: publish](../explanation/how-it-works.md#2-publish)
