//! A handle on one channel of a hub, and an event stored but not yet sent.

use partyline::server::Log;
use partyline::{Channel, Cursor, close, codec};
use worker::State;

use crate::hub::{Hub, PublishError};
use crate::sql_log::SqlLog;

/// One channel of a [`Hub`]: the channel of the hub's type with one ID.
///
/// Get it with [`Hub::channel`]. It borrows the hub and holds only the ID, so it is cheap to
/// make for every publish.
///
/// # Store, then send
///
/// [`ChannelHandle::publish_now`] stores an event and sends it in one call. An object that
/// commits its state inside a block that can roll back, such as a closure passed to the
/// storage's `transactionSync`, must not send from inside the block: if the block rolls
/// back, the event is gone but the frame is already out, and the client holds a sequence
/// number that no longer exists. The next event reuses that number, and the client drops
/// it as a duplicate.
///
/// Store inside the block with [`ChannelHandle::append`], and send after the commit.
/// `transaction_sync` stands for the app's own binding of `transactionSync`, which `worker`
/// does not provide:
///
/// ```ignore
/// let pending = transaction_sync(|| {
///     self.apply(&command)?;
///     Ok([
///         self.market.channel(&game_id).append(&tick)?,
///         self.seats.channel(&seat_id).append(&view)?,
///     ])
/// })?;
/// for event in pending {
///     event.send();
/// }
/// self.market.schedule_trim().await?;
/// ```
pub struct ChannelHandle<'a, C: Channel> {
    hub: &'a Hub<C>,
    id: String,
}

impl<C: Channel> std::fmt::Debug for ChannelHandle<'_, C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ChannelHandle")
            .field("channel", &C::NAME)
            .field("id", &self.id)
            .finish()
    }
}

impl<'a, C: Channel> ChannelHandle<'a, C> {
    pub(crate) fn new(hub: &'a Hub<C>, id: String) -> Self {
        Self { hub, id }
    }

    /// The channel ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// The channel's event log.
    pub fn log(&self) -> SqlLog {
        self.hub.log_of(&self.id)
    }

    /// The channel's current head. Return it with the initial state, so the client can
    /// connect with `since` set to it.
    pub fn head(&self) -> worker::Result<Cursor> {
        self.log().head()
    }

    /// Stores an event and trims the log by count, without awaiting and without sending it.
    /// Returns the event's frame, which [`PendingSend::send`] sends to the channel's sockets.
    ///
    /// Use it inside a block that can roll back, and send after the block commits. See
    /// [Store, then send](ChannelHandle#store-then-send).
    ///
    /// A pending send that is dropped unsent loses nothing, because the event is stored. A
    /// Log client sees the gap at the channel's next event and resumes from its cursor. A
    /// Latest client gets the value when it next connects, or with the next event.
    ///
    /// The event is checked first: an event over [`crate::HubConfig::max_event_bytes`], or
    /// one that does not decode back as the channel's event type, is an error, and nothing
    /// is stored.
    pub fn append(&self, event: &C::Event) -> worker::Result<PendingSend<'a>> {
        let body =
            codec::encode_event(event).map_err(|e| worker::Error::RustError(e.to_string()))?;
        // Clients must be able to decode what is stored. Some values encode to JSON that
        // does not decode back, such as a NaN `f64`, which encodes as `null`.
        codec::decode_event::<C::Event>(&body)
            .map_err(|e| worker::Error::RustError(e.to_string()))?;
        self.hub
            .append_encoded(&self.id, &body)
            .map_err(PublishError::into_worker)
    }

    /// Publishes an event to the channel's sockets and returns the new head, without
    /// awaiting.
    ///
    /// It is [`ChannelHandle::append`] followed by [`PendingSend::send`]. The object can
    /// change its own state and call `publish_now` on any of its channels any number of
    /// times with no `await` in between. The Durable Object then commits all of those writes
    /// together, and its output gate holds the sent events until the writes are stored.
    ///
    /// It does not schedule age-based trimming. Call [`Hub::schedule_trim`] after the last
    /// publish of the turn. Without it, an idle channel keeps expired events until a later
    /// publish schedules the trim.
    ///
    /// Committing together does not make a batch all-or-nothing. An error, such as an event
    /// over [`crate::HubConfig::max_event_bytes`], rejects only that event: the events
    /// published before it in the turn are already stored and sent, and so are the object's
    /// own writes. Check each event first, or treat the events already published as sent.
    ///
    /// # Not inside a block that can roll back
    ///
    /// Don't call `publish_now` inside a block that can roll back, such as a closure passed
    /// to the storage's `transactionSync`. Use [`ChannelHandle::append`] there instead. See
    /// [Store, then send](ChannelHandle#store-then-send).
    pub fn publish_now(&self, event: &C::Event) -> worker::Result<Cursor> {
        let pending = self.append(event)?;
        let head = pending.head();
        pending.send();
        Ok(head)
    }

    /// Publishes an event to the channel's sockets and returns the new head.
    ///
    /// It is [`ChannelHandle::publish_now`] followed by [`Hub::schedule_trim`]. To publish
    /// several events in one turn, call `publish_now` for each, then `schedule_trim` once.
    pub async fn publish(&self, event: &C::Event) -> worker::Result<Cursor> {
        let head = self.publish_now(event)?;
        self.hub.schedule_trim().await?;
        Ok(head)
    }

    /// Wipes the channel's log, starts a new epoch, and closes the channel's sockets with
    /// 1012.
    ///
    /// Clients reconnect. A Log client receives `Reset`. A Latest client adopts the new
    /// epoch, but has nothing to show until the next publish, so publish a fresh value
    /// right after a reset.
    pub fn reset(&self) -> worker::Result<Cursor> {
        self.hub.reset_channel(&self.id)
    }

    /// Closes the channel's sockets that carry `tag`, using `code`, and returns how many
    /// were closed. [`Hub::close_tagged`] closes them on every channel in the object.
    pub fn close_tagged(&self, tag: &str, code: u16) -> usize {
        self.hub.close_tagged_on(&self.id, tag, code)
    }
}

/// An event that [`ChannelHandle::append`] stored, and that its channel's sockets have not
/// received yet.
///
/// Call [`PendingSend::send`] once the writes it belongs to are committed, before the next
/// `await`. Send the pending events of one channel in the order they were appended: a Log
/// client that gets them out of order reconnects and resumes, which works but wastes a
/// round trip.
#[must_use = "the event is stored, but no socket receives it until `send` is called"]
#[derive(Debug)]
pub struct PendingSend<'a> {
    state: &'a State,
    log: SqlLog,
    tag: String,
    frame: String,
    head: Cursor,
}

impl<'a> PendingSend<'a> {
    pub(crate) fn new(
        state: &'a State,
        log: SqlLog,
        tag: String,
        frame: String,
        head: Cursor,
    ) -> Self {
        Self {
            state,
            log,
            tag,
            frame,
            head,
        }
    }

    /// The channel's head after this event.
    pub fn head(&self) -> Cursor {
        self.head
    }

    /// Sends the event to the channel's sockets, without awaiting.
    ///
    /// It sends nothing if the channel was reset since the event was stored. The reset
    /// deleted the event, and a frame carries only its sequence number, so a client in the
    /// new epoch would take the old event as current, and then drop new events up to its
    /// number as duplicates.
    pub fn send(self) {
        match self.log.head() {
            Ok(current) if current.epoch == self.head.epoch => {}
            _ => return,
        }
        for ws in self.state.get_websockets_with_tag(&self.tag) {
            if ws.send_with_str(&self.frame).is_err() {
                // The client resumes from its cursor when it reconnects.
                let _ = ws.close(Some(close::INTERNAL_ERROR), Some("send failed"));
            }
        }
    }
}
