//! The hub: a channel's sockets and event log, embedded in a Durable Object.

use std::marker::PhantomData;
use std::time::Duration;

use futures::StreamExt;

use partyline::frame::{ConnectParams, PING, PONG, decode_segment, encode_segment};
use partyline::server::{self, Limits, Log, Retention, ServerError};
use partyline::{Channel, Cursor, Mode, close, codec};
use worker::{
    Date, Method, Request, Response, ScheduledTime, State, WebSocket, WebSocketIncomingMessage,
    WebSocketPair, WebSocketRequestResponsePair,
};

use crate::channel::{ChannelHandle, PendingSend};
use crate::sql_log::{self, SqlLog};

/// The header the Worker uses to pass socket tags to the hub: a percent-encoded JSON array.
/// The hub ignores this header unless [`crate::Connect`] set it, because `Connect` strips it
/// from client requests.
pub(crate) const TAGS_HEADER: &str = "x-partyline-tags";

/// The header the Worker uses to tell the hub which channel a request is for:
/// `{name}/{id}`, each percent-encoded as a path segment, so neither contains a `/`.
///
/// [`crate::Connect`] and [`crate::Publisher`] set it on every request to the object, and
/// `Connect` replaces any value a client sent. Without it, a request is for the object's
/// own channel: the channel of the hub's type whose ID is the object's name.
pub(crate) const CHANNEL_HEADER: &str = "x-partyline-channel";

/// The value of [`CHANNEL_HEADER`] for a channel.
pub(crate) fn encode_channel(name: &str, id: &str) -> String {
    format!("{}/{}", encode_segment(name), encode_segment(id))
}

/// Reads [`CHANNEL_HEADER`]: `None` without the header, `Some(None)` for a malformed one.
fn requested_channel(req: &Request) -> worker::Result<Option<Option<(String, String)>>> {
    Ok(req.headers().get(CHANNEL_HEADER)?.map(|value| {
        let (name, id) = value.split_once('/')?;
        Some((decode_segment(name)?, decode_segment(id)?))
    }))
}

/// The most tags a socket can carry, as the runtime allows.
const RUNTIME_MAX_TAGS: usize = 10;

/// The most tags the app can give a socket: the hub uses one for the socket's channel.
pub(crate) const MAX_TAGS: usize = RUNTIME_MAX_TAGS - 1;

/// The prefix of the hub's channel tags. App tags cannot start with it.
pub(crate) const RESERVED_TAG_PREFIX: &str = "partyline:";

/// The longest tag, in UTF-16 code units, as the runtime counts.
const MAX_TAG_LEN: usize = 256;

/// The longest close reason, in bytes. The WebSocket protocol allows 123.
const MAX_REASON_BYTES: usize = 123;

/// Hub settings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HubConfig {
    retention: Option<Retention>,
    limits: Limits,
}

impl HubConfig {
    /// Keeps at most `n` events for replay. Applies to [`Mode::Log`] channels only.
    pub fn retain_events(mut self, n: u64) -> Self {
        let base = self.retention.unwrap_or(Retention::LOG_DEFAULT);
        self.retention = Some(base.with_max_events(n));
        self
    }

    /// Keeps events for at most `age`. `None` removes the age limit. Applies to
    /// [`Mode::Log`] channels only.
    pub fn retain_for(mut self, age: Option<Duration>) -> Self {
        let base = self.retention.unwrap_or(Retention::LOG_DEFAULT);
        self.retention = Some(base.with_max_age(age));
        self
    }

    /// Rejects events larger than `n` bytes when encoded. Default: 64 KiB.
    ///
    /// A rejected publish returns HTTP 413 from the hub, and an error from
    /// [`crate::Publisher::publish`] and [`Hub::publish`].
    pub fn max_event_bytes(mut self, n: usize) -> Self {
        self.limits.max_event_bytes = n;
        self
    }

    /// Replays at most `n` bytes of events on reconnect. A client further behind gets
    /// `Reset` and refetches. Default: 8 MiB.
    pub fn max_replay_bytes(mut self, n: usize) -> Self {
        self.limits.max_replay_bytes = n;
        self
    }

    /// The retention policy for a mode. [`Mode::Latest`] always keeps exactly one event.
    pub fn retention(&self, mode: Mode) -> Retention {
        match mode {
            Mode::Log => self.retention.unwrap_or(Retention::LOG_DEFAULT),
            Mode::Latest => Retention::LATEST,
        }
    }

    /// The size limits.
    pub fn limits(&self) -> Limits {
        self.limits
    }
}

/// A channel's hub. Embed it in a Durable Object and delegate the handlers to it.
///
/// The hub holds only the Durable Object state and its config. It caches nothing, so the
/// Durable Object can hibernate with sockets open.
///
/// ```ignore
/// #[durable_object]
/// pub struct OrderChannel {
///     hub: Hub<Orders>,
/// }
///
/// impl DurableObject for OrderChannel {
///     fn new(state: State, _env: Env) -> Self {
///         Self { hub: Hub::new(state, HubConfig::default()) }
///     }
///     async fn fetch(&self, req: Request) -> Result<Response> {
///         self.hub.fetch(req).await
///     }
///     // websocket_message, websocket_close, websocket_error, and alarm.
///     partyline_worker::hub_handlers!(hub);
/// }
/// ```
///
/// # Publishing in one turn
///
/// [`Hub::publish`] awaits once, to schedule age-based trimming. An object that changes its
/// own state and publishes several events in one turn uses [`Hub::publish_now`], which never
/// awaits, for each event, then calls [`Hub::schedule_trim`] once. The state change and the
/// events then commit together, and no other request runs in between.
///
/// # Several channels
///
/// A hub serves every channel of its type in the object. [`Hub::publish`], [`Hub::head`],
/// and [`Hub::reset`] act on the object's own channel, whose ID is the object's name.
/// [`Hub::channel`] reaches any other channel, such as a private view per player in the
/// game's object. For channels of another type, create a second hub with [`Hub::sibling`],
/// and route each request to its hub with [`Hub::handles`].
///
/// # Alarms
///
/// A Durable Object has one alarm. The hub uses it for age-based trimming in [`Mode::Log`].
/// An object that needs its own alarm shares it: both sides schedule with
/// [`Hub::schedule_alarm`], which keeps the earlier time, and the object's `alarm` handler
/// calls [`Hub::on_alarm`], then does its own work only if it is due, then schedules its
/// next time again. An alarm can fire early for either side, so each checks its own time.
pub struct Hub<C: Channel> {
    state: State,
    config: HubConfig,
    _channel: PhantomData<fn() -> C>,
}

impl<C: Channel> std::fmt::Debug for Hub<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Hub")
            .field("channel", &C::NAME)
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}

impl<C: Channel> Hub<C> {
    /// Creates the hub. Runs the two `CREATE TABLE IF NOT EXISTS` statements and registers
    /// the `ping`/`pong` auto-response, so heartbeats do not wake the Durable Object.
    ///
    /// # Upgrading from partyline 0.4
    ///
    /// partyline 0.4 kept one channel per object, in tables of its own. The first hub
    /// created in such an object moves that channel into the shared tables, as the channel
    /// of this hub's type whose ID is the object's name. That is the channel the object held
    /// in 0.4, so clients resume from their cursors without `Reset`. In an object that
    /// embeds several hubs, create the hub of the channel type it held in 0.4 first.
    pub fn new(state: State, config: HubConfig) -> Self {
        let sql = state.storage().sql();
        if let Err(e) = SqlLog::init(&sql) {
            worker::console_error!("partyline: creating tables failed: {e}");
        }
        if let Err(e) = SqlLog::migrate_legacy(&sql, C::NAME, &own_id(&state)) {
            worker::console_error!("partyline: migrating 0.4 tables failed: {e}");
        }
        match WebSocketRequestResponsePair::new(PING, PONG) {
            Ok(pair) => state.set_websocket_auto_response(&pair),
            Err(e) => worker::console_error!("partyline: auto-response failed: {e:?}"),
        }
        Self {
            state,
            config,
            _channel: PhantomData,
        }
    }

    /// Creates a hub for another channel type in the same object.
    ///
    /// An object that holds channels of several types has one hub per type. [`Hub::new`]
    /// takes the object's state, so create the first hub with it and the others from that
    /// hub:
    ///
    /// ```ignore
    /// fn new(state: State, _env: Env) -> Self {
    ///     let market = Hub::new(state, HubConfig::default().retain_for(None));
    ///     let seats = market.sibling(HubConfig::default());
    ///     Self { market, seats }
    /// }
    /// ```
    ///
    /// When upgrading an object from partyline 0.4, create the hub of the channel type it
    /// held then with [`Hub::new`], so that it migrates the old channel.
    ///
    /// # Panics
    ///
    /// If both channel types have the same `NAME`. Their channels would share logs and
    /// sockets, so this is a mistake in the object's code.
    pub fn sibling<D: Channel>(&self, config: HubConfig) -> Hub<D> {
        use worker::wasm_bindgen::{JsCast, JsValue};
        assert_ne!(
            C::NAME,
            D::NAME,
            "two hubs in one object must have channel types with different names"
        );
        // A second handle on the same JavaScript object.
        let raw: &JsValue = self.state.as_raw().as_ref();
        let raw = raw
            .clone()
            .unchecked_into::<worker::worker_sys::DurableObjectState>();
        Hub::new(State::from(raw), config)
    }

    /// The Durable Object state.
    pub fn state(&self) -> &State {
        &self.state
    }

    fn own_id(&self) -> String {
        own_id(&self.state)
    }

    /// The event log of the object's own channel.
    pub fn log(&self) -> SqlLog {
        self.log_of(&self.own_id())
    }

    pub(crate) fn log_of(&self, id: &str) -> SqlLog {
        SqlLog::new(self.state.storage().sql(), C::NAME, id)
    }

    /// Handles a request to the Durable Object, for the channel the request names.
    ///
    /// [`crate::Connect`] and [`crate::Publisher`] name the channel in a header. A request
    /// without it is for the object's own channel, whose ID is the object's name. A request
    /// for another channel type gets 404.
    ///
    /// | Request | Action |
    /// | --- | --- |
    /// | `GET` with `Upgrade: websocket` | Accept the socket, send `Hello`, then replay, send latest, or reset |
    /// | `POST /publish` | Append, trim, fan out, return the new cursor |
    /// | `GET /head` | Return the current cursor |
    /// | `POST /close?tag=..&code=..` | Close the channel's sockets that have a tag, with a close code |
    /// | `POST /reset` | Wipe the log, start a new epoch, close the channel's sockets with 1012 |
    pub async fn fetch(&self, req: Request) -> worker::Result<Response> {
        self.fetch_with(req, |_| Ok(())).await
    }

    /// Like [`Hub::fetch`], but calls `on_publish` with each event that
    /// [`crate::Publisher::publish`] sends, before the hub stores it and sends it out.
    ///
    /// Use it to apply the event to the object's own state, so the state change and the
    /// event land in one turn. `on_publish` runs synchronously, after the event is
    /// validated. An error from it aborts the publish.
    ///
    /// ```ignore
    /// async fn fetch(&self, req: Request) -> Result<Response> {
    ///     self.hub.fetch_with(req, |event| self.apply(event)).await
    /// }
    /// ```
    ///
    /// A hub with several channels uses [`Hub::fetch_with_channel`], which also passes the
    /// channel ID.
    pub async fn fetch_with(
        &self,
        req: Request,
        on_publish: impl FnOnce(&C::Event) -> worker::Result<()>,
    ) -> worker::Result<Response> {
        self.fetch_with_channel(req, |_, event| on_publish(event))
            .await
    }

    /// Like [`Hub::fetch_with`], but calls `on_publish` with the ID of the channel the event
    /// is for too, so a hub with several channels can apply the event to the right state.
    ///
    /// ```ignore
    /// self.seats
    ///     .fetch_with_channel(req, |seat_id, view| self.store_view(seat_id, view))
    ///     .await
    /// ```
    pub async fn fetch_with_channel(
        &self,
        mut req: Request,
        on_publish: impl FnOnce(&str, &C::Event) -> worker::Result<()>,
    ) -> worker::Result<Response> {
        let id = match requested_channel(&req)? {
            Some(Some((name, id))) if name == C::NAME => id,
            Some(Some((name, _))) => {
                return Response::error(format!("this hub holds no `{name}` channels"), 404);
            }
            Some(None) => return Response::error(format!("malformed {CHANNEL_HEADER}"), 400),
            None => self.own_id(),
        };
        if is_upgrade(&req) {
            return self.accept(&id, &req);
        }
        let url = req.url()?;
        match (req.method(), url.path()) {
            (Method::Post, "/publish") => {
                let max = self.config.limits().max_event_bytes;
                let Some(body) = read_body(&mut req, max).await? else {
                    return Response::error(
                        format!("event is more than the limit of {max} bytes"),
                        413,
                    );
                };
                match self.publish_raw(&id, &body, on_publish).await {
                    Ok(cursor) => Response::ok(cursor.to_string()),
                    Err(PublishError::Invalid(e)) => Response::error(e, 400),
                    Err(PublishError::TooLarge(e)) => Response::error(e, 413),
                    Err(PublishError::Worker(e)) => Err(e),
                }
            }
            (Method::Get, "/head") => Response::ok(self.log_of(&id).head()?.to_string()),
            (Method::Post, "/close") => {
                let mut tag = None;
                let mut code = close::FORBIDDEN;
                for (key, value) in url.query_pairs() {
                    match &*key {
                        "tag" => tag = Some(value.into_owned()),
                        "code" => code = value.parse().unwrap_or(close::FORBIDDEN),
                        _ => {}
                    }
                }
                let Some(tag) = tag else {
                    return Response::error("missing tag", 400);
                };
                Response::ok(self.close_tagged_on(&id, &tag, code).to_string())
            }
            (Method::Post, "/reset") => Response::ok(self.reset_channel(&id)?.to_string()),
            _ => Response::error("Not Found", 404),
        }
    }

    /// Accepts a WebSocket upgrade.
    ///
    /// Accepting the socket, reading the log, and sending the handshake frames run without
    /// awaiting, so no publish can land between the replay and live events.
    fn accept(&self, id: &str, req: &Request) -> worker::Result<Response> {
        let url = req.url()?;
        let pair = WebSocketPair::new()?;
        let params = match ConnectParams::parse(url.query().unwrap_or("")) {
            Ok(params) => params,
            Err(e) => {
                pair.server.accept()?;
                pair.server
                    .close(Some(e.close_code()), Some(close_reason(&e.to_string())))?;
                return Response::from_websocket(pair.client);
            }
        };
        let tags = req
            .headers()
            .get(TAGS_HEADER)?
            .and_then(|header| decode_segment(&header))
            .and_then(|json| serde_json::from_str::<Vec<String>>(&json).ok())
            .unwrap_or_default();
        // `Connect` validates tags. An invalid one would make the runtime throw, which
        // aborts the Durable Object, and a reserved one could join another channel, so drop
        // any that slip through.
        let mut tags: Vec<&str> = tags
            .iter()
            .map(String::as_str)
            .filter(|tag| match check_tag(tag) {
                Ok(()) => true,
                Err(e) => {
                    worker::console_error!("partyline: dropping socket tag: {e}");
                    false
                }
            })
            .take(MAX_TAGS)
            .collect();
        let channel = channel_tag(C::NAME, id);
        tags.push(&channel);
        self.state.accept_websocket_with_tags(&pair.server, &tags);

        let limits = self.config.limits();
        match server::handshake_with(C::MODE, params.cursor, &self.log_of(id), &limits) {
            Ok(frames) => {
                for frame in frames {
                    if let Err(e) = pair.server.send_with_str(frame) {
                        // The client reconnects and resumes from its cursor.
                        worker::console_error!("partyline: handshake send failed: {e}");
                        let _ = pair
                            .server
                            .close(Some(close::INTERNAL_ERROR), Some("send failed"));
                        break;
                    }
                }
            }
            Err(e) => {
                worker::console_error!("partyline: handshake failed: {e}");
                let _ = pair
                    .server
                    .close(Some(close::INTERNAL_ERROR), Some("handshake failed"));
            }
        }
        Response::from_websocket(pair.client)
    }

    /// A handle on the channel of this hub's type with this ID, in this object.
    ///
    /// An object that holds several channels of one type, such as a private view per player,
    /// publishes to each through its handle:
    ///
    /// ```ignore
    /// self.seats.channel(&seat_id).publish_now(&view)?;
    /// ```
    pub fn channel(&self, id: impl Into<String>) -> ChannelHandle<'_, C> {
        ChannelHandle::new(self, id.into())
    }

    /// Whether `req` names a channel of this hub's type.
    ///
    /// An object that embeds several hubs uses it to route each request to its hub. Every
    /// request from [`crate::Connect`] and [`crate::Publisher`] names its channel. A request
    /// that names none, or names it in a malformed header, is for no hub in particular, so
    /// send it to the last one, whose [`Hub::fetch`] serves the object's own channel or
    /// answers 400:
    ///
    /// ```ignore
    /// async fn fetch(&self, req: Request) -> Result<Response> {
    ///     if self.seats.handles(&req) {
    ///         return self.seats.fetch(req).await;
    ///     }
    ///     self.market.fetch_with(req, |event| self.apply(event)).await
    /// }
    /// ```
    pub fn handles(&self, req: &Request) -> bool {
        matches!(requested_channel(req), Ok(Some(Some((name, _)))) if name == C::NAME)
    }

    /// The object's own channel: the one whose ID is the object's name.
    fn own(&self) -> ChannelHandle<'_, C> {
        self.channel(self.own_id())
    }

    /// Publishes an event on the object's own channel and returns the new head.
    ///
    /// See [`ChannelHandle::publish`]. The object's own channel is the one whose ID is the
    /// object's name, as [`crate::Connect`] and [`crate::Publisher`] address it by default.
    pub async fn publish(&self, event: &C::Event) -> worker::Result<Cursor> {
        self.own().publish(event).await
    }

    /// Publishes an event on the object's own channel without awaiting, and returns the new
    /// head.
    ///
    /// See [`ChannelHandle::publish_now`], which explains how to publish several events in
    /// one turn, and why not to call it inside a block that can roll back.
    ///
    /// ```ignore
    /// self.apply(&command)?;
    /// self.hub.publish_now(&first)?;
    /// self.hub.publish_now(&second)?;
    /// self.hub.schedule_trim().await?;
    /// ```
    pub fn publish_now(&self, event: &C::Event) -> worker::Result<Cursor> {
        self.own().publish_now(event)
    }

    async fn publish_raw(
        &self,
        id: &str,
        body: &[u8],
        on_publish: impl FnOnce(&str, &C::Event) -> worker::Result<()>,
    ) -> Result<Cursor, PublishError> {
        let limits = self.config.limits();
        server::check_size::<worker::Error>(body, &limits).map_err(PublishError::from_server)?;
        // Validate that the body is this channel's event type before storing it.
        let event = codec::decode_event::<C::Event>(body)
            .map_err(|e| PublishError::Invalid(e.to_string()))?;
        on_publish(id, &event).map_err(PublishError::Worker)?;
        let pending = self.append_encoded(id, body)?;
        let head = pending.head();
        pending.send();
        self.schedule_trim().await.map_err(PublishError::Worker)?;
        Ok(head)
    }

    /// Appends an encoded event to a channel and trims its log by count, without awaiting,
    /// and returns the frame to send to the channel's sockets.
    pub(crate) fn append_encoded(
        &self,
        id: &str,
        body: &[u8],
    ) -> Result<PendingSend<'_>, PublishError> {
        let retention = self.config.retention(C::MODE);
        let limits = self.config.limits();
        let mut log = self.log_of(id);
        let (head, frame) = server::publish_with(&mut log, body, &retention, &limits, now_ms())
            .map_err(PublishError::from_server)?;
        Ok(PendingSend::new(
            &self.state,
            log,
            channel_tag(C::NAME, id),
            frame,
            head,
        ))
    }

    /// The current head of the object's own channel.
    pub fn head(&self) -> worker::Result<Cursor> {
        self.own().head()
    }

    /// Closes every socket in the object that carries `tag`, on every channel of every type,
    /// using `code`, and returns how many were closed.
    ///
    /// Use it to remove a user from every channel the object holds, for example on sign-out.
    /// [`ChannelHandle::close_tagged`] and [`crate::Publisher::close_tagged`] close the
    /// sockets on one channel only.
    pub fn close_tagged(&self, tag: &str, code: u16) -> usize {
        let sockets = self.state.get_websockets_with_tag(tag);
        for ws in &sockets {
            let _ = ws.close(Some(code), Some("closed by server"));
        }
        sockets.len()
    }

    /// Deletes every channel in the object, of every type: closes their sockets with 1000,
    /// so clients do not reconnect, and drops partyline's tables. Runs synchronously.
    ///
    /// Use it when the object deletes its own data, in the same synchronous block, so the
    /// object is never left with half of it. The hubs must not be used again in this
    /// instance of the object: their next storage call fails. The next time the object
    /// starts, [`Hub::new`] creates empty tables.
    ///
    /// It leaves the object's alarm set, because deleting it would need an `await`. If
    /// nothing else in the object uses the alarm, delete it after the block with
    /// `storage.delete_alarm()`. Otherwise a pending trim wakes the object again, which
    /// then holds empty tables.
    pub fn drop_tables(&self) -> worker::Result<()> {
        for ws in self.state.get_websockets() {
            let on_channel = self
                .state
                .get_tags(&ws)
                .iter()
                .any(|tag| tag.starts_with(RESERVED_TAG_PREFIX));
            if on_channel {
                let _ = ws.close(Some(close::NORMAL), Some("channel deleted"));
            }
        }
        sql_log::drop_tables(&self.state.storage().sql())
    }

    /// Wipes the log of the object's own channel, starts a new epoch, and closes the
    /// channel's sockets with 1012. See [`ChannelHandle::reset`].
    pub fn reset(&self) -> worker::Result<Cursor> {
        self.own().reset()
    }

    pub(crate) fn reset_channel(&self, id: &str) -> worker::Result<Cursor> {
        let head = self.log_of(id).reset()?;
        for ws in self.sockets_of(id) {
            let _ = ws.close(Some(close::SERVICE_RESTART), Some("channel reset"));
        }
        Ok(head)
    }

    /// The sockets on the channel with this ID.
    fn sockets_of(&self, id: &str) -> Vec<WebSocket> {
        self.state
            .get_websockets_with_tag(&channel_tag(C::NAME, id))
    }

    /// Closes the sockets on the channel with this ID that carry `tag`.
    pub(crate) fn close_tagged_on(&self, id: &str, tag: &str, code: u16) -> usize {
        let channel = channel_tag(C::NAME, id);
        let mut closed = 0;
        // The runtime looks up one tag at a time, so filter by the channel's tag.
        for ws in self.state.get_websockets_with_tag(tag) {
            if self.state.get_tags(&ws).contains(&channel) {
                let _ = ws.close(Some(code), Some("closed by server"));
                closed += 1;
            }
        }
        closed
    }

    /// Handles a message from a client. The auto-response normally answers `ping` without
    /// waking the Durable Object. This answers it if the runtime delivers it anyway.
    pub async fn on_message(
        &self,
        ws: WebSocket,
        message: WebSocketIncomingMessage,
    ) -> worker::Result<()> {
        if let WebSocketIncomingMessage::String(text) = message
            && text == PING
        {
            ws.send_with_str(PONG)?;
        }
        Ok(())
    }

    /// Completes the close handshake for a socket the client closed.
    ///
    /// The runtime reports 1005, 1006, and 1015 when the client sent no close frame. Then
    /// there is no handshake to complete, and a close frame sent on the dead transport
    /// would fail later, outside this handler, so none is sent.
    pub async fn on_close(
        &self,
        ws: WebSocket,
        code: usize,
        reason: String,
        _was_clean: bool,
    ) -> worker::Result<()> {
        let code = match u16::try_from(code) {
            Ok(code @ 1000..=4999) if !matches!(code, 1005 | 1006 | 1015) => code,
            _ => return Ok(()),
        };
        let _ = ws.close(Some(code), Some(close_reason(&reason)));
        Ok(())
    }

    /// Handles a socket error. The socket is already closed. The client reconnects.
    pub async fn on_error(&self, _ws: WebSocket, _error: worker::Error) -> worker::Result<()> {
        Ok(())
    }

    /// Runs age-based trimming, so idle channels are trimmed too, and schedules the next run.
    ///
    /// It trims every channel of this hub's type in the object. Call it from the object's
    /// `alarm` handler on every alarm, even one the object set for itself. It does nothing
    /// when no event has expired.
    pub async fn on_alarm(&self) -> worker::Result<Response> {
        let retention = self.config.retention(C::MODE);
        let now = now_ms();
        if let Some(cutoff) = retention.age_cutoff(now) {
            let sql = self.state.storage().sql();
            for id in sql_log::ids_with_events_before(&sql, C::NAME, cutoff)? {
                self.log_of(&id).trim(&retention, now)?;
            }
        }
        self.schedule_trim().await?;
        Response::ok("")
    }

    /// Sets the Durable Object's alarm to `at_ms`, in milliseconds since the Unix epoch,
    /// unless an earlier alarm is already set.
    ///
    /// The hub schedules its trimming with this too, so the object and the hub can share the
    /// one alarm a Durable Object has. See [the type docs](Hub#alarms).
    pub async fn schedule_alarm(&self, at_ms: u64) -> worker::Result<()> {
        let storage = self.state.storage();
        if let Some(existing) = storage.get_alarm().await?
            && existing <= at_ms as i64
        {
            return Ok(());
        }
        let date = worker::js_sys::Date::new(&(at_ms as f64).into());
        storage.set_alarm(ScheduledTime::new(date)).await
    }

    /// Schedules the alarm for when the oldest retained event expires, so age-based
    /// trimming runs even if the channel goes idle.
    ///
    /// It covers every channel of this hub's type in the object. It does nothing in
    /// [`Mode::Latest`], when retention has no age limit, or when no event is retained.
    /// [`Hub::publish`] and [`Hub::fetch`] call it for you. After [`Hub::publish_now`], call
    /// it yourself, once after the last publish of the turn. Calling it more often is
    /// harmless: like [`Hub::schedule_alarm`], it never pushes an earlier alarm back.
    pub async fn schedule_trim(&self) -> worker::Result<()> {
        let Some(max_age) = self.config.retention(C::MODE).max_age else {
            return Ok(());
        };
        let Some(oldest) = sql_log::oldest_ts_of(&self.state.storage().sql(), C::NAME)? else {
            return Ok(());
        };
        self.schedule_alarm(oldest + max_age.as_millis() as u64 + 1)
            .await
    }
}

pub(crate) enum PublishError {
    Invalid(String),
    TooLarge(String),
    Worker(worker::Error),
}

impl PublishError {
    fn from_server<E: Into<worker::Error>>(e: ServerError<E>) -> Self {
        match e {
            ServerError::Log(e) => Self::Worker(e.into()),
            ServerError::Codec(e) => Self::Invalid(e.to_string()),
            ServerError::TooLarge { size, max } => Self::TooLarge(format!(
                "event is {size} bytes, more than the limit of {max}"
            )),
        }
    }

    pub(crate) fn into_worker(self) -> worker::Error {
        match self {
            Self::Invalid(e) | Self::TooLarge(e) => worker::Error::RustError(e),
            Self::Worker(e) => e,
        }
    }
}

/// Reads a request body of at most `max` bytes, or returns `None` as soon as it is longer,
/// so an oversized publish is never buffered whole.
async fn read_body(req: &mut Request, max: usize) -> worker::Result<Option<Vec<u8>>> {
    let declared = req
        .headers()
        .get("content-length")?
        .and_then(|len| len.parse::<usize>().ok());
    if declared.is_some_and(|len| len > max) {
        return Ok(None);
    }
    if req.inner().body().is_none() {
        return Ok(Some(Vec::new()));
    }
    let mut body = Vec::new();
    let mut stream = req.stream()?;
    while let Some(chunk) = stream.next().await {
        let chunk = chunk?;
        if body.len() + chunk.len() > max {
            return Ok(None);
        }
        body.extend_from_slice(&chunk);
    }
    Ok(Some(body))
}

/// Checks a socket tag against the runtime's rules: not empty, at most 256 characters.
pub(crate) fn check_tag(tag: &str) -> Result<(), String> {
    if tag.is_empty() {
        return Err("a socket tag must not be empty".to_owned());
    }
    if tag.encode_utf16().count() > MAX_TAG_LEN {
        return Err(format!(
            "a socket tag must be at most {MAX_TAG_LEN} characters"
        ));
    }
    if tag.starts_with(RESERVED_TAG_PREFIX) {
        return Err(format!(
            "socket tags starting with `{RESERVED_TAG_PREFIX}` are reserved for partyline"
        ));
    }
    Ok(())
}

/// The tag of every socket on the channel with this name and ID: the reserved prefix and the
/// hex SHA-256 of the name and ID, 74 characters in all.
///
/// A hash keeps the tag under the runtime's length limit for any ID. Channels must never
/// share a tag, because a socket receives every event sent to its tag, so the hash must
/// resist collisions. The name goes first with its length, so that no two pairs give the
/// same input.
pub(crate) fn channel_tag(name: &str, id: &str) -> String {
    use sha2::{Digest, Sha256};
    use std::fmt::Write;

    let mut hasher = Sha256::new();
    hasher.update((name.len() as u64).to_be_bytes());
    hasher.update(name.as_bytes());
    hasher.update(id.as_bytes());
    let mut tag = String::with_capacity(RESERVED_TAG_PREFIX.len() + 64);
    tag.push_str(RESERVED_TAG_PREFIX);
    for byte in hasher.finalize() {
        let _ = write!(tag, "{byte:02x}");
    }
    tag
}

/// Shortens a close reason to the 123 bytes the protocol allows, at a character boundary.
/// A longer reason makes `close` throw.
pub(crate) fn close_reason(reason: &str) -> &str {
    if reason.len() <= MAX_REASON_BYTES {
        return reason;
    }
    let mut end = MAX_REASON_BYTES;
    while !reason.is_char_boundary(end) {
        end -= 1;
    }
    &reason[..end]
}

pub(crate) fn is_upgrade(req: &Request) -> bool {
    req.headers()
        .get("upgrade")
        .ok()
        .flatten()
        .is_some_and(|v| v.eq_ignore_ascii_case("websocket"))
}

/// The ID of the object's own channel: the object's name, as `Connect` and `Publisher`
/// address it by default. An object made without a name, such as from a unique ID, uses its
/// hex ID instead, so it still has a channel of its own.
fn own_id(state: &State) -> String {
    let id = state.id();
    id.name().unwrap_or_else(|| id.to_string())
}

fn now_ms() -> u64 {
    Date::now().as_millis()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn close_reasons_are_cut_at_a_char_boundary() {
        assert_eq!(close_reason("short"), "short");
        let long = "é".repeat(100);
        let cut = close_reason(&long);
        assert_eq!(cut.len(), 122);
        assert!(cut.chars().all(|c| c == 'é'));
        assert_eq!(close_reason(&"a".repeat(200)).len(), 123);
    }

    #[test]
    fn tags_follow_the_runtime_rules() {
        assert!(check_tag("user-1").is_ok());
        assert!(check_tag("").is_err());
        assert!(check_tag(&"日".repeat(256)).is_ok());
        assert!(check_tag(&"a".repeat(257)).is_err());
        assert!(check_tag("partyline:x").is_err(), "the prefix is reserved");
        assert!(check_tag("partyline").is_ok());
    }

    #[test]
    fn channel_tags_are_fixed_length_and_distinct() {
        let tag = channel_tag("seat", &"x".repeat(10_000));
        assert_eq!(tag.len(), 74);
        assert!(tag.starts_with(RESERVED_TAG_PREFIX));
        assert_eq!(tag, channel_tag("seat", &"x".repeat(10_000)));
        // The length prefix keeps a shifted boundary from colliding.
        assert_ne!(channel_tag("ab", "c"), channel_tag("a", "bc"));
        assert_ne!(channel_tag("seat", "1"), channel_tag("seat", "2"));
        assert_ne!(channel_tag("seat", "1"), channel_tag("market", "1"));
        // An app tag can never equal a channel tag.
        assert!(check_tag(&tag).is_err());
    }

    #[test]
    fn app_tags_leave_room_for_the_channel_tag() {
        // The runtime throws on an eleventh tag, which aborts the object and drops every
        // socket on every channel.
        assert_eq!(RUNTIME_MAX_TAGS, 10);
        assert_eq!(MAX_TAGS + 1, RUNTIME_MAX_TAGS);
    }
}
