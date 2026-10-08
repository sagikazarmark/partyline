//! The hub: a channel's sockets and event log, embedded in a Durable Object.

use std::marker::PhantomData;
use std::time::Duration;

use partyline::frame::{ConnectParams, PING, PONG, decode_segment};
use partyline::server::{self, Log, Retention, ServerError};
use partyline::{Channel, Cursor, Mode, close, codec};
use worker::{
    Date, Method, Request, Response, ScheduledTime, State, WebSocket, WebSocketIncomingMessage,
    WebSocketPair, WebSocketRequestResponsePair,
};

use crate::sql_log::SqlLog;

/// The header the Worker uses to pass socket tags to the hub: a percent-encoded JSON array.
/// The hub ignores this header unless [`crate::Connect`] set it, because `Connect` strips it
/// from client requests.
pub(crate) const TAGS_HEADER: &str = "x-partyline-tags";

/// The most tags a socket can carry. The runtime allows 10.
pub(crate) const MAX_TAGS: usize = 10;

/// The longest tag, in UTF-16 code units, as the runtime counts.
const MAX_TAG_LEN: usize = 256;

/// The longest close reason, in bytes. The WebSocket protocol allows 123.
const MAX_REASON_BYTES: usize = 123;

/// Hub settings.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct HubConfig {
    retention: Option<Retention>,
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

    /// The retention policy for a mode. [`Mode::Latest`] always keeps exactly one event.
    pub fn retention(&self, mode: Mode) -> Retention {
        match mode {
            Mode::Log => self.retention.unwrap_or(Retention::LOG_DEFAULT),
            Mode::Latest => Retention::LATEST,
        }
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
///     async fn websocket_message(&self, ws: WebSocket, msg: WebSocketIncomingMessage) -> Result<()> {
///         self.hub.on_message(ws, msg).await
///     }
///     async fn websocket_close(&self, ws: WebSocket, code: usize, reason: String, clean: bool) -> Result<()> {
///         self.hub.on_close(ws, code, reason, clean).await
///     }
///     async fn websocket_error(&self, ws: WebSocket, error: Error) -> Result<()> {
///         self.hub.on_error(ws, error).await
///     }
///     async fn alarm(&self) -> Result<Response> {
///         self.hub.on_alarm().await
///     }
/// }
/// ```
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
    pub fn new(state: State, config: HubConfig) -> Self {
        if let Err(e) = SqlLog::init(&state.storage().sql()) {
            worker::console_error!("partyline: creating tables failed: {e}");
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

    /// The Durable Object state.
    pub fn state(&self) -> &State {
        &self.state
    }

    /// The event log.
    pub fn log(&self) -> SqlLog {
        SqlLog::new(self.state.storage().sql())
    }

    /// Handles a request to the Durable Object.
    ///
    /// | Request | Action |
    /// | --- | --- |
    /// | `GET` with `Upgrade: websocket` | Accept the socket, send `Hello`, then replay, send latest, or reset |
    /// | `POST /publish` | Append, trim, fan out, return the new cursor |
    /// | `GET /head` | Return the current cursor |
    /// | `POST /close?tag=..&code=..` | Close sockets with a tag and close code |
    /// | `POST /reset` | Wipe the log, start a new epoch, close every socket with 1012 |
    pub async fn fetch(&self, mut req: Request) -> worker::Result<Response> {
        if is_upgrade(&req) {
            return self.accept(&req);
        }
        let url = req.url()?;
        match (req.method(), url.path()) {
            (Method::Post, "/publish") => {
                let body = req.bytes().await?;
                match self.publish_raw(&body).await {
                    Ok(cursor) => Response::ok(cursor.to_string()),
                    Err(PublishError::Invalid(e)) => Response::error(e, 400),
                    Err(PublishError::Worker(e)) => Err(e),
                }
            }
            (Method::Get, "/head") => Response::ok(self.head()?.to_string()),
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
                Response::ok(self.close_tagged(&tag, code).to_string())
            }
            (Method::Post, "/reset") => Response::ok(self.reset()?.to_string()),
            _ => Response::error("Not Found", 404),
        }
    }

    /// Accepts a WebSocket upgrade.
    ///
    /// Accepting the socket, reading the log, and sending the handshake frames run without
    /// awaiting, so no publish can land between the replay and live events.
    fn accept(&self, req: &Request) -> worker::Result<Response> {
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
        // aborts the Durable Object, so drop any that slip through.
        let tags: Vec<&str> = tags
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
        self.state.accept_websocket_with_tags(&pair.server, &tags);

        match server::handshake(C::MODE, params.cursor, &self.log()) {
            Ok(frames) => {
                for frame in frames {
                    pair.server.send_with_str(frame)?;
                }
            }
            Err(e) => {
                worker::console_error!("partyline: handshake failed: {e}");
                pair.server
                    .close(Some(close::INTERNAL_ERROR), Some("handshake failed"))?;
            }
        }
        Response::from_websocket(pair.client)
    }

    /// Publishes an event to every socket and returns the new head.
    pub async fn publish(&self, event: &C::Event) -> worker::Result<Cursor> {
        let body =
            codec::encode_event(event).map_err(|e| worker::Error::RustError(e.to_string()))?;
        self.publish_raw(&body)
            .await
            .map_err(PublishError::into_worker)
    }

    async fn publish_raw(&self, body: &[u8]) -> Result<Cursor, PublishError> {
        // Validate that the body is this channel's event type before storing it.
        codec::decode_event::<C::Event>(body).map_err(|e| PublishError::Invalid(e.to_string()))?;
        let retention = self.config.retention(C::MODE);
        let now = now_ms();
        let (head, frame) =
            server::publish(&mut self.log(), body, &retention, now).map_err(|e| match e {
                ServerError::Log(e) => PublishError::Worker(e),
                ServerError::Codec(e) => PublishError::Invalid(e.to_string()),
            })?;
        for ws in self.state.get_websockets() {
            if ws.send_with_str(&frame).is_err() {
                // The client resumes from its cursor when it reconnects.
                let _ = ws.close(Some(close::INTERNAL_ERROR), Some("send failed"));
            }
        }
        self.schedule_trim(&retention)
            .await
            .map_err(PublishError::Worker)?;
        Ok(head)
    }

    /// The current head.
    pub fn head(&self) -> worker::Result<Cursor> {
        self.log().head()
    }

    /// Closes every socket with `tag`, using `code`, and returns how many were closed.
    pub fn close_tagged(&self, tag: &str, code: u16) -> usize {
        let sockets = self.state.get_websockets_with_tag(tag);
        for ws in &sockets {
            let _ = ws.close(Some(code), Some("closed by server"));
        }
        sockets.len()
    }

    /// Wipes the log, starts a new epoch, and closes every socket with 1012.
    ///
    /// Clients reconnect. A Log client receives `Reset`; a Latest client adopts the new epoch.
    pub fn reset(&self) -> worker::Result<Cursor> {
        let head = self.log().reset()?;
        for ws in self.state.get_websockets() {
            let _ = ws.close(Some(close::SERVICE_RESTART), Some("channel reset"));
        }
        Ok(head)
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
    pub async fn on_alarm(&self) -> worker::Result<Response> {
        let retention = self.config.retention(C::MODE);
        self.log().trim(&retention, now_ms())?;
        self.schedule_trim(&retention).await?;
        Response::ok("")
    }

    /// Sets an alarm for when the oldest event expires, unless one is set or nothing expires.
    async fn schedule_trim(&self, retention: &Retention) -> worker::Result<()> {
        let Some(max_age) = retention.max_age else {
            return Ok(());
        };
        let Some(oldest) = self.log().oldest_ts()? else {
            return Ok(());
        };
        let storage = self.state.storage();
        if storage.get_alarm().await?.is_some() {
            return Ok(());
        }
        let at = oldest + max_age.as_millis() as u64 + 1;
        let date = worker::js_sys::Date::new(&(at as f64).into());
        storage.set_alarm(ScheduledTime::new(date)).await
    }
}

enum PublishError {
    Invalid(String),
    Worker(worker::Error),
}

impl PublishError {
    fn into_worker(self) -> worker::Error {
        match self {
            Self::Invalid(e) => worker::Error::RustError(e),
            Self::Worker(e) => e,
        }
    }
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
    Ok(())
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
    }
}
