//! Wire protocol: frames, cursors, channel modes, connect parameters, and close codes.

use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// The protocol version this crate speaks. It is sent as `v` in the connect URL and in `Hello`.
pub const PROTOCOL_VERSION: u8 = 1;

/// The heartbeat request a client sends as a text frame.
pub const PING: &str = "ping";

/// The heartbeat response the server sends as a text frame.
pub const PONG: &str = "pong";

/// The path prefix of the connect URL: `/partyline/{channel}/{id}`.
pub const PATH_PREFIX: &str = "/partyline";

/// How a channel treats its events.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mode {
    /// Every event matters. The hub retains a window of events and replays the gap on reconnect.
    Log,
    /// Each event is the full current value. Only the latest event is kept and sent.
    Latest,
}

/// A position in a channel: the log epoch and a sequence number.
///
/// The text form is `{epoch}.{seq}`. That form is used in the connect URL and in JSON.
/// The epoch is a random number the hub writes when it creates its log. If the log is wiped,
/// the epoch changes and old cursors no longer match.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Cursor {
    /// The log epoch.
    pub epoch: u64,
    /// The sequence number of the last event at this position. `0` means "before the first event".
    pub seq: u64,
}

impl Cursor {
    /// Creates a cursor.
    pub const fn new(epoch: u64, seq: u64) -> Self {
        Self { epoch, seq }
    }
}

impl fmt::Display for Cursor {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.epoch, self.seq)
    }
}

/// The error returned when a cursor string is not `{epoch}.{seq}`.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("invalid cursor {0:?}: expected `epoch.seq`")]
pub struct ParseCursorError(String);

impl FromStr for Cursor {
    type Err = ParseCursorError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let err = || ParseCursorError(s.to_owned());
        let (epoch, seq) = s.split_once('.').ok_or_else(err)?;
        let digits = |p: &str| !p.is_empty() && p.bytes().all(|b| b.is_ascii_digit());
        if !digits(epoch) || !digits(seq) {
            return Err(err());
        }
        Ok(Self {
            epoch: epoch.parse().map_err(|_| err())?,
            seq: seq.parse().map_err(|_| err())?,
        })
    }
}

impl Serialize for Cursor {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for Cursor {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let s = <std::borrow::Cow<'de, str>>::deserialize(deserializer)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// A frame sent from the server to the client.
///
/// Frames never deny unknown fields, so a later protocol version can add fields
/// without breaking older clients.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "t", rename_all = "snake_case")]
pub enum ServerFrame<E> {
    /// First frame on every connection.
    Hello {
        /// The protocol version of the server.
        v: u8,
        /// The channel mode.
        mode: Mode,
        /// The channel head when the connection was accepted.
        head: Cursor,
    },
    /// One event. In [`Mode::Log`], `seq` increases by exactly 1.
    Event {
        /// The sequence number of this event.
        seq: u64,
        /// The event.
        event: E,
    },
    /// The cursor cannot be resumed. Discard local state and refetch.
    Reset {
        /// The channel head. The client continues from here.
        head: Cursor,
    },
}

/// A WebSocket message, as seen by the protocol.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Message {
    /// A text frame.
    Text(String),
    /// A binary frame.
    Binary(Vec<u8>),
}

impl Message {
    /// The heartbeat request.
    pub fn ping() -> Self {
        Self::Text(PING.to_owned())
    }
}

/// WebSocket close codes used by partyline.
pub mod close {
    /// Normal close by either side. The client does not reconnect.
    pub const NORMAL: u16 = 1000;
    /// No status code was present. Reserved: never sent on the wire.
    pub const NO_STATUS: u16 = 1005;
    /// The connection was lost without a close frame. Reserved: never sent on the wire.
    pub const ABNORMAL: u16 = 1006;
    /// The server hit an unexpected condition. The client reconnects.
    pub const INTERNAL_ERROR: u16 = 1011;
    /// The server is restarting, or the channel was reset. The client reconnects.
    pub const SERVICE_RESTART: u16 = 1012;
    /// The server is overloaded. The client reconnects.
    pub const TRY_AGAIN_LATER: u16 = 1013;
    /// Malformed connect request. The client does not reconnect.
    pub const BAD_REQUEST: u16 = 4400;
    /// Token missing or expired. The client reconnects after asking the token provider again.
    pub const UNAUTHORIZED: u16 = 4401;
    /// Not allowed on this channel. The client does not reconnect.
    pub const FORBIDDEN: u16 = 4403;
    /// Protocol version not supported. The client does not reconnect.
    pub const UNSUPPORTED_VERSION: u16 = 4426;

    /// Reports whether a client stops for good after this close code.
    pub fn is_terminal(code: u16) -> bool {
        matches!(code, NORMAL | BAD_REQUEST | FORBIDDEN | UNSUPPORTED_VERSION)
    }
}

/// The query parameters of a connect URL: `?v=1&cursor={epoch}.{seq}&token=...`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConnectParams {
    /// The protocol version the client speaks.
    pub v: u8,
    /// The cursor to resume after. `None` means live events only.
    pub cursor: Option<Cursor>,
    /// An optional short-lived session token, for the app's own authorization.
    pub token: Option<String>,
}

/// Why a connect request was rejected. Each variant maps to a close code.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum ConnectError {
    /// The request is malformed.
    #[error("malformed connect request: {0}")]
    BadRequest(String),
    /// The client speaks a protocol version this server does not support.
    #[error("unsupported protocol version {0}")]
    UnsupportedVersion(String),
}

impl ConnectError {
    /// The close code to send for this error.
    pub fn close_code(&self) -> u16 {
        match self {
            Self::BadRequest(_) => close::BAD_REQUEST,
            Self::UnsupportedVersion(_) => close::UNSUPPORTED_VERSION,
        }
    }
}

impl ConnectParams {
    /// Creates parameters for the current protocol version.
    pub fn new(cursor: Option<Cursor>, token: Option<String>) -> Self {
        Self {
            v: PROTOCOL_VERSION,
            cursor,
            token,
        }
    }

    /// Encodes the parameters as a query string, without the leading `?`.
    pub fn to_query(&self) -> String {
        let mut query = format!("v={}", self.v);
        if let Some(cursor) = self.cursor {
            query.push_str(&format!("&cursor={cursor}"));
        }
        if let Some(token) = &self.token {
            query.push_str("&token=");
            query.push_str(&encode_segment(token));
        }
        query
    }

    /// Parses a query string, with or without the leading `?`.
    ///
    /// Unknown parameters are ignored. A missing or different `v` is an error.
    pub fn parse(query: &str) -> Result<Self, ConnectError> {
        let query = query.strip_prefix('?').unwrap_or(query);
        let mut v = None;
        let mut cursor = None;
        let mut token = None;
        for pair in query.split('&').filter(|p| !p.is_empty()) {
            let (key, value) = pair.split_once('=').unwrap_or((pair, ""));
            let value = percent_decode(value, true)
                .ok_or_else(|| ConnectError::BadRequest(format!("bad encoding in {key}")))?;
            match key {
                "v" => v = Some(value),
                "cursor" => {
                    cursor = Some(
                        value
                            .parse::<Cursor>()
                            .map_err(|e| ConnectError::BadRequest(e.to_string()))?,
                    )
                }
                "token" => token = Some(value),
                _ => {}
            }
        }
        let v = v.ok_or_else(|| ConnectError::BadRequest("missing v".to_owned()))?;
        match v.parse::<u8>() {
            Ok(PROTOCOL_VERSION) => Ok(Self {
                v: PROTOCOL_VERSION,
                cursor,
                token,
            }),
            _ => Err(ConnectError::UnsupportedVersion(v)),
        }
    }
}

/// Builds the connect path for a channel and ID: `/partyline/{channel}/{id}`.
///
/// The ID is percent-encoded, so it can hold any string.
pub fn connect_path(channel: &str, id: &str) -> String {
    format!(
        "{PATH_PREFIX}/{}/{}",
        encode_segment(channel),
        encode_segment(id)
    )
}

/// Percent-encodes a path segment or query value. Everything except the unreserved URL
/// characters `A-Z a-z 0-9 - _ . ~` is encoded, including `/`.
pub fn encode_segment(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        if b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.' | b'~') {
            out.push(b as char);
        } else {
            out.push_str(&format!("%{b:02X}"));
        }
    }
    out
}

/// Decodes a percent-encoded path segment, such as a channel ID taken from a route
/// parameter. `+` stays a literal `+`. Returns `None` on invalid encoding or invalid UTF-8.
///
/// Routers usually hand out path parameters still encoded. Decode the ID before passing it
/// to `Connect` or `Publisher`, so both name the same Durable Object.
pub fn decode_segment(s: &str) -> Option<String> {
    percent_decode(s, false)
}

/// Decodes `%XX` sequences, and `+` as a space when `plus_as_space` is set.
fn percent_decode(s: &str, plus_as_space: bool) -> Option<String> {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'%' => {
                let hex = s.get(i + 1..i + 3)?;
                out.push(u8::from_str_radix(hex, 16).ok()?);
                i += 3;
            }
            b'+' if plus_as_space => {
                out.push(b' ');
                i += 1;
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8(out).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_round_trips_as_text() {
        let cursor = Cursor::new(4_503_599_627_370_495, 42);
        assert_eq!(cursor.to_string(), "4503599627370495.42");
        assert_eq!("4503599627370495.42".parse::<Cursor>(), Ok(cursor));
        assert_eq!(
            serde_json::to_string(&cursor).unwrap(),
            "\"4503599627370495.42\""
        );
    }

    #[test]
    fn cursor_rejects_bad_text() {
        for bad in ["", "1", "1.", ".1", "1.2.3", "a.1", "-1.2", "1.+2"] {
            assert!(bad.parse::<Cursor>().is_err(), "{bad:?} should not parse");
        }
    }

    #[test]
    fn frames_use_the_documented_json_shape() {
        let hello: ServerFrame<()> = ServerFrame::Hello {
            v: 1,
            mode: Mode::Log,
            head: Cursor::new(7, 3),
        };
        assert_eq!(
            serde_json::to_string(&hello).unwrap(),
            r#"{"t":"hello","v":1,"mode":"log","head":"7.3"}"#
        );
        let event = ServerFrame::Event { seq: 4, event: "x" };
        assert_eq!(
            serde_json::to_string(&event).unwrap(),
            r#"{"t":"event","seq":4,"event":"x"}"#
        );
        let reset: ServerFrame<()> = ServerFrame::Reset {
            head: Cursor::new(7, 9),
        };
        assert_eq!(
            serde_json::to_string(&reset).unwrap(),
            r#"{"t":"reset","head":"7.9"}"#
        );
    }

    #[test]
    fn frames_accept_unknown_fields() {
        let frame: ServerFrame<()> =
            serde_json::from_str(r#"{"t":"reset","head":"1.2","snapshot":{"a":1}}"#).unwrap();
        assert_eq!(
            frame,
            ServerFrame::Reset {
                head: Cursor::new(1, 2)
            }
        );
    }

    #[test]
    fn connect_params_round_trip() {
        let params = ConnectParams::new(Some(Cursor::new(9, 1)), Some("a b/c&d=é".to_owned()));
        let query = params.to_query();
        assert_eq!(query, "v=1&cursor=9.1&token=a%20b%2Fc%26d%3D%C3%A9");
        assert_eq!(ConnectParams::parse(&format!("?{query}")), Ok(params));
        assert_eq!(
            ConnectParams::parse("v=1"),
            Ok(ConnectParams::new(None, None))
        );
    }

    #[test]
    fn connect_params_reject_bad_requests() {
        assert_eq!(
            ConnectParams::parse("cursor=1.2").unwrap_err().close_code(),
            close::BAD_REQUEST
        );
        assert_eq!(
            ConnectParams::parse("v=1&cursor=x")
                .unwrap_err()
                .close_code(),
            close::BAD_REQUEST
        );
        assert_eq!(
            ConnectParams::parse("v=2").unwrap_err().close_code(),
            close::UNSUPPORTED_VERSION
        );
        assert_eq!(
            ConnectParams::parse("v=1&token=%zz")
                .unwrap_err()
                .close_code(),
            close::BAD_REQUEST
        );
    }

    #[test]
    fn segments_round_trip() {
        for id in ["plain-id_1.0~", "a b/c", "a+b", "100%", "日本", ""] {
            assert_eq!(decode_segment(&encode_segment(id)).as_deref(), Some(id));
        }
        assert_eq!(
            decode_segment("a+b").as_deref(),
            Some("a+b"),
            "+ stays literal"
        );
        assert_eq!(decode_segment("a%2"), None);
        assert_eq!(decode_segment("%zz"), None);
        assert_eq!(decode_segment("%FF"), None, "invalid UTF-8");
    }

    #[test]
    fn connect_path_encodes_the_id() {
        assert_eq!(
            connect_path("orders", "a/b c"),
            "/partyline/orders/a%2Fb%20c"
        );
    }
}
