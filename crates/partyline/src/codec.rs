//! Frame and event encoding. Version 0.1 uses JSON text frames only.
//!
//! The `Codec` trait is private. It becomes public when a second codec exists.

use serde::Serialize;
use serde::de::{DeserializeOwned, IgnoredAny};
use serde_json::value::RawValue;

use crate::frame::ServerFrame;

/// An encoding or decoding failure.
#[derive(Debug, thiserror::Error)]
#[error("codec error: {0}")]
pub struct CodecError(#[from] serde_json::Error);

trait Codec {
    fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, CodecError>;
    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError>;
}

struct Json;

impl Codec for Json {
    fn encode<T: Serialize + ?Sized>(value: &T) -> Result<Vec<u8>, CodecError> {
        Ok(serde_json::to_vec(value)?)
    }

    fn decode<T: DeserializeOwned>(bytes: &[u8]) -> Result<T, CodecError> {
        Ok(serde_json::from_slice(bytes)?)
    }
}

/// Encodes an event body, as stored in the log and sent to the hub by a publisher.
pub fn encode_event<E: Serialize>(event: &E) -> Result<Vec<u8>, CodecError> {
    Json::encode(event)
}

/// Decodes an event body.
pub fn decode_event<E: DeserializeOwned>(body: &[u8]) -> Result<E, CodecError> {
    Json::decode(body)
}

/// Encodes a frame as the text of a WebSocket message.
pub fn encode_frame<E: Serialize>(frame: &ServerFrame<E>) -> Result<String, CodecError> {
    Ok(serde_json::to_string(frame)?)
}

/// Decodes the text of a WebSocket message as a frame.
pub fn decode_frame<E: DeserializeOwned>(text: &str) -> Result<ServerFrame<E>, CodecError> {
    Json::decode(text.as_bytes())
}

/// A frame whose event payload is not decoded yet.
pub type RawFrame = ServerFrame<Box<RawValue>>;

/// Decodes the text of a WebSocket message as a frame, leaving the event payload undecoded.
///
/// A client decodes the envelope first and the payload with [`decode_payload`] second, so it
/// can tell a malformed frame from a well-formed event its event type does not know.
pub fn decode_envelope(text: &str) -> Result<RawFrame, CodecError> {
    // An internally tagged enum buffers its fields, which `RawValue` does not support, so an
    // event frame is decoded as a plain struct.
    #[derive(serde::Deserialize)]
    struct EventEnvelope {
        seq: u64,
        event: Box<RawValue>,
    }
    if tag(text).as_deref() == Some("event") {
        let EventEnvelope { seq, event } = Json::decode(text.as_bytes())?;
        return Ok(ServerFrame::Event { seq, event });
    }
    match Json::decode::<ServerFrame<IgnoredAny>>(text.as_bytes())? {
        ServerFrame::Hello { v, mode, head } => Ok(ServerFrame::Hello { v, mode, head }),
        ServerFrame::Reset { head } => Ok(ServerFrame::Reset { head }),
        // Unreachable: the tag is not `event`.
        ServerFrame::Event { .. } => Err(CodecError(
            <serde_json::Error as serde::de::Error>::custom("ambiguous frame type"),
        )),
    }
}

/// Decodes the event payload of a frame decoded with [`decode_envelope`].
pub fn decode_payload<E: DeserializeOwned>(raw: &RawValue) -> Result<E, CodecError> {
    Json::decode(raw.get().as_bytes())
}

/// The `t` tag of a frame, if the text is a JSON object with a string tag.
fn tag(text: &str) -> Option<std::borrow::Cow<'_, str>> {
    #[derive(serde::Deserialize)]
    struct Tag<'a> {
        #[serde(borrow)]
        t: std::borrow::Cow<'a, str>,
    }
    serde_json::from_str::<Tag<'_>>(text).ok().map(|tag| tag.t)
}

/// The frame types this version knows, by their `t` tag.
const KNOWN_FRAME_TYPES: [&str; 3] = ["hello", "event", "reset"];

/// Reports whether the text is a well-formed frame of a type this version does not know.
///
/// Clients ignore such frames, so a later protocol version can add frame types without
/// breaking older clients. A malformed frame of a known type is still a protocol error.
pub fn is_unknown_frame(text: &str) -> bool {
    tag(text).is_some_and(|t| !KNOWN_FRAME_TYPES.contains(&t.as_ref()))
}

/// Builds the text of an `Event` frame from an already encoded event body.
///
/// The hub uses this to send stored events without decoding them.
pub fn event_frame(seq: u64, body: &[u8]) -> Result<String, CodecError> {
    let body = std::str::from_utf8(body).map_err(|e| {
        CodecError(<serde_json::Error as serde::de::Error>::custom(format!(
            "event body is not UTF-8: {e}"
        )))
    })?;
    let raw: &RawValue = serde_json::from_str(body)?;
    encode_frame(&ServerFrame::Event { seq, event: raw })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn event_frame_embeds_the_body_verbatim() {
        let body = encode_event(&serde_json::json!({"id": 1, "s": "ok"})).unwrap();
        let text = event_frame(5, &body).unwrap();
        assert_eq!(text, r#"{"t":"event","seq":5,"event":{"id":1,"s":"ok"}}"#);
        let frame: ServerFrame<serde_json::Value> = decode_frame(&text).unwrap();
        assert_eq!(
            frame,
            ServerFrame::Event {
                seq: 5,
                event: serde_json::json!({"id": 1, "s": "ok"})
            }
        );
    }

    #[test]
    fn unknown_frame_types_are_recognized() {
        assert!(is_unknown_frame(r#"{"t":"snapshot","state":{}}"#));
        assert!(!is_unknown_frame(r#"{"t":"event","seq":"not a number"}"#));
        assert!(!is_unknown_frame(r#"{"t":"hello"}"#));
        assert!(!is_unknown_frame(r#"{"no_tag":1}"#));
        assert!(!is_unknown_frame("not json"));
    }

    #[test]
    fn envelopes_decode_without_their_payload() {
        let frame = decode_envelope(r#"{"t":"event","seq":5,"event":{"type":"new"}}"#).unwrap();
        let ServerFrame::Event { seq, event } = frame else {
            panic!("an event frame: {frame:?}");
        };
        assert_eq!(seq, 5);
        assert_eq!(event.get(), r#"{"type":"new"}"#);
        assert!(
            decode_payload::<u64>(&event).is_err(),
            "the payload is checked apart"
        );
        assert_eq!(
            decode_payload::<serde_json::Value>(&event).unwrap(),
            serde_json::json!({"type": "new"})
        );

        let hello = r#"{"t":"hello","v":1,"mode":"log","head":"7.3","extra":1}"#;
        let head = crate::Cursor::new(7, 3);
        assert!(matches!(
            decode_envelope(hello).unwrap(),
            ServerFrame::Hello { v: 1, mode: crate::Mode::Log, head: h } if h == head
        ));
        assert!(matches!(
            decode_envelope(r#"{"t":"reset","head":"7.3"}"#).unwrap(),
            ServerFrame::Reset { head: h } if h == head
        ));
    }

    #[test]
    fn malformed_envelopes_are_errors() {
        for bad in [
            "not json",
            r#"{"t":"event","seq":"x","event":1}"#,
            r#"{"t":"event","seq":1}"#,
            r#"{"t":"event","event":1}"#,
            r#"{"t":"hello","v":1}"#,
            r#"{"t":"reset"}"#,
            r#"{"t":"snapshot"}"#,
        ] {
            assert!(decode_envelope(bad).is_err(), "{bad}");
        }
    }

    #[test]
    fn event_frame_rejects_invalid_bodies() {
        assert!(event_frame(1, b"{not json").is_err());
        assert!(event_frame(1, &[0xff, 0xfe]).is_err());
    }
}
