//! The chat channel and API types, shared by the Worker and the web client.

use partyline::{Channel, Mode};
use serde::{Deserialize, Serialize};

/// The chat channel. Log mode: every message is delivered, in order.
pub struct Chat;

impl Channel for Chat {
    const NAME: &'static str = "chat";
    const MODE: Mode = Mode::Log;
    type Event = ChatEvent;
}

/// The Durable Object binding of [`Chat`]: the `ChatChannel` class.
pub const BINDING: &str = "CHAT_CHANNEL";

/// The one room. Its channel ID.
pub const ROOM: &str = "lobby";

/// How many messages the room keeps. A new page shows at most this many.
pub const HISTORY: u64 = 50;

/// The longest display name, in characters.
pub const MAX_NAME_LEN: usize = 32;

/// The longest message, in characters.
pub const MAX_MESSAGE_LEN: usize = 500;

/// An event on the chat channel.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ChatEvent {
    /// A signed-in user posted a message.
    MessagePosted {
        /// The author's name, from their session.
        author: String,
        /// The message.
        text: String,
    },
}

/// The body of `POST /api/login`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Login {
    /// The name to sign in as.
    pub name: String,
}

/// The response of `GET /api/token`: a short-lived access token for the WebSocket.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AccessToken {
    /// The signed-in user's name.
    pub user: String,
    /// The token, for the `token` query parameter.
    pub token: String,
    /// When the token expires, in seconds since the Unix epoch.
    pub expires_at: u64,
}

/// The body of `POST /api/messages`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostMessage {
    /// The message.
    pub text: String,
}
