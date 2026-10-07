//! The socket abstraction the driver runs on.

use std::future::Future;

use futures::{Sink, Stream};
use partyline::Message;

/// A WebSocket transport failure.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum TransportError {
    /// The socket could not be opened.
    #[error("connect failed: {0}")]
    Connect(String),
    /// The connection closed. `code` is the close code, if any.
    #[error("connection closed (code {code:?})")]
    Closed {
        /// The close code.
        code: Option<u16>,
    },
    /// Sending or receiving failed.
    #[error("transport error: {0}")]
    Io(String),
}

/// Opens WebSocket connections.
///
/// A connection is a stream of incoming messages and a sink for outgoing ones. The stream
/// reports a close frame as [`TransportError::Closed`] and then ends. Closing the sink sends
/// close code 1000.
pub trait Transport {
    /// An open connection.
    type Conn: Stream<Item = Result<Message, TransportError>>
        + Sink<Message, Error = TransportError>
        + Unpin;

    /// Opens a connection. Resolves when the socket is open.
    fn connect(&self, url: &str) -> impl Future<Output = Result<Self::Conn, TransportError>>;
}

/// The transport for the current target: `BrowserSocket` on wasm32,
/// `NativeSocket` elsewhere.
#[cfg(target_arch = "wasm32")]
pub type DefaultTransport = crate::browser::BrowserSocket;

/// The transport for the current target: `BrowserSocket` on wasm32,
/// `NativeSocket` elsewhere.
#[cfg(not(target_arch = "wasm32"))]
pub type DefaultTransport = crate::native::NativeSocket;
