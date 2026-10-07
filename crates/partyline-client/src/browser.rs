//! The browser transport, built on `gloo-net`.
//!
//! Same-origin cookies are sent automatically with the handshake.

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::future::poll_fn;
use futures::{Sink, Stream};
use gloo_net::websocket::{self, State, WebSocketError, futures::WebSocket};
use partyline::{Message, close};

use crate::transport::{Transport, TransportError};

/// A browser WebSocket transport.
#[derive(Clone, Copy, Debug, Default)]
pub struct BrowserSocket;

impl Transport for BrowserSocket {
    type Conn = BrowserConn;

    async fn connect(&self, url: &str) -> Result<BrowserConn, TransportError> {
        let mut ws = WebSocket::open(url).map_err(|e| TransportError::Connect(e.to_string()))?;
        // The sink is ready once the socket leaves the connecting state.
        let _ = poll_fn(|cx| Pin::new(&mut ws).poll_ready(cx)).await;
        if !matches!(ws.state(), State::Open) {
            return Err(TransportError::Connect(format!(
                "socket is {:?}",
                ws.state()
            )));
        }
        Ok(BrowserConn { inner: Some(ws) })
    }
}

/// An open browser connection.
pub struct BrowserConn {
    inner: Option<WebSocket>,
}

impl std::fmt::Debug for BrowserConn {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BrowserConn")
            .field("open", &self.inner.is_some())
            .finish()
    }
}

impl BrowserConn {
    fn inner(&mut self) -> Result<Pin<&mut WebSocket>, TransportError> {
        self.inner
            .as_mut()
            .map(Pin::new)
            .ok_or(TransportError::Closed { code: None })
    }
}

impl Stream for BrowserConn {
    type Item = Result<Message, TransportError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Ok(inner) = self.inner() else {
            return Poll::Ready(None);
        };
        Poll::Ready(match ready!(inner.poll_next(cx)) {
            None => None,
            Some(Ok(websocket::Message::Text(text))) => Some(Ok(Message::Text(text))),
            Some(Ok(websocket::Message::Bytes(bytes))) => Some(Ok(Message::Binary(bytes))),
            Some(Err(WebSocketError::ConnectionClose(event))) => {
                Some(Err(TransportError::Closed {
                    code: Some(event.code),
                }))
            }
            Some(Err(e)) => Some(Err(TransportError::Io(e.to_string()))),
        })
    }
}

impl Sink<Message> for BrowserConn {
    type Error = TransportError;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        self.inner()?.poll_ready(cx).map_err(io)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), TransportError> {
        let message = match item {
            Message::Text(text) => websocket::Message::Text(text),
            Message::Binary(bytes) => websocket::Message::Bytes(bytes),
        };
        self.inner()?.start_send(message).map_err(io)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        self.inner()?.poll_flush(cx).map_err(io)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        if let Some(ws) = self.inner.take() {
            ws.close(Some(close::NORMAL), None)
                .map_err(|e| TransportError::Io(e.to_string()))?;
        }
        Poll::Ready(Ok(()))
    }
}

fn io(e: WebSocketError) -> TransportError {
    match e {
        WebSocketError::ConnectionClose(event) => TransportError::Closed {
            code: Some(event.code),
        },
        e => TransportError::Io(e.to_string()),
    }
}
