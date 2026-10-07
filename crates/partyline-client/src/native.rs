//! The native transport: `tokio-tungstenite` with `rustls`, so no system OpenSSL is needed.
//!
//! The driver future must run on a tokio runtime, because the socket uses tokio I/O.

use std::pin::Pin;
use std::task::{Context, Poll, ready};

use futures::{Sink, Stream};
use partyline::{Message, close};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::{
    self, protocol::CloseFrame, protocol::frame::coding::CloseCode,
};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

use crate::transport::{Transport, TransportError};

/// A native WebSocket transport built on `tokio-tungstenite` and `rustls` with the Mozilla
/// root certificates.
#[derive(Clone, Copy, Debug, Default)]
pub struct NativeSocket;

impl Transport for NativeSocket {
    type Conn = NativeConn;

    async fn connect(&self, url: &str) -> Result<NativeConn, TransportError> {
        // Use ring when no process-wide crypto provider is installed. Ignore the error when
        // the app installed one already.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (inner, _response) = tokio_tungstenite::connect_async(url)
            .await
            .map_err(|e| TransportError::Connect(e.to_string()))?;
        Ok(NativeConn {
            inner,
            close_sent: false,
        })
    }
}

/// An open native connection.
#[derive(Debug)]
pub struct NativeConn {
    inner: WebSocketStream<MaybeTlsStream<TcpStream>>,
    close_sent: bool,
}

impl Stream for NativeConn {
    type Item = Result<Message, TransportError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let item = ready!(Pin::new(&mut self.inner).poll_next(cx));
            return Poll::Ready(match item {
                None => None,
                Some(Ok(tungstenite::Message::Text(text))) => {
                    Some(Ok(Message::Text(text.as_str().to_owned())))
                }
                Some(Ok(tungstenite::Message::Binary(bytes))) => {
                    Some(Ok(Message::Binary(bytes.to_vec())))
                }
                Some(Ok(tungstenite::Message::Close(frame))) => Some(Err(TransportError::Closed {
                    code: frame.map(|f| u16::from(f.code)),
                })),
                // Protocol-level pings are answered by tungstenite.
                Some(Ok(
                    tungstenite::Message::Ping(_)
                    | tungstenite::Message::Pong(_)
                    | tungstenite::Message::Frame(_),
                )) => continue,
                Some(Err(
                    tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed,
                )) => None,
                Some(Err(e)) => Some(Err(TransportError::Io(e.to_string()))),
            });
        }
    }
}

impl Sink<Message> for NativeConn {
    type Error = TransportError;

    fn poll_ready(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        Pin::new(&mut self.inner).poll_ready(cx).map_err(io)
    }

    fn start_send(mut self: Pin<&mut Self>, item: Message) -> Result<(), TransportError> {
        let message = match item {
            Message::Text(text) => tungstenite::Message::text(text),
            Message::Binary(bytes) => tungstenite::Message::binary(bytes),
        };
        Pin::new(&mut self.inner).start_send(message).map_err(io)
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        Pin::new(&mut self.inner).poll_flush(cx).map_err(io)
    }

    fn poll_close(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Result<(), TransportError>> {
        if !self.close_sent {
            ready!(Pin::new(&mut self.inner).poll_ready(cx)).map_err(io)?;
            let frame = CloseFrame {
                code: CloseCode::from(close::NORMAL),
                reason: "".into(),
            };
            Pin::new(&mut self.inner)
                .start_send(tungstenite::Message::Close(Some(frame)))
                .map_err(io)?;
            self.close_sent = true;
        }
        match ready!(Pin::new(&mut self.inner).poll_close(cx)) {
            Ok(())
            | Err(tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed) => {
                Poll::Ready(Ok(()))
            }
            Err(e) => Poll::Ready(Err(io(e))),
        }
    }
}

fn io(e: tungstenite::Error) -> TransportError {
    match e {
        tungstenite::Error::ConnectionClosed | tungstenite::Error::AlreadyClosed => {
            TransportError::Closed { code: None }
        }
        e => TransportError::Io(e.to_string()),
    }
}
