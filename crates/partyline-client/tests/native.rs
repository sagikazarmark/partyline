//! Layer 3 (native): the socket maps open, message, close, and error correctly, and the
//! driver resumes against a local server that runs the core server logic.

#![cfg(not(target_arch = "wasm32"))]
// The handshake callback signature is fixed by tungstenite.
#![allow(clippy::result_large_err)]

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::{SinkExt, StreamExt};
use partyline::frame::ConnectParams;
use partyline::server::{self, Log, MemLog, Retention};
use partyline::{Channel, ClientConfig, Cursor, Message, Mode, Status, StopReason, close, codec};
use partyline_client::{
    BaseUrl, ClientEvent, ConnectOptions, NativeSocket, TokenProvider, TokenRequest, Transport,
    TransportError,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::handshake::server::{Request, Response};
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::{self, protocol::frame::coding::CloseCode};

struct Counter;
impl Channel for Counter {
    const NAME: &'static str = "counter";
    const MODE: Mode = Mode::Log;
    type Event = u64;
}

async fn listener() -> (TcpListener, SocketAddr) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    (listener, addr)
}

#[tokio::test]
async fn transport_maps_messages_and_close_codes() {
    let (listener, addr) = listener().await;
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        ws.send(tungstenite::Message::text("hello")).await.unwrap();
        ws.send(tungstenite::Message::binary(vec![1, 2]))
            .await
            .unwrap();
        let ping = ws.next().await.unwrap().unwrap();
        assert_eq!(ping, tungstenite::Message::text("ping"));
        ws.close(Some(CloseFrame {
            code: CloseCode::from(close::FORBIDDEN),
            reason: "no".into(),
        }))
        .await
        .unwrap();
    });

    let mut conn = NativeSocket
        .connect(&format!("ws://{addr}/"))
        .await
        .unwrap();
    assert_eq!(conn.next().await, Some(Ok(Message::Text("hello".into()))));
    assert_eq!(conn.next().await, Some(Ok(Message::Binary(vec![1, 2]))));
    conn.send(Message::ping()).await.unwrap();
    assert_eq!(
        conn.next().await,
        Some(Err(TransportError::Closed {
            code: Some(close::FORBIDDEN)
        }))
    );
    assert_eq!(conn.next().await, None);
}

#[tokio::test]
async fn transport_close_sends_1000() {
    let (listener, addr) = listener().await;
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
        while let Some(Ok(message)) = ws.next().await {
            if let tungstenite::Message::Close(frame) = message {
                tx.send(frame.map(|f| u16::from(f.code))).unwrap();
            }
        }
    });
    let mut conn = NativeSocket
        .connect(&format!("ws://{addr}/"))
        .await
        .unwrap();
    conn.close().await.unwrap();
    assert_eq!(rx.recv().await, Some(Some(close::NORMAL)));
}

#[tokio::test]
async fn transport_reports_connect_failures() {
    let (listener, addr) = listener().await;
    drop(listener);
    let result = NativeSocket.connect(&format!("ws://{addr}/")).await;
    assert!(
        matches!(result, Err(TransportError::Connect(_))),
        "{result:?}"
    );
}

/// A local partyline server: the core server logic over real sockets.
#[derive(Clone)]
struct TestServer {
    log: Arc<Mutex<MemLog>>,
    sockets: Arc<Mutex<Vec<mpsc::UnboundedSender<Control>>>>,
    queries: Arc<Mutex<Vec<String>>>,
    addr: SocketAddr,
}

enum Control {
    Frame(String),
    Drop,
    Close(u16),
}

impl TestServer {
    async fn start() -> Self {
        let (listener, addr) = listener().await;
        let server = TestServer {
            log: Arc::new(Mutex::new(MemLog::new(9))),
            sockets: Arc::default(),
            queries: Arc::default(),
            addr,
        };
        let s = server.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let s = s.clone();
                tokio::spawn(async move { s.serve(stream).await });
            }
        });
        server
    }

    async fn serve(&self, stream: tokio::net::TcpStream) {
        let mut query = String::new();
        let callback = |req: &Request, resp: Response| {
            query = req.uri().query().unwrap_or("").to_owned();
            Ok(resp)
        };
        let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await else {
            return;
        };
        self.queries.lock().unwrap().push(query.clone());
        let params = ConnectParams::parse(&query).unwrap();
        let (tx, mut rx) = mpsc::unbounded_channel();
        let frames = {
            // Accept and handshake atomically with respect to publishes.
            let log = self.log.lock().unwrap();
            self.sockets.lock().unwrap().push(tx);
            server::handshake(Mode::Log, params.cursor, &*log).unwrap()
        };
        for frame in frames {
            ws.send(tungstenite::Message::text(frame)).await.unwrap();
        }
        loop {
            tokio::select! {
                control = rx.recv() => match control {
                    Some(Control::Frame(text)) => {
                        if ws.send(tungstenite::Message::text(text)).await.is_err() { return; }
                    }
                    Some(Control::Drop) | None => return,
                    Some(Control::Close(code)) => {
                        let _ = ws.close(Some(CloseFrame { code: CloseCode::from(code), reason: "".into() })).await;
                        return;
                    }
                },
                incoming = ws.next() => match incoming {
                    Some(Ok(tungstenite::Message::Text(t))) if t.as_str() == "ping" => {
                        let _ = ws.send(tungstenite::Message::text("pong")).await;
                    }
                    Some(Ok(_)) => {}
                    _ => return,
                },
            }
        }
    }

    fn head(&self) -> Cursor {
        self.log.lock().unwrap().head().unwrap()
    }

    fn publish(&self, n: u64) {
        self.publish_body(&codec::encode_event(&n).unwrap());
    }

    fn publish_body(&self, body: &[u8]) {
        let mut log = self.log.lock().unwrap();
        let (_, frame) = server::publish(&mut *log, body, &Retention::LOG_DEFAULT, 0).unwrap();
        self.sockets
            .lock()
            .unwrap()
            .retain(|s| s.send(Control::Frame(frame.clone())).is_ok());
    }

    fn drop_all(&self) {
        for s in self.sockets.lock().unwrap().drain(..) {
            let _ = s.send(Control::Drop);
        }
    }

    fn close_all(&self, code: u16) {
        for s in self.sockets.lock().unwrap().drain(..) {
            let _ = s.send(Control::Close(code));
        }
    }

    fn options(&self) -> ConnectOptions {
        ConnectOptions {
            since: Some(self.head()),
            config: ClientConfig {
                backoff_base: Duration::from_millis(20),
                ..ClientConfig::default()
            },
            ..ConnectOptions::new(BaseUrl::Explicit(format!("http://{}", self.addr)), "c1")
        }
    }
}

async fn next_event(events: &mut partyline_client::Events<u64>) -> ClientEvent<u64> {
    tokio::time::timeout(Duration::from_secs(10), events.next())
        .await
        .expect("an event within 10 s")
        .expect("the stream is open")
}

async fn wait_for(
    events: &mut partyline_client::Events<u64>,
    want: impl Fn(&ClientEvent<u64>) -> bool,
) {
    loop {
        if want(&next_event(events).await) {
            return;
        }
    }
}

#[tokio::test]
async fn driver_resumes_after_connection_loss() {
    let server = TestServer::start().await;
    let (handle, mut events, driver) = partyline_client::connect::<Counter>(server.options());
    let driver = tokio::spawn(driver);

    wait_for(&mut events, |e| *e == ClientEvent::Status(Status::Open)).await;
    for n in 1..=3 {
        server.publish(n);
    }
    let mut seen = Vec::new();
    while seen.len() < 3 {
        if let ClientEvent::Event { seq, event, .. } = next_event(&mut events).await {
            assert_eq!(seq, event);
            seen.push(seq);
        }
    }

    server.drop_all();
    server.publish(4);
    server.publish(5);
    while seen.len() < 5 {
        if let ClientEvent::Event { seq, .. } = next_event(&mut events).await {
            seen.push(seq);
        }
    }
    assert_eq!(seen, vec![1, 2, 3, 4, 5]);
    assert_eq!(handle.cursor(), Some(Cursor::new(9, 5)));
    let queries = server.queries.lock().unwrap().clone();
    assert_eq!(queries, vec!["v=1&cursor=9.0", "v=1&cursor=9.3"]);

    handle.stop();
    wait_for(&mut events, |e| {
        *e == ClientEvent::Status(Status::Stopped {
            reason: StopReason::App,
        })
    })
    .await;
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("the driver ends after stop")
        .unwrap();
    assert_eq!(events.next().await, None, "the stream ends with the driver");
}

#[tokio::test]
async fn driver_stops_on_a_terminal_close_code() {
    let server = TestServer::start().await;
    let (handle, mut events, driver) = partyline_client::connect::<Counter>(server.options());
    let driver = tokio::spawn(driver);
    wait_for(&mut events, |e| *e == ClientEvent::Status(Status::Open)).await;
    server.close_all(close::FORBIDDEN);
    wait_for(&mut events, |e| {
        *e == ClientEvent::Status(Status::Stopped {
            reason: StopReason::Closed(close::FORBIDDEN),
        })
    })
    .await;
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        handle.status(),
        Status::Stopped {
            reason: StopReason::Closed(close::FORBIDDEN),
        }
    );
}

#[tokio::test]
async fn driver_stops_on_an_event_it_cannot_decode() {
    let server = TestServer::start().await;
    let (handle, mut events, driver) = partyline_client::connect::<Counter>(server.options());
    let driver = tokio::spawn(driver);
    wait_for(&mut events, |e| *e == ClientEvent::Status(Status::Open)).await;
    server.publish(1);
    server.publish_body(br#"{"type":"added_later"}"#);
    let stopped = Status::Stopped {
        reason: StopReason::Incompatible { seq: 2 },
    };
    wait_for(&mut events, |e| *e == ClientEvent::Status(stopped)).await;
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("the driver ends")
        .unwrap();
    assert_eq!(handle.status(), stopped);
    assert_eq!(handle.cursor(), Some(Cursor::new(9, 1)));
    assert_eq!(server.queries.lock().unwrap().len(), 1, "no reconnect");
}

#[tokio::test]
async fn driver_stops_on_an_invalid_base_url() {
    let (handle, mut events, driver) = partyline_client::connect::<Counter>(ConnectOptions::new(
        BaseUrl::Explicit("ftp://example.com".into()),
        "x",
    ));
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("the driver ends");
    let stopped = Status::Stopped {
        reason: StopReason::InvalidUrl,
    };
    assert_eq!(events.next().await, Some(ClientEvent::Status(stopped)));
    assert_eq!(handle.status(), stopped);
}

#[tokio::test]
async fn driver_asks_the_token_provider_before_every_connect() {
    let server = TestServer::start().await;
    let requests = Arc::new(Mutex::new(Vec::new()));
    let seen = requests.clone();
    let options = ConnectOptions {
        token: Some(TokenProvider::new(move |request: TokenRequest| {
            let seen = seen.clone();
            async move {
                let mut seen = seen.lock().unwrap();
                seen.push(request.refresh);
                Some(format!("t{}", seen.len()))
            }
        })),
        ..server.options()
    };
    let (handle, mut events, driver) = partyline_client::connect::<Counter>(options);
    tokio::spawn(driver);
    wait_for(&mut events, |e| *e == ClientEvent::Status(Status::Open)).await;
    server.close_all(close::UNAUTHORIZED);
    wait_for(&mut events, |e| {
        matches!(e, ClientEvent::Status(Status::Unauthorized { .. }))
    })
    .await;
    wait_for(&mut events, |e| *e == ClientEvent::Status(Status::Open)).await;
    assert_eq!(*requests.lock().unwrap(), vec![false, true]);
    let queries = server.queries.lock().unwrap().clone();
    assert!(queries[0].ends_with("&token=t1"), "{queries:?}");
    assert!(queries[1].ends_with("&token=t2"), "{queries:?}");
    handle.stop();
}

#[tokio::test]
async fn dropping_every_handle_stops_the_driver() {
    let server = TestServer::start().await;
    let (handle, mut events, driver) = partyline_client::connect::<Counter>(server.options());
    let driver = tokio::spawn(driver);
    wait_for(&mut events, |e| *e == ClientEvent::Status(Status::Open)).await;
    drop(handle);
    tokio::time::timeout(Duration::from_secs(5), driver)
        .await
        .expect("the driver ends")
        .unwrap();
}

#[test]
fn the_driver_future_is_send() {
    fn assert_send<T: Send>(_: &T) {}
    let (_, _, driver) = partyline_client::connect::<Counter>(ConnectOptions::new(
        BaseUrl::Explicit("ws://localhost".into()),
        "x",
    ));
    assert_send(&driver);
}
