//! The hooks in a real `VirtualDom`, against a local server that runs the core server logic.

#![cfg(not(target_arch = "wasm32"))]
// The handshake callback signature is fixed by tungstenite.
#![allow(clippy::result_large_err)]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use dioxus::dioxus_core::{NoOpMutations, VirtualDom};
use dioxus::prelude::*;
use futures::{SinkExt, StreamExt};
use partyline::frame::ConnectParams;
use partyline::server::{self, Log, MemLog, Retention};
use partyline::{Channel, ClientConfig, Cursor, Mode, Status, codec};
use partyline_dioxus::{
    BaseUrl, ChannelMessage, ChannelOptions, use_channel, use_channel_latest, use_channel_reducer,
};
use tokio::net::TcpListener;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite;

struct Counter;
impl Channel for Counter {
    const NAME: &'static str = "counter";
    const MODE: Mode = Mode::Log;
    type Event = u64;
}

struct Gauge;
impl Channel for Gauge {
    const NAME: &'static str = "gauge";
    const MODE: Mode = Mode::Latest;
    type Event = u64;
}

#[derive(Clone)]
struct TestServer {
    mode: Mode,
    log: Arc<Mutex<MemLog>>,
    sockets: Arc<Mutex<Vec<mpsc::UnboundedSender<String>>>>,
    connections: Arc<Mutex<usize>>,
    base: String,
}

impl TestServer {
    async fn start(mode: Mode) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let server = TestServer {
            mode,
            log: Arc::new(Mutex::new(MemLog::new(3))),
            sockets: Arc::default(),
            connections: Arc::default(),
            base: format!("http://{}", listener.local_addr().unwrap()),
        };
        let s = server.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let s = s.clone();
                tokio::spawn(async move {
                    let mut query = String::new();
                    let callback = |req: &tungstenite::handshake::server::Request, resp| {
                        query = req.uri().query().unwrap_or("").to_owned();
                        Ok(resp)
                    };
                    let mut ws = tokio_tungstenite::accept_hdr_async(stream, callback)
                        .await
                        .unwrap();
                    *s.connections.lock().unwrap() += 1;
                    let cursor = ConnectParams::parse(&query).unwrap().cursor;
                    let (tx, mut rx) = mpsc::unbounded_channel();
                    let frames = {
                        let log = s.log.lock().unwrap();
                        s.sockets.lock().unwrap().push(tx);
                        server::handshake(s.mode, cursor, &*log).unwrap()
                    };
                    for frame in frames {
                        ws.send(tungstenite::Message::text(frame)).await.unwrap();
                    }
                    loop {
                        tokio::select! {
                            frame = rx.recv() => match frame {
                                Some(f) => { if ws.send(tungstenite::Message::text(f)).await.is_err() { return; } }
                                None => return,
                            },
                            incoming = ws.next() => if !matches!(incoming, Some(Ok(_))) { return; },
                        }
                    }
                });
            }
        });
        server
    }

    fn head(&self) -> Cursor {
        self.log.lock().unwrap().head().unwrap()
    }

    fn publish(&self, n: u64) {
        let mut log = self.log.lock().unwrap();
        let body = codec::encode_event(&n).unwrap();
        let retention = Retention::for_mode(self.mode);
        let (_, frame) = server::publish(&mut *log, &body, &retention, 0).unwrap();
        self.sockets
            .lock()
            .unwrap()
            .retain(|s| s.send(frame.clone()).is_ok());
    }

    fn options(&self, since: Option<Cursor>) -> ChannelOptions {
        ChannelOptions::new("c1")
            .since(since)
            .base_url(BaseUrl::Explicit(self.base.clone()))
            .config(ClientConfig {
                backoff_base: Duration::from_millis(20),
                ..ClientConfig::default()
            })
    }
}

/// Runs the dom until `done` holds, or panics after 10 s.
async fn run_until(dom: &mut VirtualDom, done: impl Fn() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !done() {
        assert!(tokio::time::Instant::now() < deadline, "timed out");
        tokio::select! {
            _ = dom.wait_for_work() => {}
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        dom.render_immediate(&mut NoOpMutations);
    }
}

#[derive(Clone, Default)]
struct Seen {
    messages: Arc<Mutex<Vec<ChannelMessage<u64>>>>,
    statuses: Arc<Mutex<Vec<Status>>>,
    values: Arc<Mutex<Vec<Option<u64>>>>,
}

#[derive(Props, Clone)]
struct AppProps {
    options: ChannelOptions,
    seen: Seen,
}

impl PartialEq for AppProps {
    fn eq(&self, other: &Self) -> bool {
        self.options == other.options
    }
}

#[allow(non_snake_case)]
fn ChannelApp(props: AppProps) -> Element {
    let messages = props.seen.messages.clone();
    let channel = use_channel::<Counter>(props.options.clone(), move |message| {
        messages.lock().unwrap().push(message);
    });
    props.seen.statuses.lock().unwrap().push(channel.status());
    rsx! {}
}

#[tokio::test]
async fn use_channel_delivers_events_and_status() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Log).await;
            let seen = Seen::default();
            let mut dom = VirtualDom::new_with_props(
                ChannelApp,
                AppProps {
                    options: server.options(Some(server.head())),
                    seen: seen.clone(),
                },
            );
            dom.rebuild_in_place();
            run_until(&mut dom, || {
                seen.statuses.lock().unwrap().contains(&Status::Open)
            })
            .await;
            for n in 1..=3 {
                server.publish(n);
            }
            run_until(&mut dom, || seen.messages.lock().unwrap().len() == 3).await;
            assert_eq!(
                *seen.messages.lock().unwrap(),
                vec![
                    ChannelMessage::Event(1),
                    ChannelMessage::Event(2),
                    ChannelMessage::Event(3)
                ]
            );
            assert_eq!(seen.statuses.lock().unwrap()[0], Status::Idle);
        })
        .await;
}

#[tokio::test]
async fn nothing_connects_before_effects_run() {
    let server = TestServer::start(Mode::Log).await;
    let seen = Seen::default();
    let mut dom = VirtualDom::new_with_props(
        ChannelApp,
        AppProps {
            options: server.options(None),
            seen: seen.clone(),
        },
    );
    // Server-side rendering builds the dom once and never runs effects.
    dom.rebuild_in_place();
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(*server.connections.lock().unwrap(), 0);
    assert_eq!(*seen.statuses.lock().unwrap(), vec![Status::Idle]);
}

#[allow(non_snake_case)]
fn LatestApp(props: AppProps) -> Element {
    let (value, _channel) = use_channel_latest::<Gauge>(props.options.clone());
    props.seen.values.lock().unwrap().push(value());
    rsx! {}
}

#[tokio::test]
async fn use_channel_latest_starts_with_the_current_value() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Latest).await;
            server.publish(41);
            let seen = Seen::default();
            let mut dom = VirtualDom::new_with_props(
                LatestApp,
                AppProps {
                    options: server.options(None),
                    seen: seen.clone(),
                },
            );
            dom.rebuild_in_place();
            run_until(&mut dom, || seen.values.lock().unwrap().contains(&Some(41))).await;
            server.publish(42);
            run_until(&mut dom, || seen.values.lock().unwrap().contains(&Some(42))).await;
        })
        .await;
}

#[allow(non_snake_case)]
fn ReducerApp(props: AppProps) -> Element {
    // The state is the sum of all events. The refetch returns the server's view.
    let (sum, _channel) = use_channel_reducer::<Counter, u64, _, _>(
        props.options.clone(),
        || 0,
        |sum, n| *sum += n,
        || async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            (1_000, Cursor::new(3, 2))
        },
    );
    props.seen.values.lock().unwrap().push(Some(sum()));
    rsx! {}
}

#[tokio::test]
async fn use_channel_reducer_refetches_on_reset_and_keeps_later_events() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Log).await;
            server.publish(1);
            server.publish(2);
            let seen = Seen::default();
            // A cursor from another epoch: the server answers with Reset at head 3.2.
            let mut dom = VirtualDom::new_with_props(
                ReducerApp,
                AppProps {
                    options: server.options(Some(Cursor::new(99, 1))),
                    seen: seen.clone(),
                },
            );
            dom.rebuild_in_place();
            run_until(&mut dom, || *server.connections.lock().unwrap() == 1).await;
            // The Reset is on its way and the refetch takes 50 ms. An event published now
            // arrives during the refetch, and is applied to the fresh state.
            server.publish(5);
            run_until(&mut dom, || {
                seen.values.lock().unwrap().contains(&Some(1_005))
            })
            .await;
        })
        .await;
}
