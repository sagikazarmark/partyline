//! The hooks in a real `VirtualDom`, against a local server that runs the core server logic.

#![cfg(not(target_arch = "wasm32"))]
// The handshake callback signature is fixed by tungstenite.
#![allow(clippy::result_large_err)]

use std::cell::RefCell;
use std::rc::Rc;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use dioxus::dioxus_core::{NoOpMutations, VirtualDom};
use dioxus::prelude::*;
use futures::{SinkExt, StreamExt};
use partyline::frame::ConnectParams;
use partyline::server::{self, Log, MemLog, Retention};
use partyline::{Channel, ClientConfig, Cursor, Mode, Status, StopReason, codec};
use partyline_dioxus::{
    BaseUrl, ChannelMessage, ChannelOptions, UseChannel, use_channel, use_channel_latest,
    use_channel_reducer,
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
    /// The path and query of every connection.
    requests: Arc<Mutex<Vec<(String, String)>>>,
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
            requests: Arc::default(),
            base: format!("http://{}", listener.local_addr().unwrap()),
        };
        let s = server.clone();
        tokio::spawn(async move {
            loop {
                let (stream, _) = listener.accept().await.unwrap();
                let s = s.clone();
                tokio::spawn(async move {
                    let mut path = String::new();
                    let mut query = String::new();
                    let callback = |req: &tungstenite::handshake::server::Request, resp| {
                        path = req.uri().path().to_owned();
                        query = req.uri().query().unwrap_or("").to_owned();
                        Ok(resp)
                    };
                    let Ok(mut ws) = tokio_tungstenite::accept_hdr_async(stream, callback).await
                    else {
                        return;
                    };
                    *s.connections.lock().unwrap() += 1;
                    let cursor = ConnectParams::parse(&query).unwrap().cursor;
                    s.requests.lock().unwrap().push((path, query));
                    let (tx, mut rx) = mpsc::unbounded_channel();
                    let frames = {
                        let log = s.log.lock().unwrap();
                        s.sockets.lock().unwrap().push(tx);
                        server::handshake(s.mode, cursor, &*log).unwrap()
                    };
                    for frame in frames {
                        if ws.send(tungstenite::Message::text(frame)).await.is_err() {
                            return;
                        }
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
        self.options_for("c1", since)
    }

    fn options_for(&self, id: &str, since: Option<Cursor>) -> ChannelOptions {
        ChannelOptions::new(id)
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
    /// For each message, the generation of the handler that received it.
    handled_by: Arc<Mutex<Vec<u64>>>,
    /// The number of refetches started.
    refetches: Arc<Mutex<usize>>,
}

impl Seen {
    fn last_status(&self) -> Option<Status> {
        self.statuses.lock().unwrap().last().copied()
    }

    fn last_value(&self) -> Option<u64> {
        self.values.lock().unwrap().last().copied().flatten()
    }
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
    let (sum, _channel) = use_channel_reducer::<Counter, u64, _, _, _>(
        props.options.clone(),
        || 0,
        |sum, n| *sum += n,
        || async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            Ok::<_, String>((1_000, Cursor::new(3, 2)))
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

/// Runs the dom for `duration`.
async fn run_for(dom: &mut VirtualDom, duration: Duration) {
    let deadline = tokio::time::Instant::now() + duration;
    while tokio::time::Instant::now() < deadline {
        tokio::select! {
            _ = dom.wait_for_work() => {}
            _ = tokio::time::sleep(Duration::from_millis(10)) => {}
        }
        dom.render_immediate(&mut NoOpMutations);
    }
}

/// Signals a test changes from outside the dom, and the subscription they control.
#[derive(Clone, Copy)]
struct Knobs {
    id: Signal<String>,
    enabled: Signal<bool>,
    /// Bumping it gives the component a new handler.
    handler: Signal<u64>,
    channel: UseChannel,
}

type KnobSlot = Rc<RefCell<Option<Knobs>>>;

fn knobs(slot: &KnobSlot) -> Knobs {
    slot.borrow().expect("the app rendered")
}

/// How the reducer app's refetch behaves.
#[derive(Clone, Copy)]
struct Refetch {
    delay: Duration,
    /// The number of attempts that fail before one succeeds.
    failures: usize,
    head: Cursor,
}

#[derive(Props, Clone)]
struct KnobProps {
    server: TestServer,
    /// The starting cursor of channel `c1`.
    c1_since: Option<Cursor>,
    /// The starting cursor of every other channel.
    since: Option<Cursor>,
    refetch: Option<Refetch>,
    seen: Seen,
    knobs: KnobSlot,
}

impl PartialEq for KnobProps {
    fn eq(&self, _: &Self) -> bool {
        true
    }
}

impl KnobProps {
    fn options(&self, id: &str, enabled: bool) -> ChannelOptions {
        let since = if id == "c1" {
            self.c1_since
        } else {
            self.since
        };
        self.server.options_for(id, since).enabled(enabled)
    }
}

#[allow(non_snake_case)]
fn KnobApp(props: KnobProps) -> Element {
    let id = use_signal(|| "c1".to_owned());
    let enabled = use_signal(|| true);
    let handler = use_signal(|| 0u64);
    // A new closure on every render, which knows the handler generation it was made in.
    let generation = handler();
    let messages = props.seen.messages.clone();
    let handled_by = props.seen.handled_by.clone();
    let channel = use_channel::<Counter>(props.options(&id(), enabled()), move |message| {
        messages.lock().unwrap().push(message);
        handled_by.lock().unwrap().push(generation);
    });
    props.seen.statuses.lock().unwrap().push(channel.status());
    *props.knobs.borrow_mut() = Some(Knobs {
        id,
        enabled,
        handler,
        channel,
    });
    rsx! {}
}

#[allow(non_snake_case)]
fn ReducerKnobApp(props: KnobProps) -> Element {
    let id = use_signal(|| "c1".to_owned());
    let enabled = use_signal(|| true);
    let handler = use_signal(|| 0u64);
    let refetches = props.seen.refetches.clone();
    let refetch = props.refetch.expect("a refetch");
    let (sum, channel) = use_channel_reducer::<Counter, u64, _, _, _>(
        props.options(&id(), enabled()),
        || 0,
        |sum, n| *sum += n,
        move || {
            let refetches = refetches.clone();
            async move {
                let attempt = {
                    let mut refetches = refetches.lock().unwrap();
                    *refetches += 1;
                    *refetches
                };
                tokio::time::sleep(refetch.delay).await;
                if attempt <= refetch.failures {
                    Err(format!("attempt {attempt} failed"))
                } else {
                    Ok((1_000, refetch.head))
                }
            }
        },
    );
    props.seen.values.lock().unwrap().push(Some(sum()));
    props.seen.statuses.lock().unwrap().push(channel.status());
    *props.knobs.borrow_mut() = Some(Knobs {
        id,
        enabled,
        handler,
        channel,
    });
    rsx! {}
}

fn knob_props(server: &TestServer) -> KnobProps {
    KnobProps {
        server: server.clone(),
        c1_since: Some(server.head()),
        since: Some(server.head()),
        refetch: None,
        seen: Seen::default(),
        knobs: KnobSlot::default(),
    }
}

fn mount(app: fn(KnobProps) -> Element, props: &KnobProps) -> VirtualDom {
    let mut dom = VirtualDom::new_with_props(app, props.clone());
    dom.rebuild_in_place();
    dom
}

#[tokio::test]
async fn changing_the_id_restarts_the_driver() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Log).await;
            let props = knob_props(&server);
            let mut dom = mount(KnobApp, &props);
            let seen = &props.seen;
            run_until(&mut dom, || seen.last_status() == Some(Status::Open)).await;
            let mut id = knobs(&props.knobs).id;
            dom.in_runtime(|| id.set("c2".to_owned()));
            run_until(&mut dom, || *server.connections.lock().unwrap() == 2).await;
            let paths: Vec<String> = server
                .requests
                .lock()
                .unwrap()
                .iter()
                .map(|(path, _)| path.clone())
                .collect();
            assert_eq!(
                paths,
                vec!["/partyline/counter/c1", "/partyline/counter/c2"]
            );
            run_until(&mut dom, || seen.last_status() == Some(Status::Open)).await;
        })
        .await;
}

#[tokio::test]
async fn a_new_handler_does_not_restart_the_driver() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Log).await;
            let props = knob_props(&server);
            let mut dom = mount(KnobApp, &props);
            let seen = &props.seen;
            run_until(&mut dom, || seen.last_status() == Some(Status::Open)).await;
            let mut handler = knobs(&props.knobs).handler;
            for _ in 0..3 {
                let renders = seen.statuses.lock().unwrap().len();
                dom.in_runtime(|| handler += 1);
                run_until(&mut dom, || seen.statuses.lock().unwrap().len() > renders).await;
            }
            server.publish(1);
            run_until(&mut dom, || seen.messages.lock().unwrap().len() == 1).await;
            assert_eq!(
                *seen.handled_by.lock().unwrap(),
                vec![3],
                "the latest handler"
            );
            assert_eq!(*server.connections.lock().unwrap(), 1);
        })
        .await;
}

#[tokio::test]
async fn reconnect_resumes_from_the_cursor() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Log).await;
            let props = knob_props(&server);
            let mut dom = mount(KnobApp, &props);
            let seen = &props.seen;
            run_until(&mut dom, || seen.last_status() == Some(Status::Open)).await;
            server.publish(1);
            server.publish(2);
            run_until(&mut dom, || seen.messages.lock().unwrap().len() == 2).await;
            let channel = knobs(&props.knobs).channel;
            dom.in_runtime(|| channel.reconnect());
            run_until(&mut dom, || *server.connections.lock().unwrap() == 2).await;
            let queries: Vec<String> = server
                .requests
                .lock()
                .unwrap()
                .iter()
                .map(|(_, query)| query.clone())
                .collect();
            assert_eq!(queries, vec!["v=1&cursor=3.0", "v=1&cursor=3.2"]);
            run_until(&mut dom, || seen.last_status() == Some(Status::Open)).await;
            server.publish(3);
            run_until(&mut dom, || seen.messages.lock().unwrap().len() == 3).await;
            assert_eq!(
                *seen.messages.lock().unwrap(),
                vec![
                    ChannelMessage::Event(1),
                    ChannelMessage::Event(2),
                    ChannelMessage::Event(3)
                ]
            );
        })
        .await;
}

#[tokio::test]
async fn disabling_stops_and_enabling_resumes_from_the_cursor() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Log).await;
            let props = knob_props(&server);
            let mut dom = mount(KnobApp, &props);
            let seen = &props.seen;
            run_until(&mut dom, || seen.last_status() == Some(Status::Open)).await;
            server.publish(1);
            run_until(&mut dom, || seen.messages.lock().unwrap().len() == 1).await;

            let mut enabled = knobs(&props.knobs).enabled;
            dom.in_runtime(|| enabled.set(false));
            let stopped = Status::Stopped {
                reason: StopReason::App,
            };
            run_until(&mut dom, || seen.last_status() == Some(stopped)).await;
            server.publish(2);
            server.publish(3);
            run_for(&mut dom, Duration::from_millis(100)).await;
            assert_eq!(
                seen.messages.lock().unwrap().len(),
                1,
                "nothing while disabled"
            );
            let channel = knobs(&props.knobs).channel;
            assert_eq!(
                dom.in_scope(ScopeId::ROOT, || *channel.cursor_signal().peek()),
                Some(Cursor::new(3, 1)),
                "the cursor is kept"
            );

            dom.in_runtime(|| enabled.set(true));
            run_until(&mut dom, || seen.messages.lock().unwrap().len() == 3).await;
            let queries: Vec<String> = server
                .requests
                .lock()
                .unwrap()
                .iter()
                .map(|(_, query)| query.clone())
                .collect();
            assert_eq!(queries, vec!["v=1&cursor=3.0", "v=1&cursor=3.1"]);
        })
        .await;
}

#[tokio::test]
async fn use_channel_reducer_starts_over_when_the_id_changes() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Log).await;
            let props = KnobProps {
                // Channel c2 starts with live events only.
                since: None,
                refetch: Some(Refetch {
                    delay: Duration::ZERO,
                    failures: 0,
                    head: server.head(),
                }),
                ..knob_props(&server)
            };
            let mut dom = mount(ReducerKnobApp, &props);
            let seen = &props.seen;
            run_until(&mut dom, || seen.last_status() == Some(Status::Open)).await;
            server.publish(1);
            server.publish(2);
            run_until(&mut dom, || seen.last_value() == Some(3)).await;

            let mut id = knobs(&props.knobs).id;
            dom.in_runtime(|| id.set("c2".to_owned()));
            run_until(&mut dom, || *server.connections.lock().unwrap() == 2).await;
            run_until(&mut dom, || seen.last_value() == Some(0)).await;
            run_until(&mut dom, || seen.last_status() == Some(Status::Open)).await;
            server.publish(4);
            run_until(&mut dom, || seen.last_value() == Some(4)).await;
        })
        .await;
}

#[tokio::test]
async fn use_channel_reducer_discards_a_refetch_for_the_old_id() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Log).await;
            server.publish(1);
            let props = KnobProps {
                // A cursor from another epoch: c1 gets Reset and refetches for 300 ms.
                c1_since: Some(Cursor::new(99, 1)),
                refetch: Some(Refetch {
                    delay: Duration::from_millis(300),
                    failures: 0,
                    head: server.head(),
                }),
                ..knob_props(&server)
            };
            let mut dom = mount(ReducerKnobApp, &props);
            let seen = &props.seen;
            run_until(&mut dom, || *seen.refetches.lock().unwrap() == 1).await;

            let mut id = knobs(&props.knobs).id;
            dom.in_runtime(|| id.set("c2".to_owned()));
            run_for(&mut dom, Duration::from_millis(600)).await;
            assert_eq!(*seen.refetches.lock().unwrap(), 1);
            assert!(
                !seen.values.lock().unwrap().contains(&Some(1_000)),
                "the old refetch is discarded"
            );
            assert_eq!(seen.last_value(), Some(0));
        })
        .await;
}

#[tokio::test]
async fn use_channel_reducer_retries_a_failed_refetch() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Log).await;
            server.publish(1);
            server.publish(2);
            let props = KnobProps {
                c1_since: Some(Cursor::new(99, 1)),
                refetch: Some(Refetch {
                    delay: Duration::ZERO,
                    failures: 1,
                    head: server.head(),
                }),
                ..knob_props(&server)
            };
            let mut dom = mount(ReducerKnobApp, &props);
            let seen = &props.seen;
            run_until(&mut dom, || *seen.refetches.lock().unwrap() == 1).await;
            // Buffered while the refetch waits to retry, then applied to the fresh state.
            server.publish(5);
            run_until(&mut dom, || seen.last_value() == Some(1_005)).await;
            assert_eq!(*seen.refetches.lock().unwrap(), 2);
        })
        .await;
}

#[tokio::test]
async fn use_channel_reducer_drops_buffered_events_from_another_epoch() {
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let server = TestServer::start(Mode::Log).await;
            server.publish(1);
            server.publish(2);
            let props = KnobProps {
                c1_since: Some(Cursor::new(99, 1)),
                // The refetch reads a log that was wiped again since the reset.
                refetch: Some(Refetch {
                    delay: Duration::from_millis(200),
                    failures: 0,
                    head: Cursor::new(4, 0),
                }),
                ..knob_props(&server)
            };
            let mut dom = mount(ReducerKnobApp, &props);
            let seen = &props.seen;
            run_until(&mut dom, || *seen.refetches.lock().unwrap() == 1).await;
            server.publish(5);
            run_until(&mut dom, || seen.last_value() == Some(1_000)).await;
            run_for(&mut dom, Duration::from_millis(100)).await;
            assert_eq!(
                seen.last_value(),
                Some(1_000),
                "event 3 of epoch 3 is dropped"
            );
            server.publish(6);
            run_until(&mut dom, || seen.last_value() == Some(1_006)).await;
        })
        .await;
}
