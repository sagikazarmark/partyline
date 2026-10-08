//! Layer 4: the Durable Object, the SQLite log, and the Worker helpers in workerd.
//!
//! Run with `dagger check examples:end-to-end`, which starts the orders example under
//! `wrangler dev` and sets `PARTYLINE_E2E_URL`. Without it, every test passes without
//! doing anything.

use std::time::Duration;

use futures::{SinkExt, StreamExt};
use orders_shared::{OrderEvent, OrderSnapshot, OrderStatus, Orders};
use partyline::frame::{ConnectParams, connect_path, encode_segment};
use partyline::{Cursor, ServerFrame, close, codec};
use partyline_client::{BaseUrl, ClientEvent, ConnectOptions, Handle, Status};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite;

const TIMEOUT: Duration = Duration::from_secs(10);

fn base() -> Option<String> {
    std::env::var("PARTYLINE_E2E_URL").ok()
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

fn note(n: u32) -> OrderEvent {
    OrderEvent::NoteAdded {
        note: format!("note {n}"),
    }
}

async fn publish(base: &str, id: &str, event: &OrderEvent) -> Cursor {
    reqwest::Client::new()
        .post(format!("{base}/api/orders/{}/events", encode_segment(id)))
        .json(event)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap()
}

async fn snapshot(base: &str, id: &str) -> OrderSnapshot {
    reqwest::get(format!("{base}/api/orders/{}", encode_segment(id)))
        .await
        .unwrap()
        .json()
        .await
        .unwrap()
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

/// A scripted native socket that speaks the wire protocol directly.
async fn open(base: &str, id: &str, cursor: Option<Cursor>, extra: &str) -> Socket {
    let ws_base = BaseUrl::Explicit(base.to_owned()).resolve().unwrap();
    let query = ConnectParams::new(cursor, None).to_query();
    let url = format!("{ws_base}{}?{query}{extra}", connect_path("orders", id));
    tokio_tungstenite::connect_async(url).await.unwrap().0
}

async fn frame(ws: &mut Socket) -> ServerFrame<OrderEvent> {
    loop {
        match timeout(TIMEOUT, ws.next()).await.expect("a frame in time") {
            Some(Ok(tungstenite::Message::Text(text))) if text.as_str() != "pong" => {
                return codec::decode_frame(text.as_str()).unwrap();
            }
            Some(Ok(_)) => {}
            other => panic!("expected a frame, got {other:?}"),
        }
    }
}

async fn close_code(ws: &mut Socket) -> Option<u16> {
    loop {
        match timeout(TIMEOUT, ws.next()).await.expect("a close in time") {
            Some(Ok(tungstenite::Message::Close(frame))) => {
                return frame.map(|f| u16::from(f.code));
            }
            Some(Ok(_)) => {}
            _ => return None,
        }
    }
}

#[tokio::test]
async fn scripted_socket_resumes_with_exactly_the_missed_events() {
    let Some(base) = base() else { return };
    let id = unique("resume");

    let mut ws = open(&base, &id, None, "").await;
    let ServerFrame::Hello { v: 1, head, .. } = frame(&mut ws).await else {
        panic!("expected Hello")
    };
    assert_eq!(head.seq, 0);

    for n in 1..=3 {
        publish(&base, &id, &note(n)).await;
    }
    for n in 1..=3u64 {
        assert_eq!(
            frame(&mut ws).await,
            ServerFrame::Event {
                seq: n,
                event: note(n as u32)
            }
        );
    }
    let cursor = Cursor::new(head.epoch, 3);
    ws.close(None).await.unwrap();

    // Offline: two events are missed.
    publish(&base, &id, &note(4)).await;
    publish(&base, &id, &note(5)).await;

    let mut ws = open(&base, &id, Some(cursor), "").await;
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Hello {
            v: 1,
            mode: partyline::Mode::Log,
            head: Cursor::new(head.epoch, 5)
        }
    );
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event {
            seq: 4,
            event: note(4)
        }
    );
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event {
            seq: 5,
            event: note(5)
        }
    );

    // Live events follow the replay.
    publish(&base, &id, &note(6)).await;
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event {
            seq: 6,
            event: note(6)
        }
    );

    // The heartbeat is answered.
    ws.send(tungstenite::Message::text("ping")).await.unwrap();
    let pong = timeout(TIMEOUT, ws.next()).await.unwrap().unwrap().unwrap();
    assert_eq!(pong, tungstenite::Message::text("pong"));
}

#[tokio::test]
async fn a_cursor_from_another_epoch_gets_reset() {
    let Some(base) = base() else { return };
    let id = unique("epoch");
    let head = publish(&base, &id, &note(1)).await;
    let mut ws = open(&base, &id, Some(Cursor::new(head.epoch ^ 1, 1)), "").await;
    assert!(matches!(frame(&mut ws).await, ServerFrame::Hello { .. }));
    assert_eq!(frame(&mut ws).await, ServerFrame::Reset { head });
}

#[tokio::test]
async fn malformed_and_unsupported_requests_get_close_codes() {
    let Some(base) = base() else { return };
    let ws_base = BaseUrl::Explicit(base.clone()).resolve().unwrap();
    for (query, code) in [
        ("v=1&cursor=nope", close::BAD_REQUEST),
        ("cursor=1.1", close::BAD_REQUEST),
        ("v=2", close::UNSUPPORTED_VERSION),
    ] {
        let url = format!("{ws_base}/partyline/orders/codes?{query}");
        let mut ws = tokio_tungstenite::connect_async(url).await.unwrap().0;
        assert_eq!(close_code(&mut ws).await, Some(code), "{query}");
    }
}

#[tokio::test]
async fn the_snapshot_head_closes_the_load_subscribe_race() {
    let Some(base) = base() else { return };
    let id = unique("snapshot");
    publish(
        &base,
        &id,
        &OrderEvent::StatusChanged {
            status: OrderStatus::Preparing,
        },
    )
    .await;
    let snap = snapshot(&base, &id).await;
    assert_eq!(snap.order.status, OrderStatus::Preparing);
    assert_eq!(snap.head.seq, 1);

    // An event lands between loading and subscribing.
    publish(&base, &id, &note(1)).await;

    let mut ws = open(&base, &id, Some(snap.head), "").await;
    frame(&mut ws).await;
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event {
            seq: 2,
            event: note(1)
        }
    );
}

async fn wait_for(
    events: &mut partyline_client::Events<OrderEvent>,
    want: impl Fn(&ClientEvent<OrderEvent>) -> bool,
) -> ClientEvent<OrderEvent> {
    loop {
        let event = timeout(TIMEOUT, events.next())
            .await
            .expect("an event in time")
            .expect("the stream is open");
        if want(&event) {
            return event;
        }
    }
}

fn client(
    base: &str,
    id: &str,
    since: Option<Cursor>,
) -> (Handle, partyline_client::Events<OrderEvent>) {
    let (handle, events, driver) = partyline_client::connect::<Orders>(ConnectOptions {
        since,
        ..ConnectOptions::new(BaseUrl::Explicit(base.to_owned()), id)
    });
    tokio::spawn(driver);
    (handle, events)
}

#[tokio::test]
async fn the_real_client_goes_offline_and_resumes() {
    let Some(base) = base() else { return };
    let id = unique("client");
    let snap = snapshot(&base, &id).await;

    let (handle, mut events) = client(&base, &id, Some(snap.head));
    wait_for(&mut events, |e| *e == ClientEvent::Status(Status::Open)).await;
    publish(&base, &id, &note(1)).await;
    wait_for(&mut events, |e| {
        matches!(e, ClientEvent::Event { seq: 1, .. })
    })
    .await;

    // Go offline, keeping the cursor. Three events are missed.
    handle.stop();
    wait_for(&mut events, |e| {
        matches!(e, ClientEvent::Status(Status::Stopped { .. }))
    })
    .await;
    let cursor = handle.cursor();
    assert_eq!(cursor.map(|c| c.seq), Some(1));
    for n in 2..=4 {
        publish(&base, &id, &note(n)).await;
    }

    // Back online from the cursor: exactly the missed events, in order.
    let (handle, mut events) = client(&base, &id, cursor);
    let mut seen = Vec::new();
    while seen.len() < 3 {
        if let ClientEvent::Event { seq, event, .. } =
            wait_for(&mut events, |e| matches!(e, ClientEvent::Event { .. })).await
        {
            seen.push((seq, event));
        }
    }
    assert_eq!(seen, vec![(2, note(2)), (3, note(3)), (4, note(4))]);
    assert_eq!(handle.cursor().map(|c| c.seq), Some(4));
    handle.stop();
}

#[tokio::test]
async fn ids_with_reserved_characters_reach_one_durable_object() {
    let Some(base) = base() else { return };
    // The client percent-encodes the ID in the connect path. The Worker must decode it, so
    // that every encoding of an ID names the same Durable Object, and so does the plain ID a
    // backend passes to `Publisher`. Publishing through a different but equivalent encoding
    // only reaches the socket's Durable Object when the Worker decodes.
    let id = format!("{} a/b+c%d é", unique("odd"));
    let every_byte_encoded: String = id.bytes().map(|b| format!("%{b:02X}")).collect();

    let mut ws = open(&base, &id, None, "").await;
    assert!(matches!(frame(&mut ws).await, ServerFrame::Hello { .. }));
    let response = reqwest::Client::new()
        .post(format!("{base}/api/orders/{every_byte_encoded}/events"))
        .json(&note(1))
        .send()
        .await
        .unwrap();
    assert!(response.status().is_success());
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event {
            seq: 1,
            event: note(1)
        }
    );
    assert_eq!(
        snapshot(&base, &id).await.order.notes,
        vec!["note 1".to_owned()]
    );

    // The real client encodes the same way.
    let (handle, mut events) = client(&base, &id, Some(snapshot(&base, &id).await.head));
    wait_for(&mut events, |e| *e == ClientEvent::Status(Status::Open)).await;
    publish(&base, &id, &note(2)).await;
    let event = wait_for(&mut events, |e| matches!(e, ClientEvent::Event { .. })).await;
    let head = snapshot(&base, &id).await.head;
    assert_eq!(
        event,
        ClientEvent::Event {
            epoch: head.epoch,
            seq: 2,
            event: note(2)
        }
    );
    handle.stop();
}

#[tokio::test]
async fn an_undecodable_id_is_rejected_with_4400() {
    let Some(base) = base() else { return };
    let ws_base = BaseUrl::Explicit(base).resolve().unwrap();
    let url = format!("{ws_base}/partyline/orders/%FF?v=1");
    let mut ws = tokio_tungstenite::connect_async(url).await.unwrap().0;
    assert_eq!(close_code(&mut ws).await, Some(close::BAD_REQUEST));
}
