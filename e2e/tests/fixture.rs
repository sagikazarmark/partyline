//! Layer 4, against the e2e fixture Worker: upgrades through an axum router, trimming in
//! SQLite by count and by age, authorization close codes, closing by tag, and channel reset.
//!
//! Run with `just e2e-fixture`, which starts the fixture under `wrangler dev`
//! and sets `PARTYLINE_E2E_FIXTURE_URL`. Without it, every test passes without
//! doing anything.

use std::time::Duration;

use futures::StreamExt;
use partyline::frame::{ConnectParams, connect_path, encode_segment};
use partyline::{Channel, Cursor, Mode, ServerFrame, close, codec};
use partyline_client::{BaseUrl, ClientEvent, ConnectOptions, Status};
use tokio::time::timeout;
use tokio_tungstenite::tungstenite;

/// The fixture's channel.
struct Ticks;

impl Channel for Ticks {
    const NAME: &'static str = "ticks";
    const MODE: Mode = Mode::Log;
    type Event = u64;
}

const TIMEOUT: Duration = Duration::from_secs(10);

fn base() -> Option<String> {
    std::env::var("PARTYLINE_E2E_FIXTURE_URL").ok()
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{nanos}")
}

async fn publish(base: &str, id: &str, n: u64) -> Cursor {
    let text = reqwest::Client::new()
        .post(format!("{base}/api/ticks/{}", encode_segment(id)))
        .body(n.to_string())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    text.parse().unwrap()
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn open(base: &str, id: &str, cursor: Option<Cursor>) -> Socket {
    open_with(base, id, cursor, "").await
}

/// Opens a socket with extra query parameters, such as `&user=alice`.
async fn open_with(base: &str, id: &str, cursor: Option<Cursor>, extra: &str) -> Socket {
    open_route(base, "ticks", id, cursor, extra).await
}

/// Opens a socket on one of the fixture's upgrade routes, `/partyline/{route}/{id}`.
async fn open_route(
    base: &str,
    route: &str,
    id: &str,
    cursor: Option<Cursor>,
    extra: &str,
) -> Socket {
    let ws_base = BaseUrl::Explicit(base.to_owned()).resolve().unwrap();
    let query = ConnectParams::new(cursor, None).to_query();
    let url = format!("{ws_base}{}?{query}{extra}", connect_path(route, id));
    tokio_tungstenite::connect_async(url).await.unwrap().0
}

/// Posts a body to the fixture and returns the text of the response.
async fn post_body(base: &str, path: &str, body: &str) -> String {
    reqwest::Client::new()
        .post(format!("{base}{path}"))
        .body(body.to_owned())
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap()
}

async fn frame(ws: &mut Socket) -> ServerFrame<u64> {
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

async fn hello(ws: &mut Socket) -> Cursor {
    match frame(ws).await {
        ServerFrame::Hello { head, .. } => head,
        other => panic!("expected Hello, got {other:?}"),
    }
}

#[tokio::test]
async fn upgrades_pass_through_an_axum_router() {
    let Some(base) = base() else { return };
    // axum's `Path` extractor decodes the ID, so reserved characters are fine.
    let id = unique("axum a/b");

    let mut ws = open(&base, &id, None).await;
    let head = hello(&mut ws).await;
    publish(&base, &id, 1).await;
    publish(&base, &id, 2).await;
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event { seq: 1, event: 1 }
    );
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event { seq: 2, event: 2 }
    );
    drop(ws);

    publish(&base, &id, 3).await;
    let mut ws = open(&base, &id, Some(Cursor::new(head.epoch, 2))).await;
    hello(&mut ws).await;
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event { seq: 3, event: 3 }
    );
}

#[tokio::test]
async fn count_trimming_resets_cursors_older_than_the_window() {
    let Some(base) = base() else { return };
    let id = unique("count");
    let mut head = Cursor::new(0, 0);
    for n in 1..=8 {
        head = publish(&base, &id, n).await;
    }
    // The fixture keeps 5 events: 4 to 8.
    let mut ws = open(&base, &id, Some(Cursor::new(head.epoch, 2))).await;
    hello(&mut ws).await;
    assert_eq!(frame(&mut ws).await, ServerFrame::Reset { head });

    let mut ws = open(&base, &id, Some(Cursor::new(head.epoch, 3))).await;
    hello(&mut ws).await;
    for n in 4..=8 {
        assert_eq!(
            frame(&mut ws).await,
            ServerFrame::Event { seq: n, event: n }
        );
    }
}

#[tokio::test]
async fn the_alarm_trims_by_age_on_an_idle_channel() {
    let Some(base) = base() else { return };
    let id = unique("age");
    let mut head = Cursor::new(0, 0);
    for n in 1..=3 {
        head = publish(&base, &id, n).await;
    }
    let start = Cursor::new(head.epoch, 0);

    // Fresh events replay.
    let mut ws = open(&base, &id, Some(start)).await;
    hello(&mut ws).await;
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event { seq: 1, event: 1 }
    );
    drop(ws);

    // The fixture keeps events for 3 seconds. Nothing is published meanwhile, so only the
    // alarm can trim them.
    tokio::time::sleep(Duration::from_millis(4_500)).await;
    let mut ws = open(&base, &id, Some(start)).await;
    assert_eq!(hello(&mut ws).await, head);
    assert_eq!(frame(&mut ws).await, ServerFrame::Reset { head });

    // A client at the head is unaffected, and the channel keeps working.
    let mut ws = open(&base, &id, Some(head)).await;
    hello(&mut ws).await;
    publish(&base, &id, 4).await;
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event { seq: 4, event: 4 }
    );
}

#[tokio::test]
async fn a_batch_published_in_one_turn_arrives_in_order_and_is_trimmed_by_age() {
    let Some(base) = base() else { return };
    let id = unique("batch");
    let mut ws = open(&base, &id, None).await;
    let start = hello(&mut ws).await;

    // `publish_now` three times, then `schedule_trim` once, in one Durable Object turn.
    let text = reqwest::Client::new()
        .post(format!("{base}/api/ticks/{}/batch", encode_segment(&id)))
        .body("1,2,3")
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap();
    let head: Cursor = text.parse().unwrap();
    assert_eq!(head, Cursor::new(start.epoch, 3));
    for n in 1..=3 {
        assert_eq!(
            frame(&mut ws).await,
            ServerFrame::Event { seq: n, event: n }
        );
    }
    drop(ws);

    // `schedule_trim` set the alarm, so the idle channel is trimmed by age.
    tokio::time::sleep(Duration::from_millis(4_500)).await;
    let mut ws = open(&base, &id, Some(start)).await;
    assert_eq!(hello(&mut ws).await, head);
    assert_eq!(frame(&mut ws).await, ServerFrame::Reset { head });
}

async fn post(base: &str, path: &str) -> String {
    reqwest::Client::new()
        .post(format!("{base}{path}"))
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap()
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
async fn rejected_tokens_get_their_close_codes() {
    let Some(base) = base() else { return };
    for (token, code) in [
        ("expired", close::UNAUTHORIZED),
        ("forbidden", close::FORBIDDEN),
    ] {
        let mut ws = open_with(&base, "auth", None, &format!("&token={token}")).await;
        assert_eq!(close_code(&mut ws).await, Some(code), "{token}");
    }
}

#[tokio::test]
async fn close_by_tag_closes_only_that_users_sockets() {
    let Some(base) = base() else { return };
    let id = unique("tags");
    let mut alice = open_with(&base, &id, None, "&user=alice").await;
    let mut bob = open_with(&base, &id, None, "&user=bob").await;
    hello(&mut alice).await;
    hello(&mut bob).await;

    let closed = post(&base, &format!("/api/ticks/{id}/close?tag=alice")).await;
    assert_eq!(closed, "1");
    assert_eq!(close_code(&mut alice).await, Some(close::FORBIDDEN));

    publish(&base, &id, 1).await;
    assert_eq!(
        frame(&mut bob).await,
        ServerFrame::Event { seq: 1, event: 1 }
    );
}

async fn next_event(
    events: &mut partyline_client::Events<u64>,
    want: impl Fn(&ClientEvent<u64>) -> bool,
) -> ClientEvent<u64> {
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

#[tokio::test]
async fn the_real_client_receives_reset_after_a_channel_reset() {
    let Some(base) = base() else { return };
    let id = unique("reset");
    publish(&base, &id, 1).await;
    let head: Cursor = reqwest::get(format!("{base}/api/ticks/{id}/head"))
        .await
        .unwrap()
        .text()
        .await
        .unwrap()
        .parse()
        .unwrap();

    let (handle, mut events, driver) = partyline_client::connect::<Ticks>(ConnectOptions {
        since: Some(head),
        ..ConnectOptions::new(BaseUrl::Explicit(base.clone()), id.clone())
    });
    tokio::spawn(driver);
    next_event(&mut events, |e| *e == ClientEvent::Status(Status::Open)).await;

    let new_head: Cursor = post(&base, &format!("/api/ticks/{id}/reset"))
        .await
        .parse()
        .unwrap();
    assert_ne!(new_head.epoch, head.epoch);

    // The hub closes with 1012, the client reconnects, and the old cursor gets Reset.
    next_event(&mut events, |e| *e == ClientEvent::Reset).await;
    assert_eq!(handle.cursor(), Some(new_head));
    publish(&base, &id, 9).await;
    let event = next_event(&mut events, |e| matches!(e, ClientEvent::Event { .. })).await;
    assert_eq!(
        event,
        ClientEvent::Event {
            epoch: new_head.epoch,
            seq: 1,
            event: 9
        }
    );
    handle.stop();
}

#[tokio::test]
async fn events_over_the_size_limit_are_rejected() {
    let Some(base) = base() else { return };
    let id = unique("size");
    // The fixture accepts events of at most 16 bytes.
    let head = publish(&base, &id, 1_234_567_890_123_456).await;
    let response = reqwest::Client::new()
        .post(format!("{base}/api/ticks/{}", encode_segment(&id)))
        .body("12345678901234567")
        .send()
        .await
        .unwrap();
    assert!(!response.status().is_success(), "{}", response.status());
    let text = get_text(&base, &format!("/api/ticks/{}/head", encode_segment(&id))).await;
    assert_eq!(text.parse::<Cursor>().unwrap(), head, "nothing is stored");
}

async fn get_text(base: &str, path: &str) -> String {
    reqwest::get(format!("{base}{path}"))
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .text()
        .await
        .unwrap()
}

#[tokio::test]
async fn the_token_is_stripped_but_the_cursor_is_kept() {
    let Some(base) = base() else { return };
    let id = unique("token");
    let head = publish(&base, &id, 1).await;
    publish(&base, &id, 2).await;
    let mut ws = open_with(&base, &id, Some(Cursor::new(head.epoch, 1)), "&token=%FF").await;
    hello(&mut ws).await;
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event { seq: 2, event: 2 }
    );
}

#[tokio::test]
async fn non_ascii_tags_work_and_invalid_tags_are_refused() {
    let Some(base) = base() else { return };
    let id = unique("tags-utf8");
    let user = "Usuário 日本";
    let mut ws = open_with(&base, &id, None, &format!("&user={}", encode_segment(user))).await;
    hello(&mut ws).await;
    let closed = post(
        &base,
        &format!("/api/ticks/{id}/close?tag={}", encode_segment(user)),
    )
    .await;
    assert_eq!(closed, "1");
    assert_eq!(close_code(&mut ws).await, Some(close::FORBIDDEN));

    // An empty tag would make the runtime throw, so `Connect` refuses to forward.
    let ws_base = BaseUrl::Explicit(base.clone()).resolve().unwrap();
    let url = format!("{ws_base}{}?v=1&user=", connect_path("ticks", &id));
    assert!(tokio_tungstenite::connect_async(url).await.is_err());
}

#[tokio::test]
async fn a_0_4_channel_is_migrated_and_clients_resume_without_reset() {
    let Some(base) = base() else { return };
    let id = unique("legacy");
    let api = format!("/api/legacy/{}", encode_segment(&id));

    // The object holds a 0.4 channel at seq 3, and its hub does not exist yet.
    let epoch: u64 = post(&base, &format!("{api}/seed")).await.parse().unwrap();
    assert_eq!(
        get_text(&base, &format!("{api}/tables")).await,
        "partyline_log,partyline_meta"
    );

    // A client that saw event 1 under 0.4 resumes from its cursor. Connecting creates the
    // hub, which migrates the channel.
    let mut ws = open_route(&base, "legacy", &id, Some(Cursor::new(epoch, 1)), "").await;
    assert_eq!(hello(&mut ws).await, Cursor::new(epoch, 3));
    for n in 2..=3 {
        assert_eq!(
            frame(&mut ws).await,
            ServerFrame::Event { seq: n, event: n }
        );
    }
    assert_eq!(
        get_text(&base, &format!("{api}/tables")).await,
        "partyline_channels,partyline_events"
    );

    // The channel continues in the same epoch.
    let head: Cursor = post_body(&base, &api, "4").await.parse().unwrap();
    assert_eq!(head, Cursor::new(epoch, 4));
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event { seq: 4, event: 4 }
    );
}

#[tokio::test]
async fn reserved_tags_are_refused() {
    let Some(base) = base() else { return };
    let id = unique("tags-reserved");
    // A tag with the hub's prefix could join another channel, so `Connect` refuses it.
    let ws_base = BaseUrl::Explicit(base.clone()).resolve().unwrap();
    let url = format!(
        "{ws_base}{}?v=1&user={}",
        connect_path("ticks", &id),
        encode_segment("partyline:x")
    );
    assert!(tokio_tungstenite::connect_async(url).await.is_err());
}

#[tokio::test]
async fn channels_in_one_object_are_independent() {
    let Some(base) = base() else { return };
    let room = unique("room");
    let (a, b) = (unique("a"), unique("b"));
    let in_room = format!("&room={}", encode_segment(&room));
    let mut ws_a = open_with(&base, &a, None, &in_room).await;
    let mut ws_b = open_with(&base, &b, None, &in_room).await;
    let head_a = hello(&mut ws_a).await;
    let head_b = hello(&mut ws_b).await;
    assert_ne!(head_a.epoch, head_b.epoch, "each channel has its own epoch");

    let path = |id: &str| {
        format!(
            "/api/ticks/{}?room={}",
            encode_segment(id),
            encode_segment(&room)
        )
    };
    let after_a: Cursor = post_body(&base, &path(&a), "10").await.parse().unwrap();
    let after_b: Cursor = post_body(&base, &path(&b), "20").await.parse().unwrap();
    assert_eq!(after_a, Cursor::new(head_a.epoch, 1));
    assert_eq!(after_b, Cursor::new(head_b.epoch, 1));

    // Each socket gets only its own channel's event.
    assert_eq!(
        frame(&mut ws_a).await,
        ServerFrame::Event { seq: 1, event: 10 }
    );
    assert_eq!(
        frame(&mut ws_b).await,
        ServerFrame::Event { seq: 1, event: 20 }
    );
    let head = |id: &str| {
        format!(
            "/api/ticks/{}/head?room={}",
            encode_segment(id),
            encode_segment(&room)
        )
    };
    assert_eq!(get_text(&base, &head(&a)).await, after_a.to_string());
    assert_eq!(get_text(&base, &head(&b)).await, after_b.to_string());
}

#[tokio::test]
async fn close_and_reset_act_on_one_channel_in_an_object() {
    let Some(base) = base() else { return };
    let room = unique("room");
    let (a, b) = (unique("a"), unique("b"));
    let in_room = format!("&room={}&user=alice", encode_segment(&room));
    let api = |id: &str, action: &str| {
        format!(
            "/api/ticks/{}{action}?room={}",
            encode_segment(id),
            encode_segment(&room)
        )
    };

    // Alice has a socket on each channel. Closing her on channel a leaves channel b open.
    let mut ws_a = open_with(&base, &a, None, &in_room).await;
    let mut ws_b = open_with(&base, &b, None, &in_room).await;
    hello(&mut ws_a).await;
    hello(&mut ws_b).await;
    let closed = post(&base, &format!("{}&tag=alice", api(&a, "/close"))).await;
    assert_eq!(closed, "1");
    assert_eq!(close_code(&mut ws_a).await, Some(close::FORBIDDEN));
    post_body(&base, &api(&b, ""), "1").await;
    assert_eq!(
        frame(&mut ws_b).await,
        ServerFrame::Event { seq: 1, event: 1 }
    );

    // Resetting channel a closes only channel a's sockets.
    let mut ws_a = open_with(&base, &a, None, &in_room).await;
    hello(&mut ws_a).await;
    post(&base, &api(&a, "/reset")).await;
    assert_eq!(close_code(&mut ws_a).await, Some(close::SERVICE_RESTART));
    post_body(&base, &api(&b, ""), "2").await;
    assert_eq!(
        frame(&mut ws_b).await,
        ServerFrame::Event { seq: 2, event: 2 }
    );
}

/// Asserts that no frame arrives on the socket for a while.
async fn assert_silent(ws: &mut Socket) {
    let next = timeout(Duration::from_millis(500), ws.next()).await;
    assert!(next.is_err(), "expected no frame, got {next:?}");
}

/// The fixture's room API for `room`.
fn room_api(room: &str, action: &str) -> String {
    format!("/api/rooms/{}{action}", encode_segment(room))
}

#[tokio::test]
async fn hubs_of_two_types_share_an_object() {
    let Some(base) = base() else { return };
    let room = unique("room");
    let in_room = format!("&room={}", encode_segment(&room));
    let mut ticks = open_route(&base, "room-ticks", "feed", None, &in_room).await;
    let mut view = open_route(&base, "room-views", "alice", None, &in_room).await;
    hello(&mut ticks).await;
    hello(&mut view).await;

    // One turn stores and sends an event on each hub's channel.
    let heads = post_body(
        &base,
        &room_api(&room, "/commit"),
        "ticks:feed=1;views:alice=7",
    )
    .await;
    assert_eq!(heads.split(',').count(), 2);
    assert_eq!(
        frame(&mut ticks).await,
        ServerFrame::Event { seq: 1, event: 1 }
    );
    assert_eq!(
        frame(&mut view).await,
        ServerFrame::Event { seq: 1, event: 7 }
    );

    // `Publisher` reaches the Latest hub, which `Hub::handles` picks out.
    post_body(&base, &room_api(&room, "/views/alice"), "8").await;
    assert_eq!(
        frame(&mut view).await,
        ServerFrame::Event { seq: 2, event: 8 }
    );
    assert_silent(&mut ticks).await;

    // A Latest client that is behind gets the latest value.
    let mut ws = open_route(
        &base,
        "room-views",
        "alice",
        Some(Cursor::new(0, 0)),
        &in_room,
    )
    .await;
    hello(&mut ws).await;
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event { seq: 2, event: 8 }
    );
}

#[tokio::test]
async fn appended_events_reach_sockets_only_when_sent_and_none_is_lost() {
    let Some(base) = base() else { return };
    let room = unique("room");
    let in_room = format!("&room={}", encode_segment(&room));
    let mut ws = open_route(&base, "room-ticks", "feed", None, &in_room).await;
    let start = hello(&mut ws).await;

    // Stored, but never sent, as if the object stopped after the commit.
    post_body(&base, &room_api(&room, "/commit?send=0"), "ticks:feed=1").await;
    assert_silent(&mut ws).await;

    // The next event is sent. The socket sees a gap, which a client resumes over.
    post_body(&base, &room_api(&room, "/commit"), "ticks:feed=2").await;
    assert_eq!(
        frame(&mut ws).await,
        ServerFrame::Event { seq: 2, event: 2 }
    );

    // Resuming from the start replays both events: the unsent one was stored.
    let mut ws = open_route(
        &base,
        "room-ticks",
        "feed",
        Some(Cursor::new(start.epoch, 0)),
        &in_room,
    )
    .await;
    hello(&mut ws).await;
    for n in 1..=2 {
        assert_eq!(
            frame(&mut ws).await,
            ServerFrame::Event { seq: n, event: n }
        );
    }
}

#[tokio::test]
async fn the_hub_closes_a_tag_on_every_channel_in_the_object() {
    let Some(base) = base() else { return };
    let room = unique("room");
    let alice = format!("&room={}&user=alice", encode_segment(&room));
    let bob = format!("&room={}&user=bob", encode_segment(&room));
    let mut feed = open_route(&base, "room-ticks", "feed", None, &alice).await;
    let mut view = open_route(&base, "room-views", "alice", None, &alice).await;
    let mut other = open_route(&base, "room-ticks", "feed", None, &bob).await;
    hello(&mut feed).await;
    hello(&mut view).await;
    hello(&mut other).await;

    let closed = post(&base, &room_api(&room, "/close?tag=alice")).await;
    assert_eq!(closed, "2");
    assert_eq!(close_code(&mut feed).await, Some(close::FORBIDDEN));
    assert_eq!(close_code(&mut view).await, Some(close::FORBIDDEN));

    // Bob stays connected.
    post_body(&base, &room_api(&room, "/commit"), "ticks:feed=1").await;
    assert_eq!(
        frame(&mut other).await,
        ServerFrame::Event { seq: 1, event: 1 }
    );
}

#[tokio::test]
async fn the_shared_alarm_trims_every_channel_in_the_object_by_age() {
    let Some(base) = base() else { return };
    let room = unique("room");
    let in_room = format!("&room={}", encode_segment(&room));
    let heads: Vec<Cursor> = post_body(&base, &room_api(&room, "/commit"), "ticks:a=1;ticks:b=1")
        .await
        .split(',')
        .map(|head| head.parse().unwrap())
        .collect();

    // The fixture keeps events for 3 seconds. Nothing is published meanwhile, so only the
    // alarm can trim them, and it must trim both channels.
    tokio::time::sleep(Duration::from_millis(4_500)).await;
    for (id, head) in ["a", "b"].into_iter().zip(heads) {
        let mut ws = open_route(
            &base,
            "room-ticks",
            id,
            Some(Cursor::new(head.epoch, 0)),
            &in_room,
        )
        .await;
        assert_eq!(hello(&mut ws).await, head);
        assert_eq!(
            frame(&mut ws).await,
            ServerFrame::Reset { head },
            "channel {id}"
        );
    }
}

#[tokio::test]
async fn dropping_the_tables_closes_every_channel_in_the_object() {
    let Some(base) = base() else { return };
    let room = unique("room");
    let in_room = format!("&room={}", encode_segment(&room));
    post_body(
        &base,
        &room_api(&room, "/commit"),
        "ticks:feed=1;views:alice=1",
    )
    .await;
    let mut feed = open_route(&base, "room-ticks", "feed", None, &in_room).await;
    let mut view = open_route(&base, "room-views", "alice", None, &in_room).await;
    hello(&mut feed).await;
    hello(&mut view).await;
    // The Latest client is behind, so it gets the value first.
    frame(&mut view).await;
    assert_eq!(
        get_text(&base, &room_api(&room, "/tables")).await,
        "partyline_channels,partyline_events"
    );

    post(&base, &room_api(&room, "/drop")).await;
    // 1000 tells the clients not to reconnect.
    assert_eq!(close_code(&mut feed).await, Some(close::NORMAL));
    assert_eq!(close_code(&mut view).await, Some(close::NORMAL));
    assert_eq!(get_text(&base, &room_api(&room, "/tables")).await, "");
}
