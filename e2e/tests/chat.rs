//! Layer 4, against the chat example: sign-in, access tokens at upgrade, and sign-out
//! closing a user's sockets by tag.
//!
//! Run with `just examples e2e chat`, which starts the chat example under
//! `wrangler dev` and sets `PARTYLINE_E2E_CHAT_URL`. Without it, every test passes
//! without doing anything.

use std::time::Duration;

use chat_shared::{AccessToken, Chat, ChatEvent, Login, PostMessage, ROOM};
use futures::StreamExt;
use partyline::frame::{ConnectParams, connect_path};
use partyline::{Channel, Cursor, ServerFrame, close, codec};
use partyline_client::BaseUrl;
use tokio::time::timeout;
use tokio_tungstenite::tungstenite;

const TIMEOUT: Duration = Duration::from_secs(10);

fn base() -> Option<String> {
    std::env::var("PARTYLINE_E2E_CHAT_URL").ok()
}

fn unique(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{prefix}-{}", nanos % 1_000_000_000)
}

/// Signs in, and returns the `Cookie` header value and the access token.
async fn login(base: &str, name: &str) -> (String, AccessToken) {
    let response = reqwest::Client::new()
        .post(format!("{base}/api/login"))
        .json(&Login {
            name: name.to_owned(),
        })
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap();
    let cookie = response.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    (cookie, response.json().await.unwrap())
}

async fn post(base: &str, cookie: Option<&str>, text: &str) -> reqwest::Response {
    let mut request = reqwest::Client::new()
        .post(format!("{base}/api/messages"))
        .json(&PostMessage {
            text: text.to_owned(),
        });
    if let Some(cookie) = cookie {
        request = request.header("cookie", cookie);
    }
    request.send().await.unwrap()
}

type Socket =
    tokio_tungstenite::WebSocketStream<tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>>;

async fn open(base: &str, token: Option<&str>) -> Socket {
    let ws_base = BaseUrl::Explicit(base.to_owned()).resolve().unwrap();
    let query = ConnectParams::new(None, token.map(str::to_owned)).to_query();
    let url = format!("{ws_base}{}?{query}", connect_path(Chat::NAME, ROOM));
    tokio_tungstenite::connect_async(url).await.unwrap().0
}

async fn frame(ws: &mut Socket) -> ServerFrame<ChatEvent> {
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

/// Reads frames until the message with this text arrives.
async fn message(ws: &mut Socket, text: &str) -> String {
    loop {
        if let ServerFrame::Event {
            event: ChatEvent::MessagePosted { author, text: t },
            ..
        } = frame(ws).await
            && t == text
        {
            return author;
        }
    }
}

#[tokio::test]
async fn messages_carry_the_session_user_as_author() {
    let Some(base) = base() else { return };
    let name = unique("alice");
    let (cookie, token) = login(&base, &name).await;
    assert_eq!(token.user, name);

    let mut ws = open(&base, Some(&token.token)).await;
    assert!(matches!(frame(&mut ws).await, ServerFrame::Hello { .. }));

    let text = unique("hello");
    let head: Cursor = post(&base, Some(&cookie), &text)
        .await
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert!(head.seq > 0);
    assert_eq!(message(&mut ws, &text).await, name);
}

#[tokio::test]
async fn the_socket_needs_a_valid_access_token() {
    let Some(base) = base() else { return };
    let (cookie, token) = login(&base, &unique("mallory")).await;
    let session = cookie.strip_prefix("session=").unwrap();
    // Change the signature's first character: it carries six bits of the signature.
    let (claims, signature) = token.token.split_once('.').unwrap();
    let first = if signature.starts_with('A') { 'B' } else { 'A' };
    let tampered = format!("{claims}.{first}{}", &signature[1..]);

    for (case, token) in [
        ("missing", None),
        ("garbage", Some("not-a-token")),
        ("the session cookie", Some(session)),
        ("a changed signature", Some(tampered.as_str())),
    ] {
        let mut ws = open(&base, token).await;
        assert_eq!(
            close_code(&mut ws).await,
            Some(close::UNAUTHORIZED),
            "{case}"
        );
    }
}

#[tokio::test]
async fn the_api_needs_a_session() {
    let Some(base) = base() else { return };
    let client = reqwest::Client::new();

    let token = client
        .get(format!("{base}/api/token"))
        .send()
        .await
        .unwrap();
    assert_eq!(token.status(), 401);
    assert_eq!(post(&base, None, "anonymous").await.status(), 401);

    let empty = client
        .post(format!("{base}/api/login"))
        .json(&Login {
            name: "  ".to_owned(),
        })
        .send()
        .await
        .unwrap();
    assert_eq!(empty.status(), 400);

    let name = unique("carol");
    let (cookie, _) = login(&base, &name).await;
    let token: AccessToken = client
        .get(format!("{base}/api/token"))
        .header("cookie", &cookie)
        .send()
        .await
        .unwrap()
        .error_for_status()
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(token.user, name);
}

#[tokio::test]
async fn sign_out_closes_only_that_users_sockets() {
    let Some(base) = base() else { return };
    let (alice_cookie, alice) = login(&base, &unique("alice")).await;
    let (bob_cookie, bob) = login(&base, &unique("bob")).await;

    let mut alice_tabs = [
        open(&base, Some(&alice.token)).await,
        open(&base, Some(&alice.token)).await,
    ];
    let mut bob_tab = open(&base, Some(&bob.token)).await;
    for ws in alice_tabs.iter_mut().chain([&mut bob_tab]) {
        assert!(matches!(frame(ws).await, ServerFrame::Hello { .. }));
    }

    let logout = reqwest::Client::new()
        .post(format!("{base}/api/logout"))
        .header("cookie", &alice_cookie)
        .send()
        .await
        .unwrap();
    assert_eq!(logout.status(), 204);
    let cleared = logout.headers()["set-cookie"].to_str().unwrap();
    assert!(cleared.contains("Max-Age=0"), "{cleared}");

    for ws in &mut alice_tabs {
        assert_eq!(close_code(ws).await, Some(close::FORBIDDEN));
    }
    let text = unique("still here");
    post(&base, Some(&bob_cookie), &text)
        .await
        .error_for_status()
        .unwrap();
    assert_eq!(message(&mut bob_tab, &text).await, bob.user);
}
