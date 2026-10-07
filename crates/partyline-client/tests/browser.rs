//! Layer 3 (browser): `BrowserSocket` and the driver in headless Chrome, against a running
//! partyline hub: the orders example under `wrangler dev`.
//!
//! Set `PARTYLINE_TEST_URL` at build time, for example `http://127.0.0.1:8787`. Without it,
//! the tests pass without doing anything.

#![cfg(target_arch = "wasm32")]

use futures::{SinkExt, StreamExt};
use partyline::{Channel, ClientConfig, Message, Mode, Status, close};
use partyline_client::{
    BaseUrl, BrowserSocket, ClientEvent, ConnectOptions, Transport, TransportError,
};
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

const URL: Option<&str> = option_env!("PARTYLINE_TEST_URL");

struct Orders;
impl Channel for Orders {
    const NAME: &'static str = "orders";
    const MODE: Mode = Mode::Log;
    type Event = serde_json::Value;
}

fn ws_url(base: &str, path_and_query: &str) -> String {
    let base = BaseUrl::Explicit(base.to_owned()).resolve().unwrap();
    format!("{base}{path_and_query}")
}

fn unique_id(prefix: &str) -> String {
    format!("{prefix}-{}", (js_sys::Math::random() * 1e12) as u64)
}

#[wasm_bindgen_test]
async fn browser_socket_receives_hello_and_pong() {
    let Some(base) = URL else { return };
    let url = ws_url(base, &format!("/partyline/orders/{}?v=1", unique_id("b")));
    let mut conn = BrowserSocket.connect(&url).await.unwrap();
    let Some(Ok(Message::Text(hello))) = conn.next().await else {
        panic!("expected a text frame")
    };
    assert!(
        hello.starts_with(r#"{"t":"hello","v":1,"mode":"log""#),
        "{hello}"
    );
    conn.send(Message::ping()).await.unwrap();
    assert_eq!(conn.next().await, Some(Ok(Message::Text("pong".into()))));
    conn.close().await.unwrap();
}

#[wasm_bindgen_test]
async fn browser_socket_reports_close_codes() {
    let Some(base) = URL else { return };
    // The hub rejects an unsupported protocol version itself.
    let url = ws_url(base, "/partyline/orders/x?v=2");
    let mut conn = BrowserSocket.connect(&url).await.unwrap();
    assert_eq!(
        conn.next().await,
        Some(Err(TransportError::Closed {
            code: Some(close::UNSUPPORTED_VERSION)
        }))
    );
}

#[wasm_bindgen_test]
async fn browser_socket_reports_connect_failures() {
    let result = BrowserSocket.connect("ws://127.0.0.1:9/").await;
    assert!(
        matches!(result, Err(TransportError::Connect(_))),
        "{result:?}"
    );
}

#[wasm_bindgen_test]
async fn driver_receives_a_published_event() {
    let Some(base) = URL else { return };
    let id = unique_id("d");
    let options = ConnectOptions {
        config: ClientConfig::default(),
        ..ConnectOptions::new(BaseUrl::Explicit(base.to_owned()), id.clone())
    };
    let (handle, mut events, driver) = partyline_client::connect::<Orders>(options);
    wasm_bindgen_futures::spawn_local(driver);
    loop {
        if events.next().await == Some(ClientEvent::Status(Status::Open)) {
            break;
        }
    }
    // The test page is on another origin. A no-cors POST still reaches the Worker, but the
    // browser may report the opaque response as an error, so the result is ignored.
    let _ = gloo_net::http::Request::post(&format!("{base}/api/orders/{id}/events"))
        .mode(web_sys::RequestMode::NoCors)
        .body(r#"{"type":"note_added","note":"from the browser"}"#)
        .unwrap()
        .send()
        .await;
    let event = loop {
        if let Some(ClientEvent::Event { seq, event }) = events.next().await {
            assert_eq!(seq, 1);
            break event;
        }
    };
    assert_eq!(event["note"], "from the browser");
    handle.stop();
}
