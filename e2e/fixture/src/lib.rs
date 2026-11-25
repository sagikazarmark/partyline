//! A Worker for end-to-end tests.
//!
//! - It routes through an axum `Router`, to check that `Connect::forward_http` passes
//!   WebSocket upgrades through axum on Workers.
//! - Its channel keeps at most 5 events for at most 3 seconds, to check count-based
//!   trimming and the alarm that runs age-based trimming in SQLite. It accepts events of
//!   at most 16 bytes, so a 17-digit number checks the size limit.
//! - It holds the hooks that only tests need: magic tokens, socket tags from a query
//!   parameter, and routes to close sockets by tag and to reset the channel. The examples
//!   stay free of them.
//! - On the `ticks` routes, a `room` query parameter puts the channel in the object named by
//!   the room instead of the one named by the ID, so one object holds several channels.
//!
//! | Route | Action |
//! | --- | --- |
//! | `GET /partyline/ticks/{id}` | WebSocket upgrade |
//! | `POST /api/ticks/{id}` | Publish the number in the body |
//! | `POST /api/ticks/{id}/batch` | Publish the comma-separated numbers in the body, in one turn |
//! | `GET /api/ticks/{id}/head` | The channel head |
//! | `POST /api/ticks/{id}/close?tag={user}` | Close a user's sockets with 4403 |
//! | `POST /api/ticks/{id}/reset` | Wipe the channel log with a new epoch |
//! | `GET /partyline/legacy/{id}` | WebSocket upgrade to an object that may hold 0.4 tables |
//! | `POST /api/legacy/{id}` | Publish the number in the body there |
//! | `POST /api/legacy/{id}/seed[?epoch={n}&again=1]` | Write 0.4 tables into the object, before its hub exists, or again after it |
//! | `POST /api/legacy/{id}/remigrate` | Run the migration again in the same instance, as the next wake would |
//! | `GET /api/legacy/{id}/head` | The channel head |
//! | `GET /api/legacy/{id}/tables` | The object's partyline tables |
//! | `GET /partyline/room-ticks/{id}?room={room}` | WebSocket upgrade to a Log channel in a room's object |
//! | `GET /partyline/room-views/{id}?room={room}` | WebSocket upgrade to a Latest channel in the same object |
//! | `POST /api/rooms/{room}/commit[?send=0]` | Store events on the room's channels in one turn, then send them |
//! | `POST /api/rooms/{room}/close?tag={user}` | Close a user's sockets on every channel in the room |
//! | `POST /api/rooms/{room}/views/{id}` | Publish the number in the body to a Latest channel, with `Publisher` |
//! | `POST /api/rooms/{room}/drop` | Delete every channel in the room, with `Hub::drop_tables` |
//! | `GET /api/rooms/{room}/tables` | The room object's partyline tables |

// `#[durable_object]` adds undocumented public methods (the JS entry points) to the Durable Object,
// in an impl block that no attribute here can reach.
#![allow(missing_docs)]

mod legacy;
mod room;

use std::time::Duration;

use axum::Router;
use axum::extract::{Extension, Path};
use axum::http::{StatusCode, Uri};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use partyline::frame::encode_segment;
use partyline::{Channel, Mode};
use partyline_worker::{Connect, Hub, HubConfig, Publisher, close, decode_segment, reject};
use tower_service::Service;
use worker::{
    Context, DurableObject, Env, HttpRequest, Method, Request, RequestInit, State, durable_object,
    event, wasm_bindgen,
};

/// A channel of numbers.
pub struct Ticks;

impl Channel for Ticks {
    const NAME: &'static str = "ticks";
    const MODE: Mode = Mode::Log;
    type Event = u64;
}

const BINDING: &str = "TICK_CHANNEL";
const LEGACY_BINDING: &str = "LEGACY_CHANNEL";
const ROOM_BINDING: &str = "ROOM_CHANNEL";

/// The base URL of the fixture's own requests to the Durable Object. The host is never
/// resolved.
const INTERNAL: &str = "https://fixture.internal";

/// A channel with a tiny retention window.
///
/// It embeds the hub by hand, for one route of its own: `POST /batch` publishes a
/// comma-separated list of numbers in one turn, with `Hub::publish_now`.
#[durable_object]
pub struct TickChannel {
    hub: Hub<Ticks>,
}

impl DurableObject for TickChannel {
    fn new(state: State, _env: Env) -> Self {
        let config = HubConfig::default()
            .retain_events(5)
            .retain_for(Some(Duration::from_secs(3)))
            .max_event_bytes(16);
        Self {
            hub: Hub::new(state, config),
        }
    }

    async fn fetch(&self, mut req: Request) -> worker::Result<worker::Response> {
        if req.path() != "/batch" {
            return self.hub.fetch(req).await;
        }
        let body = req.text().await?;
        let Ok(numbers) = body
            .split(',')
            .map(|n| n.trim().parse::<u64>())
            .collect::<Result<Vec<_>, _>>()
        else {
            return worker::Response::error("expected comma-separated numbers", 400);
        };
        let mut head = self.hub.head()?;
        for n in &numbers {
            head = self.hub.publish_now(n)?;
        }
        self.hub.schedule_trim().await?;
        worker::Response::ok(head.to_string())
    }

    partyline_worker::hub_handlers!(hub);
}

fn error(e: worker::Error) -> Response {
    (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response()
}

/// A query parameter, percent-decoded.
fn query_param(uri: &axum::http::Uri, name: &str) -> Option<String> {
    uri.query()?.split('&').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key == name).then(|| decode_segment(value)).flatten()
    })
}

/// Converts a `worker` response, such as one from `reject`, for axum.
fn from_worker(response: worker::Result<worker::Response>) -> Response {
    match response.and_then(worker::HttpResponse::try_from) {
        Ok(response) => response.map(axum::body::Body::new),
        Err(e) => error(e),
    }
}

/// axum decodes the path parameter, so the ID matches what the publisher uses.
///
/// The tokens `expired` and `forbidden` are rejected with 4401 and 4403, and the `user`
/// parameter becomes a socket tag, so tests can exercise authorization without a real
/// identity provider.
#[worker::send]
async fn connect(
    Extension(env): Extension<Env>,
    Path(id): Path<String>,
    req: axum::extract::Request,
) -> Response {
    match query_param(req.uri(), "token").as_deref() {
        Some("expired") => return from_worker(reject(close::UNAUTHORIZED, "token expired")),
        Some("forbidden") => return from_worker(reject(close::FORBIDDEN, "not allowed")),
        _ => {}
    }
    let mut connect = Connect::<Ticks>::new(id);
    if let Some(user) = query_param(req.uri(), "user") {
        connect = connect.tag(user);
    }
    if let Some(room) = query_param(req.uri(), "room") {
        connect = connect.object(room);
    }
    match connect.forward_http(&env, BINDING, req).await {
        Ok(response) => response.map(axum::body::Body::new),
        Err(e) => error(e),
    }
}

/// The publisher for the ticks channels. With a `room` query parameter, the channel is in
/// the object named by the room, with the room's other channels.
fn publisher(env: &Env, uri: &Uri) -> worker::Result<Publisher<Ticks>> {
    let publisher = Publisher::<Ticks>::new(env, BINDING)?;
    Ok(match query_param(uri, "room") {
        Some(room) => publisher.object(room),
        None => publisher,
    })
}

fn text<T: ToString>(result: worker::Result<T>) -> Response {
    match result {
        Ok(value) => value.to_string().into_response(),
        Err(e) => error(e),
    }
}

#[worker::send]
async fn publish(
    Extension(env): Extension<Env>,
    Path(id): Path<String>,
    uri: Uri,
    body: String,
) -> Response {
    let Ok(n) = body.trim().parse::<u64>() else {
        return (StatusCode::BAD_REQUEST, "expected a number").into_response();
    };
    text(async { publisher(&env, &uri)?.publish(&id, &n).await }.await)
}

/// The names of an object's partyline tables, comma-separated and sorted.
pub(crate) fn partyline_tables(sql: &worker::SqlStorage) -> worker::Result<String> {
    let mut names = Vec::new();
    for row in sql
        .exec(
            "SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'partyline%' ORDER BY name",
            None,
        )?
        .raw()
    {
        if let [worker::SqlStorageValue::String(name)] = row?.as_slice() {
            names.push(name.clone());
        }
    }
    Ok(names.join(","))
}

/// Sends a request to one of the fixture's own routes on an object, and returns the text
/// of a 200 response.
async fn call_object(
    env: &Env,
    binding: &str,
    object: &str,
    method: Method,
    path: &str,
    body: Option<String>,
) -> worker::Result<String> {
    let mut init = RequestInit::new();
    init.with_method(method);
    if let Some(body) = body {
        init.with_body(Some(body.into()));
    }
    let req = Request::new_with_init(&format!("{INTERNAL}{path}"), &init)?;
    let mut response = env
        .durable_object(binding)?
        .id_from_name(object)?
        .get_stub()?
        .fetch_with_request(req)
        .await?;
    let text = response.text().await?;
    if response.status_code() != 200 {
        return Err(worker::Error::RustError(text));
    }
    Ok(text)
}

/// Forwards the body to the object's `/batch` route, which publishes every number in it in
/// one turn.
#[worker::send]
async fn publish_batch(
    Extension(env): Extension<Env>,
    Path(id): Path<String>,
    body: String,
) -> Response {
    text(call_object(&env, BINDING, &id, Method::Post, "/batch", Some(body)).await)
}

#[worker::send]
async fn connect_legacy(
    Extension(env): Extension<Env>,
    Path(id): Path<String>,
    req: axum::extract::Request,
) -> Response {
    match Connect::<Ticks>::new(id)
        .forward_http(&env, LEGACY_BINDING, req)
        .await
    {
        Ok(response) => response.map(axum::body::Body::new),
        Err(e) => error(e),
    }
}

#[worker::send]
async fn publish_legacy(
    Extension(env): Extension<Env>,
    Path(id): Path<String>,
    body: String,
) -> Response {
    let Ok(n) = body.trim().parse::<u64>() else {
        return (StatusCode::BAD_REQUEST, "expected a number").into_response();
    };
    text(
        async {
            Publisher::<Ticks>::new(&env, LEGACY_BINDING)?
                .publish(&id, &n)
                .await
        }
        .await,
    )
}

#[worker::send]
async fn seed_legacy(Extension(env): Extension<Env>, Path(id): Path<String>, uri: Uri) -> Response {
    let path = match uri.query() {
        Some(query) => format!("/seed?{query}"),
        None => "/seed".to_owned(),
    };
    text(call_object(&env, LEGACY_BINDING, &id, Method::Post, &path, None).await)
}

#[worker::send]
async fn remigrate_legacy(Extension(env): Extension<Env>, Path(id): Path<String>) -> Response {
    text(call_object(&env, LEGACY_BINDING, &id, Method::Post, "/remigrate", None).await)
}

#[worker::send]
async fn legacy_head(Extension(env): Extension<Env>, Path(id): Path<String>) -> Response {
    text(
        async {
            Publisher::<Ticks>::new(&env, LEGACY_BINDING)?
                .head(&id)
                .await
        }
        .await,
    )
}

#[worker::send]
async fn legacy_tables(Extension(env): Extension<Env>, Path(id): Path<String>) -> Response {
    text(call_object(&env, LEGACY_BINDING, &id, Method::Get, "/tables", None).await)
}

/// Forwards an upgrade to a channel of type `C` in the room's object. The `room` query
/// parameter names the room, and `user` becomes a socket tag.
async fn connect_room<C: Channel>(env: &Env, id: String, req: axum::extract::Request) -> Response {
    let Some(room) = query_param(req.uri(), "room") else {
        return (StatusCode::BAD_REQUEST, "missing room").into_response();
    };
    let mut connect = Connect::<C>::new(id).object(room);
    if let Some(user) = query_param(req.uri(), "user") {
        connect = connect.tag(user);
    }
    match connect.forward_http(env, ROOM_BINDING, req).await {
        Ok(response) => response.map(axum::body::Body::new),
        Err(e) => error(e),
    }
}

#[worker::send]
async fn connect_room_ticks(
    Extension(env): Extension<Env>,
    Path(id): Path<String>,
    req: axum::extract::Request,
) -> Response {
    connect_room::<Ticks>(&env, id, req).await
}

#[worker::send]
async fn connect_room_views(
    Extension(env): Extension<Env>,
    Path(id): Path<String>,
    req: axum::extract::Request,
) -> Response {
    connect_room::<room::Views>(&env, id, req).await
}

#[worker::send]
async fn commit_room(
    Extension(env): Extension<Env>,
    Path(room): Path<String>,
    uri: Uri,
    body: String,
) -> Response {
    let path = match uri.query() {
        Some(query) => format!("/commit?{query}"),
        None => "/commit".to_owned(),
    };
    text(call_object(&env, ROOM_BINDING, &room, Method::Post, &path, Some(body)).await)
}

#[worker::send]
async fn close_in_room(
    Extension(env): Extension<Env>,
    Path(room): Path<String>,
    uri: Uri,
) -> Response {
    let Some(tag) = query_param(&uri, "tag") else {
        return (StatusCode::BAD_REQUEST, "missing tag").into_response();
    };
    let path = format!("/close?tag={}", encode_segment(&tag));
    text(call_object(&env, ROOM_BINDING, &room, Method::Post, &path, None).await)
}

#[worker::send]
async fn publish_room_view(
    Extension(env): Extension<Env>,
    Path((room, id)): Path<(String, String)>,
    body: String,
) -> Response {
    let Ok(n) = body.trim().parse::<u64>() else {
        return (StatusCode::BAD_REQUEST, "expected a number").into_response();
    };
    text(
        async {
            Publisher::<room::Views>::new(&env, ROOM_BINDING)?
                .object(room)
                .publish(&id, &n)
                .await
        }
        .await,
    )
}

#[worker::send]
async fn drop_room(Extension(env): Extension<Env>, Path(room): Path<String>) -> Response {
    text(call_object(&env, ROOM_BINDING, &room, Method::Post, "/drop", None).await)
}

#[worker::send]
async fn room_tables(Extension(env): Extension<Env>, Path(room): Path<String>) -> Response {
    text(call_object(&env, ROOM_BINDING, &room, Method::Get, "/tables", None).await)
}

#[worker::send]
async fn head(Extension(env): Extension<Env>, Path(id): Path<String>, uri: Uri) -> Response {
    text(async { publisher(&env, &uri)?.head(&id).await }.await)
}

#[worker::send]
async fn close_tagged(
    Extension(env): Extension<Env>,
    Path(id): Path<String>,
    req: axum::extract::Request,
) -> Response {
    let Some(tag) = query_param(req.uri(), "tag") else {
        return (StatusCode::BAD_REQUEST, "missing tag").into_response();
    };
    text(
        async {
            publisher(&env, req.uri())?
                .close_tagged(&id, &tag, close::FORBIDDEN)
                .await
        }
        .await,
    )
}

#[worker::send]
async fn reset(Extension(env): Extension<Env>, Path(id): Path<String>, uri: Uri) -> Response {
    text(async { publisher(&env, &uri)?.reset(&id).await }.await)
}

#[event(fetch)]
async fn fetch(
    req: HttpRequest,
    env: Env,
    _ctx: Context,
) -> worker::Result<axum::http::Response<axum::body::Body>> {
    let mut router = Router::new()
        .route("/partyline/ticks/{id}", get(connect))
        .route("/api/ticks/{id}", post(publish))
        .route("/api/ticks/{id}/batch", post(publish_batch))
        .route("/api/ticks/{id}/head", get(head))
        .route("/api/ticks/{id}/close", post(close_tagged))
        .route("/api/ticks/{id}/reset", post(reset))
        .route("/partyline/legacy/{id}", get(connect_legacy))
        .route("/api/legacy/{id}", post(publish_legacy))
        .route("/api/legacy/{id}/seed", post(seed_legacy))
        .route("/api/legacy/{id}/remigrate", post(remigrate_legacy))
        .route("/api/legacy/{id}/head", get(legacy_head))
        .route("/api/legacy/{id}/tables", get(legacy_tables))
        .route("/partyline/room-ticks/{id}", get(connect_room_ticks))
        .route("/partyline/room-views/{id}", get(connect_room_views))
        .route("/api/rooms/{room}/commit", post(commit_room))
        .route("/api/rooms/{room}/close", post(close_in_room))
        .route("/api/rooms/{room}/views/{id}", post(publish_room_view))
        .route("/api/rooms/{room}/drop", post(drop_room))
        .route("/api/rooms/{room}/tables", get(room_tables))
        .layer(Extension(env));
    Ok(router.call(req).await?)
}
