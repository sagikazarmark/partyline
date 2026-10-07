//! A Worker for end-to-end tests.
//!
//! - It routes through an axum `Router`, to check that `Connect::forward_http` passes
//!   WebSocket upgrades through axum on Workers.
//! - Its channel keeps at most 5 events for at most 3 seconds, to check count-based
//!   trimming and the alarm that runs age-based trimming in SQLite.
//! - It holds the hooks that only tests need: magic tokens, socket tags from a query
//!   parameter, and routes to close sockets by tag and to reset the channel. The examples
//!   stay free of them.
//!
//! | Route | Action |
//! | --- | --- |
//! | `GET /partyline/ticks/{id}` | WebSocket upgrade |
//! | `POST /api/ticks/{id}` | Publish the number in the body |
//! | `GET /api/ticks/{id}/head` | The channel head |
//! | `POST /api/ticks/{id}/close?tag={user}` | Close a user's sockets with 4403 |
//! | `POST /api/ticks/{id}/reset` | Wipe the channel log with a new epoch |

use std::time::Duration;

use axum::Router;
use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use partyline::{Channel, Mode};
use partyline_worker::{Connect, HubConfig, Publisher, close, decode_segment, reject};
use tower_service::Service;
use worker::{Context, Env, HttpRequest, event};

/// A channel of numbers.
pub struct Ticks;

impl Channel for Ticks {
    const NAME: &'static str = "ticks";
    const MODE: Mode = Mode::Log;
    type Event = u64;
}

const BINDING: &str = "TICK_CHANNEL";

partyline_worker::channel_object! {
    /// A channel with a tiny retention window.
    pub struct TickChannel: Ticks {
        config = HubConfig::default()
            .retain_events(5)
            .retain_for(Some(Duration::from_secs(3)));
    }
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
    match connect.forward_http(&env, BINDING, req).await {
        Ok(response) => response.map(axum::body::Body::new),
        Err(e) => error(e),
    }
}

fn text<T: ToString>(result: worker::Result<T>) -> Response {
    match result {
        Ok(value) => value.to_string().into_response(),
        Err(e) => error(e),
    }
}

#[worker::send]
async fn publish(Extension(env): Extension<Env>, Path(id): Path<String>, body: String) -> Response {
    let Ok(n) = body.trim().parse::<u64>() else {
        return (StatusCode::BAD_REQUEST, "expected a number").into_response();
    };
    text(
        async {
            Publisher::<Ticks>::new(&env, BINDING)?
                .publish(&id, &n)
                .await
        }
        .await,
    )
}

#[worker::send]
async fn head(Extension(env): Extension<Env>, Path(id): Path<String>) -> Response {
    text(async { Publisher::<Ticks>::new(&env, BINDING)?.head(&id).await }.await)
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
            Publisher::<Ticks>::new(&env, BINDING)?
                .close_tagged(&id, &tag, close::FORBIDDEN)
                .await
        }
        .await,
    )
}

#[worker::send]
async fn reset(Extension(env): Extension<Env>, Path(id): Path<String>) -> Response {
    text(async { Publisher::<Ticks>::new(&env, BINDING)?.reset(&id).await }.await)
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
        .route("/api/ticks/{id}/head", get(head))
        .route("/api/ticks/{id}/close", post(close_tagged))
        .route("/api/ticks/{id}/reset", post(reset))
        .layer(Extension(env));
    Ok(router.call(req).await?)
}
