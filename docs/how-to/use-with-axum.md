# How to use partyline with axum

This guide shows how to route partyline upgrades and publishes through an [axum](https://docs.rs/axum) router on Cloudflare Workers.
The end-to-end test Worker in [`e2e/fixture`](../../e2e/fixture) is a complete example.

partyline depends on no web framework. `Connect` takes the request type your Worker already has, and `Connect::forward_http` takes an `http::Request`.

## 1. Turn on the `http` features

```shell
cargo add axum --no-default-features
cargo add partyline-worker --features http
cargo add tower-service
cargo add worker --features http,axum
```

The `http` feature of `partyline-worker` adds `Connect::forward_http`.

## 2. Pass the environment to the handlers

axum handlers cannot take `Env` as an argument. Put it in an `Extension` layer:

```rust
use axum::Router;
use axum::routing::{get, post};
use tower_service::Service;
use worker::{Context, Env, HttpRequest, event};

#[event(fetch)]
async fn fetch(
    req: HttpRequest,
    env: Env,
    _ctx: Context,
) -> worker::Result<axum::http::Response<axum::body::Body>> {
    let mut router = Router::new()
        .route("/partyline/orders/{id}", get(connect))
        .route("/api/orders/{id}/events", post(publish))
        .layer(axum::Extension(env));
    Ok(router.call(req).await?)
}
```

## 3. Forward upgrades

```rust
use axum::extract::{Extension, Path};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use partyline_worker::{Connect, close, reject};

#[worker::send]
async fn connect(
    Extension(env): Extension<Env>,
    Path(id): Path<String>,
    req: axum::extract::Request,
) -> Response {
    // Authorize here. To refuse, convert a `reject` response for axum.
    if !allowed(&req) {
        return from_worker(reject(close::FORBIDDEN, "not allowed"));
    }
    match Connect::<Orders>::new(id).forward_http(&env, "ORDER_CHANNEL", req).await {
        Ok(response) => response.map(axum::body::Body::new),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}

/// Converts a `worker` response, such as one from `reject`, for axum.
fn from_worker(response: worker::Result<worker::Response>) -> Response {
    match response.and_then(worker::HttpResponse::try_from) {
        Ok(response) => response.map(axum::body::Body::new),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
```

Three details:

- **`#[worker::send]`.** axum handlers must be `Send`, and `worker` types are not. The attribute marks the handler's future as `Send`. A Worker runs on one thread, so this is safe.
- **The ID is already decoded.** axum's `Path` extractor percent-decodes the segment. Pass it to `Connect` and `Publisher` as it is. With `worker::Router`, decode it yourself with `decode_segment`.
- **The socket survives the conversion.** `forward_http` returns a `101` response that carries the WebSocket in its extensions. Return it unchanged.

## 4. Publish

```rust
use partyline_worker::Publisher;

#[worker::send]
async fn publish(
    Extension(env): Extension<Env>,
    Path(id): Path<String>,
    axum::Json(event): axum::Json<OrderEvent>,
) -> Response {
    let result = async {
        Publisher::<Orders>::new(&env, "ORDER_CHANNEL")?.publish(&id, &event).await
    }
    .await;
    match result {
        Ok(head) => head.to_string().into_response(),
        Err(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into_response(),
    }
}
```

`axum::Json` needs axum's `json` feature.

## Related

- [M0 spike notes](../design/m0-spike.md): the axum findings
- [Authenticate with Clerk](authenticate-with-clerk.md)
