//! Helpers for the Worker: forward a client upgrade, publish, read the head, close sockets.

use std::marker::PhantomData;

use partyline::frame::{InvalidId, encode_segment, match_connect_path};
use partyline::{Channel, Cursor, codec};
use worker::{Env, Method, ObjectNamespace, Request, RequestInit, Response, Stub};

use crate::hub::{MAX_TAGS, TAGS_HEADER, check_tag, close_reason, is_upgrade};

/// The base URL of internal requests to the hub. The host is never resolved.
const INTERNAL: &str = "https://partyline.internal";

/// Maps a channel ID to its Durable Object. One channel ID is one Durable Object.
///
/// This is the one place to change when channels are sharded.
fn stub(namespace: &ObjectNamespace, id: &str) -> worker::Result<Stub> {
    namespace.id_from_name(id)?.get_stub()
}

/// Forwards a client's WebSocket upgrade to the channel's Durable Object.
///
/// The route handler parses the path and authorizes the request first, then calls this:
///
/// ```ignore
/// let response = Connect::<Orders>::new(&order_id)
///     .tag(&user_id) // optional, from the app's auth layer
///     .forward(&env, "ORDER_CHANNEL", req)
///     .await?;
/// ```
///
/// `Connect` refuses any request that is not a WebSocket upgrade with HTTP 426, so the
/// hub's internal routes are never reachable by clients.
#[derive(Clone, Debug)]
pub struct Connect<C: Channel> {
    id: String,
    tags: Vec<String>,
    _channel: PhantomData<fn() -> C>,
}

impl<C: Channel> Connect<C> {
    /// Targets the channel with this ID.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            tags: Vec::new(),
            _channel: PhantomData,
        }
    }

    /// Matches a request path against this channel's connect path,
    /// `/partyline/{C::NAME}/{id}`, and targets the decoded ID.
    ///
    /// Returns `None` when the path is another route, and `Some(Err(_))` when the ID is not
    /// valid percent-encoding. Use it in a Worker without a router; with a router, decode
    /// the route parameter with [`crate::decode_segment`].
    ///
    /// ```ignore
    /// match Connect::<Orders>::from_path(&req.path()) {
    ///     Some(Ok(connect)) => connect.forward(&env, "ORDER_CHANNEL", req).await,
    ///     Some(Err(_)) => reject(close::BAD_REQUEST, "invalid id"),
    ///     None => other_routes(req, env).await,
    /// }
    /// ```
    pub fn from_path(path: &str) -> Option<Result<Self, InvalidId>> {
        match_connect_path(C::NAME, path).map(|id| id.map(Self::new))
    }

    /// Adds a socket tag, such as the user ID. [`Publisher::close_tagged`] closes sockets by tag.
    ///
    /// A socket carries at most 10 tags. Each must be non-empty and at most 256 characters.
    /// [`Connect::forward`] fails if a tag breaks these rules.
    pub fn tag(mut self, tag: impl Into<String>) -> Self {
        self.tags.push(tag.into());
        self
    }

    /// Forwards the upgrade request and returns the Durable Object's `101` response.
    ///
    /// The `token` query parameter is removed first. The Worker has already checked it, and
    /// it would otherwise show up in the Durable Object's request logs.
    pub async fn forward(self, env: &Env, binding: &str, req: Request) -> worker::Result<Response> {
        if !is_upgrade(&req) {
            return Response::error("Expected a WebSocket upgrade", 426);
        }
        if self.tags.len() > MAX_TAGS {
            return Err(worker::Error::RustError(format!(
                "a socket carries at most {MAX_TAGS} tags, got {}",
                self.tags.len()
            )));
        }
        for tag in &self.tags {
            check_tag(tag).map_err(worker::Error::RustError)?;
        }
        // Copy the headers, replacing any tags header the client sent.
        let headers = req.headers().clone();
        headers.delete(TAGS_HEADER)?;
        if !self.tags.is_empty() {
            let tags = serde_json::to_string(&self.tags)
                .map_err(|e| worker::Error::RustError(e.to_string()))?;
            // Header values must be ASCII, and tags are often names.
            headers.set(TAGS_HEADER, &encode_segment(&tags))?;
        }
        let mut url = req.url()?;
        strip_token(&mut url);
        let mut init = RequestInit::new();
        init.with_method(Method::Get).with_headers(headers);
        let forwarded = Request::new_with_init(url.as_str(), &init)?;
        let namespace = env.durable_object(binding)?;
        stub(&namespace, &self.id)?
            .fetch_with_request(forwarded)
            .await
    }

    /// Like [`Connect::forward`], for Workers that use `http::Request`, such as axum on Workers.
    #[cfg(feature = "http")]
    pub async fn forward_http<B>(
        self,
        env: &Env,
        binding: &str,
        req: http::Request<B>,
    ) -> worker::Result<worker::HttpResponse>
    where
        B: http_body::Body<Data = bytes::Bytes> + 'static,
    {
        let req = Request::try_from(req)?;
        let response = self.forward(env, binding, req).await?;
        worker::HttpResponse::try_from(response)
    }
}

/// Publishes events and manages a channel from any Worker code path.
///
/// ```ignore
/// let publisher = Publisher::<Orders>::new(&env, "ORDER_CHANNEL")?;
/// let cursor = publisher.publish(&order_id, &OrderEvent::StatusChanged { status }).await?;
/// let head = publisher.head(&order_id).await?;
/// ```
#[derive(Debug)]
pub struct Publisher<C: Channel> {
    namespace: ObjectNamespace,
    _channel: PhantomData<fn() -> C>,
}

impl<C: Channel> Publisher<C> {
    /// Uses the Durable Object namespace bound as `binding`.
    pub fn new(env: &Env, binding: &str) -> worker::Result<Self> {
        Ok(Self {
            namespace: env.durable_object(binding)?,
            _channel: PhantomData,
        })
    }

    /// Publishes an event to the channel with this ID and returns the new head.
    pub async fn publish(&self, id: &str, event: &C::Event) -> worker::Result<Cursor> {
        let body =
            codec::encode_event(event).map_err(|e| worker::Error::RustError(e.to_string()))?;
        let text = self.call(id, Method::Post, "/publish", Some(body)).await?;
        parse_cursor(&text)
    }

    /// Returns the head of the channel with this ID. Return it with the initial state, so
    /// the client can connect with `since` set to it.
    pub async fn head(&self, id: &str) -> worker::Result<Cursor> {
        let text = self.call(id, Method::Get, "/head", None).await?;
        parse_cursor(&text)
    }

    /// Closes every socket on the channel that carries `tag`, using `code`, and returns how
    /// many were closed. Use [`partyline::close::FORBIDDEN`] on sign-out, so the client does
    /// not reconnect.
    pub async fn close_tagged(&self, id: &str, tag: &str, code: u16) -> worker::Result<usize> {
        let mut url = worker::Url::parse(INTERNAL).expect("valid URL");
        url.set_path("/close");
        url.query_pairs_mut()
            .append_pair("tag", tag)
            .append_pair("code", &code.to_string());
        let path = format!("/close?{}", url.query().unwrap_or(""));
        let text = self.call(id, Method::Post, &path, None).await?;
        text.trim()
            .parse()
            .map_err(|_| worker::Error::RustError(format!("unexpected response: {text}")))
    }

    /// Wipes the channel's log and starts a new epoch. Connected clients reconnect; Log
    /// clients receive `Reset`.
    pub async fn reset(&self, id: &str) -> worker::Result<Cursor> {
        let text = self.call(id, Method::Post, "/reset", None).await?;
        parse_cursor(&text)
    }

    async fn call(
        &self,
        id: &str,
        method: Method,
        path: &str,
        body: Option<Vec<u8>>,
    ) -> worker::Result<String> {
        let mut init = RequestInit::new();
        init.with_method(method);
        if let Some(body) = body {
            let array = worker::js_sys::Uint8Array::from(body.as_slice());
            init.with_body(Some(array.into()));
        }
        let req = Request::new_with_init(&format!("{INTERNAL}{path}"), &init)?;
        let mut response = stub(&self.namespace, id)?.fetch_with_request(req).await?;
        let text = response.text().await?;
        if response.status_code() != 200 {
            return Err(worker::Error::RustError(format!(
                "partyline hub returned {}: {text}",
                response.status_code()
            )));
        }
        Ok(text)
    }
}

/// Removes the `token` query parameter, keeping the others exactly as encoded.
fn strip_token(url: &mut worker::Url) {
    let Some(query) = url.query() else {
        return;
    };
    let kept: Vec<&str> = query
        .split('&')
        .filter(|pair| pair.split_once('=').map_or(*pair, |(key, _)| key) != "token")
        .collect();
    let kept = kept.join("&");
    url.set_query((!kept.is_empty()).then_some(kept.as_str()));
}

fn parse_cursor(text: &str) -> worker::Result<Cursor> {
    text.trim()
        .parse()
        .map_err(|e: partyline::frame::ParseCursorError| worker::Error::RustError(e.to_string()))
}

/// Rejects a WebSocket upgrade with a close code, such as [`partyline::close::UNAUTHORIZED`]
/// or [`partyline::close::FORBIDDEN`].
///
/// Browsers cannot read the HTTP status of a failed upgrade, so the route handler accepts
/// the socket and closes it at once. The client reads the close code and decides whether
/// to reconnect.
///
/// ```ignore
/// if !authorized {
///     return partyline_worker::reject(close::FORBIDDEN, "not allowed");
/// }
/// ```
pub fn reject(code: u16, reason: &str) -> worker::Result<Response> {
    let pair = worker::WebSocketPair::new()?;
    pair.server.accept()?;
    pair.server.close(Some(code), Some(close_reason(reason)))?;
    Response::from_websocket(pair.client)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strip_token_keeps_the_other_parameters() {
        let strip = |s: &str| {
            let mut url = worker::Url::parse(s).unwrap();
            strip_token(&mut url);
            url.to_string()
        };
        assert_eq!(
            strip("https://a/partyline/c/1?v=1&token=secret&cursor=2.3"),
            "https://a/partyline/c/1?v=1&cursor=2.3"
        );
        assert_eq!(
            strip("https://a/partyline/c/1?token=x"),
            "https://a/partyline/c/1"
        );
        assert_eq!(strip("https://a/p?v=1&x=a%20b"), "https://a/p?v=1&x=a%20b");
        assert_eq!(strip("https://a/p"), "https://a/p");
    }
}
