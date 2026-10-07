//! The partyline client: connects the [`partyline`] state machine to a real socket and real
//! timers.
//!
//! The crate is runtime-agnostic. [`connect`] returns a driver future and never spawns, so
//! Dioxus, tokio, or `wasm-bindgen-futures` can run it. On native targets the driver must
//! run on a tokio runtime, because the socket uses tokio I/O.
//!
//! ```no_run
//! # use partyline::{Channel, Mode};
//! # #[derive(Clone, serde::Serialize, serde::Deserialize)] struct OrderEvent;
//! # struct Orders;
//! # impl Channel for Orders { const NAME: &'static str = "orders"; const MODE: Mode = Mode::Log; type Event = OrderEvent; }
//! # async fn example(head: partyline::Cursor) {
//! use futures::StreamExt;
//! use partyline_client::{BaseUrl, ClientEvent, ConnectOptions};
//!
//! let (handle, mut events, driver) = partyline_client::connect::<Orders>(ConnectOptions {
//!     since: Some(head),
//!     ..ConnectOptions::new(BaseUrl::Explicit("https://example.com".into()), "order-1")
//! });
//! # fn spawn(_: impl std::future::Future<Output = ()>) {}
//! spawn(driver); // the caller chooses the executor
//!
//! while let Some(message) = events.next().await {
//!     match message {
//!         ClientEvent::Event { event, .. } => { /* apply(event) */ }
//!         ClientEvent::Reset => { /* refetch().await */ }
//!         ClientEvent::Status(status) => { /* show(status) */ }
//!     }
//! }
//! handle.stop();
//! # }
//! ```
//!
//! partyline is not affiliated with Cloudflare or PartyKit, and is not wire-compatible with
//! PartyServer or partysocket.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod driver;
mod transport;

#[cfg(target_arch = "wasm32")]
mod browser;
#[cfg(not(target_arch = "wasm32"))]
mod native;
#[cfg(all(target_arch = "wasm32", feature = "web-wake"))]
mod wake;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use futures::Stream;
use futures::channel::mpsc;

pub use partyline::{self, Channel, ClientConfig, Cursor, Status};
pub use transport::{DefaultTransport, Transport, TransportError};

#[cfg(target_arch = "wasm32")]
pub use browser::{BrowserConn, BrowserSocket};
#[cfg(not(target_arch = "wasm32"))]
pub use native::{NativeConn, NativeSocket};

/// `Send` on native targets, where drivers may run on a multi-threaded runtime. Every type
/// on wasm32, where nothing is `Send`.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSend: Send {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Send + ?Sized> MaybeSend for T {}

/// `Send` on native targets, where drivers may run on a multi-threaded runtime. Every type
/// on wasm32, where nothing is `Send`.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSend {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSend for T {}

/// `Sync` on native targets. Every type on wasm32.
#[cfg(not(target_arch = "wasm32"))]
pub trait MaybeSync: Sync {}
#[cfg(not(target_arch = "wasm32"))]
impl<T: Sync + ?Sized> MaybeSync for T {}

/// `Sync` on native targets. Every type on wasm32.
#[cfg(target_arch = "wasm32")]
pub trait MaybeSync {}
#[cfg(target_arch = "wasm32")]
impl<T: ?Sized> MaybeSync for T {}

#[cfg(not(target_arch = "wasm32"))]
type BoxFuture<T> = futures::future::BoxFuture<'static, T>;
#[cfg(target_arch = "wasm32")]
type BoxFuture<T> = futures::future::LocalBoxFuture<'static, T>;

#[cfg(not(target_arch = "wasm32"))]
type TokenFn = dyn Fn(TokenRequest) -> BoxFuture<Option<String>> + Send + Sync;
#[cfg(target_arch = "wasm32")]
type TokenFn = dyn Fn(TokenRequest) -> BoxFuture<Option<String>>;

/// What the driver asks the token provider for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct TokenRequest {
    /// The server rejected the last token with close code 4401. Skip any cached token.
    pub refresh: bool,
}

/// An async function called before every connect. Its result goes in the `token` query
/// parameter, because browsers cannot set headers on a WebSocket.
///
/// With Clerk, call `getToken()` here, and skip its cache when [`TokenRequest::refresh`]
/// is set. The session token is short-lived, so it does the job of a ticket.
#[derive(Clone)]
pub struct TokenProvider(Arc<TokenFn>);

impl TokenProvider {
    /// Wraps an async function.
    pub fn new<F, Fut>(f: F) -> Self
    where
        F: Fn(TokenRequest) -> Fut + MaybeSend + MaybeSync + 'static,
        Fut: Future<Output = Option<String>> + MaybeSend + 'static,
    {
        Self(Arc::new(move |request| Box::pin(f(request))))
    }

    /// Asks for a token.
    pub async fn get(&self, request: TokenRequest) -> Option<String> {
        (self.0)(request).await
    }
}

impl fmt::Debug for TokenProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TokenProvider")
    }
}

impl PartialEq for TokenProvider {
    fn eq(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0, &other.0)
    }
}

/// Where the server is.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub enum BaseUrl {
    /// The page's origin. Web only.
    #[default]
    SameOrigin,
    /// An explicit origin, such as `https://example.com`. `http` and `https` map to `ws`
    /// and `wss`. Required on native targets.
    Explicit(String),
}

impl BaseUrl {
    /// Resolves to a `ws://` or `wss://` origin with no trailing slash.
    pub fn resolve(&self) -> Result<String, TransportError> {
        let url = match self {
            BaseUrl::Explicit(url) => url.clone(),
            BaseUrl::SameOrigin => same_origin()?,
        };
        let url = url.trim_end_matches('/');
        let url = if let Some(rest) = url.strip_prefix("https://") {
            format!("wss://{rest}")
        } else if let Some(rest) = url.strip_prefix("http://") {
            format!("ws://{rest}")
        } else if url.starts_with("ws://") || url.starts_with("wss://") {
            url.to_owned()
        } else {
            return Err(TransportError::Connect(format!(
                "base URL {url:?} must start with http://, https://, ws://, or wss://"
            )));
        };
        Ok(url)
    }
}

#[cfg(target_arch = "wasm32")]
fn same_origin() -> Result<String, TransportError> {
    web_sys::window()
        .and_then(|w| w.location().origin().ok())
        .ok_or_else(|| TransportError::Connect("no window location".to_owned()))
}

#[cfg(not(target_arch = "wasm32"))]
fn same_origin() -> Result<String, TransportError> {
    Err(TransportError::Connect(
        "BaseUrl::SameOrigin is only available in a browser; use BaseUrl::Explicit".to_owned(),
    ))
}

/// Connection settings.
#[derive(Clone, Debug, PartialEq)]
pub struct ConnectOptions {
    /// Where the server is.
    pub base_url: BaseUrl,
    /// The channel ID, for example the order ID.
    pub id: String,
    /// The cursor to resume after: usually the head returned with the initial state.
    /// `None` means live events only.
    pub since: Option<Cursor>,
    /// Called before every connect.
    pub token: Option<TokenProvider>,
    /// Timing settings.
    pub config: ClientConfig,
}

impl ConnectOptions {
    /// Options with no cursor, no token provider, and the default config.
    pub fn new(base_url: BaseUrl, id: impl Into<String>) -> Self {
        Self {
            base_url,
            id: id.into(),
            since: None,
            token: None,
            config: ClientConfig::default(),
        }
    }
}

/// A message for the app.
#[derive(Clone, Debug, PartialEq)]
pub enum ClientEvent<E> {
    /// An event, delivered once and in order.
    Event {
        /// The sequence number.
        seq: u64,
        /// The event.
        event: E,
    },
    /// The cursor could not be resumed. Discard local state and refetch.
    Reset,
    /// The connection status changed.
    Status(Status),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Command {
    Wake,
    Stop,
}

#[derive(Clone, Debug)]
pub(crate) struct Shared(Arc<Mutex<(Status, Option<Cursor>)>>);

impl Shared {
    fn new(since: Option<Cursor>) -> Self {
        Self(Arc::new(Mutex::new((Status::Idle, since))))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, (Status, Option<Cursor>)> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    pub(crate) fn set_status(&self, status: Status) {
        self.lock().0 = status;
    }

    pub(crate) fn set_cursor(&self, cursor: Option<Cursor>) {
        self.lock().1 = cursor;
    }
}

/// Controls a running driver. Clones control the same driver. When every handle is
/// dropped, the driver stops.
#[derive(Clone, Debug)]
pub struct Handle {
    commands: mpsc::UnboundedSender<Command>,
    shared: Shared,
}

impl Handle {
    /// Connects now if waiting to reconnect, or probes an open socket with a ping.
    pub fn wake(&self) {
        let _ = self.commands.unbounded_send(Command::Wake);
    }

    /// Closes the socket with 1000 and ends the driver.
    pub fn stop(&self) {
        let _ = self.commands.unbounded_send(Command::Stop);
    }

    /// The current status.
    pub fn status(&self) -> Status {
        self.shared.lock().0
    }

    /// The cursor of the last event delivered. Pass it as `since` to resume later.
    pub fn cursor(&self) -> Option<Cursor> {
        self.shared.lock().1
    }
}

/// The stream of [`ClientEvent`]s. It ends when the driver ends.
pub struct Events<E> {
    receiver: mpsc::UnboundedReceiver<ClientEvent<E>>,
}

impl<E> fmt::Debug for Events<E> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("Events")
    }
}

impl<E> Stream for Events<E> {
    type Item = ClientEvent<E>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Pin::new(&mut self.receiver).poll_next(cx)
    }
}

/// Connects to channel `C` with the default transport for the target.
///
/// Returns a control handle, the event stream, and the driver future. Nothing happens until
/// the driver runs. The driver ends after [`Handle::stop`], after a terminal close code,
/// or when every handle is dropped.
pub fn connect<C: Channel>(
    options: ConnectOptions,
) -> (Handle, Events<C::Event>, impl Future<Output = ()>) {
    connect_with::<C, _>(DefaultTransport::default(), options)
}

/// Connects to channel `C` with a custom transport.
pub fn connect_with<C: Channel, T: Transport>(
    transport: T,
    options: ConnectOptions,
) -> (Handle, Events<C::Event>, impl Future<Output = ()>) {
    let (command_tx, command_rx) = mpsc::unbounded();
    let (event_tx, event_rx) = mpsc::unbounded();
    let shared = Shared::new(options.since);
    let handle = Handle {
        commands: command_tx,
        shared: shared.clone(),
    };
    let driver = driver::run::<C, T>(transport, options, command_rx, event_tx, shared);
    (handle, Events { receiver: event_rx }, driver)
}

/// A random source for backoff jitter.
#[cfg(target_arch = "wasm32")]
pub(crate) fn random() -> impl FnMut() -> f64 + Send + 'static {
    || js_sys::Math::random()
}

/// A random source for backoff jitter.
#[cfg(not(target_arch = "wasm32"))]
pub(crate) fn random() -> impl FnMut() -> f64 + Send + 'static {
    use std::hash::{BuildHasher, Hasher};
    let mut state = std::collections::hash_map::RandomState::new()
        .build_hasher()
        .finish();
    move || {
        // SplitMix64.
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^= z >> 31;
        (z >> 11) as f64 / (1u64 << 53) as f64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn base_url_resolves_to_websocket_schemes() {
        let resolve = |s: &str| BaseUrl::Explicit(s.to_owned()).resolve();
        assert_eq!(resolve("https://a.dev/").unwrap(), "wss://a.dev");
        assert_eq!(
            resolve("http://localhost:8787").unwrap(),
            "ws://localhost:8787"
        );
        assert_eq!(resolve("wss://a.dev").unwrap(), "wss://a.dev");
        assert!(resolve("a.dev").is_err());
        assert!(BaseUrl::SameOrigin.resolve().is_err());
    }

    #[test]
    fn random_is_in_range() {
        let mut rng = random();
        for _ in 0..1000 {
            let x = rng();
            assert!((0.0..1.0).contains(&x));
        }
    }
}
