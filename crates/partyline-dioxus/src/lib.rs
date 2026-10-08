//! Dioxus hooks for partyline.
//!
//! A thin set of hooks over [`partyline_client`]. The hooks own the driver's lifetime,
//! expose status and cursor as signals, and stay inert during server-side rendering.
//!
//! | Hook | Use it for | Returns |
//! | --- | --- | --- |
//! | [`use_channel`] | Any channel. Full control over events and resets | [`UseChannel`] |
//! | [`use_channel_latest`] | Latest-mode channels | The latest value, plus [`UseChannel`] |
//! | [`use_channel_reducer`] | Log-mode channels that fold events into state | The state, plus [`UseChannel`] |
//!
//! ```ignore
//! #[component]
//! fn OrderStatus(id: String, initial: Order, head: Cursor) -> Element {
//!     let mut order = use_signal(|| initial);
//!
//!     let channel = use_channel::<Orders>(
//!         ChannelOptions::new(id.clone()).since(head),
//!         move |msg| match msg {
//!             ChannelMessage::Event(event) => order.write().apply(&event),
//!             ChannelMessage::Reset => { /* refetch the order, then order.set(..) */ }
//!         },
//!     );
//!
//!     rsx! {
//!         if channel.status() != Status::Open {
//!             span { class: "badge", "Reconnecting" }
//!         }
//!         OrderView { order }
//!     }
//! }
//! ```
//!
//! # Lifecycle
//!
//! - **Start.** The driver starts from an effect, which does not run during server-side
//!   rendering. On the server the hook returns [`Status::Idle`] and opens nothing.
//! - **Stop.** On unmount the hook stops the driver, which closes the socket with 1000.
//! - **Change.** When the channel ID changes, the hook stops the old driver and starts a new
//!   one from the new options' `since`. When the effective base URL or client config
//!   changes, or after [`UseChannel::reconnect`], it starts a new one from the current
//!   cursor. A new handler or token provider does not restart the driver: the latest one
//!   is called.
//! - **Disable.** [`ChannelOptions::enabled`] set to `false` stops the driver and keeps the
//!   cursor. Enabling it again resumes from the cursor.
//! - **Wake.** On wasm, the `web-wake` feature of the client crate is on.
//!
//! partyline is not affiliated with Cloudflare or PartyKit, and is not wire-compatible with
//! PartyServer or partysocket.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::{Arc, Mutex, PoisonError};

use dioxus::prelude::*;
use futures::StreamExt;
use partyline_client::{ClientEvent, ConnectOptions, Handle};

pub use partyline::{Channel, ClientConfig, Cursor, Mode, Status, StopReason};
pub use partyline_client::{BaseUrl, TokenProvider, TokenRequest};

/// Settings shared by every hook below a [`PartylineProvider`].
#[derive(Clone, Debug, Default, PartialEq)]
pub struct PartylineSettings {
    /// Where the server is. Default: the page origin.
    pub base_url: Option<BaseUrl>,
    /// Called before every connect.
    pub token: Option<TokenProvider>,
    /// Client timing settings.
    pub config: Option<ClientConfig>,
}

/// Puts shared settings in context for every partyline hook below it.
///
/// On web the base URL defaults to the page origin, so most apps configure nothing.
#[component]
pub fn PartylineProvider(
    /// Where the server is. Default: the page origin.
    base_url: Option<BaseUrl>,
    /// Called before every connect.
    token: Option<TokenProvider>,
    /// Client timing settings.
    config: Option<ClientConfig>,
    /// The app.
    children: Element,
) -> Element {
    let settings = PartylineSettings {
        base_url,
        token,
        config,
    };
    let mut context = use_context_provider(|| Signal::new(settings.clone()));
    if *context.peek() != settings {
        context.set(settings);
    }
    children
}

/// Options for one channel subscription.
#[derive(Clone, Debug, PartialEq)]
pub struct ChannelOptions {
    id: String,
    since: Option<Cursor>,
    base_url: Option<BaseUrl>,
    token: Option<TokenProvider>,
    config: Option<ClientConfig>,
    enabled: bool,
}

impl ChannelOptions {
    /// Subscribes to the channel with this ID, with live events only.
    pub fn new(id: impl Into<String>) -> Self {
        Self {
            id: id.into(),
            since: None,
            base_url: None,
            token: None,
            config: None,
            enabled: true,
        }
    }

    /// Resumes after this cursor: usually the head returned with the initial state.
    pub fn since(mut self, cursor: impl Into<Option<Cursor>>) -> Self {
        self.since = cursor.into();
        self
    }

    /// Overrides the provider's base URL.
    pub fn base_url(mut self, base_url: BaseUrl) -> Self {
        self.base_url = Some(base_url);
        self
    }

    /// Overrides the provider's token provider.
    pub fn token(mut self, token: TokenProvider) -> Self {
        self.token = Some(token);
        self
    }

    /// Overrides the provider's client config.
    pub fn config(mut self, config: ClientConfig) -> Self {
        self.config = Some(config);
        self
    }

    /// Connects only while `enabled` is `true`. Default: `true`.
    ///
    /// Disabling stops the driver, keeps the cursor, and sets the status to
    /// `Stopped { reason: StopReason::App }`. Enabling again resumes from the cursor, so the
    /// app receives exactly the events it missed.
    pub fn enabled(mut self, enabled: bool) -> Self {
        self.enabled = enabled;
        self
    }

    /// The channel ID.
    pub fn id(&self) -> &str {
        &self.id
    }
}

/// What [`use_channel`] delivers to the app.
#[derive(Clone, Debug, PartialEq)]
pub enum ChannelMessage<E> {
    /// An event, delivered once and in order.
    Event(E),
    /// The cursor could not be resumed. Discard local state and refetch.
    Reset,
}

/// What the driver is started with. A change restarts it.
#[derive(Clone, PartialEq)]
struct Target {
    id: String,
    base_url: BaseUrl,
    config: ClientConfig,
    enabled: bool,
}

#[derive(Default)]
struct Driver {
    handle: Option<Handle>,
    generation: u64,
    id: Option<String>,
}

impl Driver {
    fn stop(&mut self) {
        if let Some(handle) = self.handle.take() {
            handle.stop();
        }
    }
}

/// A channel subscription: status and cursor as signals, and controls.
#[derive(Clone, Copy, PartialEq)]
pub struct UseChannel {
    status: Signal<Status>,
    cursor: Signal<Option<Cursor>>,
    restart: Signal<u64>,
    driver: CopyValue<Rc<RefCell<Driver>>>,
}

impl std::fmt::Debug for UseChannel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UseChannel")
            .field("status", &*self.status.peek())
            .field("cursor", &*self.cursor.peek())
            .finish()
    }
}

impl UseChannel {
    /// The connection status. Reading it subscribes the component.
    pub fn status(&self) -> Status {
        (self.status)()
    }

    /// The cursor of the last event delivered. Reading it subscribes the component.
    pub fn cursor(&self) -> Option<Cursor> {
        (self.cursor)()
    }

    /// The status as a read-only signal.
    pub fn status_signal(&self) -> ReadSignal<Status> {
        ReadSignal::new(self.status)
    }

    /// The cursor as a read-only signal.
    pub fn cursor_signal(&self) -> ReadSignal<Option<Cursor>> {
        ReadSignal::new(self.cursor)
    }

    /// Connects now if waiting to reconnect, or probes an open socket with a ping.
    pub fn wake(&self) {
        if let Some(handle) = &self.driver.read().borrow().handle {
            handle.wake();
        }
    }

    /// Stops the driver and starts a new one from the current cursor. It also restarts a
    /// driver that stopped for good, for example after [`StopReason::Closed`]. It does
    /// nothing while the subscription is disabled.
    pub fn reconnect(&self) {
        let mut restart = self.restart;
        restart += 1;
    }
}

/// A token provider that calls whichever provider `current` holds at each connect.
fn latest_token(current: Arc<Mutex<Option<TokenProvider>>>) -> TokenProvider {
    TokenProvider::new(move |request| {
        let provider = current
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        async move {
            match provider {
                Some(provider) => provider.get(request).await,
                None => None,
            }
        }
    })
}

fn use_channel_events<C: Channel>(
    options: ChannelOptions,
    on_event: Callback<ClientEvent<C::Event>>,
) -> UseChannel {
    // Reading the settings subscribes the component, so a provider change is seen here.
    let settings = try_use_context::<Signal<PartylineSettings>>()
        .map(|s| s.read().clone())
        .unwrap_or_default();
    let mut status = use_signal(|| Status::Idle);
    let mut cursor = use_signal(|| options.since);
    let restart = use_signal(|| 0u64);
    let driver = use_hook(|| Rc::new(RefCell::new(Driver::default())));
    let driver_value = use_hook(|| CopyValue::new(driver.clone()));
    let since = use_hook(|| Rc::new(Cell::new(options.since)));
    since.set(options.since);
    // The driver asks this cell at each connect, so a new provider needs no restart. It is
    // `Send` on native targets, where the token provider must be. On wasm nothing is.
    #[allow(clippy::arc_with_non_send_sync)]
    let token = use_hook(|| Arc::new(Mutex::new(None)));
    *token.lock().unwrap_or_else(PoisonError::into_inner) =
        options.token.clone().or(settings.token);

    {
        let driver = driver.clone();
        use_drop(move || driver.borrow_mut().stop());
    }

    let target = Target {
        id: options.id.clone(),
        base_url: options.base_url.or(settings.base_url).unwrap_or_default(),
        config: options.config.or(settings.config).unwrap_or_default(),
        enabled: options.enabled,
    };
    use_effect(use_reactive(&target, move |target| {
        // Subscribe to restarts.
        let _ = restart();

        let mut state = driver.borrow_mut();
        state.stop();
        state.generation += 1;
        let generation = state.generation;
        let same_channel = state.id.as_deref() == Some(target.id.as_str());
        // A restart of the same channel resumes from the current cursor.
        let start = if same_channel {
            *cursor.peek()
        } else {
            since.get()
        };
        state.id = Some(target.id.clone());

        let connection = target.enabled.then(|| {
            let (handle, events, run) = partyline_client::connect::<C>(ConnectOptions {
                base_url: target.base_url,
                id: target.id,
                since: start,
                token: Some(latest_token(token.clone())),
                config: target.config,
            });
            // The driver outlives the component, so it can close the socket cleanly after
            // stop.
            dioxus::dioxus_core::spawn_forever(run);
            state.handle = Some(handle.clone());
            (handle, events)
        });
        drop(state);
        if *cursor.peek() != start {
            cursor.set(start);
        }
        if connection.is_none() {
            let stopped = Status::Stopped {
                reason: StopReason::App,
            };
            if *status.peek() != stopped {
                status.set(stopped);
            }
        }

        let driver = driver.clone();
        spawn(async move {
            let Some((handle, mut events)) = connection else {
                return;
            };
            while let Some(event) = events.next().await {
                if driver.borrow().generation != generation {
                    break;
                }
                match &event {
                    ClientEvent::Status(s) => {
                        if *status.peek() != *s {
                            status.set(*s);
                        }
                    }
                    ClientEvent::Event { .. } | ClientEvent::Reset => {
                        let current = handle.cursor();
                        if *cursor.peek() != current {
                            cursor.set(current);
                        }
                    }
                }
                on_event.call(event);
            }
        });
    }));

    UseChannel {
        status,
        cursor,
        restart,
        driver: driver_value,
    }
}

/// Subscribes to any channel, with full control over events and resets.
///
/// `on_message` is called for every event, in order, and for every reset. The latest
/// closure passed is the one called.
pub fn use_channel<C: Channel>(
    options: ChannelOptions,
    mut on_message: impl FnMut(ChannelMessage<C::Event>) + 'static,
) -> UseChannel {
    let callback = use_callback(move |event: ClientEvent<C::Event>| match event {
        ClientEvent::Event { event, .. } => on_message(ChannelMessage::Event(event)),
        ClientEvent::Reset => on_message(ChannelMessage::Reset),
        ClientEvent::Status(_) => {}
    });
    use_channel_events::<C>(options, callback)
}

/// Subscribes to a [`Mode::Latest`] channel and keeps its latest value in a signal.
///
/// The value is `None` until the first event arrives. A Latest channel sends its current
/// value on connect, so this needs no initial fetch.
pub fn use_channel_latest<C: Channel>(
    options: ChannelOptions,
) -> (ReadSignal<Option<C::Event>>, UseChannel) {
    let mut value = use_signal(|| None);
    let callback = use_callback(move |event: ClientEvent<C::Event>| {
        if let ClientEvent::Event { event, .. } = event {
            value.set(Some(event));
        }
    });
    let channel = use_channel_events::<C>(options, callback);
    (ReadSignal::new(value), channel)
}

/// Subscribes to a [`Mode::Log`] channel and folds its events into state.
///
/// - `initial` builds the starting state, which matches `since` in `options`.
/// - `reduce` applies one event.
/// - `refetch` loads fresh state after a reset and returns it with the channel head it was
///   read at. Events that arrive during the refetch are buffered, and the ones after that
///   head are applied to the fresh state, so none is lost or applied twice. The returned
///   head must not be older than the reset, which holds for any read that starts after
///   the reset arrives.
pub fn use_channel_reducer<C, S, F, Fut>(
    options: ChannelOptions,
    initial: impl FnOnce() -> S,
    mut reduce: impl FnMut(&mut S, C::Event) + 'static,
    mut refetch: F,
) -> (ReadSignal<S>, UseChannel)
where
    C: Channel,
    S: 'static,
    F: FnMut() -> Fut + 'static,
    Fut: Future<Output = (S, Cursor)> + 'static,
{
    let mut state = use_signal(initial);
    // `Some` while a refetch runs: the events received since the reset.
    let buffer: Shared<Option<Buffered<C::Event>>> = use_hook(|| Rc::new(RefCell::new(None)));
    let refetches = use_hook(|| Rc::new(RefCell::new(0u64)));
    // The latest reducer, shared by the event handler and a refetch in flight.
    let reducer: Shared<Option<Reducer<S, C::Event>>> = use_hook(|| Rc::new(RefCell::new(None)));
    *reducer.borrow_mut() = Some(Box::new(move |s: &mut S, e| reduce(s, e)));

    let callback = use_callback(move |event: ClientEvent<C::Event>| match event {
        ClientEvent::Event { seq, event } => {
            if let Some(buffered) = buffer.borrow_mut().as_mut() {
                buffered.push((seq, event));
                return;
            }
            if let Some(reduce) = reducer.borrow_mut().as_mut() {
                state.with_mut(|s| reduce(s, event));
            }
        }
        ClientEvent::Reset => {
            *buffer.borrow_mut() = Some(Vec::new());
            *refetches.borrow_mut() += 1;
            let generation = *refetches.borrow();
            let fetch = refetch();
            let buffer = buffer.clone();
            let refetches = refetches.clone();
            let reducer = reducer.clone();
            spawn(async move {
                let (mut fresh, head) = fetch.await;
                if *refetches.borrow() != generation {
                    return; // A newer reset started another refetch.
                }
                let buffered = buffer.borrow_mut().take().unwrap_or_default();
                if let Some(reduce) = reducer.borrow_mut().as_mut() {
                    for (seq, event) in buffered {
                        if seq > head.seq {
                            reduce(&mut fresh, event);
                        }
                    }
                }
                state.set(fresh);
            });
        }
        ClientEvent::Status(_) => {}
    });
    let channel = use_channel_events::<C>(options, callback);
    (ReadSignal::new(state), channel)
}

type Reducer<S, E> = Box<dyn FnMut(&mut S, E)>;
type Shared<T> = Rc<RefCell<T>>;
type Buffered<E> = Vec<(u64, E)>;
