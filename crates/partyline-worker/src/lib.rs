//! Cloudflare Durable Object hub for partyline.
//!
//! partyline pushes sequenced, resumable events from a Durable Object to clients over
//! WebSockets. This crate adapts the core server logic of [`partyline`] to a Durable Object:
//!
//! - [`Hub`]: embed it in your own Durable Object and delegate the handlers to it with
//!   [`hub_handlers!`], or generate the whole object with [`channel_object!`].
//! - [`Connect`]: forward a client's WebSocket upgrade from the Worker to the hub.
//! - [`Publisher`]: publish events, read the head, close sockets by tag, and reset.
//!
//! The Durable Object class must use the SQLite storage backend.
//!
//! partyline is not affiliated with Cloudflare or PartyKit, and is not wire-compatible with
//! PartyServer or partysocket.
//!
//! # Requirement
//!
//! `#[durable_object]` emits absolute `::worker::` paths, so the app crate must depend on
//! `worker` under that name. With `wasm-bindgen` 0.2.129 its expansion also names `wasm_bindgen`
//! unqualified, so a module that applies `#[durable_object]` by hand needs `wasm_bindgen` in scope:
//! `use worker::*;` provides it. [`channel_object!`] handles this itself.

#![cfg_attr(docsrs, feature(doc_cfg))]

mod helpers;
mod hub;
mod sql_log;

pub use helpers::{Connect, Publisher, reject};
pub use hub::{Hub, HubConfig};
pub use sql_log::SqlLog;

pub use partyline::frame::decode_segment;
pub use partyline::{self, Channel, Cursor, Mode, close};

/// Generates a Durable Object that holds only a [`Hub`], with every handler delegated to it.
///
/// ```ignore
/// partyline_worker::channel_object! {
///     /// Durable Object for the orders channel.
///     pub struct OrderChannel: Orders;
/// }
///
/// // With configuration:
/// partyline_worker::channel_object! {
///     pub struct ActivityChannel: Activity {
///         config = HubConfig::default().retain_events(500);
///     }
/// }
/// ```
///
/// The struct name is the `class_name` in the wrangler configuration.
///
/// The expansion is the documented manual form: a `#[durable_object]` struct with a `hub`
/// field, `new`, and the `fetch`, `websocket_message`, `websocket_close`, `websocket_error`,
/// and `alarm` handlers. To customize the object, copy the expansion and edit it.
///
/// The items are generated inside an anonymous `const`, so the struct cannot be named from
/// other code. Nothing needs to: the runtime finds the class by its name. An object that other
/// code calls into embeds the hub by hand instead.
#[macro_export]
macro_rules! channel_object {
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident : $channel:ty ;
    ) => {
        $crate::channel_object! {
            $(#[$meta])*
            $vis struct $name : $channel {
                config = $crate::HubConfig::default();
            }
        }
    };
    (
        $(#[$meta:meta])*
        $vis:vis struct $name:ident : $channel:ty {
            config = $config:expr ;
        }
    ) => {
        // `#[durable_object]` expands to code that names `wasm_bindgen` directly, so it must be
        // in scope. The anonymous const keeps that import from leaking into the caller's module.
        const _: () = {
            #[allow(unused_imports)]
            use ::worker::wasm_bindgen;

            $(#[$meta])*
            #[::worker::durable_object]
            $vis struct $name {
                hub: $crate::Hub<$channel>,
            }

            impl ::worker::DurableObject for $name {
                fn new(state: ::worker::State, _env: ::worker::Env) -> Self {
                    Self {
                        hub: $crate::Hub::new(state, $config),
                    }
                }

                async fn fetch(&self, req: ::worker::Request) -> ::worker::Result<::worker::Response> {
                    self.hub.fetch(req).await
                }

                $crate::hub_handlers!(hub);
            }
        };
    };
}

/// Generates the `websocket_message`, `websocket_close`, `websocket_error`, and `alarm`
/// handlers of a Durable Object that embeds a [`Hub`], delegating each to the hub.
///
/// Use it inside `impl DurableObject`, with the name of the hub field. The object writes
/// `new` and `fetch` itself.
///
/// ```ignore
/// impl DurableObject for OrderChannel {
///     fn new(state: State, _env: Env) -> Self { /* .. */ }
///
///     async fn fetch(&self, req: Request) -> Result<Response> {
///         self.hub.fetch_with(req, |event| self.apply(event)).await
///     }
///
///     partyline_worker::hub_handlers!(hub);
/// }
/// ```
///
/// An object with its own alarm uses [`hub_websocket_handlers!`] and writes `alarm` itself.
#[macro_export]
macro_rules! hub_handlers {
    ($hub:ident) => {
        $crate::hub_websocket_handlers!($hub);

        async fn alarm(&self) -> ::worker::Result<::worker::Response> {
            self.$hub.on_alarm().await
        }
    };
}

/// Generates the `websocket_message`, `websocket_close`, and `websocket_error` handlers of a
/// Durable Object that embeds a [`Hub`], delegating each to the hub.
///
/// Use it in an object with its own alarm. Its `alarm` handler calls [`Hub::on_alarm`]; see
/// [the `Hub` docs](Hub#alarms).
///
/// ```ignore
/// impl DurableObject for PollObject {
///     // new, fetch ..
///
///     partyline_worker::hub_websocket_handlers!(hub);
///
///     async fn alarm(&self) -> Result<Response> {
///         self.hub.on_alarm().await?;
///         // The object's own work, if due, then schedule its next time again.
///         Response::ok("")
///     }
/// }
/// ```
#[macro_export]
macro_rules! hub_websocket_handlers {
    ($hub:ident) => {
        async fn websocket_message(
            &self,
            ws: ::worker::WebSocket,
            message: ::worker::WebSocketIncomingMessage,
        ) -> ::worker::Result<()> {
            self.$hub.on_message(ws, message).await
        }

        async fn websocket_close(
            &self,
            ws: ::worker::WebSocket,
            code: usize,
            reason: ::std::string::String,
            was_clean: bool,
        ) -> ::worker::Result<()> {
            self.$hub.on_close(ws, code, reason, was_clean).await
        }

        async fn websocket_error(
            &self,
            ws: ::worker::WebSocket,
            error: ::worker::Error,
        ) -> ::worker::Result<()> {
            self.$hub.on_error(ws, error).await
        }
    };
}
