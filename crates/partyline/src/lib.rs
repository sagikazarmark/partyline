//! Sequenced, resumable server-to-client events.
//!
//! partyline pushes events from a Cloudflare Durable Object to Dioxus clients over
//! WebSockets. A client that loses its connection reconnects with its last cursor and
//! receives exactly the events it missed.
//!
//! This crate holds everything that needs no I/O:
//!
//! - [`frame`]: the wire protocol types.
//! - [`codec`]: JSON encoding.
//! - [`client`]: the sans-IO client state machine.
//! - [`server`]: the replay decision, retention policy, and the [`server::Log`] trait.
//! - [`testing`]: a loopback harness that runs the client against the server logic over a
//!   faulty in-memory pipe.
//!
//! partyline is inspired by PartyKit, but is not affiliated with Cloudflare or PartyKit and
//! is not wire-compatible with PartyServer or partysocket.
//!
//! # Defining a channel
//!
//! Define each channel once, in a crate shared by the Worker and the client:
//!
//! ```
//! use partyline::{Channel, Mode};
//! use serde::{Deserialize, Serialize};
//!
//! #[derive(Clone, Debug, Serialize, Deserialize)]
//! pub enum OrderEvent {
//!     StatusChanged { status: String },
//! }
//!
//! pub struct Orders;
//!
//! impl Channel for Orders {
//!     const NAME: &'static str = "orders";
//!     const MODE: Mode = Mode::Log;
//!     type Event = OrderEvent;
//! }
//! ```

#![cfg_attr(docsrs, feature(doc_cfg))]

pub mod client;
pub mod codec;
pub mod frame;
pub mod server;
pub mod testing;

use serde::Serialize;
use serde::de::DeserializeOwned;

pub use client::{Client, ClientConfig, Input, Output, Status, TimerId};
pub use frame::{Cursor, Message, Mode, ServerFrame, close};

/// A channel definition, shared by the server and the client.
pub trait Channel: 'static {
    /// The URL path segment, for example `"orders"`.
    const NAME: &'static str;
    /// The channel mode.
    const MODE: Mode;
    /// The event type.
    type Event: Serialize + DeserializeOwned + Clone + 'static;
}
