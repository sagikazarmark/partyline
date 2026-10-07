//! The orders channel, shared by the Worker and the web client.

use partyline::{Channel, Cursor, Mode};
use serde::{Deserialize, Serialize};

/// The orders channel: one channel per order ID.
pub struct Orders;

impl Channel for Orders {
    const NAME: &'static str = "orders";
    const MODE: Mode = Mode::Log;
    type Event = OrderEvent;
}

/// The Durable Object binding name in the wrangler configuration.
pub const BINDING: &str = "ORDER_CHANNEL";

/// An order's lifecycle stage.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OrderStatus {
    /// The order was placed.
    #[default]
    Placed,
    /// The kitchen is preparing it.
    Preparing,
    /// Ready for pickup.
    Ready,
    /// Delivered.
    Delivered,
}

impl OrderStatus {
    /// Every status, in order.
    pub const ALL: [OrderStatus; 4] = [
        OrderStatus::Placed,
        OrderStatus::Preparing,
        OrderStatus::Ready,
        OrderStatus::Delivered,
    ];

    /// A display label.
    pub fn label(self) -> &'static str {
        match self {
            OrderStatus::Placed => "Placed",
            OrderStatus::Preparing => "Preparing",
            OrderStatus::Ready => "Ready",
            OrderStatus::Delivered => "Delivered",
        }
    }
}

/// An event on the orders channel.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum OrderEvent {
    /// The status changed.
    StatusChanged {
        /// The new status.
        status: OrderStatus,
    },
    /// A note was added.
    NoteAdded {
        /// The note.
        note: String,
    },
}

/// An order.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Order {
    /// The status.
    pub status: OrderStatus,
    /// Notes, oldest first.
    pub notes: Vec<String>,
}

impl Order {
    /// Applies an event.
    pub fn apply(&mut self, event: &OrderEvent) {
        match event {
            OrderEvent::StatusChanged { status } => self.status = *status,
            OrderEvent::NoteAdded { note } => self.notes.push(note.clone()),
        }
    }
}

/// The response of `GET /api/orders/{id}`: the order and the channel head it was read at.
///
/// Both are read in one Durable Object turn, so connecting with `since: head` misses nothing.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderSnapshot {
    /// The order.
    pub order: Order,
    /// The channel head the order was read at.
    pub head: Cursor,
}
