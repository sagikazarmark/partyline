//! The orders example web client: the quick start from the README.
//!
//! It follows one order. Every tab updates live.

use dioxus::prelude::*;
use orders_shared::{Order, OrderEvent, OrderSnapshot, OrderStatus, Orders};
use partyline::frame::encode_segment;
use partyline_dioxus::{
    ChannelMessage, ChannelOptions, Cursor, PartylineProvider, Status, use_channel,
};

/// Compiled by `dx` from `tailwind.css`.
const TAILWIND: Asset = asset!("/assets/tailwind.css");

/// The order the page follows.
const ORDER_ID: &str = "demo";

/// A card: the surface every panel sits on.
const CARD: &str = "rounded-2xl bg-white p-5 shadow-sm ring-1 ring-slate-200 dark:bg-slate-900 dark:ring-slate-800";

fn main() {
    dioxus::launch(App);
}

async fn fetch_order(id: &str) -> Result<OrderSnapshot, gloo_net::Error> {
    gloo_net::http::Request::get(&format!("/api/orders/{}", encode_segment(id)))
        .send()
        .await?
        .json()
        .await
}

async fn send_event(id: &str, event: &OrderEvent) -> Result<(), gloo_net::Error> {
    gloo_net::http::Request::post(&format!("/api/orders/{}/events", encode_segment(id)))
        .json(event)?
        .send()
        .await?;
    Ok(())
}

#[component]
fn App() -> Element {
    let snapshot = use_resource(|| fetch_order(ORDER_ID));
    rsx! {
        document::Stylesheet { href: TAILWIND }
        PartylineProvider {
            main { class: "mx-auto max-w-lg space-y-5 p-4 pt-10",
                p { class: "text-sm font-semibold uppercase tracking-wider text-indigo-600 dark:text-indigo-400",
                    "Order {ORDER_ID}"
                }
                match &*snapshot.read() {
                    Some(Ok(snap)) => rsx! {
                        OrderStatusView { id: ORDER_ID, initial: snap.order.clone(), head: snap.head }
                    },
                    Some(Err(e)) => rsx! {
                        p { class: "rounded-xl bg-rose-50 px-4 py-3 text-sm text-rose-800 dark:bg-rose-500/15 dark:text-rose-200",
                            "Could not load the order: {e}"
                        }
                    },
                    None => rsx! {
                        p { class: "animate-pulse text-slate-500 dark:text-slate-400", "Loading…" }
                    },
                }
            }
        }
    }
}

#[component]
fn StatusBadge(status: Status, cursor: Option<Cursor>) -> Element {
    let (badge, dot, text) = match status {
        Status::Open => (
            "bg-emerald-100 text-emerald-800 dark:bg-emerald-500/15 dark:text-emerald-300",
            "bg-emerald-500 animate-pulse",
            "Live".to_owned(),
        ),
        Status::Waiting { retry_in, .. } | Status::Unauthorized { retry_in, .. } => (
            "bg-amber-100 text-amber-800 dark:bg-amber-500/15 dark:text-amber-300",
            "bg-amber-500",
            format!("Reconnecting in {:.1} s", retry_in.as_secs_f32()),
        ),
        Status::Stopped { .. } => (
            "bg-slate-200 text-slate-700 dark:bg-slate-800 dark:text-slate-300",
            "bg-slate-400",
            "Stopped".to_owned(),
        ),
        Status::Idle | Status::Connecting => (
            "bg-sky-100 text-sky-800 dark:bg-sky-500/15 dark:text-sky-300",
            "bg-sky-500 animate-pulse",
            "Connecting".to_owned(),
        ),
    };
    let cursor = cursor.map(|c| c.seq).unwrap_or_default();
    rsx! {
        div { class: "flex flex-wrap items-center gap-x-3 gap-y-1",
            span { class: "inline-flex items-center gap-2 rounded-full px-3 py-1 text-sm font-medium {badge}",
                span { class: "size-2 rounded-full {dot}" }
                "{text}"
            }
            span { class: "font-mono text-xs text-slate-500 dark:text-slate-400", "cursor {cursor}" }
        }
    }
}

#[component]
fn OrderStatusView(id: String, initial: Order, head: Cursor) -> Element {
    let mut order = use_signal(|| initial);

    let channel = use_channel::<Orders>(ChannelOptions::new(id.clone()).since(head), {
        let id = id.clone();
        move |msg| match msg {
            ChannelMessage::Event(event) => order.write().apply(&event),
            ChannelMessage::Reset => {
                let id = id.clone();
                spawn(async move {
                    if let Ok(snap) = fetch_order(&id).await {
                        order.set(snap.order);
                    }
                });
            }
        }
    });

    let mut note = use_signal(String::new);
    let current = order.read().status;

    rsx! {
        StatusBadge { status: channel.status(), cursor: channel.cursor() }
        h1 { class: "text-4xl font-bold tracking-tight", "{current.label()}" }
        section { class: "{CARD} space-y-4",
            div { class: "grid grid-cols-2 gap-2 sm:grid-cols-4",
                for status in OrderStatus::ALL {
                    button {
                        class: "rounded-xl px-3 py-2 text-sm font-semibold ring-1 transition enabled:ring-slate-200 enabled:hover:ring-indigo-400 enabled:active:scale-[0.98] disabled:bg-indigo-600 disabled:text-white disabled:ring-indigo-600 dark:enabled:ring-slate-700",
                        disabled: current == status,
                        onclick: {
                            let id = id.clone();
                            move |_| {
                                let id = id.clone();
                                spawn(async move {
                                    let _ = send_event(&id, &OrderEvent::StatusChanged { status }).await;
                                });
                            }
                        },
                        "{status.label()}"
                    }
                }
            }
            form {
                class: "flex gap-2",
                onsubmit: {
                    let id = id.clone();
                    move |e: FormEvent| {
                        e.prevent_default();
                        let text = note.read().trim().to_owned();
                        if text.is_empty() {
                            return;
                        }
                        note.set(String::new());
                        let id = id.clone();
                        spawn(async move {
                            let _ = send_event(&id, &OrderEvent::NoteAdded { note: text }).await;
                        });
                    }
                },
                input {
                    class: "min-w-0 flex-1 rounded-xl bg-transparent px-3 py-2 text-sm ring-1 ring-slate-200 placeholder:text-slate-400 focus:ring-2 focus:ring-indigo-500 focus:outline-none dark:ring-slate-700",
                    value: "{note}",
                    placeholder: "Add a note",
                    oninput: move |e| note.set(e.value()),
                }
                button {
                    class: "rounded-xl bg-indigo-600 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-indigo-500 active:scale-[0.98]",
                    r#type: "submit",
                    "Add"
                }
            }
        }
        section { class: CARD,
            h2 { class: "mb-2 text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400",
                "Notes"
            }
            if order.read().notes.is_empty() {
                p { class: "py-2 text-sm text-slate-400 dark:text-slate-500", "No notes yet" }
            }
            ul { class: "divide-y divide-slate-100 dark:divide-slate-800",
                for (i, n) in order.read().notes.iter().enumerate() {
                    li { key: "{i}", class: "py-2 text-sm", "{n}" }
                }
            }
        }
    }
}
