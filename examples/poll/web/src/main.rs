//! The live poll web client.
//!
//! - `/present`: the presenter view. Question, QR code, live tally, activity feed, and a
//!   "Reset demo" button.
//! - `/`: the phone view. Answer buttons, live tally, a status badge with the cursor, the
//!   activity feed, and a "Go offline" switch.

use dioxus::prelude::*;
use partyline::frame::encode_segment;
use partyline_dioxus::{
    ChannelMessage, ChannelOptions, Cursor, PartylineProvider, Status, use_channel,
};
use poll_shared::{ActivityEvent, PollActivity, PollSnapshot, PollTally, Tally, Vote};

/// Compiled by `dx` from `tailwind.css`.
const TAILWIND: Asset = asset!("/assets/tailwind.css");

fn main() {
    dioxus::launch(App);
}

/// The poll both views show.
const POLL_ID: &str = "demo";

fn location() -> (String, String) {
    let Some(l) = web_sys::window().map(|w| w.location()) else {
        return Default::default();
    };
    (
        l.origin().unwrap_or_default(),
        l.pathname().unwrap_or_default(),
    )
}

async fn fetch_poll(id: &str) -> Result<PollSnapshot, gloo_net::Error> {
    gloo_net::http::Request::get(&format!("/api/polls/{}", encode_segment(id)))
        .send()
        .await?
        .json()
        .await
}

#[component]
fn App() -> Element {
    let (origin, path) = use_hook(location);
    rsx! {
        document::Stylesheet { href: TAILWIND }
        PartylineProvider {
            if path.starts_with("/present") {
                Presenter { id: POLL_ID, join_url: format!("{origin}/") }
            } else {
                Phone { id: POLL_ID }
            }
        }
    }
}

/// The poll state shared by both views.
#[derive(Clone, Copy, PartialEq)]
struct PollState {
    snapshot: Resource<Result<PollSnapshot, gloo_net::Error>>,
    tally: Signal<Tally>,
    feed: Signal<Vec<String>>,
    status: Signal<Status>,
    cursor: Signal<Option<Cursor>>,
}

fn use_poll(id: &str) -> PollState {
    let id = id.to_owned();
    let mut tally = use_signal(Tally::default);
    let snapshot = use_resource(move || {
        let id = id.clone();
        async move {
            let snapshot = fetch_poll(&id).await;
            // Show the loaded tally at once. The Latest channel replaces it when it connects.
            if let Ok(snap) = &snapshot {
                tally.set(snap.tally.clone());
            }
            snapshot
        }
    });
    PollState {
        snapshot,
        tally,
        feed: use_signal(Vec::new),
        status: use_signal(|| Status::Idle),
        cursor: use_signal(|| None),
    }
}

/// Subscribes to both channels and writes into the parent's state. Unmounting it is how the
/// phone goes offline: the cursor stays in the parent, and mounting it again resumes.
#[component]
fn Live(id: String, since: Option<Cursor>, state: PollState) -> Element {
    let PollState {
        snapshot: _,
        mut tally,
        mut feed,
        mut status,
        mut cursor,
    } = state;

    use_channel::<PollTally>(ChannelOptions::new(id.clone()), move |message| {
        if let ChannelMessage::Event(t) = message {
            tally.set(t);
        }
    });

    let activity = use_channel::<PollActivity>(
        ChannelOptions::new(id.clone()).since(since),
        move |message| match message {
            ChannelMessage::Event(ActivityEvent::PollReset) => {
                feed.set(vec![ActivityEvent::PollReset.describe()])
            }
            ChannelMessage::Event(event) => feed.write().insert(0, event.describe()),
            // The activity log was wiped with a new epoch, so the poll was reset. The feed
            // starts over, and the Latest tally channel already delivers the fresh tally.
            ChannelMessage::Reset => feed.set(vec![ActivityEvent::PollReset.describe()]),
        },
    );

    use_effect(move || {
        let s = activity.status();
        if *status.peek() != s {
            status.set(s);
        }
    });
    use_effect(move || {
        let c = activity.cursor();
        if *cursor.peek() != c {
            cursor.set(c);
        }
    });
    rsx! {}
}

/// A card: the surface every panel sits on.
const CARD: &str = "rounded-2xl bg-white p-5 shadow-sm ring-1 ring-slate-200 dark:bg-slate-900 dark:ring-slate-800";

#[component]
fn Tallies(state: PollState, options: Vec<String>, large: bool) -> Element {
    let tally = state.tally.read();
    let total = tally.total();
    let row = if large { "text-xl" } else { "text-sm" };
    let track = if large { "h-5" } else { "h-3" };
    rsx! {
        div { class: "space-y-4",
            for (i, option) in options.iter().enumerate() {
                {
                    let count = tally.counts.get(i).copied().unwrap_or(0);
                    let percent = (count * 100).checked_div(total).unwrap_or(0);
                    rsx! {
                        div {
                            div { class: "mb-1.5 flex items-baseline justify-between gap-4 font-medium {row}",
                                span { "{option}" }
                                span { class: "tabular-nums text-slate-500 dark:text-slate-400",
                                    "{count} · {percent}%"
                                }
                            }
                            div { class: "{track} overflow-hidden rounded-full bg-slate-200 dark:bg-slate-800",
                                div {
                                    class: "h-full rounded-full bg-indigo-500 transition-[width] duration-500 ease-out",
                                    style: "width: {percent}%",
                                }
                            }
                        }
                    }
                }
            }
            p { class: "text-sm text-slate-500 dark:text-slate-400",
                if total == 1 { "1 vote" } else { "{total} votes" }
            }
        }
    }
}

#[component]
fn StatusBadge(state: PollState) -> Element {
    let (badge, dot, text) = match (state.status)() {
        Status::Open => (
            "bg-emerald-100 text-emerald-800 dark:bg-emerald-500/15 dark:text-emerald-300",
            "bg-emerald-500 animate-pulse",
            "Live".to_owned(),
        ),
        Status::Waiting { retry_in } | Status::Unauthorized { retry_in } => (
            "bg-amber-100 text-amber-800 dark:bg-amber-500/15 dark:text-amber-300",
            "bg-amber-500",
            format!("Reconnecting in {:.1} s", retry_in.as_secs_f32()),
        ),
        Status::Stopped { .. } => (
            "bg-slate-200 text-slate-700 dark:bg-slate-800 dark:text-slate-300",
            "bg-slate-400",
            "Offline".to_owned(),
        ),
        Status::Idle | Status::Connecting => (
            "bg-sky-100 text-sky-800 dark:bg-sky-500/15 dark:text-sky-300",
            "bg-sky-500 animate-pulse",
            "Connecting".to_owned(),
        ),
    };
    let cursor = (state.cursor)()
        .map(|c| c.to_string())
        .unwrap_or_else(|| "–".to_owned());
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
fn Feed(state: PollState) -> Element {
    let feed = state.feed.read();
    rsx! {
        section { class: CARD,
            h2 { class: "mb-2 text-xs font-semibold uppercase tracking-wider text-slate-500 dark:text-slate-400",
                "Activity"
            }
            if feed.is_empty() {
                p { class: "py-2 text-sm text-slate-400 dark:text-slate-500", "No votes yet" }
            }
            ul { class: "max-h-72 divide-y divide-slate-100 overflow-y-auto dark:divide-slate-800",
                for (i, line) in feed.iter().enumerate() {
                    li { key: "{i}-{line}", class: "py-2 text-sm", "{line}" }
                }
            }
        }
    }
}

#[component]
fn Loading() -> Element {
    rsx! {
        main { class: "grid min-h-dvh place-items-center text-slate-500 dark:text-slate-400",
            p { class: "animate-pulse", "Loading the poll…" }
        }
    }
}

#[component]
fn Presenter(id: String, join_url: String) -> Element {
    let state = use_poll(&id);
    let snapshot = state.snapshot.read();
    let Some(Ok(snap)) = &*snapshot else {
        return rsx! { Loading {} };
    };
    let qr = qrcode::QrCode::new(join_url.as_bytes())
        .map(|code| {
            code.render::<qrcode::render::svg::Color>()
                .min_dimensions(240, 240)
                .build()
        })
        .unwrap_or_default();
    let reset = {
        let id = id.clone();
        move |_| {
            let id = id.clone();
            spawn(async move {
                let _ = gloo_net::http::Request::post(&format!(
                    "/api/polls/{}/reset",
                    encode_segment(&id)
                ))
                .send()
                .await;
            });
        }
    };
    rsx! {
        main { class: "mx-auto grid max-w-6xl gap-8 p-6 lg:grid-cols-[1fr_18rem] lg:p-12",
            Live { id: id.clone(), since: Some(snap.activity_head), state }
            div { class: "space-y-8",
                header { class: "space-y-4",
                    p { class: "text-sm font-semibold uppercase tracking-wider text-indigo-600 dark:text-indigo-400",
                        "partyline live poll"
                    }
                    h1 { class: "text-4xl font-bold tracking-tight text-balance lg:text-5xl", "{snap.question}" }
                    StatusBadge { state }
                }
                section { class: CARD,
                    Tallies { state, options: snap.options.clone(), large: true }
                }
                Feed { state }
            }
            aside { class: "space-y-4 lg:sticky lg:top-12 lg:self-start",
                div { class: "rounded-2xl bg-white p-4 shadow-sm ring-1 ring-slate-200 dark:ring-slate-800 [&_svg]:h-auto [&_svg]:w-full",
                    dangerous_inner_html: "{qr}",
                }
                p { class: "text-center text-sm text-slate-500 dark:text-slate-400",
                    "Scan to vote"
                    br {}
                    span { class: "font-mono text-xs break-all", "{join_url}" }
                }
                button {
                    class: "w-full rounded-xl bg-rose-600 px-4 py-2.5 font-semibold text-white shadow-sm transition hover:bg-rose-500 active:scale-[0.98]",
                    onclick: reset,
                    "Reset demo"
                }
            }
        }
    }
}

fn guest_name() -> String {
    format!("Guest {}", (js_sys::Math::random() * 9000.0) as u32 + 1000)
}

#[component]
fn Phone(id: String) -> Element {
    let state = use_poll(&id);
    let name = use_hook(guest_name);
    let mut online = use_signal(|| true);
    // The activity cursor when the phone went offline.
    let mut offline_at = use_signal(|| None::<Cursor>);
    let mut replayed = use_signal(|| None::<u64>);
    let mut my_vote = use_signal(|| None::<usize>);

    let snapshot = state.snapshot.read();
    let Some(Ok(snap)) = &*snapshot else {
        return rsx! { Loading {} };
    };
    let since = if online() {
        offline_at().or(Some(snap.activity_head))
    } else {
        None
    };

    let toggle = {
        let id = id.clone();
        move |_| {
            if online() {
                // Unmounting `Live` stops its drivers. Keep the cursor to resume from.
                offline_at.set((state.cursor)());
                replayed.set(None);
                online.set(false);
                let mut status = state.status;
                status.set(Status::Stopped { code: None });
            } else {
                // The replay count is the difference between the head now and the cursor.
                let id = id.clone();
                spawn(async move {
                    if let (Ok(snap), Some(at)) = (fetch_poll(&id).await, offline_at())
                        && snap.activity_head.epoch == at.epoch
                    {
                        replayed.set(Some(snap.activity_head.seq.saturating_sub(at.seq)));
                    }
                    online.set(true);
                });
            }
        }
    };

    rsx! {
        main { class: "mx-auto max-w-md space-y-5 p-4 pb-10",
            if online() {
                Live { key: "{offline_at():?}", id: id.clone(), since, state }
            }
            header { class: "flex items-center justify-between gap-3 pt-2",
                StatusBadge { state }
                label { class: "inline-flex shrink-0 cursor-pointer items-center gap-2 text-sm font-medium",
                    input {
                        class: "peer sr-only",
                        r#type: "checkbox",
                        checked: !online(),
                        onchange: toggle,
                    }
                    span { class: "relative h-6 w-11 rounded-full bg-slate-300 transition-colors after:absolute after:top-0.5 after:left-0.5 after:size-5 after:rounded-full after:bg-white after:shadow after:transition-transform peer-checked:bg-amber-500 peer-checked:after:translate-x-5 peer-focus-visible:ring-2 peer-focus-visible:ring-indigo-500 dark:bg-slate-700" }
                    "Go offline"
                }
            }
            h1 { class: "text-2xl font-bold tracking-tight text-balance", "{snap.question}" }
            if let Some(n) = replayed() {
                p { class: "rounded-xl bg-indigo-50 px-4 py-3 text-sm text-indigo-800 dark:bg-indigo-500/15 dark:text-indigo-200",
                    "Back online: {n} missed events replayed"
                }
            }
            div { id: "options", class: "grid gap-3",
                for (i, option) in snap.options.iter().enumerate() {
                    button {
                        class: if my_vote() == Some(i) {
                            "w-full rounded-2xl bg-indigo-600 px-5 py-4 text-left text-lg font-semibold text-white shadow-sm ring-1 ring-indigo-600 transition active:scale-[0.98]"
                        } else {
                            "w-full rounded-2xl bg-white px-5 py-4 text-left text-lg font-semibold shadow-sm ring-1 ring-slate-200 transition hover:ring-indigo-400 active:scale-[0.98] dark:bg-slate-900 dark:ring-slate-800"
                        },
                        onclick: {
                            let id = id.clone();
                            let name = name.clone();
                            move |_| {
                                my_vote.set(Some(i));
                                let id = id.clone();
                                let vote = Vote { option: i, voter: name.clone() };
                                spawn(async move {
                                    if let Ok(req) = gloo_net::http::Request::post(&format!("/api/polls/{}/vote", encode_segment(&id))).json(&vote) {
                                        let _ = req.send().await;
                                    }
                                });
                            }
                        },
                        "{option}"
                    }
                }
            }
            section { class: CARD,
                Tallies { state, options: snap.options.clone(), large: false }
            }
            p { class: "text-center text-sm text-slate-500 dark:text-slate-400", "You are {name}" }
            Feed { state }
        }
    }
}
