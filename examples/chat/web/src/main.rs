//! The chat example web client: sign in, then chat in a room that needs an access token.
//!
//! - The token provider works like Clerk's `getToken()`: it returns the cached access token,
//!   and fetches a fresh one from `/api/token` when there is none or the server rejected it
//!   with 4401.
//! - When `/api/token` answers 401, the session is gone, and the page shows the sign-in form.
//! - Sign-out closes the user's sockets in every tab with 4403. The other tabs show the
//!   sign-in form.

use chat_shared::{AccessToken, Chat, ChatEvent, HISTORY, Login, PostMessage, ROOM};
use dioxus::prelude::*;
use partyline::close;
use partyline_dioxus::{
    ChannelMessage, ChannelOptions, Cursor, PartylineProvider, Status, StopReason, TokenProvider,
    use_channel,
};

/// Compiled by `dx` from `tailwind.css`.
const TAILWIND: Asset = asset!("/assets/tailwind.css");

/// A card: the surface every panel sits on.
const CARD: &str = "rounded-2xl bg-white p-5 shadow-sm ring-1 ring-slate-200 dark:bg-slate-900 dark:ring-slate-800";

const INPUT: &str = "min-w-0 flex-1 rounded-xl bg-transparent px-3 py-2 text-sm ring-1 ring-slate-200 placeholder:text-slate-400 focus:ring-2 focus:ring-indigo-500 focus:outline-none dark:ring-slate-700";

const BUTTON: &str = "rounded-xl bg-indigo-600 px-4 py-2 text-sm font-semibold text-white shadow-sm transition hover:bg-indigo-500 active:scale-[0.98]";

const SECONDARY: &str = "rounded-xl px-3 py-1.5 text-sm font-medium ring-1 ring-slate-200 transition hover:ring-indigo-400 active:scale-[0.98] dark:ring-slate-700";

fn main() {
    dioxus::launch(App);
}

#[derive(Clone, Debug, PartialEq)]
enum Session {
    Loading,
    SignedOut { notice: Option<&'static str> },
    SignedIn(AccessToken),
}

/// `Ok(None)` when the session cookie is missing or expired.
async fn fetch_token() -> Result<Option<AccessToken>, gloo_net::Error> {
    let response = gloo_net::http::Request::get("/api/token").send().await?;
    if response.status() == 401 {
        return Ok(None);
    }
    response.json().await.map(Some)
}

async fn fetch_head() -> Result<Cursor, gloo_net::Error> {
    gloo_net::http::Request::get("/api/head")
        .send()
        .await?
        .json()
        .await
}

/// Works like Clerk's `getToken()`: returns the cached access token, and fetches a fresh one
/// when there is none or the server rejected it with 4401.
#[cfg(target_arch = "wasm32")]
fn token_provider(session: Signal<Session>) -> Option<TokenProvider> {
    Some(TokenProvider::new(
        move |request: partyline_dioxus::TokenRequest| {
            let mut session = session;
            async move {
                if !request.refresh
                    && let Session::SignedIn(cached) = &*session.peek()
                {
                    return Some(cached.token.clone());
                }
                match fetch_token().await {
                    Ok(Some(fresh)) => {
                        let token = fresh.token.clone();
                        session.set(Session::SignedIn(fresh));
                        Some(token)
                    }
                    Ok(None) => {
                        session.set(Session::SignedOut {
                            notice: Some("Your session ended. Sign in again."),
                        });
                        None
                    }
                    // A network error. The client backs off and asks again.
                    Err(_) => None,
                }
            }
        },
    ))
}

/// This client only runs in the browser. On native targets `TokenProvider` must be `Send`,
/// and browser futures are not, so a native build (such as `cargo clippy --workspace`)
/// gets no provider.
#[cfg(not(target_arch = "wasm32"))]
fn token_provider(_: Signal<Session>) -> Option<TokenProvider> {
    None
}

#[component]
fn App() -> Element {
    let mut session = use_context_provider(|| Signal::new(Session::Loading));
    use_hook(move || {
        spawn(async move {
            session.set(match fetch_token().await {
                Ok(Some(token)) => Session::SignedIn(token),
                _ => Session::SignedOut { notice: None },
            });
        })
    });

    // Created once, so the provider's settings stay equal across renders.
    let token = use_hook(move || token_provider(session));

    rsx! {
        document::Stylesheet { href: TAILWIND }
        PartylineProvider { token,
            main { class: "mx-auto max-w-lg space-y-5 p-4 pt-10",
                p { class: "text-sm font-semibold uppercase tracking-wider text-indigo-600 dark:text-indigo-400",
                    "partyline chat"
                }
                match &*session.read() {
                    Session::Loading => rsx! {
                        p { class: "animate-pulse text-slate-500 dark:text-slate-400", "Loading…" }
                    },
                    Session::SignedOut { notice } => rsx! {
                        SignIn { notice: *notice }
                    },
                    Session::SignedIn(token) => rsx! {
                        Room { user: token.user.clone(), expires_at: token.expires_at }
                    },
                }
            }
        }
    }
}

#[component]
fn SignIn(notice: Option<&'static str>) -> Element {
    let mut session = use_context::<Signal<Session>>();
    let mut name = use_signal(String::new);
    let mut error = use_signal(|| None::<String>);

    let submit = move |e: FormEvent| {
        e.prevent_default();
        let login = Login {
            name: name.read().trim().to_owned(),
        };
        spawn(async move {
            let result = async {
                let response = gloo_net::http::Request::post("/api/login")
                    .json(&login)?
                    .send()
                    .await?;
                if !response.ok() {
                    return Ok(Err(response.text().await?));
                }
                response.json::<AccessToken>().await.map(Ok)
            }
            .await;
            match result {
                Ok(Ok(token)) => session.set(Session::SignedIn(token)),
                Ok(Err(message)) => error.set(Some(message)),
                Err(e) => error.set(Some(e.to_string())),
            }
        });
    };

    rsx! {
        h1 { class: "text-3xl font-bold tracking-tight", "Sign in" }
        if let Some(notice) = notice {
            p { class: "rounded-xl bg-amber-50 px-4 py-3 text-sm text-amber-800 dark:bg-amber-500/15 dark:text-amber-200",
                "{notice}"
            }
        }
        form { class: "{CARD} space-y-3", onsubmit: submit,
            p { class: "text-sm text-slate-500 dark:text-slate-400",
                "Any name works. This sign-in stands in for an identity provider such as Clerk."
            }
            div { class: "flex gap-2",
                input {
                    class: INPUT,
                    value: "{name}",
                    placeholder: "Your name",
                    autofocus: true,
                    oninput: move |e| name.set(e.value()),
                }
                button { class: BUTTON, r#type: "submit", "Sign in" }
            }
            if let Some(error) = error() {
                p { class: "text-sm text-rose-600 dark:text-rose-400", "{error}" }
            }
        }
    }
}

#[component]
fn Room(user: String, expires_at: u64) -> Element {
    let head = use_resource(fetch_head);
    match &*head.read() {
        Some(Ok(head)) => {
            // Replay the retained history: the last `HISTORY` messages.
            let since = Cursor::new(head.epoch, head.seq.saturating_sub(HISTORY));
            rsx! {
                Messages { user, expires_at, since }
            }
        }
        Some(Err(e)) => rsx! {
            p { class: "rounded-xl bg-rose-50 px-4 py-3 text-sm text-rose-800 dark:bg-rose-500/15 dark:text-rose-200",
                "Could not load the room: {e}"
            }
        },
        None => rsx! {
            p { class: "animate-pulse text-slate-500 dark:text-slate-400", "Loading…" }
        },
    }
}

#[component]
fn Messages(user: String, expires_at: u64, since: Cursor) -> Element {
    let mut session = use_context::<Signal<Session>>();
    let mut messages = use_signal(Vec::<(String, String)>::new);
    let mut draft = use_signal(String::new);

    let channel = use_channel::<Chat>(ChannelOptions::new(ROOM).since(since), move |msg| {
        match msg {
            ChannelMessage::Event(ChatEvent::MessagePosted { author, text }) => {
                messages.write().push((author, text))
            }
            // The history was trimmed past the cursor. Start the list over.
            ChannelMessage::Reset => messages.write().clear(),
        }
    });

    // 4403: this user signed out, in this tab or another one.
    use_effect(move || {
        if channel.status()
            == (Status::Stopped {
                reason: StopReason::Closed(close::FORBIDDEN),
            })
        {
            session.set(Session::SignedOut {
                notice: Some("You signed out in another tab."),
            });
        }
    });

    let sign_out = move |_| {
        // Unmount the room first, so this tab's own socket closes normally, not with 4403.
        session.set(Session::SignedOut { notice: None });
        dioxus::dioxus_core::spawn_forever(async move {
            let _ = gloo_net::http::Request::post("/api/logout").send().await;
        });
    };

    let send = move |e: FormEvent| {
        e.prevent_default();
        let text = draft.read().trim().to_owned();
        if text.is_empty() {
            return;
        }
        draft.set(String::new());
        spawn(async move {
            let Ok(request) =
                gloo_net::http::Request::post("/api/messages").json(&PostMessage { text })
            else {
                return;
            };
            if let Ok(response) = request.send().await
                && response.status() == 401
            {
                session.set(Session::SignedOut {
                    notice: Some("Your session ended. Sign in again."),
                });
            }
        });
    };

    let expires = js_sys::Date::new(&((expires_at * 1000) as f64).into())
        .to_locale_time_string("en-GB")
        .as_string()
        .unwrap_or_default();

    rsx! {
        header { class: "flex flex-wrap items-center justify-between gap-3",
            StatusBadge { status: channel.status(), cursor: channel.cursor() }
            div { class: "flex items-center gap-2",
                span { class: "text-sm font-medium", "{user}" }
                button { class: SECONDARY, onclick: sign_out, "Sign out" }
            }
        }
        div { class: "flex flex-wrap items-center justify-between gap-2 text-xs text-slate-500 dark:text-slate-400",
            span { class: "font-mono", "access token expires {expires}" }
            button {
                class: SECONDARY,
                title: "Reconnect with the cached token. After it expires, the server answers 4401 and the client fetches a fresh one.",
                onclick: move |_| channel.reconnect(),
                "Reconnect"
            }
        }
        section { class: CARD,
            if messages.read().is_empty() {
                p { class: "py-2 text-sm text-slate-400 dark:text-slate-500", "No messages yet" }
            }
            // Reversed, so the browser keeps the newest message in view.
            ul { class: "flex max-h-[60vh] flex-col-reverse gap-2 overflow-y-auto",
                for (i, (author, text)) in messages.read().iter().enumerate().rev() {
                    li {
                        key: "{i}",
                        class: if *author == user { "self-end max-w-[85%] rounded-2xl bg-indigo-600 px-3 py-2 text-sm text-white" } else { "self-start max-w-[85%] rounded-2xl bg-slate-100 px-3 py-2 text-sm dark:bg-slate-800" },
                        if *author != user {
                            p { class: "text-xs font-semibold text-slate-500 dark:text-slate-400", "{author}" }
                        }
                        p { class: "break-words", "{text}" }
                    }
                }
            }
        }
        form { class: "flex gap-2", onsubmit: send,
            input {
                class: INPUT,
                value: "{draft}",
                placeholder: "Message",
                oninput: move |e| draft.set(e.value()),
            }
            button { class: BUTTON, r#type: "submit", "Send" }
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
        Status::Unauthorized { retry_in, .. } => (
            "bg-rose-100 text-rose-800 dark:bg-rose-500/15 dark:text-rose-300",
            "bg-rose-500",
            format!(
                "Token rejected, renewing in {:.1} s",
                retry_in.as_secs_f32()
            ),
        ),
        Status::Waiting { retry_in, .. } => (
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
