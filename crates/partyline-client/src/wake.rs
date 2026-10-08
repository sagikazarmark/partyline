//! Browser wake sources:
//!
//! - `visibilitychange` on the document, when the page becomes visible.
//! - `online` on the window.
//! - `pageshow` on the window, when the page is restored from the back-forward cache.
//! - `resume` on the document, when a frozen page resumes (Page Lifecycle API).
//! - `change` on `navigator.connection`, when the network changes (Network Information API),
//!   in browsers that have it.
//!
//! Each one is cheap, and the state machine treats repeated wakes as one.

use futures::channel::mpsc::UnboundedSender;
use wasm_bindgen::closure::Closure;
use wasm_bindgen::{JsCast, JsValue};
use web_sys::{EventTarget, PageTransitionEvent, VisibilityState};

use crate::Command;

type Listener = (
    EventTarget,
    &'static str,
    Closure<dyn FnMut(web_sys::Event)>,
);

/// Registered listeners. Dropping this removes them.
pub(crate) struct WakeListeners {
    listeners: Vec<Listener>,
}

impl WakeListeners {
    /// Registers the listeners. Returns `None` outside a browser window.
    pub(crate) fn install(commands: UnboundedSender<Command>) -> Option<Self> {
        let window = web_sys::window()?;
        let document = window.document()?;
        let wake = move |commands: &UnboundedSender<Command>| {
            let _ = commands.unbounded_send(Command::Wake);
        };
        let mut listeners = Vec::new();

        let visible = {
            let commands = commands.clone();
            let document = document.clone();
            Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
                if document.visibility_state() == VisibilityState::Visible {
                    wake(&commands);
                }
            })
        };
        listeners.push((
            document.clone().unchecked_into::<EventTarget>(),
            "visibilitychange",
            visible,
        ));

        let resume = {
            let commands = commands.clone();
            Closure::<dyn FnMut(web_sys::Event)>::new(move |_| wake(&commands))
        };
        listeners.push((document.unchecked_into::<EventTarget>(), "resume", resume));

        let online = {
            let commands = commands.clone();
            Closure::<dyn FnMut(web_sys::Event)>::new(move |_| wake(&commands))
        };
        listeners.push((
            window.clone().unchecked_into::<EventTarget>(),
            "online",
            online,
        ));

        let pageshow = {
            let commands = commands.clone();
            Closure::<dyn FnMut(web_sys::Event)>::new(move |event: web_sys::Event| {
                // Only a restore from the back-forward cache. A normal load starts the
                // client anyway.
                if event
                    .dyn_ref::<PageTransitionEvent>()
                    .is_some_and(PageTransitionEvent::persisted)
                {
                    wake(&commands);
                }
            })
        };
        listeners.push((
            window.clone().unchecked_into::<EventTarget>(),
            "pageshow",
            pageshow,
        ));

        // The Network Information API is missing in some browsers, so it is looked up by name.
        if let Some(connection) = js_sys::Reflect::get(&window.navigator(), &"connection".into())
            .ok()
            .filter(|c| !c.is_undefined() && !c.is_null())
            .and_then(|c: JsValue| c.dyn_into::<EventTarget>().ok())
        {
            let change = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| wake(&commands));
            listeners.push((connection, "change", change));
        }

        for (target, event, closure) in &listeners {
            let _ =
                target.add_event_listener_with_callback(event, closure.as_ref().unchecked_ref());
        }
        Some(Self { listeners })
    }
}

impl Drop for WakeListeners {
    fn drop(&mut self) {
        for (target, event, closure) in &self.listeners {
            let _ =
                target.remove_event_listener_with_callback(event, closure.as_ref().unchecked_ref());
        }
    }
}
