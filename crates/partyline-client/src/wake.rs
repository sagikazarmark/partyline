//! Browser wake sources: `visibilitychange` (when the page becomes visible) and `online`.

use futures::channel::mpsc::UnboundedSender;
use wasm_bindgen::JsCast;
use wasm_bindgen::closure::Closure;
use web_sys::{EventTarget, VisibilityState};

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
        let mut listeners = Vec::new();

        let visible = {
            let commands = commands.clone();
            let document = document.clone();
            Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
                if document.visibility_state() == VisibilityState::Visible {
                    let _ = commands.unbounded_send(Command::Wake);
                }
            })
        };
        listeners.push((
            document.unchecked_into::<EventTarget>(),
            "visibilitychange",
            visible,
        ));

        let online = Closure::<dyn FnMut(web_sys::Event)>::new(move |_| {
            let _ = commands.unbounded_send(Command::Wake);
        });
        listeners.push((window.unchecked_into::<EventTarget>(), "online", online));

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
