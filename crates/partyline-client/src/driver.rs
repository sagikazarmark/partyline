//! The driver loop: one `select` over the socket, the pending timers, and the command
//! channel. Each wake-up becomes one [`Input`], and each [`Output`] is performed in order.
//! It contains no protocol decisions.

use std::collections::{BTreeMap, VecDeque};
use std::future::Future;
use std::pin::pin;
use std::task::Poll;
use std::time::Duration;

use futures::channel::mpsc::{UnboundedReceiver, UnboundedSender};
use futures::future::{poll_fn, select};
use futures::{FutureExt, SinkExt, StreamExt};
use futures_timer::Delay;
use partyline::frame::{ConnectParams, connect_path};
use partyline::{Channel, Client, Cursor, Input, Output, Status, StopReason, TimerId};

use crate::transport::{Transport, TransportError};
use crate::{ClientEvent, Command, ConnectOptions, Shared, TokenProvider, TokenRequest};

/// How long a clean close may take before the socket is dropped.
const CLOSE_TIMEOUT: Duration = Duration::from_secs(1);

pub(crate) async fn run<C: Channel, T: Transport>(
    transport: T,
    options: ConnectOptions,
    mut commands: UnboundedReceiver<Command>,
    events: UnboundedSender<ClientEvent<C::Event>>,
    shared: Shared,
) {
    // Wake listeners get their own channel, so the command stream still ends when every
    // handle is dropped.
    #[cfg(all(target_arch = "wasm32", feature = "web-wake"))]
    let (mut wakes, _wake_listeners) = {
        let (tx, rx) = futures::channel::mpsc::unbounded();
        (rx, crate::wake::WakeListeners::install(tx))
    };

    let ConnectOptions {
        base_url,
        id,
        since,
        token,
        config,
    } = options;
    let log = Log {
        channel: C::NAME,
        id: &id,
    };
    let base = match base_url.resolve() {
        Ok(base) => base,
        Err(e) => {
            log_error(&format!("partyline: {e}"));
            let status = Status::Stopped {
                reason: StopReason::InvalidUrl,
            };
            shared.set_status(status);
            let _ = events.unbounded_send(ClientEvent::Status(status));
            return;
        }
    };
    let url = format!("{base}{}", connect_path(C::NAME, &id));

    let mut client = Client::<C>::new(config, since, crate::random());
    let mut conn: Option<T::Conn> = None;
    let mut connecting = None;
    let mut timers: BTreeMap<TimerId, Delay> = BTreeMap::new();
    let mut pending: VecDeque<Input> = VecDeque::from([Input::Start]);
    let mut refresh_token = false;
    let mut out = Vec::new();

    loop {
        let input = match pending.pop_front() {
            Some(input) => input,
            None => {
                poll_fn(|cx| {
                    match commands.poll_next_unpin(cx) {
                        Poll::Ready(Some(Command::Wake)) => return Poll::Ready(Input::Wake),
                        Poll::Ready(Some(Command::Stop) | None) => return Poll::Ready(Input::Stop),
                        Poll::Pending => {}
                    }
                    #[cfg(all(target_arch = "wasm32", feature = "web-wake"))]
                    if let Poll::Ready(Some(_)) = wakes.poll_next_unpin(cx) {
                        return Poll::Ready(Input::Wake);
                    }
                    let fired = timers
                        .iter_mut()
                        .find_map(|(id, delay)| delay.poll_unpin(cx).is_ready().then_some(*id));
                    if let Some(id) = fired {
                        timers.remove(&id);
                        return Poll::Ready(Input::Timer(id));
                    }
                    if let Some(attempt) = connecting.as_mut()
                        && let Poll::Ready(result) =
                            Future::poll(std::pin::Pin::as_mut(attempt), cx)
                    {
                        connecting = None;
                        return Poll::Ready(match result {
                            Ok(c) => {
                                conn = Some(c);
                                Input::Opened
                            }
                            Err(error) => {
                                log.transport_error("connect", &error);
                                Input::Closed { code: None }
                            }
                        });
                    }
                    if let Some(c) = conn.as_mut() {
                        match c.poll_next_unpin(cx) {
                            Poll::Ready(Some(Ok(message))) => {
                                return Poll::Ready(Input::Frame(message));
                            }
                            Poll::Ready(Some(Err(TransportError::Closed { code }))) => {
                                conn = None;
                                return Poll::Ready(Input::Closed { code });
                            }
                            Poll::Ready(Some(Err(error))) => {
                                conn = None;
                                log.transport_error("receive", &error);
                                return Poll::Ready(Input::Closed { code: None });
                            }
                            Poll::Ready(None) => {
                                conn = None;
                                return Poll::Ready(Input::Closed { code: None });
                            }
                            Poll::Pending => {}
                        }
                    }
                    Poll::Pending
                })
                .await
            }
        };

        log.input(&input);
        client.handle(input, &mut out);
        let mut stopped = false;
        for output in out.drain(..) {
            log.output(&output);
            match output {
                Output::Connect { cursor } => {
                    conn = None;
                    let request = TokenRequest {
                        refresh: std::mem::take(&mut refresh_token),
                    };
                    connecting = Some(Box::pin(open(
                        &transport,
                        &url,
                        cursor,
                        token.clone(),
                        request,
                    )));
                }
                Output::Send(message) => {
                    if let Some(c) = conn.as_mut()
                        && let Err(error) = c.send(message).await
                    {
                        log.transport_error("send", &error);
                        conn = None;
                        pending.push_back(Input::Closed { code: None });
                    }
                }
                Output::Close => {
                    connecting = None;
                    if let Some(mut c) = conn.take() {
                        let closing = pin!(c.close());
                        if let futures::future::Either::Left((Err(error), _)) =
                            select(closing, Delay::new(CLOSE_TIMEOUT)).await
                        {
                            log.transport_error("close", &error);
                        }
                    }
                }
                Output::SetTimer { id, after } => {
                    timers.insert(id, Delay::new(after));
                }
                Output::Event { seq, event } => {
                    let _ = events.unbounded_send(ClientEvent::Event { seq, event });
                }
                Output::Reset => {
                    let _ = events.unbounded_send(ClientEvent::Reset);
                }
                Output::Status(status) => {
                    if matches!(status, Status::Unauthorized { .. }) {
                        refresh_token = true;
                    }
                    stopped = matches!(status, Status::Stopped { .. });
                    shared.set_status(status);
                    let _ = events.unbounded_send(ClientEvent::Status(status));
                }
            }
        }
        shared.set_cursor(client.cursor());
        if stopped {
            break;
        }
    }
}

/// Asks the token provider, then opens the socket.
async fn open<T: Transport>(
    transport: &T,
    url: &str,
    cursor: Option<Cursor>,
    token: Option<TokenProvider>,
    request: TokenRequest,
) -> Result<T::Conn, TransportError> {
    let token = match token {
        Some(provider) => provider.get(request).await,
        None => None,
    };
    let url = format!("{url}?{}", ConnectParams::new(cursor, token).to_query());
    transport.connect(&url).await
}

/// Driver logging. It logs only with the `tracing` feature.
#[derive(Clone, Copy)]
#[cfg_attr(not(feature = "tracing"), allow(dead_code))]
struct Log<'a> {
    channel: &'static str,
    id: &'a str,
}

#[cfg(feature = "tracing")]
impl Log<'_> {
    fn input(&self, input: &Input) {
        tracing::trace!(
            channel = self.channel,
            id = self.id,
            ?input,
            "partyline input"
        );
    }

    fn output<E>(&self, output: &Output<E>) {
        tracing::trace!(
            channel = self.channel,
            id = self.id,
            output = ?OutputSummary(output),
            "partyline output"
        );
    }

    fn transport_error(&self, during: &str, error: &TransportError) {
        tracing::debug!(
            channel = self.channel,
            id = self.id,
            during,
            %error,
            "partyline transport error"
        );
    }
}

#[cfg(not(feature = "tracing"))]
impl Log<'_> {
    fn input(&self, _: &Input) {}

    fn output<E>(&self, _: &Output<E>) {}

    fn transport_error(&self, _: &str, _: &TransportError) {}
}

/// An output without its event payload, which need not implement `Debug`.
#[cfg(feature = "tracing")]
struct OutputSummary<'a, E>(&'a Output<E>);

#[cfg(feature = "tracing")]
impl<E> std::fmt::Debug for OutputSummary<'_, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Output::Connect { cursor } => {
                f.debug_struct("Connect").field("cursor", cursor).finish()
            }
            Output::Send(message) => f.debug_tuple("Send").field(message).finish(),
            Output::Close => f.write_str("Close"),
            Output::SetTimer { id, after } => f
                .debug_struct("SetTimer")
                .field("id", id)
                .field("after", after)
                .finish(),
            Output::Event { seq, .. } => f
                .debug_struct("Event")
                .field("seq", seq)
                .finish_non_exhaustive(),
            Output::Reset => f.write_str("Reset"),
            Output::Status(status) => f.debug_tuple("Status").field(status).finish(),
        }
    }
}

fn log_error(message: &str) {
    #[cfg(target_arch = "wasm32")]
    web_sys_console_error(message);
    #[cfg(not(target_arch = "wasm32"))]
    eprintln!("{message}");
}

#[cfg(target_arch = "wasm32")]
fn web_sys_console_error(message: &str) {
    #[wasm_bindgen::prelude::wasm_bindgen]
    extern "C" {
        #[wasm_bindgen(js_namespace = console, js_name = error)]
        fn console_error(s: &str);
    }
    console_error(message);
}
