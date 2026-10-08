//! The sans-IO client state machine.
//!
//! The state machine never reads a clock, opens a socket, or sleeps. A driver feeds it
//! [`Input`]s and performs the [`Output`]s in order. All reconnect, heartbeat, and resume
//! logic lives here, so it is tested without a browser or a runtime.
//!
//! # Driver contract
//!
//! - [`Output::Connect`]: drop any current connection and open a new one. Report
//!   [`Input::Opened`] when the socket is open, or [`Input::Closed`] if it fails.
//! - [`Output::Close`]: close the current connection with code 1000, or cancel the attempt in
//!   progress. Do not report anything more from that connection.
//! - [`Output::SetTimer`]: start a timer. It replaces any pending timer with the same ID.
//!   Report [`Input::Timer`] when it fires. A timer the state machine no longer needs is
//!   ignored when it fires, so the driver never has to cancel one.
//! - [`Output::Send`]: send the message on the current connection.
//! - [`Output::Event`], [`Output::Reset`], [`Output::Status`]: deliver to the app.

use std::marker::PhantomData;
use std::time::Duration;

use crate::Channel;
use crate::codec;
use crate::frame::{Cursor, Message, Mode, PONG, PROTOCOL_VERSION, ServerFrame, close};

/// Client timing settings.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClientConfig {
    /// The first reconnect delay before jitter. Default: 500 ms.
    pub backoff_base: Duration,
    /// The growth factor of the reconnect delay per attempt. Default: 2.
    pub backoff_factor: u32,
    /// The largest reconnect delay before jitter. Default: 30 s.
    pub backoff_cap: Duration,
    /// How long a connection must stay open before the attempt counter resets. Default: 10 s.
    pub stable_after: Duration,
    /// How long to wait for the socket to open and `Hello` to arrive. Default: 10 s.
    pub connect_timeout: Duration,
    /// How long an open connection stays quiet before the client sends a ping. Default: 25 s.
    pub heartbeat_interval: Duration,
    /// How long to wait for a pong. Default: 10 s.
    pub heartbeat_timeout: Duration,
    /// How long to wait for a pong after [`Input::Wake`]. Default: 3 s.
    pub wake_timeout: Duration,
}

impl Default for ClientConfig {
    fn default() -> Self {
        Self {
            backoff_base: Duration::from_millis(500),
            backoff_factor: 2,
            backoff_cap: Duration::from_secs(30),
            stable_after: Duration::from_secs(10),
            connect_timeout: Duration::from_secs(10),
            heartbeat_interval: Duration::from_secs(25),
            heartbeat_timeout: Duration::from_secs(10),
            wake_timeout: Duration::from_secs(3),
        }
    }
}

/// A timer the state machine asks the driver for.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum TimerId {
    /// The backoff delay before the next connect.
    Reconnect,
    /// The limit on opening the socket and receiving `Hello`.
    ConnectTimeout,
    /// The quiet period before the next ping.
    HeartbeatSend,
    /// The limit on receiving a pong.
    HeartbeatTimeout,
    /// The time a connection must stay open before the attempt counter resets.
    Stable,
}

impl TimerId {
    /// Every timer ID.
    pub const ALL: [TimerId; 5] = [
        TimerId::Reconnect,
        TimerId::ConnectTimeout,
        TimerId::HeartbeatSend,
        TimerId::HeartbeatTimeout,
        TimerId::Stable,
    ];

    const fn index(self) -> usize {
        self as usize
    }
}

/// Something that happened, reported by the driver.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Input {
    /// Start connecting. Also restarts a stopped client.
    Start,
    /// The socket requested by [`Output::Connect`] is open.
    Opened,
    /// A message arrived on the current connection.
    Frame(Message),
    /// The current connection closed or failed to open. `None` means no close code.
    Closed {
        /// The close code, if any.
        code: Option<u16>,
    },
    /// A timer requested earlier has fired.
    Timer(TimerId),
    /// The app became visible, or the network came back.
    ///
    /// It resets the attempt counter and connects at once while waiting. It replaces a
    /// connect attempt in progress unless a wake started that attempt, and probes an open
    /// socket with a ping. It does not restart a stopped client.
    Wake,
    /// Close the connection and stop.
    Stop,
}

/// Something the driver must do or deliver.
#[derive(Clone, Debug, PartialEq)]
pub enum Output<E> {
    /// Open a connection that resumes after `cursor`.
    Connect {
        /// The cursor to put in the connect URL.
        cursor: Option<Cursor>,
    },
    /// Send a message. Only the heartbeat ping is sent.
    Send(Message),
    /// Close the current connection.
    Close,
    /// Start a timer.
    SetTimer {
        /// The timer.
        id: TimerId,
        /// The delay.
        after: Duration,
    },
    /// Deliver an event to the app.
    Event {
        /// The event's sequence number.
        seq: u64,
        /// The event.
        event: E,
    },
    /// Tell the app to discard local state and refetch.
    Reset,
    /// The connection status changed.
    Status(Status),
}

/// The connection status.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Status {
    /// Not started.
    Idle,
    /// Opening a socket and waiting for `Hello`.
    Connecting,
    /// Connected and receiving events.
    Open,
    /// Waiting before the next connect.
    Waiting {
        /// The delay before the next connect.
        retry_in: Duration,
        /// The number of the next connect attempt, counted from 1 since the last stable
        /// connection or wake.
        attempt: u32,
        /// The close code of the connection that failed, or `None` when it failed without
        /// one: a network error, a timeout, or a protocol error.
        last_code: Option<u16>,
    },
    /// The server rejected the token (close code 4401). The driver asks the token provider
    /// for a fresh token before the next connect.
    Unauthorized {
        /// The delay before the next connect.
        retry_in: Duration,
        /// The number of the next connect attempt, counted from 1 since the last stable
        /// connection or wake.
        attempt: u32,
    },
    /// Stopped for good. Only [`Input::Start`] restarts the client; a wake does not.
    Stopped {
        /// Why the client stopped.
        reason: StopReason,
    },
}

/// Why the client stopped.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum StopReason {
    /// The app stopped the client.
    App,
    /// The server closed the connection with a terminal close code, such as 4403. A `Hello`
    /// with an unsupported protocol version stops the client with 4426.
    Closed(u16),
    /// The server sent an event this client cannot decode, so the app is older than the
    /// server. Reload the app to get the new event type. Reconnecting would replay the
    /// same event.
    Incompatible {
        /// The sequence number of the event.
        seq: u64,
    },
    /// The base URL could not be resolved. The driver reports this before it connects.
    InvalidUrl,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    Connecting,
    Handshaking,
    Open,
    Waiting,
    Stopped,
}

type Rng = Box<dyn FnMut() -> f64 + Send>;

/// The client state machine for channel `C`.
pub struct Client<C: Channel> {
    config: ClientConfig,
    rng: Rng,
    cursor: Option<Cursor>,
    mode: Mode,
    phase: Phase,
    status: Status,
    attempt: u32,
    /// The connect attempt in progress was started by a wake.
    woken: bool,
    armed: [bool; TimerId::ALL.len()],
    ping_outstanding: bool,
    _channel: PhantomData<fn() -> C>,
}

impl<C: Channel> std::fmt::Debug for Client<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("channel", &C::NAME)
            .field("cursor", &self.cursor)
            .field("mode", &self.mode)
            .field("phase", &self.phase)
            .field("attempt", &self.attempt)
            .finish_non_exhaustive()
    }
}

impl<C: Channel> Client<C> {
    /// Creates a client that resumes after `since`.
    ///
    /// `rng` returns a uniform random number in `[0, 1)`. It sets the backoff jitter, and is
    /// injected so tests are deterministic.
    pub fn new(
        config: ClientConfig,
        since: Option<Cursor>,
        rng: impl FnMut() -> f64 + Send + 'static,
    ) -> Self {
        Self {
            config,
            rng: Box::new(rng),
            cursor: since,
            mode: C::MODE,
            phase: Phase::Idle,
            status: Status::Idle,
            attempt: 0,
            woken: false,
            armed: [false; TimerId::ALL.len()],
            ping_outstanding: false,
            _channel: PhantomData,
        }
    }

    /// The position of the last event delivered, or the starting cursor.
    pub fn cursor(&self) -> Option<Cursor> {
        self.cursor
    }

    /// The current status.
    pub fn status(&self) -> Status {
        self.status
    }

    /// The channel mode, as announced by the server in `Hello`.
    pub fn mode(&self) -> Mode {
        self.mode
    }

    /// Handles one input and appends the resulting outputs to `out`.
    pub fn handle(&mut self, input: Input, out: &mut Vec<Output<C::Event>>) {
        match input {
            Input::Start => {
                if matches!(self.phase, Phase::Idle | Phase::Stopped) {
                    self.attempt = 0;
                    self.connect(false, out);
                }
            }
            Input::Opened => {
                if self.phase == Phase::Connecting {
                    self.phase = Phase::Handshaking;
                }
            }
            Input::Frame(message) => self.on_frame(message, out),
            Input::Closed { code } => self.on_closed(code, out),
            Input::Timer(id) => self.on_timer(id, out),
            // Wakes come from the OS or the user, never from the server, so resetting the
            // attempt counter here cannot cause a tight loop.
            Input::Wake => match self.phase {
                Phase::Waiting => {
                    self.attempt = 0;
                    self.connect(true, out);
                }
                Phase::Connecting | Phase::Handshaking => {
                    self.attempt = 0;
                    // An attempt started before the wake likely started while offline, and
                    // is doomed. One started by a wake is left alone, because wake sources
                    // often fire together.
                    if !self.woken {
                        out.push(Output::Close);
                        self.connect(true, out);
                    }
                }
                Phase::Open => {
                    out.push(Output::Send(Message::ping()));
                    self.ping_outstanding = true;
                    self.disarm(TimerId::HeartbeatSend);
                    self.arm(TimerId::HeartbeatTimeout, self.config.wake_timeout, out);
                }
                _ => {}
            },
            Input::Stop => {
                if self.has_connection() {
                    out.push(Output::Close);
                }
                if self.phase != Phase::Stopped {
                    self.stop(StopReason::App, out);
                }
            }
        }
    }

    fn on_frame(&mut self, message: Message, out: &mut Vec<Output<C::Event>>) {
        if !matches!(self.phase, Phase::Handshaking | Phase::Open) {
            return;
        }
        let text = match message {
            Message::Text(text) => text,
            Message::Binary(_) => return self.protocol_error(out),
        };
        if text == PONG {
            if self.phase == Phase::Open && self.ping_outstanding {
                self.ping_outstanding = false;
                self.disarm(TimerId::HeartbeatTimeout);
                self.arm(TimerId::HeartbeatSend, self.config.heartbeat_interval, out);
            }
            return;
        }
        // The envelope and the event payload are decoded apart, so a malformed frame (a
        // protocol error) is told from an event this build does not know (an outdated app).
        let frame = match codec::decode_envelope(&text) {
            Ok(frame) => frame,
            // A frame type from a later protocol version: skip it.
            Err(_) if codec::is_unknown_frame(&text) => return,
            Err(_) => return self.protocol_error(out),
        };
        match (self.phase, frame) {
            (Phase::Handshaking, ServerFrame::Hello { v, mode, head }) => {
                if v != PROTOCOL_VERSION {
                    out.push(Output::Close);
                    return self.stop(StopReason::Closed(close::UNSUPPORTED_VERSION), out);
                }
                self.mode = mode;
                self.cursor = match (mode, self.cursor) {
                    // Live only: start at the head.
                    (Mode::Log, None) => Some(head),
                    // A Log cursor from another epoch is answered with Reset. Keep it until then.
                    (Mode::Log, Some(cursor)) => Some(cursor),
                    // A Latest channel sends its current value next. Accept any sequence number.
                    (Mode::Latest, None) => Some(Cursor::new(head.epoch, 0)),
                    (Mode::Latest, Some(cursor)) if cursor.epoch != head.epoch => {
                        Some(Cursor::new(head.epoch, 0))
                    }
                    (Mode::Latest, Some(cursor)) => Some(cursor),
                };
                self.phase = Phase::Open;
                self.ping_outstanding = false;
                self.disarm(TimerId::ConnectTimeout);
                self.arm(TimerId::Stable, self.config.stable_after, out);
                self.arm(TimerId::HeartbeatSend, self.config.heartbeat_interval, out);
                self.set_status(Status::Open, out);
            }
            (Phase::Open, ServerFrame::Event { seq, event }) => {
                let Some(cursor) = self.cursor else {
                    return self.protocol_error(out);
                };
                if seq <= cursor.seq {
                    return; // Duplicate: already delivered.
                }
                if self.mode == Mode::Log && seq != cursor.seq + 1 {
                    // A missed event. Reconnect and let the server replay the gap.
                    return self.protocol_error(out);
                }
                let Ok(event) = codec::decode_payload::<C::Event>(&event) else {
                    // The server knows an event type this build does not. A reconnect would
                    // replay the same event, so stop until the app restarts.
                    out.push(Output::Close);
                    return self.stop(StopReason::Incompatible { seq }, out);
                };
                self.cursor = Some(Cursor::new(cursor.epoch, seq));
                out.push(Output::Event { seq, event });
            }
            (Phase::Open, ServerFrame::Reset { head }) => {
                self.cursor = Some(head);
                out.push(Output::Reset);
            }
            _ => self.protocol_error(out),
        }
    }

    fn on_closed(&mut self, code: Option<u16>, out: &mut Vec<Output<C::Event>>) {
        if !self.has_connection() {
            return;
        }
        match code {
            Some(code) if close::is_terminal(code) => self.stop(StopReason::Closed(code), out),
            _ => self.backoff(code, out),
        }
    }

    fn on_timer(&mut self, id: TimerId, out: &mut Vec<Output<C::Event>>) {
        if !self.armed[id.index()] {
            return;
        }
        self.armed[id.index()] = false;
        match (id, self.phase) {
            (TimerId::Reconnect, Phase::Waiting) => self.connect(false, out),
            (TimerId::ConnectTimeout, Phase::Connecting | Phase::Handshaking) => {
                out.push(Output::Close);
                self.backoff(None, out);
            }
            (TimerId::Stable, Phase::Open) => self.attempt = 0,
            (TimerId::HeartbeatSend, Phase::Open) => {
                out.push(Output::Send(Message::ping()));
                self.ping_outstanding = true;
                self.arm(
                    TimerId::HeartbeatTimeout,
                    self.config.heartbeat_timeout,
                    out,
                );
            }
            (TimerId::HeartbeatTimeout, Phase::Open) => {
                out.push(Output::Close);
                self.backoff(None, out);
            }
            _ => {}
        }
    }

    fn has_connection(&self) -> bool {
        matches!(
            self.phase,
            Phase::Connecting | Phase::Handshaking | Phase::Open
        )
    }

    fn connect(&mut self, woken: bool, out: &mut Vec<Output<C::Event>>) {
        self.disarm_all();
        self.phase = Phase::Connecting;
        self.woken = woken;
        self.ping_outstanding = false;
        out.push(Output::Connect {
            cursor: self.cursor,
        });
        self.arm(TimerId::ConnectTimeout, self.config.connect_timeout, out);
        self.set_status(Status::Connecting, out);
    }

    fn protocol_error(&mut self, out: &mut Vec<Output<C::Event>>) {
        out.push(Output::Close);
        self.backoff(None, out);
    }

    /// Waits before the next connect. `code` is the close code of the failed connection.
    fn backoff(&mut self, code: Option<u16>, out: &mut Vec<Output<C::Event>>) {
        self.disarm_all();
        self.phase = Phase::Waiting;
        let retry_in = self.next_delay();
        self.attempt = self.attempt.saturating_add(1);
        let attempt = self.attempt;
        self.arm(TimerId::Reconnect, retry_in, out);
        let status = if code == Some(close::UNAUTHORIZED) {
            Status::Unauthorized { retry_in, attempt }
        } else {
            Status::Waiting {
                retry_in,
                attempt,
                last_code: code,
            }
        };
        self.set_status(status, out);
    }

    /// Exponential backoff with full jitter: a uniform delay in `[0, min(cap, base * factor^n))`.
    fn next_delay(&mut self) -> Duration {
        let base = self.config.backoff_base.as_secs_f64();
        let cap = self.config.backoff_cap.as_secs_f64();
        let exp = f64::from(self.config.backoff_factor.max(1)).powi(self.attempt.min(64) as i32);
        let ceiling = (base * exp).min(cap);
        let jitter = (self.rng)().clamp(0.0, 1.0);
        Duration::from_secs_f64(ceiling * jitter)
    }

    fn stop(&mut self, reason: StopReason, out: &mut Vec<Output<C::Event>>) {
        self.disarm_all();
        self.phase = Phase::Stopped;
        self.set_status(Status::Stopped { reason }, out);
    }

    fn arm(&mut self, id: TimerId, after: Duration, out: &mut Vec<Output<C::Event>>) {
        self.armed[id.index()] = true;
        out.push(Output::SetTimer { id, after });
    }

    fn disarm(&mut self, id: TimerId) {
        self.armed[id.index()] = false;
    }

    fn disarm_all(&mut self) {
        self.armed = [false; TimerId::ALL.len()];
    }

    fn set_status(&mut self, status: Status, out: &mut Vec<Output<C::Event>>) {
        if self.status != status {
            self.status = status;
            out.push(Output::Status(status));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
    struct Ev(u32);

    struct LogChan;
    impl Channel for LogChan {
        const NAME: &'static str = "log";
        const MODE: Mode = Mode::Log;
        type Event = Ev;
    }

    struct LatestChan;
    impl Channel for LatestChan {
        const NAME: &'static str = "latest";
        const MODE: Mode = Mode::Latest;
        type Event = Ev;
    }

    const EPOCH: u64 = 77;

    fn client<C: Channel>(since: Option<Cursor>) -> Client<C> {
        Client::new(ClientConfig::default(), since, || 0.5)
    }

    fn step<C: Channel>(c: &mut Client<C>, input: Input) -> Vec<Output<C::Event>> {
        let mut out = Vec::new();
        c.handle(input, &mut out);
        out
    }

    fn text(s: &str) -> Input {
        Input::Frame(Message::Text(s.to_owned()))
    }

    fn hello(mode: &str, head: &str) -> Input {
        text(&format!(
            r#"{{"t":"hello","v":1,"mode":"{mode}","head":"{head}"}}"#
        ))
    }

    fn event(seq: u64, v: u32) -> Input {
        text(&format!(r#"{{"t":"event","seq":{seq},"event":{v}}}"#))
    }

    fn open<C: Channel>(c: &mut Client<C>, mode: &str, head: &str) {
        step(c, Input::Start);
        step(c, Input::Opened);
        step(c, hello(mode, head));
        assert_eq!(c.status(), Status::Open);
    }

    fn delays<E>(out: &[Output<E>]) -> Vec<(TimerId, Duration)> {
        out.iter()
            .filter_map(|o| match o {
                Output::SetTimer { id, after } => Some((*id, *after)),
                _ => None,
            })
            .collect()
    }

    fn events(out: &[Output<Ev>]) -> Vec<u64> {
        out.iter()
            .filter_map(|o| match o {
                Output::Event { seq, .. } => Some(*seq),
                _ => None,
            })
            .collect()
    }

    #[test]
    fn start_connects_with_the_starting_cursor() {
        let since = Cursor::new(EPOCH, 3);
        let mut c = client::<LogChan>(Some(since));
        let out = step(&mut c, Input::Start);
        assert_eq!(
            out,
            vec![
                Output::Connect {
                    cursor: Some(since)
                },
                Output::SetTimer {
                    id: TimerId::ConnectTimeout,
                    after: Duration::from_secs(10)
                },
                Output::Status(Status::Connecting),
            ]
        );
        assert!(step(&mut c, Input::Start).is_empty(), "start is idempotent");
    }

    #[test]
    fn hello_opens_and_live_only_starts_at_head() {
        let mut c = client::<LogChan>(None);
        step(&mut c, Input::Start);
        step(&mut c, Input::Opened);
        let out = step(&mut c, hello("log", "77.12"));
        assert_eq!(
            delays(&out),
            vec![
                (TimerId::Stable, Duration::from_secs(10)),
                (TimerId::HeartbeatSend, Duration::from_secs(25)),
            ]
        );
        assert_eq!(c.cursor(), Some(Cursor::new(EPOCH, 12)));
        assert_eq!(c.status(), Status::Open);
    }

    #[test]
    fn events_advance_the_cursor_and_duplicates_are_dropped() {
        let mut c = client::<LogChan>(Some(Cursor::new(EPOCH, 3)));
        open(&mut c, "log", "77.5");
        assert_eq!(events(&step(&mut c, event(4, 4))), vec![4]);
        assert_eq!(events(&step(&mut c, event(4, 4))), Vec::<u64>::new());
        assert_eq!(events(&step(&mut c, event(3, 3))), Vec::<u64>::new());
        assert_eq!(events(&step(&mut c, event(5, 5))), vec![5]);
        assert_eq!(c.cursor(), Some(Cursor::new(EPOCH, 5)));
    }

    #[test]
    fn a_gap_in_log_mode_closes_and_reconnects_from_the_cursor() {
        let mut c = client::<LogChan>(Some(Cursor::new(EPOCH, 3)));
        open(&mut c, "log", "77.9");
        let out = step(&mut c, event(5, 5));
        assert_eq!(out[0], Output::Close);
        assert!(matches!(
            out.last(),
            Some(Output::Status(Status::Waiting { .. }))
        ));
        assert_eq!(c.cursor(), Some(Cursor::new(EPOCH, 3)));
        let out = step(&mut c, Input::Timer(TimerId::Reconnect));
        assert_eq!(
            out[0],
            Output::Connect {
                cursor: Some(Cursor::new(EPOCH, 3))
            }
        );
    }

    #[test]
    fn latest_mode_accepts_gaps() {
        let mut c = client::<LatestChan>(Some(Cursor::new(EPOCH, 3)));
        open(&mut c, "latest", "77.9");
        assert_eq!(events(&step(&mut c, event(9, 9))), vec![9]);
        assert_eq!(events(&step(&mut c, event(12, 12))), vec![12]);
    }

    #[test]
    fn latest_mode_adopts_a_new_epoch() {
        let mut c = client::<LatestChan>(Some(Cursor::new(1, 30)));
        open(&mut c, "latest", "2.4");
        assert_eq!(c.cursor(), Some(Cursor::new(2, 0)));
        assert_eq!(events(&step(&mut c, event(4, 4))), vec![4]);
    }

    #[test]
    fn latest_mode_without_a_cursor_accepts_the_current_value() {
        let mut c = client::<LatestChan>(None);
        open(&mut c, "latest", "2.4");
        assert_eq!(events(&step(&mut c, event(4, 4))), vec![4]);
    }

    #[test]
    fn reset_moves_the_cursor_to_head() {
        let mut c = client::<LogChan>(Some(Cursor::new(1, 3)));
        open(&mut c, "log", "2.40");
        let out = step(&mut c, text(r#"{"t":"reset","head":"2.40"}"#));
        assert_eq!(out, vec![Output::Reset]);
        assert_eq!(c.cursor(), Some(Cursor::new(2, 40)));
        assert_eq!(events(&step(&mut c, event(41, 1))), vec![41]);
    }

    #[test]
    fn malformed_frames_and_frames_out_of_place_are_protocol_errors() {
        for bad in [
            text("not json"),
            Input::Frame(Message::Binary(vec![1])),
            event(1, 1), // before Hello
        ] {
            let mut c = client::<LogChan>(None);
            step(&mut c, Input::Start);
            step(&mut c, Input::Opened);
            let out = step(&mut c, bad.clone());
            assert_eq!(out[0], Output::Close, "{bad:?}");
            assert!(matches!(c.status(), Status::Waiting { .. }), "{bad:?}");
        }
        let mut c = client::<LogChan>(None);
        open(&mut c, "log", "1.1");
        let out = step(&mut c, hello("log", "1.1"));
        assert_eq!(out[0], Output::Close, "a second Hello");
    }

    #[test]
    fn unknown_frame_types_are_skipped() {
        let mut c = client::<LogChan>(Some(Cursor::new(EPOCH, 3)));
        step(&mut c, Input::Start);
        step(&mut c, Input::Opened);
        // Before Hello and after it, the connection survives a frame from a later version.
        assert!(step(&mut c, text(r#"{"t":"snapshot","state":{"a":1}}"#)).is_empty());
        step(&mut c, hello("log", "77.5"));
        assert!(step(&mut c, text(r#"{"t":"presence","users":3}"#)).is_empty());
        assert_eq!(c.status(), Status::Open);
        assert_eq!(events(&step(&mut c, event(4, 4))), vec![4]);
        // A malformed frame of a known type is still a protocol error.
        let out = step(&mut c, text(r#"{"t":"event","seq":"x"}"#));
        assert_eq!(out[0], Output::Close);
    }

    #[test]
    fn unsupported_hello_version_stops() {
        let mut c = client::<LogChan>(None);
        step(&mut c, Input::Start);
        step(&mut c, Input::Opened);
        let out = step(
            &mut c,
            text(r#"{"t":"hello","v":9,"mode":"log","head":"1.1"}"#),
        );
        assert_eq!(out[0], Output::Close);
        assert_eq!(
            c.status(),
            Status::Stopped {
                reason: StopReason::Closed(close::UNSUPPORTED_VERSION)
            }
        );
    }

    #[test]
    fn backoff_is_exponential_with_full_jitter_and_capped() {
        let mut c = Client::<LogChan>::new(ClientConfig::default(), None, || 0.999_999);
        step(&mut c, Input::Start);
        let mut seen = Vec::new();
        for _ in 0..10 {
            let out = step(&mut c, Input::Closed { code: Some(1006) });
            let (_, after) = delays(&out)[0];
            seen.push(after.as_millis());
            step(&mut c, Input::Timer(TimerId::Reconnect));
        }
        assert_eq!(
            seen,
            vec![
                499, 999, 1999, 3999, 7999, 15999, 29999, 29999, 29999, 29999
            ]
        );

        let mut c = Client::<LogChan>::new(ClientConfig::default(), None, || 0.0);
        step(&mut c, Input::Start);
        let out = step(&mut c, Input::Closed { code: None });
        assert_eq!(delays(&out), vec![(TimerId::Reconnect, Duration::ZERO)]);
    }

    #[test]
    fn attempts_reset_only_after_a_stable_connection() {
        let mut c = Client::<LogChan>::new(ClientConfig::default(), None, || 0.999_999);
        step(&mut c, Input::Start);
        for _ in 0..3 {
            step(&mut c, Input::Closed { code: Some(1006) });
            step(&mut c, Input::Timer(TimerId::Reconnect));
        }
        // Accept, then close at once: no reset.
        step(&mut c, Input::Opened);
        step(&mut c, hello("log", "1.0"));
        let out = step(&mut c, Input::Closed { code: Some(1011) });
        assert_eq!(delays(&out)[0].1.as_millis(), 3999);
        step(&mut c, Input::Timer(TimerId::Reconnect));
        // Stay open past the stable timer: reset.
        step(&mut c, Input::Opened);
        step(&mut c, hello("log", "1.0"));
        step(&mut c, Input::Timer(TimerId::Stable));
        let out = step(&mut c, Input::Closed { code: Some(1006) });
        assert_eq!(delays(&out)[0].1.as_millis(), 499);
    }

    #[test]
    fn terminal_codes_stop() {
        for code in [1000, 4400, 4403, 4426] {
            let mut c = client::<LogChan>(None);
            open(&mut c, "log", "1.1");
            let out = step(&mut c, Input::Closed { code: Some(code) });
            assert_eq!(
                out,
                vec![Output::Status(Status::Stopped {
                    reason: StopReason::Closed(code)
                })]
            );
            assert!(
                step(&mut c, Input::Wake).is_empty(),
                "wake does not restart"
            );
            assert!(step(&mut c, Input::Timer(TimerId::Reconnect)).is_empty());
        }
    }

    #[test]
    fn reconnecting_codes_back_off() {
        for code in [None, Some(1006), Some(1011), Some(1012), Some(1013)] {
            let mut c = client::<LogChan>(None);
            open(&mut c, "log", "1.1");
            let out = step(&mut c, Input::Closed { code });
            assert!(
                matches!(out.last(), Some(Output::Status(Status::Waiting { .. }))),
                "{code:?}: {out:?}"
            );
        }
    }

    #[test]
    fn unauthorized_backs_off_with_its_own_status() {
        let mut c = client::<LogChan>(None);
        open(&mut c, "log", "1.1");
        let out = step(&mut c, Input::Closed { code: Some(4401) });
        assert!(matches!(
            out.last(),
            Some(Output::Status(Status::Unauthorized { .. }))
        ));
        let out = step(&mut c, Input::Timer(TimerId::Reconnect));
        assert!(matches!(out[0], Output::Connect { .. }));
    }

    #[test]
    fn heartbeat_pings_and_times_out() {
        let mut c = client::<LogChan>(None);
        open(&mut c, "log", "1.1");
        let out = step(&mut c, Input::Timer(TimerId::HeartbeatSend));
        assert_eq!(
            out,
            vec![
                Output::Send(Message::ping()),
                Output::SetTimer {
                    id: TimerId::HeartbeatTimeout,
                    after: Duration::from_secs(10)
                },
            ]
        );
        let out = step(&mut c, text("pong"));
        assert_eq!(
            delays(&out),
            vec![(TimerId::HeartbeatSend, Duration::from_secs(25))]
        );
        assert!(
            step(&mut c, Input::Timer(TimerId::HeartbeatTimeout)).is_empty(),
            "an answered ping's timeout is ignored"
        );
        step(&mut c, Input::Timer(TimerId::HeartbeatSend));
        let out = step(&mut c, Input::Timer(TimerId::HeartbeatTimeout));
        assert_eq!(out[0], Output::Close);
        assert!(matches!(c.status(), Status::Waiting { .. }));
    }

    #[test]
    fn connect_timeout_closes_and_backs_off() {
        let mut c = client::<LogChan>(None);
        step(&mut c, Input::Start);
        step(&mut c, Input::Opened);
        let out = step(&mut c, Input::Timer(TimerId::ConnectTimeout));
        assert_eq!(out[0], Output::Close);
        assert!(matches!(c.status(), Status::Waiting { .. }));
    }

    #[test]
    fn stale_timers_are_ignored() {
        let mut c = client::<LogChan>(None);
        step(&mut c, Input::Start);
        step(&mut c, Input::Opened);
        step(&mut c, hello("log", "1.1"));
        // The connect timeout of the attempt that just opened fires late.
        assert!(step(&mut c, Input::Timer(TimerId::ConnectTimeout)).is_empty());
        step(&mut c, Input::Closed { code: None });
        // The heartbeat of the closed connection fires late.
        assert!(step(&mut c, Input::Timer(TimerId::HeartbeatSend)).is_empty());
    }

    #[test]
    fn wake_connects_at_once_while_waiting() {
        let mut c = client::<LogChan>(None);
        step(&mut c, Input::Start);
        step(&mut c, Input::Closed { code: None });
        let out = step(&mut c, Input::Wake);
        assert!(matches!(out[0], Output::Connect { .. }));
        assert!(
            step(&mut c, Input::Timer(TimerId::Reconnect)).is_empty(),
            "the old backoff timer is ignored"
        );
    }

    #[test]
    fn wake_probes_an_open_socket_with_a_short_timeout() {
        let mut c = client::<LogChan>(None);
        open(&mut c, "log", "1.1");
        let out = step(&mut c, Input::Wake);
        assert_eq!(
            out,
            vec![
                Output::Send(Message::ping()),
                Output::SetTimer {
                    id: TimerId::HeartbeatTimeout,
                    after: Duration::from_secs(3)
                },
            ]
        );
        assert!(
            step(&mut c, Input::Timer(TimerId::HeartbeatSend)).is_empty(),
            "the regular heartbeat is replaced by the probe"
        );
        let out = step(&mut c, Input::Timer(TimerId::HeartbeatTimeout));
        assert_eq!(out[0], Output::Close);
    }

    #[test]
    fn stop_closes_and_start_restarts() {
        let mut c = client::<LogChan>(Some(Cursor::new(1, 1)));
        open(&mut c, "log", "1.1");
        let out = step(&mut c, Input::Stop);
        assert_eq!(
            out,
            vec![
                Output::Close,
                Output::Status(Status::Stopped {
                    reason: StopReason::App
                })
            ]
        );
        assert!(step(&mut c, Input::Stop).is_empty());
        assert!(step(&mut c, Input::Closed { code: Some(1000) }).is_empty());
        let out = step(&mut c, Input::Start);
        assert_eq!(
            out[0],
            Output::Connect {
                cursor: Some(Cursor::new(1, 1))
            }
        );
    }

    #[test]
    fn stop_while_waiting_does_not_close() {
        let mut c = client::<LogChan>(None);
        step(&mut c, Input::Start);
        step(&mut c, Input::Closed { code: None });
        let out = step(&mut c, Input::Stop);
        assert_eq!(
            out,
            vec![Output::Status(Status::Stopped {
                reason: StopReason::App
            })]
        );
    }

    #[test]
    fn inputs_for_no_connection_are_ignored() {
        let mut c = client::<LogChan>(None);
        assert!(step(&mut c, Input::Opened).is_empty());
        assert!(step(&mut c, event(1, 1)).is_empty());
        assert!(step(&mut c, Input::Closed { code: None }).is_empty());
        assert!(step(&mut c, Input::Wake).is_empty());
    }

    /// An event of a variant this build does not know: `Ev` is a number.
    fn new_variant(seq: u64) -> Input {
        text(&format!(
            r#"{{"t":"event","seq":{seq},"event":{{"type":"added_later"}}}}"#
        ))
    }

    fn assert_incompatible<C: Channel<Event = Ev>>(
        mode: &str,
        since: Cursor,
        head: &str,
        seq: u64,
    ) {
        let mut c = client::<C>(Some(since));
        open(&mut c, mode, head);
        let out = step(&mut c, new_variant(seq));
        assert_eq!(
            out,
            vec![
                Output::Close,
                Output::Status(Status::Stopped {
                    reason: StopReason::Incompatible { seq }
                })
            ]
        );
        assert_eq!(c.cursor(), Some(since), "the event is not consumed");
        assert!(
            step(&mut c, Input::Wake).is_empty(),
            "wake does not restart"
        );
        for id in TimerId::ALL {
            assert!(step(&mut c, Input::Timer(id)).is_empty(), "{id:?}");
        }
        assert!(step(&mut c, Input::Closed { code: None }).is_empty());
        // Only an app restart connects again, from the same cursor.
        let out = step(&mut c, Input::Start);
        assert_eq!(
            out[0],
            Output::Connect {
                cursor: Some(since)
            }
        );
    }

    #[test]
    fn an_undecodable_event_stops_instead_of_reconnecting() {
        assert_incompatible::<LogChan>("log", Cursor::new(EPOCH, 3), "77.4", 4);
    }

    #[test]
    fn an_undecodable_latest_value_stops_instead_of_reconnecting() {
        assert_incompatible::<LatestChan>("latest", Cursor::new(EPOCH, 3), "77.9", 9);
    }

    #[test]
    fn an_undecodable_event_is_checked_only_when_it_would_be_delivered() {
        let mut c = client::<LogChan>(Some(Cursor::new(EPOCH, 3)));
        open(&mut c, "log", "77.9");
        assert!(
            step(&mut c, new_variant(3)).is_empty(),
            "a duplicate is dropped"
        );
        let out = step(&mut c, new_variant(6));
        assert_eq!(out[0], Output::Close, "a gap is a protocol error first");
        assert!(matches!(c.status(), Status::Waiting { .. }));
        // An event frame without its payload is malformed, not incompatible.
        let mut c = client::<LogChan>(Some(Cursor::new(EPOCH, 3)));
        open(&mut c, "log", "77.9");
        step(&mut c, text(r#"{"t":"event","seq":4}"#));
        assert!(matches!(c.status(), Status::Waiting { .. }));
    }

    #[test]
    fn waiting_reports_the_attempt_and_the_close_code() {
        let mut c = Client::<LogChan>::new(ClientConfig::default(), None, || 0.999_999);
        step(&mut c, Input::Start);
        step(&mut c, Input::Closed { code: Some(1006) });
        assert!(matches!(
            c.status(),
            Status::Waiting {
                attempt: 1,
                last_code: Some(1006),
                ..
            }
        ));
        step(&mut c, Input::Timer(TimerId::Reconnect));
        step(&mut c, Input::Timer(TimerId::ConnectTimeout));
        assert!(matches!(
            c.status(),
            Status::Waiting {
                attempt: 2,
                last_code: None,
                ..
            }
        ));
        step(&mut c, Input::Timer(TimerId::Reconnect));
        step(&mut c, Input::Closed { code: Some(4401) });
        assert!(matches!(
            c.status(),
            Status::Unauthorized { attempt: 3, .. }
        ));
    }

    /// Fails `n` connect attempts in a row with the slowest jitter.
    fn fail_attempts(c: &mut Client<LogChan>, n: usize) {
        for _ in 0..n {
            step(c, Input::Closed { code: Some(1006) });
            step(c, Input::Timer(TimerId::Reconnect));
        }
    }

    #[test]
    fn wake_while_waiting_resets_the_attempt_counter() {
        let mut c = Client::<LogChan>::new(ClientConfig::default(), None, || 0.999_999);
        step(&mut c, Input::Start);
        fail_attempts(&mut c, 8);
        let out = step(&mut c, Input::Closed { code: Some(1006) });
        assert_eq!(delays(&out)[0].1.as_millis(), 29999, "at the cap");
        let out = step(&mut c, Input::Wake);
        assert!(matches!(out[0], Output::Connect { .. }));
        let out = step(&mut c, Input::Closed { code: Some(1006) });
        assert_eq!(delays(&out)[0].1.as_millis(), 499, "back at the base");
        assert!(matches!(c.status(), Status::Waiting { attempt: 1, .. }));
    }

    #[test]
    fn wake_replaces_a_connect_attempt_not_started_by_a_wake() {
        for opened in [false, true] {
            let mut c = Client::<LogChan>::new(ClientConfig::default(), None, || 0.999_999);
            step(&mut c, Input::Start);
            fail_attempts(&mut c, 8);
            if opened {
                step(&mut c, Input::Opened);
            }
            let out = step(&mut c, Input::Wake);
            assert_eq!(
                out,
                vec![
                    Output::Close,
                    Output::Connect { cursor: None },
                    Output::SetTimer {
                        id: TimerId::ConnectTimeout,
                        after: Duration::from_secs(10)
                    },
                ],
                "opened: {opened}"
            );
            let out = step(&mut c, Input::Closed { code: Some(1006) });
            assert_eq!(delays(&out)[0].1.as_millis(), 499, "the counter reset");
        }
    }

    #[test]
    fn wake_leaves_a_connect_attempt_started_by_a_wake_alone() {
        let mut c = Client::<LogChan>::new(ClientConfig::default(), None, || 0.999_999);
        step(&mut c, Input::Start);
        step(&mut c, Input::Closed { code: None });
        assert!(matches!(
            step(&mut c, Input::Wake)[0],
            Output::Connect { .. }
        ));
        // `visibilitychange` and `online` often fire together.
        assert!(step(&mut c, Input::Wake).is_empty());
        step(&mut c, Input::Opened);
        assert!(step(&mut c, Input::Wake).is_empty());
        step(&mut c, hello("log", "1.1"));
        assert_eq!(c.status(), Status::Open);
    }

    #[test]
    fn a_wake_reset_still_needs_a_stable_connection_for_server_closes() {
        let mut c = Client::<LogChan>::new(ClientConfig::default(), None, || 0.999_999);
        step(&mut c, Input::Start);
        step(&mut c, Input::Closed { code: None });
        step(&mut c, Input::Wake);
        // The server accepts and closes at once, again and again: the backoff still grows.
        let mut seen = Vec::new();
        for _ in 0..3 {
            step(&mut c, Input::Opened);
            step(&mut c, hello("log", "1.0"));
            let out = step(&mut c, Input::Closed { code: Some(1011) });
            seen.push(delays(&out)[0].1.as_millis());
            step(&mut c, Input::Timer(TimerId::Reconnect));
        }
        assert_eq!(seen, vec![499, 999, 1999]);
    }
}
