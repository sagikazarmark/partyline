//! A loopback harness that runs the [`Client`] state machine against the server logic over a
//! faulty in-memory pipe, in virtual time.
//!
//! The pipe can drop the connection at any frame, duplicate frames, delay them, and fail
//! connect attempts. The server side uses [`MemLog`] with the same [`server::handshake`] and
//! [`server::publish`] functions the Workers hub uses.
//!
//! ```
//! use partyline::testing::{Faults, Loopback};
//! use partyline::{Channel, Mode};
//!
//! struct Counter;
//! impl Channel for Counter {
//!     const NAME: &'static str = "counter";
//!     const MODE: Mode = Mode::Log;
//!     type Event = u64;
//! }
//!
//! let mut lb = Loopback::<Counter>::new(1, Faults::default());
//! lb.start();
//! for n in 1..=20 {
//!     lb.publish(&n);
//!     lb.advance_ms(700);
//! }
//! lb.heal();
//! lb.settle();
//! assert_eq!(lb.client().cursor(), Some(lb.head()));
//! ```

use std::collections::{BTreeMap, VecDeque};
use std::time::Duration;

use crate::client::{Client, ClientConfig, Input, Output, Status, TimerId};
use crate::codec;
use crate::frame::{Cursor, Message, PING, PONG, ServerFrame, close};
use crate::server::{self, Log, MemLog, Retention};
use crate::{Channel, Mode};

/// A small deterministic random number generator (SplitMix64).
#[derive(Clone, Debug)]
pub struct TestRng(u64);

impl TestRng {
    /// Creates a generator from a seed.
    pub fn new(seed: u64) -> Self {
        Self(seed)
    }

    /// Returns the next 64 random bits.
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns a uniform number in `[0, 1)`.
    pub fn next_f64(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }

    /// Returns `true` with probability `p`.
    pub fn chance(&mut self, p: f64) -> bool {
        p > 0.0 && self.next_f64() < p
    }

    /// Returns a uniform number in `[0, max]`.
    pub fn up_to(&mut self, max: u64) -> u64 {
        if max == 0 {
            0
        } else {
            self.next_u64() % (max + 1)
        }
    }
}

/// The faults the pipe injects.
#[derive(Clone, Debug, PartialEq)]
pub struct Faults {
    /// The probability that the connection drops before a frame is delivered.
    pub drop: f64,
    /// The probability that a single frame is lost while the connection stays up, as a
    /// faulty proxy or server bug would cause.
    pub lose: f64,
    /// The probability that a delivered frame is delivered twice.
    pub duplicate: f64,
    /// The maximum extra delay of a frame. Frames stay in order.
    pub max_delay: Duration,
    /// The probability that a connect attempt fails.
    pub connect_fail: f64,
}

impl Faults {
    /// A pipe that never fails.
    pub const NONE: Self = Self {
        drop: 0.0,
        lose: 0.0,
        duplicate: 0.0,
        max_delay: Duration::ZERO,
        connect_fail: 0.0,
    };
}

impl Default for Faults {
    /// A moderately hostile pipe.
    fn default() -> Self {
        Self {
            drop: 0.05,
            lose: 0.01,
            duplicate: 0.05,
            max_delay: Duration::from_millis(500),
            connect_fail: 0.1,
        }
    }
}

/// What the app saw, in order.
#[derive(Clone, Debug, PartialEq)]
pub enum Observed<E> {
    /// An event was delivered.
    Event {
        /// The sequence number.
        seq: u64,
        /// The event.
        event: E,
    },
    /// A reset was delivered. `cursor` is the client cursor right after it.
    Reset {
        /// The client cursor after the reset.
        cursor: Option<Cursor>,
    },
    /// The status changed.
    Status(Status),
}

/// Statistics about a run.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Stats {
    /// Connect attempts.
    pub connects: u64,
    /// Connections dropped by the pipe.
    pub drops: u64,
    /// Frames delivered twice.
    pub duplicates: u64,
    /// Frames lost while the connection stayed up.
    pub lost: u64,
    /// Frames delivered.
    pub frames: u64,
}

const CONNECT_LATENCY_MS: u64 = 20;

#[derive(Debug)]
enum Conn {
    Connecting {
        ready_at: u64,
        cursor: Option<Cursor>,
    },
    Open {
        inbound: VecDeque<(u64, Message)>,
    },
}

/// A client state machine wired to an in-memory server.
pub struct Loopback<C: Channel> {
    client: Client<C>,
    mode: Mode,
    log: MemLog,
    retention: Retention,
    now: u64,
    timers: BTreeMap<TimerId, u64>,
    conn: Option<Conn>,
    faults: Faults,
    rng: TestRng,
    observed: Vec<Observed<C::Event>>,
    stats: Stats,
    epochs: u64,
}

impl<C: Channel> std::fmt::Debug for Loopback<C> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Loopback")
            .field("client", &self.client)
            .field("now", &self.now)
            .field("head", &self.head())
            .field("stats", &self.stats)
            .finish_non_exhaustive()
    }
}

impl<C: Channel> Loopback<C> {
    /// Creates a harness whose client starts at the server's head, so it must see every
    /// event published afterwards.
    pub fn new(seed: u64, faults: Faults) -> Self {
        let log = MemLog::new(1);
        let since = log.head().ok();
        Self::with_options(
            seed,
            faults,
            Retention::for_mode(C::MODE),
            ClientConfig::default(),
            since,
        )
    }

    /// Creates a harness with every setting explicit.
    pub fn with_options(
        seed: u64,
        faults: Faults,
        retention: Retention,
        config: ClientConfig,
        since: Option<Cursor>,
    ) -> Self {
        let mut rng = TestRng::new(seed);
        let mut client_rng = TestRng::new(rng.next_u64());
        Self {
            client: Client::new(config, since, move || client_rng.next_f64()),
            mode: C::MODE,
            log: MemLog::new(1),
            retention,
            now: 0,
            timers: BTreeMap::new(),
            conn: None,
            faults,
            rng,
            observed: Vec::new(),
            stats: Stats::default(),
            epochs: 1,
        }
    }

    /// The client state machine.
    pub fn client(&self) -> &Client<C> {
        &self.client
    }

    /// Everything the app saw so far.
    pub fn observed(&self) -> &[Observed<C::Event>] {
        &self.observed
    }

    /// The events the app saw, in order.
    pub fn events(&self) -> Vec<(u64, C::Event)> {
        self.observed
            .iter()
            .filter_map(|o| match o {
                Observed::Event { seq, event } => Some((*seq, event.clone())),
                _ => None,
            })
            .collect()
    }

    /// Run statistics.
    pub fn stats(&self) -> Stats {
        self.stats
    }

    /// The server head.
    pub fn head(&self) -> Cursor {
        self.log.head().unwrap_or_else(|never| match never {})
    }

    /// The virtual time in milliseconds.
    pub fn now_ms(&self) -> u64 {
        self.now
    }

    /// The server log.
    pub fn log(&self) -> &MemLog {
        &self.log
    }

    /// Replaces the fault settings.
    pub fn set_faults(&mut self, faults: Faults) {
        self.faults = faults;
    }

    /// Turns all faults off.
    pub fn heal(&mut self) {
        self.faults = Faults::NONE;
    }

    /// Starts the client.
    pub fn start(&mut self) {
        self.input(Input::Start);
    }

    /// Stops the client.
    pub fn stop(&mut self) {
        self.input(Input::Stop);
    }

    /// Sends a wake signal to the client.
    pub fn wake(&mut self) {
        self.input(Input::Wake);
    }

    /// Publishes an event on the server and returns its sequence number.
    pub fn publish(&mut self, event: &C::Event) -> u64 {
        let body = codec::encode_event(event).expect("event encodes");
        self.publish_body(&body)
    }

    /// Publishes an encoded event body on the server and returns its sequence number.
    ///
    /// The body need not decode as `C::Event`, so a test can play a server that is newer
    /// than the client.
    pub fn publish_body(&mut self, body: &[u8]) -> u64 {
        let (head, frame) = server::publish(&mut self.log, body, &self.retention, self.now)
            .expect("publish succeeds");
        self.send_to_client(Message::Text(frame));
        head.seq
    }

    /// Wipes the server log with a new epoch and closes the connection with 1012, as the
    /// Workers hub does on reset.
    pub fn reset_server(&mut self) {
        self.epochs += 1;
        self.log.reset(self.epochs);
        if self.conn.take().is_some() {
            self.input(Input::Closed {
                code: Some(close::SERVICE_RESTART),
            });
        }
    }

    /// Breaks the connection, as a network loss would.
    pub fn disconnect(&mut self) {
        if self.conn.take().is_some() {
            self.stats.drops += 1;
            self.input(Input::Closed {
                code: Some(close::ABNORMAL),
            });
        }
    }

    /// Closes the connection from the server with the given code.
    pub fn server_close(&mut self, code: u16) {
        if self.conn.take().is_some() {
            self.input(Input::Closed { code: Some(code) });
        }
    }

    /// Advances virtual time, processing everything due on the way.
    pub fn advance_ms(&mut self, ms: u64) {
        let deadline = self.now + ms;
        while let Some(at) = self.next_due().filter(|at| *at <= deadline) {
            self.now = self.now.max(at);
            self.fire_due();
        }
        self.now = deadline;
    }

    /// Advances virtual time by `d`.
    pub fn advance(&mut self, d: Duration) {
        self.advance_ms(d.as_millis() as u64);
    }

    /// Advances time until the client has been open and idle long enough to have resumed:
    /// two minutes, which covers the largest backoff delay.
    pub fn settle(&mut self) {
        self.advance(Duration::from_secs(120));
    }

    fn next_due(&self) -> Option<u64> {
        let timer = self.timers.values().min().copied();
        let conn = match &self.conn {
            Some(Conn::Connecting { ready_at, .. }) => Some(*ready_at),
            Some(Conn::Open { inbound }) => inbound.front().map(|(at, _)| *at),
            None => None,
        };
        [timer, conn].into_iter().flatten().min()
    }

    fn fire_due(&mut self) {
        // The connection goes first, so a frame due at the same time as a timer arrives first.
        match &mut self.conn {
            Some(Conn::Connecting { ready_at, cursor }) if *ready_at <= self.now => {
                let cursor = *cursor;
                return self.accept(cursor);
            }
            Some(Conn::Open { inbound })
                if inbound.front().is_some_and(|(at, _)| *at <= self.now) =>
            {
                let (_, message) = inbound.pop_front().expect("front exists");
                return self.deliver(message);
            }
            _ => {}
        }
        let due = self
            .timers
            .iter()
            .find(|(_, at)| **at <= self.now)
            .map(|(id, _)| *id);
        if let Some(id) = due {
            self.timers.remove(&id);
            self.input(Input::Timer(id));
        }
    }

    fn accept(&mut self, cursor: Option<Cursor>) {
        if self.rng.chance(self.faults.connect_fail) {
            self.conn = None;
            return self.input(Input::Closed { code: None });
        }
        // Accepting the socket and building the handshake is one atomic step on the server.
        let frames = server::handshake(self.mode, cursor, &self.log).expect("handshake succeeds");
        self.conn = Some(Conn::Open {
            inbound: VecDeque::new(),
        });
        for frame in frames {
            self.send_to_client(Message::Text(frame));
        }
        self.input(Input::Opened);
    }

    fn deliver(&mut self, message: Message) {
        if self.rng.chance(self.faults.drop) {
            return self.disconnect();
        }
        if self.rng.chance(self.faults.lose) {
            self.stats.lost += 1;
            return;
        }
        let copies = if self.rng.chance(self.faults.duplicate) {
            self.stats.duplicates += 1;
            2
        } else {
            1
        };
        for _ in 0..copies {
            if self.conn.is_none() {
                break;
            }
            self.stats.frames += 1;
            self.input(Input::Frame(message.clone()));
        }
    }

    fn send_to_client(&mut self, message: Message) {
        let delay = self.rng.up_to(self.faults.max_delay.as_millis() as u64);
        let now = self.now;
        if let Some(Conn::Open { inbound }) = &mut self.conn {
            let after_last = inbound.back().map_or(now, |(at, _)| *at);
            inbound.push_back(((now + delay).max(after_last), message));
        }
    }

    fn input(&mut self, input: Input) {
        let mut out = Vec::new();
        self.client.handle(input, &mut out);
        for output in out {
            self.perform(output);
        }
    }

    fn perform(&mut self, output: Output<C::Event>) {
        match output {
            Output::Connect { cursor } => {
                self.stats.connects += 1;
                self.conn = Some(Conn::Connecting {
                    ready_at: self.now + CONNECT_LATENCY_MS,
                    cursor,
                });
            }
            Output::Send(Message::Text(text)) if text == PING => {
                // The server answers pings with an auto-response.
                self.send_to_client(Message::Text(PONG.to_owned()));
            }
            Output::Send(_) => {}
            Output::Close => self.conn = None,
            Output::SetTimer { id, after } => {
                self.timers.insert(id, self.now + after.as_millis() as u64);
            }
            Output::Event { seq, event } => self.observed.push(Observed::Event { seq, event }),
            Output::Reset => self.observed.push(Observed::Reset {
                cursor: self.client.cursor(),
            }),
            Output::Status(status) => self.observed.push(Observed::Status(status)),
        }
    }
}

/// Checks the Log-mode delivery guarantee over everything the app saw: events arrive exactly
/// once and in order, starting after `start`, except that a `Reset` moves the expected
/// position to its cursor.
///
/// Returns the sequence number of the last event the app accounted for.
pub fn check_log_delivery<E>(start: u64, observed: &[Observed<E>]) -> Result<u64, String> {
    let mut last = start;
    for (i, o) in observed.iter().enumerate() {
        match o {
            Observed::Event { seq, .. } if *seq == last + 1 => last = *seq,
            Observed::Event { seq, .. } => {
                return Err(format!(
                    "observation {i}: event {seq} delivered, expected {}",
                    last + 1
                ));
            }
            Observed::Reset { cursor } => {
                let seq = cursor.map_or(0, |c| c.seq);
                last = seq;
            }
            Observed::Status(_) => {}
        }
    }
    Ok(last)
}

/// Checks the Latest-mode delivery guarantee: sequence numbers strictly increase.
///
/// Returns the sequence number of the last event the app saw.
pub fn check_latest_delivery<E>(start: u64, observed: &[Observed<E>]) -> Result<u64, String> {
    let mut last = start;
    for (i, o) in observed.iter().enumerate() {
        if let Observed::Event { seq, .. } = o {
            if *seq <= last {
                return Err(format!(
                    "observation {i}: event {seq} delivered after {last}"
                ));
            }
            last = *seq;
        }
    }
    Ok(last)
}

/// Decodes a frame from a message, for tests that inspect frames.
pub fn decode<E: serde::de::DeserializeOwned>(message: &Message) -> Option<ServerFrame<E>> {
    match message {
        Message::Text(text) => codec::decode_frame(text).ok(),
        Message::Binary(_) => None,
    }
}
