//! Server logic with no I/O: the replay decision, the retention policy, and the [`Log`] trait.
//!
//! A backend stores events through a [`Log`] and calls [`handshake`] and [`publish`] to get
//! the exact frames to send. The Workers hub and the loopback test harness both use these
//! functions, so the tested rules are the deployed rules.

use std::collections::VecDeque;
use std::convert::Infallible;
use std::time::Duration;

use crate::codec::{self, CodecError};
use crate::frame::{Cursor, Mode, PROTOCOL_VERSION, ServerFrame};

/// How many events a channel keeps for replay.
///
/// An event is removed when it is more than `max_events` behind the head, or older than
/// `max_age`, whichever comes first. A cursor older than the retained window gets `Reset`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Retention {
    /// The maximum number of events to keep. Always at least 1.
    pub max_events: u64,
    /// The maximum age of an event. `None` keeps events regardless of age.
    pub max_age: Option<Duration>,
}

impl Retention {
    /// The default for [`Mode::Log`]: 1,000 events or 24 hours, whichever is smaller.
    pub const LOG_DEFAULT: Self = Self {
        max_events: 1_000,
        max_age: Some(Duration::from_secs(24 * 60 * 60)),
    };

    /// The fixed policy for [`Mode::Latest`]: one event, with no age limit.
    pub const LATEST: Self = Self {
        max_events: 1,
        max_age: None,
    };

    /// The default policy for a mode.
    pub const fn for_mode(mode: Mode) -> Self {
        match mode {
            Mode::Log => Self::LOG_DEFAULT,
            Mode::Latest => Self::LATEST,
        }
    }

    /// The highest sequence number that count-based trimming removes, if any.
    pub fn count_cutoff(&self, head: u64) -> Option<u64> {
        head.checked_sub(self.max_events.max(1))
            .filter(|&cutoff| cutoff > 0)
    }

    /// Sets the maximum number of events. Values below 1 are raised to 1.
    pub const fn with_max_events(mut self, max_events: u64) -> Self {
        self.max_events = if max_events == 0 { 1 } else { max_events };
        self
    }

    /// Sets the maximum event age. `None` removes the age limit.
    pub const fn with_max_age(mut self, max_age: Option<Duration>) -> Self {
        self.max_age = max_age;
        self
    }

    /// The timestamp before which age-based trimming removes events, if any.
    pub fn age_cutoff(&self, now_ms: u64) -> Option<u64> {
        self.max_age
            .map(|age| now_ms.saturating_sub(age.as_millis() as u64))
    }
}

/// Size limits on events and replays.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Limits {
    /// The largest encoded event a publish accepts, in bytes.
    pub max_event_bytes: usize,
    /// The most event bytes a reconnect replays. A longer replay gets `Reset` instead, and
    /// the client refetches.
    pub max_replay_bytes: usize,
}

impl Limits {
    /// The defaults: 64 KiB per event and 8 MiB per replay.
    pub const DEFAULT: Self = Self {
        max_event_bytes: 64 * 1024,
        max_replay_bytes: 8 * 1024 * 1024,
    };
}

impl Default for Limits {
    fn default() -> Self {
        Self::DEFAULT
    }
}

/// Storage for a channel's event log.
///
/// Sequence numbers start at 1 and increase by 1 per append. Trimming only removes events
/// from the old end, so the retained events are always contiguous.
pub trait Log {
    /// The storage error.
    type Error;

    /// The current head: the epoch and the sequence number of the last appended event.
    ///
    /// A new log creates its epoch on first use. Its head sequence number is 0.
    fn head(&self) -> Result<Cursor, Self::Error>;

    /// The sequence number of the oldest retained event, or `None` if no event is retained.
    fn oldest(&self) -> Result<Option<u64>, Self::Error>;

    /// Appends an encoded event and returns its sequence number.
    fn append(&mut self, body: &[u8], now_ms: u64) -> Result<u64, Self::Error>;

    /// Returns every retained event after `after`, in order.
    fn range(&self, after: u64) -> Result<Vec<(u64, Vec<u8>)>, Self::Error>;

    /// Removes events that fall outside the retention policy.
    ///
    /// Age-based trimming removes every event up to and including the newest one older
    /// than the age limit, so a clock that went backwards cannot leave a hole.
    fn trim(&mut self, policy: &Retention, now_ms: u64) -> Result<(), Self::Error>;
}

/// What the server sends after `Hello`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OnConnect {
    /// Nothing. The client is already at the head, or asked for live events only.
    LiveOnly,
    /// Every retained event after `after`.
    Replay {
        /// The client's last sequence number.
        after: u64,
    },
    /// The latest event, in [`Mode::Latest`].
    SendLatest,
    /// A `Reset` frame. The cursor cannot be resumed.
    Reset,
}

/// Decides what a new connection receives after `Hello`.
///
/// - Without a cursor, a Log channel sends live events only. A Latest channel sends its
///   current value, because each event is the full value.
/// - A Latest channel sends its current value whenever the cursor is behind or from
///   another epoch. It never sends `Reset`.
/// - A Log channel replays the gap when every missed event is retained, and sends `Reset`
///   when the epoch differs, the cursor is ahead of the head, or events were trimmed.
pub fn on_connect<L: Log + ?Sized>(
    mode: Mode,
    since: Option<Cursor>,
    log: &L,
) -> Result<OnConnect, L::Error> {
    let head = log.head()?;
    let decision = match (mode, since) {
        (_, Some(c)) if c == head => OnConnect::LiveOnly,
        (Mode::Latest, _) if head.seq == 0 => OnConnect::LiveOnly,
        (Mode::Latest, _) => OnConnect::SendLatest,
        (Mode::Log, None) => OnConnect::LiveOnly,
        (Mode::Log, Some(c)) if c.epoch != head.epoch || c.seq > head.seq => OnConnect::Reset,
        (Mode::Log, Some(c)) => match log.oldest()? {
            Some(oldest) if oldest <= c.seq + 1 => OnConnect::Replay { after: c.seq },
            _ => OnConnect::Reset,
        },
    };
    Ok(decision)
}

/// A server-side failure: a storage error or an encoding error.
#[derive(Debug, thiserror::Error)]
pub enum ServerError<E> {
    /// The log storage failed.
    #[error("log storage failed: {0}")]
    Log(E),
    /// An event could not be encoded or decoded.
    #[error(transparent)]
    Codec(#[from] CodecError),
    /// An event is larger than [`Limits::max_event_bytes`].
    #[error("event is {size} bytes, more than the limit of {max}")]
    TooLarge {
        /// The size of the encoded event.
        size: usize,
        /// The limit.
        max: usize,
    },
}

/// Builds every frame a new connection receives, with the default [`Limits`]. See
/// [`handshake_with`].
pub fn handshake<L: Log + ?Sized>(
    mode: Mode,
    since: Option<Cursor>,
    log: &L,
) -> Result<Vec<String>, ServerError<L::Error>> {
    handshake_with(mode, since, log, &Limits::DEFAULT)
}

/// Builds every frame a new connection receives, in order: `Hello`, then the replay,
/// the latest event, or `Reset`.
///
/// A replay is sent only when the retained events after the cursor run without a gap up to
/// the head and fit in [`Limits::max_replay_bytes`]. Otherwise the client gets `Reset`, and
/// the refetch that follows is always correct.
///
/// The caller must send all of them without yielding to other work, so that no publish
/// lands between the replay and live events.
pub fn handshake_with<L: Log + ?Sized>(
    mode: Mode,
    since: Option<Cursor>,
    log: &L,
    limits: &Limits,
) -> Result<Vec<String>, ServerError<L::Error>> {
    let head = log.head().map_err(ServerError::Log)?;
    let mut frames = vec![codec::encode_frame::<()>(&ServerFrame::Hello {
        v: PROTOCOL_VERSION,
        mode,
        head,
    })?];
    let reset = codec::encode_frame::<()>(&ServerFrame::Reset { head })?;
    match on_connect(mode, since, log).map_err(ServerError::Log)? {
        OnConnect::LiveOnly => {}
        OnConnect::Replay { after } => {
            let rows = log.range(after).map_err(ServerError::Log)?;
            if is_replayable(&rows, after, head.seq, limits) {
                for (seq, body) in rows {
                    frames.push(codec::event_frame(seq, &body)?);
                }
            } else {
                frames.push(reset);
            }
        }
        OnConnect::SendLatest => {
            let rows = log.range(head.seq - 1).map_err(ServerError::Log)?;
            if let Some((seq, body)) = rows.last() {
                frames.push(codec::event_frame(*seq, body)?);
            }
        }
        OnConnect::Reset => frames.push(reset),
    }
    Ok(frames)
}

/// Reports whether `rows` are exactly the events `after + 1 ..= head`, and fit the replay limit.
fn is_replayable(rows: &[(u64, Vec<u8>)], after: u64, head: u64, limits: &Limits) -> bool {
    let complete = rows.len() as u64 == head - after
        && rows
            .iter()
            .zip(after + 1..)
            .all(|((seq, _), expected)| *seq == expected);
    let bytes: usize = rows.iter().map(|(_, body)| body.len()).sum();
    complete && bytes <= limits.max_replay_bytes
}

/// Appends an encoded event with the default [`Limits`]. See [`publish_with`].
pub fn publish<L: Log + ?Sized>(
    log: &mut L,
    body: &[u8],
    retention: &Retention,
    now_ms: u64,
) -> Result<(Cursor, String), ServerError<L::Error>> {
    publish_with(log, body, retention, &Limits::DEFAULT, now_ms)
}

/// Appends an encoded event, trims the log, and returns the new head and the `Event` frame
/// text to send to every socket.
///
/// The body is checked against [`Limits::max_event_bytes`] and validated as an encoded
/// event before it is stored.
pub fn publish_with<L: Log + ?Sized>(
    log: &mut L,
    body: &[u8],
    retention: &Retention,
    limits: &Limits,
    now_ms: u64,
) -> Result<(Cursor, String), ServerError<L::Error>> {
    check_size(body, limits)?;
    let next = log.head().map_err(ServerError::Log)?.seq + 1;
    // Building the frame first validates the body, so an invalid body is never stored.
    let frame = codec::event_frame(next, body)?;
    let seq = log.append(body, now_ms).map_err(ServerError::Log)?;
    debug_assert_eq!(seq, next);
    log.trim(retention, now_ms).map_err(ServerError::Log)?;
    let head = log.head().map_err(ServerError::Log)?;
    Ok((head, frame))
}

/// Checks an encoded event against [`Limits::max_event_bytes`].
pub fn check_size<E>(body: &[u8], limits: &Limits) -> Result<(), ServerError<E>> {
    if body.len() > limits.max_event_bytes {
        return Err(ServerError::TooLarge {
            size: body.len(),
            max: limits.max_event_bytes,
        });
    }
    Ok(())
}

/// An in-memory [`Log`], for tests and for backends that do not need durability.
#[derive(Clone, Debug)]
pub struct MemLog {
    epoch: u64,
    head: u64,
    rows: VecDeque<(u64, u64, Vec<u8>)>,
}

impl MemLog {
    /// Creates an empty log with the given epoch.
    pub fn new(epoch: u64) -> Self {
        Self {
            epoch,
            head: 0,
            rows: VecDeque::new(),
        }
    }

    /// Wipes the log and starts a new epoch. Old cursors no longer match.
    pub fn reset(&mut self, epoch: u64) {
        *self = Self::new(epoch);
    }

    /// The number of retained events.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Reports whether no event is retained.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

impl Log for MemLog {
    type Error = Infallible;

    fn head(&self) -> Result<Cursor, Infallible> {
        Ok(Cursor::new(self.epoch, self.head))
    }

    fn oldest(&self) -> Result<Option<u64>, Infallible> {
        Ok(self.rows.front().map(|(seq, _, _)| *seq))
    }

    fn append(&mut self, body: &[u8], now_ms: u64) -> Result<u64, Infallible> {
        self.head += 1;
        self.rows.push_back((self.head, now_ms, body.to_vec()));
        Ok(self.head)
    }

    fn range(&self, after: u64) -> Result<Vec<(u64, Vec<u8>)>, Infallible> {
        Ok(self
            .rows
            .iter()
            .filter(|(seq, _, _)| *seq > after)
            .map(|(seq, _, body)| (*seq, body.clone()))
            .collect())
    }

    fn trim(&mut self, policy: &Retention, now_ms: u64) -> Result<(), Infallible> {
        let count_cutoff = policy.count_cutoff(self.head).unwrap_or(0);
        // Everything up to the newest expired event goes, the same rule as the SQLite log,
        // so the log has no gap even if the clock went backwards between two appends.
        let age_cutoff = policy
            .age_cutoff(now_ms)
            .and_then(|cutoff| {
                self.rows
                    .iter()
                    .filter(|(_, ts, _)| *ts < cutoff)
                    .map(|(seq, _, _)| *seq)
                    .max()
            })
            .unwrap_or(0);
        let cutoff = count_cutoff.max(age_cutoff);
        while self.rows.front().is_some_and(|(seq, _, _)| *seq <= cutoff) {
            self.rows.pop_front();
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn log_with(epoch: u64, n: u64) -> MemLog {
        let mut log = MemLog::new(epoch);
        for i in 1..=n {
            log.append(format!("{i}").as_bytes(), i * 1000).unwrap();
        }
        log
    }

    fn c(epoch: u64, seq: u64) -> Option<Cursor> {
        Some(Cursor::new(epoch, seq))
    }

    #[test]
    fn log_mode_decisions() {
        let log = log_with(7, 5);
        assert_eq!(on_connect(Mode::Log, None, &log), Ok(OnConnect::LiveOnly));
        assert_eq!(
            on_connect(Mode::Log, c(7, 5), &log),
            Ok(OnConnect::LiveOnly)
        );
        assert_eq!(
            on_connect(Mode::Log, c(7, 2), &log),
            Ok(OnConnect::Replay { after: 2 })
        );
        assert_eq!(
            on_connect(Mode::Log, c(7, 0), &log),
            Ok(OnConnect::Replay { after: 0 })
        );
        assert_eq!(on_connect(Mode::Log, c(8, 2), &log), Ok(OnConnect::Reset));
        assert_eq!(on_connect(Mode::Log, c(7, 6), &log), Ok(OnConnect::Reset));
    }

    #[test]
    fn log_mode_resets_when_the_gap_was_trimmed() {
        let mut log = log_with(1, 10);
        log.trim(
            &Retention {
                max_events: 3,
                max_age: None,
            },
            0,
        )
        .unwrap();
        assert_eq!(log.oldest(), Ok(Some(8)));
        assert_eq!(
            on_connect(Mode::Log, c(1, 7), &log),
            Ok(OnConnect::Replay { after: 7 })
        );
        assert_eq!(on_connect(Mode::Log, c(1, 6), &log), Ok(OnConnect::Reset));
    }

    #[test]
    fn log_mode_resets_when_everything_aged_out() {
        let mut log = log_with(1, 3);
        log.trim(
            &Retention {
                max_events: 100,
                max_age: Some(Duration::from_secs(1)),
            },
            100_000,
        )
        .unwrap();
        assert!(log.is_empty());
        assert_eq!(
            on_connect(Mode::Log, c(1, 3), &log),
            Ok(OnConnect::LiveOnly)
        );
        assert_eq!(on_connect(Mode::Log, c(1, 2), &log), Ok(OnConnect::Reset));
    }

    #[test]
    fn latest_mode_decisions() {
        let empty = MemLog::new(3);
        assert_eq!(
            on_connect(Mode::Latest, None, &empty),
            Ok(OnConnect::LiveOnly)
        );
        let log = log_with(3, 4);
        assert_eq!(
            on_connect(Mode::Latest, None, &log),
            Ok(OnConnect::SendLatest)
        );
        assert_eq!(
            on_connect(Mode::Latest, c(3, 4), &log),
            Ok(OnConnect::LiveOnly)
        );
        assert_eq!(
            on_connect(Mode::Latest, c(3, 1), &log),
            Ok(OnConnect::SendLatest)
        );
        assert_eq!(
            on_connect(Mode::Latest, c(9, 4), &log),
            Ok(OnConnect::SendLatest)
        );
    }

    #[test]
    fn trim_by_count_keeps_the_newest() {
        let mut log = log_with(1, 5);
        log.trim(&Retention::LATEST, 0).unwrap();
        assert_eq!(log.range(0), Ok(vec![(5, b"5".to_vec())]));
    }

    #[test]
    fn trim_by_age_removes_old_events() {
        let mut log = log_with(1, 5);
        let policy = Retention {
            max_events: 100,
            max_age: Some(Duration::from_millis(2500)),
        };
        log.trim(&policy, 5000).unwrap();
        assert_eq!(log.oldest(), Ok(Some(3)));
    }

    #[test]
    fn handshake_frames() {
        let log = log_with(2, 3);
        let frames = handshake(Mode::Log, c(2, 1), &log).unwrap();
        assert_eq!(
            frames,
            vec![
                r#"{"t":"hello","v":1,"mode":"log","head":"2.3"}"#,
                r#"{"t":"event","seq":2,"event":2}"#,
                r#"{"t":"event","seq":3,"event":3}"#,
            ]
        );
        let frames = handshake(Mode::Log, c(5, 1), &log).unwrap();
        assert_eq!(frames[1], r#"{"t":"reset","head":"2.3"}"#);
        let frames = handshake(Mode::Latest, None, &log).unwrap();
        assert_eq!(frames[1], r#"{"t":"event","seq":3,"event":3}"#);
        assert_eq!(handshake(Mode::Log, None, &log).unwrap().len(), 1);
    }

    #[test]
    fn trim_by_age_keeps_the_log_contiguous_when_the_clock_goes_back() {
        let mut log = MemLog::new(1);
        for ts in [1000, 5000, 2000, 6000] {
            log.append(b"0", ts).unwrap();
        }
        let policy = Retention {
            max_events: 100,
            max_age: Some(Duration::from_millis(1000)),
        };
        log.trim(&policy, 4000).unwrap();
        // Event 3 expired, so event 2 goes too, though its timestamp is newer.
        assert_eq!(log.oldest(), Ok(Some(4)));
        assert_eq!(log.len(), 1);
    }

    /// A log that loses one event, as a broken trim would.
    struct HoleyLog(MemLog, u64);

    impl Log for HoleyLog {
        type Error = Infallible;
        fn head(&self) -> Result<Cursor, Infallible> {
            self.0.head()
        }
        fn oldest(&self) -> Result<Option<u64>, Infallible> {
            self.0.oldest()
        }
        fn append(&mut self, body: &[u8], now_ms: u64) -> Result<u64, Infallible> {
            self.0.append(body, now_ms)
        }
        fn range(&self, after: u64) -> Result<Vec<(u64, Vec<u8>)>, Infallible> {
            let mut rows = self.0.range(after)?;
            rows.retain(|(seq, _)| *seq != self.1);
            Ok(rows)
        }
        fn trim(&mut self, policy: &Retention, now_ms: u64) -> Result<(), Infallible> {
            self.0.trim(policy, now_ms)
        }
    }

    #[test]
    fn handshake_resets_instead_of_replaying_a_gap() {
        let log = HoleyLog(log_with(2, 5), 3);
        let frames = handshake(Mode::Log, c(2, 1), &log).unwrap();
        assert_eq!(frames.len(), 2);
        assert_eq!(frames[1], r#"{"t":"reset","head":"2.5"}"#);
        // A replay that skips the missing event is still sent.
        assert_eq!(handshake(Mode::Log, c(2, 3), &log).unwrap().len(), 3);
    }

    #[test]
    fn handshake_resets_when_the_replay_is_too_large() {
        let log = log_with(2, 5);
        let limits = Limits {
            max_replay_bytes: 3,
            ..Limits::DEFAULT
        };
        let frames = handshake_with(Mode::Log, c(2, 1), &log, &limits).unwrap();
        assert_eq!(frames[1], r#"{"t":"reset","head":"2.5"}"#);
        // Three one-byte events fit.
        let frames = handshake_with(Mode::Log, c(2, 2), &log, &limits).unwrap();
        assert_eq!(frames.len(), 4);
    }

    #[test]
    fn publish_rejects_oversized_events() {
        let mut log = MemLog::new(1);
        let limits = Limits {
            max_event_bytes: 4,
            ..Limits::DEFAULT
        };
        let result = publish_with(&mut log, br#""abcd""#, &Retention::LATEST, &limits, 0);
        assert!(matches!(
            result,
            Err(ServerError::TooLarge { size: 6, max: 4 })
        ));
        assert_eq!(log.head(), Ok(Cursor::new(1, 0)), "nothing is stored");
        assert!(publish_with(&mut log, b"1234", &Retention::LATEST, &limits, 0).is_ok());
    }

    #[test]
    fn publish_appends_trims_and_frames() {
        let mut log = MemLog::new(1);
        let (head, frame) = publish(&mut log, br#"{"a":1}"#, &Retention::LATEST, 10).unwrap();
        assert_eq!(head, Cursor::new(1, 1));
        assert_eq!(frame, r#"{"t":"event","seq":1,"event":{"a":1}}"#);
        publish(&mut log, br#"{"a":2}"#, &Retention::LATEST, 11).unwrap();
        assert_eq!(log.len(), 1);
        assert!(publish(&mut log, b"nope", &Retention::LATEST, 12).is_err());
        assert_eq!(
            log.head(),
            Ok(Cursor::new(1, 2)),
            "invalid body is not stored"
        );
    }
}
