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
}

/// Builds every frame a new connection receives, in order: `Hello`, then the replay,
/// the latest event, or `Reset`.
///
/// The caller must send all of them without yielding to other work, so that no publish
/// lands between the replay and live events.
pub fn handshake<L: Log + ?Sized>(
    mode: Mode,
    since: Option<Cursor>,
    log: &L,
) -> Result<Vec<String>, ServerError<L::Error>> {
    let head = log.head().map_err(ServerError::Log)?;
    let mut frames = vec![codec::encode_frame::<()>(&ServerFrame::Hello {
        v: PROTOCOL_VERSION,
        mode,
        head,
    })?];
    match on_connect(mode, since, log).map_err(ServerError::Log)? {
        OnConnect::LiveOnly => {}
        OnConnect::Replay { after } => {
            for (seq, body) in log.range(after).map_err(ServerError::Log)? {
                frames.push(codec::event_frame(seq, &body)?);
            }
        }
        OnConnect::SendLatest => {
            let rows = log.range(head.seq - 1).map_err(ServerError::Log)?;
            if let Some((seq, body)) = rows.last() {
                frames.push(codec::event_frame(*seq, body)?);
            }
        }
        OnConnect::Reset => {
            frames.push(codec::encode_frame::<()>(&ServerFrame::Reset { head })?);
        }
    }
    Ok(frames)
}

/// Appends an encoded event, trims the log, and returns the new head and the `Event` frame
/// text to send to every socket.
///
/// The body is validated as an encoded event before it is stored.
pub fn publish<L: Log + ?Sized>(
    log: &mut L,
    body: &[u8],
    retention: &Retention,
    now_ms: u64,
) -> Result<(Cursor, String), ServerError<L::Error>> {
    let next = log.head().map_err(ServerError::Log)?.seq + 1;
    // Building the frame first validates the body, so an invalid body is never stored.
    let frame = codec::event_frame(next, body)?;
    let seq = log.append(body, now_ms).map_err(ServerError::Log)?;
    debug_assert_eq!(seq, next);
    log.trim(retention, now_ms).map_err(ServerError::Log)?;
    let head = log.head().map_err(ServerError::Log)?;
    Ok((head, frame))
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
        let age_cutoff = policy.age_cutoff(now_ms);
        while let Some((seq, ts, _)) = self.rows.front() {
            let too_many = *seq <= count_cutoff;
            let too_old = age_cutoff.is_some_and(|cutoff| *ts < cutoff);
            if !(too_many || too_old) {
                break;
            }
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
