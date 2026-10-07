//! The live poll's channels and API types, shared by the Worker and the web client.

use partyline::{Channel, Cursor, Mode};
use serde::{Deserialize, Serialize};

/// The poll question.
pub const QUESTION: &str = "What should partyline build next?";

/// The answer options.
pub const OPTIONS: [&str; 4] = ["Presence", "Sharding", "Web Push", "A proc macro"];

/// The Durable Object binding of [`PollTally`]: the `PollObject` class.
pub const TALLY_BINDING: &str = "POLL_OBJECT";

/// The Durable Object binding of [`PollActivity`]: the `ActivityChannel` class.
pub const ACTIVITY_BINDING: &str = "ACTIVITY_CHANNEL";

/// The live tally. Latest mode: each event is the full tally, and a client that missed
/// updates only needs the final value.
pub struct PollTally;

impl Channel for PollTally {
    const NAME: &'static str = "poll-tally";
    const MODE: Mode = Mode::Latest;
    type Event = Tally;
}

/// The activity feed. Log mode: every vote is listed, in order.
pub struct PollActivity;

impl Channel for PollActivity {
    const NAME: &'static str = "poll-activity";
    const MODE: Mode = Mode::Log;
    type Event = ActivityEvent;
}

/// One count per option.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Tally {
    /// The vote count of each option, in [`OPTIONS`] order.
    pub counts: Vec<u64>,
}

impl Default for Tally {
    fn default() -> Self {
        Self {
            counts: vec![0; OPTIONS.len()],
        }
    }
}

impl Tally {
    /// The total number of votes.
    pub fn total(&self) -> u64 {
        self.counts.iter().sum()
    }
}

/// An event on the activity feed.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ActivityEvent {
    /// Someone voted.
    VoteCast {
        /// The voter's display name.
        voter: String,
        /// The option index.
        option: usize,
    },
    /// The poll was reset.
    PollReset,
}

impl ActivityEvent {
    /// A one-line description for the feed.
    pub fn describe(&self) -> String {
        match self {
            ActivityEvent::VoteCast { voter, option } => {
                let option = OPTIONS.get(*option).copied().unwrap_or("?");
                format!("{voter} voted {option}")
            }
            ActivityEvent::PollReset => "The poll was reset".to_owned(),
        }
    }
}

/// The body of `POST /api/polls/{id}/vote`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Vote {
    /// The option index.
    pub option: usize,
    /// The voter's display name, at most [`MAX_VOTER_LEN`] characters.
    pub voter: String,
}

/// The longest accepted voter name.
pub const MAX_VOTER_LEN: usize = 32;

/// The response of `GET /api/polls/{id}`.
///
/// The feed lists the votes after `activity_head`. The tally channel needs no head: a
/// Latest channel sends its current value on connect.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PollSnapshot {
    /// The question.
    pub question: String,
    /// The options.
    pub options: Vec<String>,
    /// The tally.
    pub tally: Tally,
    /// The activity channel head.
    pub activity_head: Cursor,
}
