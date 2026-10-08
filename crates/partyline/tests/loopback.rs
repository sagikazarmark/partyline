//! Layer 2: the client state machine and the server logic agree under faults.

use std::time::Duration;

use partyline::server::Retention;
use partyline::testing::{Faults, Loopback, Observed, check_latest_delivery, check_log_delivery};
use partyline::{Channel, ClientConfig, Cursor, Mode, Status, StopReason};
use proptest::prelude::*;

struct LogChan;
impl Channel for LogChan {
    const NAME: &'static str = "log";
    const MODE: Mode = Mode::Log;
    type Event = u64;
}

struct LatestChan;
impl Channel for LatestChan {
    const NAME: &'static str = "latest";
    const MODE: Mode = Mode::Latest;
    type Event = u64;
}

#[derive(Clone, Debug)]
enum Op {
    Publish,
    Advance(u64),
    Wake,
    Disconnect,
}

fn op() -> impl Strategy<Value = Op> {
    prop_oneof![
        5 => Just(Op::Publish),
        4 => (0u64..3_000).prop_map(Op::Advance),
        1 => Just(Op::Wake),
        1 => Just(Op::Disconnect),
    ]
}

fn faults() -> impl Strategy<Value = Faults> {
    (
        0.0..0.3f64,
        0.0..0.1f64,
        0.0..0.3f64,
        0u64..2_000,
        0.0..0.5f64,
    )
        .prop_map(|(drop, lose, duplicate, delay, connect_fail)| Faults {
            drop,
            lose,
            duplicate,
            max_delay: Duration::from_millis(delay),
            connect_fail,
        })
}

/// Applies `ops`. Event `n` has sequence number `n`, so `published` counts every event.
fn apply<C: Channel<Event = u64>>(lb: &mut Loopback<C>, ops: &[Op], published: &mut u64) {
    for op in ops {
        match op {
            Op::Publish => {
                *published += 1;
                let seq = lb.publish(published);
                assert_eq!(seq, *published);
            }
            Op::Advance(ms) => lb.advance_ms(*ms),
            Op::Wake => lb.wake(),
            Op::Disconnect => lb.disconnect(),
        }
    }
}

fn run<C: Channel<Event = u64>>(lb: &mut Loopback<C>, ops: &[Op]) -> u64 {
    lb.start();
    let mut published = 0;
    apply(lb, ops, &mut published);
    // A lost frame is noticed at the next event, as a gap. Publish one after healing so
    // every run ends with the client able to notice.
    lb.heal();
    lb.advance_ms(60_000);
    published += 1;
    lb.publish(&published);
    lb.settle();
    published
}

fn resets<E>(observed: &[Observed<E>]) -> usize {
    observed
        .iter()
        .filter(|o| matches!(o, Observed::Reset { .. }))
        .count()
}

proptest! {
    #![proptest_config(ProptestConfig { cases: 512, ..ProptestConfig::default() })]

    /// The main guarantee: in Log mode the app sees every event exactly once and in order,
    /// or it sees Reset.
    #[test]
    fn log_mode_delivers_every_event_once_in_order_or_resets(
        seed in any::<u64>(),
        faults in faults(),
        retain in 1u64..60,
        ops in prop::collection::vec(op(), 0..200),
    ) {
        let retention = Retention::LOG_DEFAULT.with_max_events(retain);
        let mut lb = Loopback::<LogChan>::with_options(
            seed, faults, retention, ClientConfig::default(), Some(Cursor::new(1, 0)),
        );
        let published = run(&mut lb, &ops);

        let last = check_log_delivery(0, lb.observed()).map_err(TestCaseError::fail)?;
        prop_assert_eq!(last, published, "the app caught up with the head");
        prop_assert_eq!(lb.client().cursor(), Some(lb.head()));
        prop_assert_eq!(lb.client().status(), Status::Open);
        for (seq, event) in lb.events() {
            prop_assert_eq!(seq, event, "event payload matches its sequence number");
        }
    }

    /// With a retention window larger than the run, a client never needs Reset.
    #[test]
    fn log_mode_never_resets_within_retention(
        seed in any::<u64>(),
        faults in faults(),
        ops in prop::collection::vec(op(), 0..200),
    ) {
        let mut lb = Loopback::<LogChan>::with_options(
            seed, faults, Retention::LOG_DEFAULT, ClientConfig::default(), Some(Cursor::new(1, 0)),
        );
        let published = run(&mut lb, &ops);
        prop_assert_eq!(resets(lb.observed()), 0);
        let seqs: Vec<u64> = lb.events().into_iter().map(|(seq, _)| seq).collect();
        prop_assert_eq!(seqs, (1..=published).collect::<Vec<_>>());
    }

    /// In Latest mode, sequence numbers only move forward and the app ends on the latest value.
    #[test]
    fn latest_mode_ends_on_the_latest_value(
        seed in any::<u64>(),
        faults in faults(),
        ops in prop::collection::vec(op(), 0..200),
    ) {
        let mut lb = Loopback::<LatestChan>::new(seed, faults);
        let published = run(&mut lb, &ops);
        let last = check_latest_delivery(0, lb.observed()).map_err(TestCaseError::fail)?;
        prop_assert_eq!(last, published);
        prop_assert_eq!(resets(lb.observed()), 0, "Latest mode never resets");
        if published > 0 {
            prop_assert_eq!(lb.events().last().map(|(_, e)| *e), Some(published));
        }
    }

    /// A server newer than the client publishes an event the client cannot decode. The app
    /// sees every event before it, then the client stops for good and never loops.
    #[test]
    fn an_undecodable_event_stops_the_client_and_it_never_loops(
        seed in any::<u64>(),
        faults in faults(),
        before in prop::collection::vec(op(), 0..100),
        after in prop::collection::vec(op(), 0..100),
    ) {
        let mut lb = Loopback::<LogChan>::with_options(
            seed, faults, Retention::LOG_DEFAULT, ClientConfig::default(), Some(Cursor::new(1, 0)),
        );
        lb.start();
        let mut published = 0;
        apply(&mut lb, &before, &mut published);
        published += 1;
        let bad = lb.publish_body(UNDECODABLE);
        prop_assert_eq!(bad, published);
        apply(&mut lb, &after, &mut published);
        // As in `run`: a lost frame is noticed at the next event, so publish one after healing.
        lb.heal();
        lb.advance_ms(60_000);
        published += 1;
        lb.publish(&published);
        lb.settle();

        prop_assert_eq!(
            lb.client().status(),
            Status::Stopped { reason: StopReason::Incompatible { seq: bad } }
        );
        let last = check_log_delivery(0, lb.observed()).map_err(TestCaseError::fail)?;
        prop_assert_eq!(last, bad - 1, "every event before the undecodable one");
        prop_assert_eq!(lb.client().cursor(), Some(Cursor::new(1, bad - 1)));

        let connects = lb.stats().connects;
        lb.wake();
        lb.disconnect();
        lb.settle();
        prop_assert_eq!(lb.stats().connects, connects, "no reconnect after the stop");
    }
}

/// A Latest or Log event body that does not decode as `u64`: an event variant added later.
const UNDECODABLE: &[u8] = br#"{"type":"added_later"}"#;

#[test]
fn an_undecodable_event_stops_the_client_once() {
    let mut lb = Loopback::<LogChan>::new(5, Faults::NONE);
    lb.start();
    lb.advance_ms(100);
    lb.publish(&1);
    let bad = lb.publish_body(UNDECODABLE);
    lb.publish(&3);
    // A day of wakes would wake the Durable Object once per attempt if the client looped.
    for _ in 0..100 {
        lb.wake();
        lb.advance(Duration::from_secs(60));
    }
    assert_eq!(lb.events(), vec![(1, 1)]);
    assert_eq!(
        lb.client().status(),
        Status::Stopped {
            reason: StopReason::Incompatible { seq: bad }
        }
    );
    assert_eq!(lb.stats().connects, 1);
    // An app restart (here, the same build) meets the same event and stops again.
    lb.start();
    lb.settle();
    assert_eq!(lb.stats().connects, 2);
    assert_eq!(
        lb.client().status(),
        Status::Stopped {
            reason: StopReason::Incompatible { seq: bad }
        }
    );
}

#[test]
fn an_undecodable_latest_value_stops_the_client() {
    let mut lb = Loopback::<LatestChan>::new(5, Faults::NONE);
    lb.publish_body(UNDECODABLE);
    lb.start();
    lb.settle();
    assert_eq!(
        lb.client().status(),
        Status::Stopped {
            reason: StopReason::Incompatible { seq: 1 }
        }
    );
    assert_eq!(lb.stats().connects, 1);
}

#[test]
fn going_offline_and_back_replays_the_missed_events() {
    let mut lb = Loopback::<LogChan>::new(7, Faults::NONE);
    lb.start();
    lb.advance_ms(100);
    lb.publish(&1);
    lb.advance_ms(100);
    lb.disconnect();
    for n in 2..=5 {
        lb.publish(&n);
    }
    lb.settle();
    let seqs: Vec<u64> = lb.events().into_iter().map(|(seq, _)| seq).collect();
    assert_eq!(seqs, vec![1, 2, 3, 4, 5]);
    assert_eq!(lb.stats().connects, 2);
}

#[test]
fn falling_behind_retention_resets() {
    let retention = Retention::LOG_DEFAULT.with_max_events(3);
    let mut lb = Loopback::<LogChan>::with_options(
        1,
        Faults::NONE,
        retention,
        ClientConfig::default(),
        Some(Cursor::new(1, 0)),
    );
    lb.start();
    lb.advance_ms(100);
    lb.disconnect();
    for n in 1..=10 {
        lb.publish(&n);
    }
    lb.settle();
    assert_eq!(resets(lb.observed()), 1);
    assert_eq!(lb.client().cursor(), Some(Cursor::new(1, 10)));
    lb.publish(&11);
    lb.advance_ms(100);
    assert_eq!(lb.events(), vec![(11, 11)]);
}

#[test]
fn a_server_reset_changes_the_epoch_and_the_client_resets() {
    let mut lb = Loopback::<LogChan>::new(3, Faults::NONE);
    lb.start();
    lb.advance_ms(100);
    lb.publish(&1);
    lb.publish(&2);
    lb.advance_ms(100);
    lb.reset_server();
    lb.settle();
    assert_eq!(resets(lb.observed()), 1);
    assert_eq!(lb.client().cursor(), Some(Cursor::new(2, 0)));
    lb.publish(&100);
    lb.advance_ms(100);
    assert_eq!(lb.events(), vec![(1, 1), (2, 2), (1, 100)]);
}

#[test]
fn a_latest_client_survives_a_server_reset_without_reset() {
    let mut lb = Loopback::<LatestChan>::new(3, Faults::NONE);
    lb.start();
    lb.advance_ms(100);
    lb.publish(&1);
    lb.publish(&2);
    lb.advance_ms(100);
    lb.reset_server();
    lb.publish(&3);
    lb.settle();
    assert_eq!(resets(lb.observed()), 0);
    assert_eq!(lb.events().last(), Some(&(1, 3)));
}

#[test]
fn a_forbidden_close_stops_the_client() {
    let mut lb = Loopback::<LogChan>::new(3, Faults::NONE);
    lb.start();
    lb.advance_ms(100);
    lb.server_close(partyline::close::FORBIDDEN);
    lb.settle();
    assert_eq!(
        lb.client().status(),
        Status::Stopped {
            reason: StopReason::Closed(partyline::close::FORBIDDEN)
        }
    );
    assert_eq!(lb.stats().connects, 1);
}

#[test]
fn a_dead_socket_is_found_by_the_heartbeat() {
    let mut lb = Loopback::<LogChan>::new(3, Faults::NONE);
    lb.start();
    lb.advance_ms(100);
    // Every frame from now on is lost, but the connection is not closed: a dead socket.
    lb.set_faults(Faults {
        max_delay: Duration::from_secs(3_600),
        ..Faults::NONE
    });
    lb.publish(&1);
    lb.advance(Duration::from_secs(25 + 10));
    assert!(matches!(lb.client().status(), Status::Waiting { .. }));
    lb.heal();
    lb.settle();
    assert_eq!(lb.events(), vec![(1, 1)]);
}

#[test]
fn wake_finds_a_dead_socket_quickly() {
    let mut lb = Loopback::<LogChan>::new(3, Faults::NONE);
    lb.start();
    lb.advance_ms(100);
    lb.set_faults(Faults {
        max_delay: Duration::from_secs(3_600),
        ..Faults::NONE
    });
    lb.wake();
    lb.advance(Duration::from_secs(3));
    assert!(matches!(lb.client().status(), Status::Waiting { .. }));
}
