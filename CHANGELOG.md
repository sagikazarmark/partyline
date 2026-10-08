# Changelog

All notable changes to the partyline crates are documented in this file.
The four crates share one version, so one entry covers all of them.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **worker:** `HubConfig::max_event_bytes`, 64 KiB by default. The hub rejects a larger publish with HTTP 413, and `Publisher::publish` returns an error.
- **worker:** `HubConfig::max_replay_bytes`, 8 MiB by default. A reconnect whose replay would be larger gets `Reset`.
- **worker:** `Hub::schedule_alarm`, so an object with its own alarm can share it with the hub. The earlier time wins.
- **worker:** `hub_handlers!` and `hub_websocket_handlers!` generate the delegating handlers inside a hand-written `impl DurableObject`.
- **worker:** `Hub::fetch_with` calls a closure with each published event before the hub stores and sends it. It replaces matching the hub's `/publish` route by hand.
- **worker:** `Connect::from_path` and `partyline::frame::match_connect_path` match `/partyline/{channel}/{id}` and decode the ID.
- **client:** A `tracing` feature: trace-level logs of the state machine's inputs and outputs, and debug-level transport errors.
- **dioxus:** `ChannelOptions::enabled`, to keep a hook idle until the app is ready to connect.
- `examples/orders/tail`: a native command-line client for the orders example.

### Changed

- **Breaking:** **client:** `Status::Stopped` carries a `StopReason`: `App`, `Closed(code)`, `Incompatible { seq }`, or `InvalidUrl`. `Status::Waiting` and `Status::Unauthorized` carry the attempt number, and `Waiting` carries the last close code.
- **Breaking:** **client:** An event the client cannot decode stops it with `StopReason::Incompatible`, instead of reconnecting forever. Prompt the user to reload.
- **Breaking:** **dioxus:** `use_channel_reducer` takes `initial` as `impl FnMut() -> S`, because it runs again when the channel ID changes, and `refetch` returns `Result<(S, Cursor), E>` with `E: Display`. A failed refetch is retried after 1 s, doubling up to 30 s. Update calls such as `use_channel_reducer::<C, S, _, _>(..)` to `use_channel_reducer::<C, S, _, _, _>(..)`, and wrap the refetch result in `Ok`.
- **Breaking:** **client:** `ClientEvent::Event` has an `epoch` field. A pattern that lists the fields needs `..` or `epoch`.
- **dioxus:** The hooks restart when the base URL or the client config changes. A new token provider takes effect at the next connect, without a restart.
- **client:** A wake (page visible, network online, back-forward cache restore, `resume`, network change) resets the backoff, and replaces a connect attempt that started while offline.
- **worker:** `Connect::forward` fails for an invalid tag: empty, longer than 256 characters, or more than 10 tags. Tags are percent-encoded on the way to the Durable Object, so non-ASCII tags work.
- **worker:** `Connect::forward` removes the `token` query parameter, so tokens stay out of the Durable Object's logs.
- The four crates share one version and are released together with one `v{version}` tag. CI publishes them to crates.io.

### Fixed

- **worker:** A replay checks that the retained range has no gaps. A client whose range has one gets `Reset`.
- **worker:** Age-based trimming removes only the oldest events, never a range in the middle of the log.
- **worker:** Close reasons are cut to 123 bytes, the protocol limit. The hub does not echo the reserved close codes 1005, 1006, and 1015.

## [0.2.0] - 2026-10-07

### Fixed

- Release config

## [0.1.0] - 2026-10-07

### Added

- Initial release

### Fixed

- Add workspace version

[Unreleased]: https://github.com/sagikazarmark/partyline/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/sagikazarmark/partyline/compare/partyline-v0.1.0...v0.2.0
[0.1.0]: https://github.com/sagikazarmark/partyline/releases/tag/partyline-v0.1.0
