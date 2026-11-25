//! An object with three hubs, the way an app holds shared feeds and private views together.
//!
//! - `ticks`: Log channels of numbers, with the fixture's tiny retention window.
//! - `beats`: a second Log channel type with the same window, so the alarm runs `on_alarm`
//!   on more than one Log hub.
//! - `views`: Latest channels of numbers.
//!
//! `POST /commit` stores events on any of its channels in one turn with
//! `ChannelHandle::append`, then sends them. Its body is a list of `{type}:{id}={n}`,
//! separated by `;`, where the type is `ticks`, `beats`, or `views`. With `send=0` it never
//! sends them. With `hold_ms={n}` it waits that long between storing and sending, so other
//! requests run in between.

use std::time::Duration;

use partyline::{Channel, Mode};
use partyline_worker::{Hub, HubConfig, PendingSend};
use worker::{
    Delay, DurableObject, Env, Request, Response, Result, State, durable_object, wasm_bindgen,
};

use crate::Ticks;

/// A second Log channel of numbers.
pub struct Beats;

impl Channel for Beats {
    const NAME: &'static str = "beats";
    const MODE: Mode = Mode::Log;
    type Event = u64;
}

/// A Latest channel of numbers.
pub struct Views;

impl Channel for Views {
    const NAME: &'static str = "views";
    const MODE: Mode = Mode::Latest;
    type Event = u64;
}

#[durable_object]
pub struct RoomChannel {
    ticks: Hub<Ticks>,
    beats: Hub<Beats>,
    views: Hub<Views>,
}

impl RoomChannel {
    /// Stores every event in the body in one turn, and returns the pending sends.
    fn append_all(&self, body: &str) -> Result<std::result::Result<Vec<PendingSend<'_>>, String>> {
        let mut pending = Vec::new();
        for entry in body.split(';').filter(|entry| !entry.is_empty()) {
            let parsed = entry.split_once(':').and_then(|(kind, rest)| {
                let (id, n) = rest.split_once('=')?;
                Some((kind, id, n.parse::<u64>().ok()?))
            });
            let Some((kind, id, n)) = parsed else {
                return Ok(Err(format!("malformed entry: {entry}")));
            };
            pending.push(match kind {
                "ticks" => self.ticks.channel(id).append(&n)?,
                "beats" => self.beats.channel(id).append(&n)?,
                "views" => self.views.channel(id).append(&n)?,
                _ => return Ok(Err(format!("unknown channel type: {kind}"))),
            });
        }
        Ok(Ok(pending))
    }

    /// Stores the events, waits `hold` if asked, then sends them unless `send` is false.
    /// Returns the head after each event, comma-separated.
    async fn commit(&self, body: &str, send: bool, hold: Option<Duration>) -> Result<Response> {
        let pending = match self.append_all(body)? {
            Ok(pending) => pending,
            Err(e) => return Response::error(e, 400),
        };
        let heads: Vec<String> = pending.iter().map(|p| p.head().to_string()).collect();
        if let Some(hold) = hold {
            Delay::from(hold).await;
        }
        if send {
            for event in pending {
                event.send();
            }
        }
        self.ticks.schedule_trim().await?;
        self.beats.schedule_trim().await?;
        Response::ok(heads.join(","))
    }
}

impl DurableObject for RoomChannel {
    fn new(state: State, _env: Env) -> Self {
        let config = HubConfig::default()
            .retain_events(5)
            .retain_for(Some(Duration::from_secs(3)));
        let ticks = Hub::new(state, config);
        let beats = ticks.sibling(config);
        let views = ticks.sibling(HubConfig::default());
        Self {
            ticks,
            beats,
            views,
        }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        // Requests that name a channel go to its hub first, so the object's own routes,
        // such as `/close`, never catch a hub's internal route of the same path.
        if self.views.handles(&req) {
            return self.views.fetch(req).await;
        }
        if self.beats.handles(&req) {
            return self.beats.fetch(req).await;
        }
        if self.ticks.handles(&req) {
            return self.ticks.fetch(req).await;
        }
        let url = req.url()?;
        let param = |name: &str| {
            url.query_pairs()
                .find_map(|(k, v)| (k == name).then(|| v.into_owned()))
        };
        match url.path() {
            "/commit" => {
                let send = param("send").as_deref() != Some("0");
                let hold = param("hold_ms")
                    .and_then(|ms| ms.parse().ok())
                    .map(Duration::from_millis);
                let body = req.text().await?;
                self.commit(&body, send, hold).await
            }
            "/close" => {
                let Some(tag) = param("tag") else {
                    return Response::error("missing tag", 400);
                };
                let closed = self.ticks.close_tagged(&tag, partyline::close::FORBIDDEN);
                Response::ok(closed.to_string())
            }
            "/drop" => {
                self.ticks.drop_tables()?;
                Response::ok("")
            }
            "/tables" => Response::ok(crate::partyline_tables(
                &self.ticks.state().storage().sql(),
            )?),
            // A request that names no channel is for the object's own ticks channel.
            _ => self.ticks.fetch(req).await,
        }
    }

    partyline_worker::hub_handlers!(ticks, beats, views);
}
