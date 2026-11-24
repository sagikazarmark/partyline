//! An object with two hubs, the way an app holds a shared feed and private views together.
//!
//! - `ticks`: Log channels of numbers, with the fixture's tiny retention window.
//! - `views`: Latest channels of numbers.
//!
//! `POST /commit` stores events on any of its channels in one turn with
//! `ChannelHandle::append`, then sends them, unless `send=0` asks it not to. Its body is a
//! list of `ticks:{id}={n}` and `views:{id}={n}`, separated by `;`.

use std::time::Duration;

use partyline::{Channel, Mode};
use partyline_worker::{Hub, HubConfig, PendingSend};
use worker::{DurableObject, Env, Request, Response, Result, State, durable_object, wasm_bindgen};

use crate::Ticks;

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
    views: Hub<Views>,
}

impl RoomChannel {
    /// Stores every event in the body in one turn, then sends them unless `send` is false.
    /// Returns the head after each event, comma-separated.
    fn commit(&self, body: &str, send: bool) -> Result<Response> {
        let mut pending: Vec<PendingSend<'_>> = Vec::new();
        for entry in body.split(';').filter(|entry| !entry.is_empty()) {
            let parsed = entry.split_once(':').and_then(|(kind, rest)| {
                let (id, n) = rest.split_once('=')?;
                Some((kind, id, n.parse::<u64>().ok()?))
            });
            let Some((kind, id, n)) = parsed else {
                return Response::error(format!("malformed entry: {entry}"), 400);
            };
            pending.push(match kind {
                "ticks" => self.ticks.channel(id).append(&n)?,
                "views" => self.views.channel(id).append(&n)?,
                _ => return Response::error(format!("unknown channel type: {kind}"), 400),
            });
        }
        let heads: Vec<String> = pending.iter().map(|p| p.head().to_string()).collect();
        if send {
            for event in pending {
                event.send();
            }
        }
        Response::ok(heads.join(","))
    }
}

impl DurableObject for RoomChannel {
    fn new(state: State, _env: Env) -> Self {
        let config = HubConfig::default()
            .retain_events(5)
            .retain_for(Some(Duration::from_secs(3)));
        let ticks = Hub::new(state, config);
        let views = ticks.sibling(HubConfig::default());
        Self { ticks, views }
    }

    async fn fetch(&self, mut req: Request) -> Result<Response> {
        let url = req.url()?;
        match url.path() {
            "/commit" => {
                let send = !url.query_pairs().any(|(k, v)| k == "send" && v == "0");
                let body = req.text().await?;
                let response = self.commit(&body, send)?;
                self.ticks.schedule_trim().await?;
                Ok(response)
            }
            "/close" => {
                let Some(tag) = url
                    .query_pairs()
                    .find_map(|(k, v)| (k == "tag").then(|| v.into_owned()))
                else {
                    return Response::error("missing tag", 400);
                };
                let closed = self.ticks.close_tagged(&tag, partyline::close::FORBIDDEN);
                Response::ok(closed.to_string())
            }
            _ if self.views.handles(&req) => self.views.fetch(req).await,
            _ => self.ticks.fetch(req).await,
        }
    }

    partyline_worker::hub_handlers!(ticks, views);
}
