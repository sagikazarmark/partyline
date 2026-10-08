//! The orders example Worker: serves the web client, the order API, and the `OrderChannel`
//! Durable Object.
//!
//! | Route | Action |
//! | --- | --- |
//! | `GET /partyline/orders/{id}` | WebSocket upgrade, forwarded to the order's Durable Object |
//! | `GET /api/orders/{id}` | The order and the channel head it was read at |
//! | `POST /api/orders/{id}/events` | Apply and publish an `OrderEvent` |

use orders_shared::{BINDING, Order, OrderEvent, OrderSnapshot, Orders};
use partyline_worker::{Connect, Hub, HubConfig, Publisher, close, decode_segment, reject};
use worker::*;

/// The order's Durable Object. It owns the order state and the channel hub, so an event is
/// applied and published in one turn, and a snapshot and its head are read in one turn.
#[durable_object]
pub struct OrderChannel {
    hub: Hub<Orders>,
}

impl OrderChannel {
    fn load(&self) -> Result<Order> {
        let rows: Vec<serde_json::Value> = self
            .hub
            .state()
            .storage()
            .sql()
            .exec("SELECT body FROM orders WHERE id = 1", None)?
            .to_array()?;
        match rows
            .first()
            .and_then(|row| row.get("body")?.as_str().map(str::to_owned))
        {
            Some(body) => serde_json::from_str(&body).map_err(|e| Error::RustError(e.to_string())),
            None => Ok(Order::default()),
        }
    }

    fn save(&self, order: &Order) -> Result<()> {
        let body = serde_json::to_string(order).map_err(|e| Error::RustError(e.to_string()))?;
        self.hub.state().storage().sql().exec(
            "INSERT INTO orders (id, body) VALUES (1, ?) ON CONFLICT(id) DO UPDATE SET body = excluded.body",
            vec![body.into()],
        )?;
        Ok(())
    }
}

impl DurableObject for OrderChannel {
    fn new(state: State, _env: Env) -> Self {
        let hub = Hub::new(state, HubConfig::default());
        let created = hub.state().storage().sql().exec(
            "CREATE TABLE IF NOT EXISTS orders (id INTEGER PRIMARY KEY CHECK (id = 1), body TEXT NOT NULL)",
            None,
        );
        if let Err(e) = created {
            console_error!("creating the orders table failed: {e}");
        }
        Self { hub }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let upgrade = req.headers().get("upgrade")?.is_some();
        match (upgrade, req.method(), req.path().as_str()) {
            (false, Method::Get, "/order") => Response::from_json(&OrderSnapshot {
                order: self.load()?,
                head: self.hub.head()?,
            }),
            // Publisher::publish lands in the hub, which applies the event to the order in
            // the same turn as it stores and sends it.
            _ => {
                self.hub
                    .fetch_with(req, |event| {
                        let mut order = self.load()?;
                        order.apply(event);
                        self.save(&order)
                    })
                    .await
            }
        }
    }

    partyline_worker::hub_handlers!(hub);
}

/// The `{id}` route parameter, percent-decoded. Routers hand it out still encoded, and the
/// client encodes the ID, so `Connect` and `Publisher` must both use the decoded form.
fn route_id<D>(ctx: &RouteContext<D>) -> Option<String> {
    ctx.param("id").and_then(|id| decode_segment(id))
}

#[event(fetch)]
async fn fetch(req: Request, env: Env, _ctx: Context) -> Result<Response> {
    Router::new()
        .get_async("/partyline/orders/:id", |req, ctx| async move {
            let Some(id) = route_id(&ctx) else {
                return reject(close::BAD_REQUEST, "invalid id");
            };
            // A real app authorizes the request here, and tags the socket with the user ID.
            // See docs/how-to/authenticate-with-clerk.md.
            Connect::<Orders>::new(id)
                .forward(&ctx.env, BINDING, req)
                .await
        })
        .get_async("/api/orders/:id", |_req, ctx| async move {
            let Some(id) = route_id(&ctx) else {
                return Response::error("invalid id", 400);
            };
            let stub = ctx
                .env
                .durable_object(BINDING)?
                .id_from_name(&id)?
                .get_stub()?;
            stub.fetch_with_str("https://order/order").await
        })
        .post_async("/api/orders/:id/events", |mut req, ctx| async move {
            let Some(id) = route_id(&ctx) else {
                return Response::error("invalid id", 400);
            };
            let event: OrderEvent = match req.json().await {
                Ok(event) => event,
                Err(e) => return Response::error(e.to_string(), 400),
            };
            let head = Publisher::<Orders>::new(&ctx.env, BINDING)?
                .publish(&id, &event)
                .await?;
            Response::from_json(&head)
        })
        .run(req, env)
        .await
}
