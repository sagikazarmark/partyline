//! An object that starts with partyline 0.4's tables, to test the migration.
//!
//! The hub is created on the first request that needs it, not in the constructor. A test
//! first writes the 0.4 tables with `POST /seed` into an object that has no hub yet, then
//! the next request creates the hub, which migrates them. A wake does the same in an
//! object that a 0.4 Worker wrote.

use std::cell::{OnceCell, RefCell};

use partyline_worker::{Hub, HubConfig};
use worker::{
    DurableObject, Env, Request, Response, Result, SqlStorage, SqlStorageValue, State,
    durable_object, wasm_bindgen,
};

use crate::Ticks;

/// The epoch `POST /seed` writes.
pub const LEGACY_EPOCH: i64 = 4_242;

/// The tables, as partyline 0.4 created them.
const LEGACY_SCHEMA: [&str; 2] = [
    "CREATE TABLE IF NOT EXISTS partyline_meta (
        id    INTEGER PRIMARY KEY CHECK (id = 1),
        epoch INTEGER NOT NULL,
        head  INTEGER NOT NULL
    )",
    "CREATE TABLE IF NOT EXISTS partyline_log (
        seq  INTEGER PRIMARY KEY,
        ts   INTEGER NOT NULL,
        body BLOB    NOT NULL
    )",
];

#[durable_object]
pub struct LegacyChannel {
    /// The state until the hub takes it.
    state: RefCell<Option<State>>,
    hub: OnceCell<Hub<Ticks>>,
}

impl LegacyChannel {
    fn hub(&self) -> &Hub<Ticks> {
        self.hub.get_or_init(|| {
            let state = self.state.borrow_mut().take().expect("the state is unused");
            Hub::new(state, HubConfig::default())
        })
    }

    fn sql(&self) -> SqlStorage {
        match self.hub.get() {
            Some(hub) => hub.state().storage().sql(),
            None => self
                .state
                .borrow()
                .as_ref()
                .expect("the state is unused")
                .storage()
                .sql(),
        }
    }

    /// Writes a 0.4 channel in `epoch`, at seq 3, with the events 1, 2, and 3.
    ///
    /// With `again`, it writes them even though the hub exists, as a 0.4 Worker would after
    /// a rollback. `POST /remigrate` then runs the migration again.
    fn seed(&self, epoch: i64, again: bool) -> Result<Response> {
        if self.hub.get().is_some() && !again {
            return Response::error("the hub already exists", 409);
        }
        let sql = self.sql();
        for statement in LEGACY_SCHEMA {
            sql.exec(statement, None)?;
        }
        sql.exec(
            "INSERT INTO partyline_meta (id, epoch, head) VALUES (1, ?, 3)",
            vec![epoch.into()],
        )?;
        let now = worker::Date::now().as_millis() as i64;
        for seq in 1..=3_i64 {
            sql.exec(
                "INSERT INTO partyline_log (seq, ts, body) VALUES (?, ?, ?)",
                vec![
                    seq.into(),
                    now.into(),
                    SqlStorageValue::Blob(seq.to_string().into_bytes()),
                ],
            )?;
        }
        Response::ok(epoch.to_string())
    }

    /// Creates another hub on the same state, which runs the migration again, as the next
    /// wake of the object would.
    fn remigrate(&self) -> Result<Response> {
        use worker::wasm_bindgen::{JsCast, JsValue};
        let raw: &JsValue = self.hub().state().as_raw().as_ref();
        let raw = raw
            .clone()
            .unchecked_into::<worker::worker_sys::DurableObjectState>();
        drop(Hub::<Ticks>::new(State::from(raw), HubConfig::default()));
        Response::ok("")
    }
}

impl DurableObject for LegacyChannel {
    fn new(state: State, _env: Env) -> Self {
        Self {
            state: RefCell::new(Some(state)),
            hub: OnceCell::new(),
        }
    }

    async fn fetch(&self, req: Request) -> Result<Response> {
        let url = req.url()?;
        let param = |name: &str| {
            url.query_pairs()
                .find_map(|(k, v)| (k == name).then(|| v.into_owned()))
        };
        match req.path().as_str() {
            "/seed" => {
                let epoch = param("epoch")
                    .and_then(|e| e.parse().ok())
                    .unwrap_or(LEGACY_EPOCH);
                self.seed(epoch, param("again").as_deref() == Some("1"))
            }
            "/remigrate" => self.remigrate(),
            "/tables" => Response::ok(crate::partyline_tables(&self.sql())?),
            _ => self.hub().fetch(req).await,
        }
    }

    async fn websocket_message(
        &self,
        ws: worker::WebSocket,
        message: worker::WebSocketIncomingMessage,
    ) -> Result<()> {
        self.hub().on_message(ws, message).await
    }

    async fn websocket_close(
        &self,
        ws: worker::WebSocket,
        code: usize,
        reason: String,
        was_clean: bool,
    ) -> Result<()> {
        self.hub().on_close(ws, code, reason, was_clean).await
    }

    async fn alarm(&self) -> Result<Response> {
        self.hub().on_alarm().await
    }
}
