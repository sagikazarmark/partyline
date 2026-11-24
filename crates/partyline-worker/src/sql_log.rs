//! The [`Log`] implementation over Durable Object SQLite storage.

use partyline::Cursor;
use partyline::server::{Log, Retention};
use worker::{SqlStorage, SqlStorageValue};

/// The statements the hub runs in its constructor.
///
/// Every channel in the object shares the two tables, keyed by the channel's name and ID.
pub(crate) const SCHEMA: [&str; 2] = [
    "CREATE TABLE IF NOT EXISTS partyline_channels (
        name  TEXT    NOT NULL,
        id    TEXT    NOT NULL,
        epoch INTEGER NOT NULL,
        head  INTEGER NOT NULL,
        PRIMARY KEY (name, id)
    ) WITHOUT ROWID",
    "CREATE TABLE IF NOT EXISTS partyline_events (
        name TEXT    NOT NULL,
        id   TEXT    NOT NULL,
        seq  INTEGER NOT NULL,
        ts   INTEGER NOT NULL,
        body BLOB    NOT NULL,
        PRIMARY KEY (name, id, seq)
    ) WITHOUT ROWID",
];

/// The tables of partyline 0.4 and earlier, which held one channel per object.
const LEGACY_META: &str = "partyline_meta";
const LEGACY_LOG: &str = "partyline_log";

/// The largest integer that survives the trip through a JavaScript number.
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// One channel's event log in the Durable Object's SQLite storage.
///
/// Every method runs synchronous SQL statements only. A Durable Object commits all writes
/// made without an intervening `await` atomically, so an append and its head update land
/// together.
#[derive(Clone, Debug)]
pub struct SqlLog {
    sql: SqlStorage,
    name: String,
    id: String,
}

impl SqlLog {
    /// The log of the channel with this name and ID. Call [`SqlLog::init`] once per Durable
    /// Object instance first.
    pub fn new(sql: SqlStorage, name: impl Into<String>, id: impl Into<String>) -> Self {
        Self {
            sql,
            name: name.into(),
            id: id.into(),
        }
    }

    /// Creates the tables if they do not exist.
    pub fn init(sql: &SqlStorage) -> worker::Result<()> {
        for statement in SCHEMA {
            sql.exec(statement, None)?;
        }
        Ok(())
    }

    /// Moves the one channel of a partyline 0.4 object into the shared tables, as the
    /// channel with this name and ID, keeping its epoch, head, and events. Then drops the
    /// old tables, so it runs once. Returns whether there was anything to move.
    ///
    /// Every statement is synchronous, so the move commits as one write. A move that failed
    /// partway runs again on the next call: the inserts skip rows already moved.
    pub(crate) fn migrate_legacy(sql: &SqlStorage, name: &str, id: &str) -> worker::Result<bool> {
        let has_meta = table_exists(sql, LEGACY_META)?;
        let has_log = table_exists(sql, LEGACY_LOG)?;
        if !has_meta && !has_log {
            return Ok(false);
        }
        if has_meta {
            sql.exec(
                "INSERT OR IGNORE INTO partyline_channels (name, id, epoch, head)
                 SELECT ?, ?, epoch, head FROM partyline_meta WHERE id = 1",
                vec![name.into(), id.into()],
            )?;
        }
        if has_log {
            sql.exec(
                "INSERT OR IGNORE INTO partyline_events (name, id, seq, ts, body)
                 SELECT ?, ?, seq, ts, body FROM partyline_log",
                vec![name.into(), id.into()],
            )?;
        }
        sql.exec("DROP TABLE IF EXISTS partyline_log", None)?;
        sql.exec("DROP TABLE IF EXISTS partyline_meta", None)?;
        Ok(true)
    }

    /// The channel name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The channel ID.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Wipes the log and starts a new epoch. Old cursors no longer match.
    pub fn reset(&self) -> worker::Result<Cursor> {
        self.sql.exec(
            "DELETE FROM partyline_events WHERE name = ? AND id = ?",
            self.key(),
        )?;
        self.sql.exec(
            "DELETE FROM partyline_channels WHERE name = ? AND id = ?",
            self.key(),
        )?;
        self.head()
    }

    /// The timestamp of the oldest retained event, in milliseconds since the Unix epoch.
    pub fn oldest_ts(&self) -> worker::Result<Option<u64>> {
        scalar(
            &self.sql,
            "SELECT MIN(ts) FROM partyline_events WHERE name = ? AND id = ?",
            self.key(),
        )
    }

    fn key(&self) -> Vec<SqlStorageValue> {
        vec![self.name.as_str().into(), self.id.as_str().into()]
    }

    fn meta(&self) -> worker::Result<Option<Cursor>> {
        let row = self
            .sql
            .exec(
                "SELECT epoch, head FROM partyline_channels WHERE name = ? AND id = ?",
                self.key(),
            )?
            .raw()
            .next()
            .transpose()?;
        match row.as_deref() {
            Some([epoch, head]) => Ok(Some(Cursor::new(as_u64(epoch)?, as_u64(head)?))),
            _ => Ok(None),
        }
    }
}

impl Log for SqlLog {
    type Error = worker::Error;

    fn head(&self) -> worker::Result<Cursor> {
        if let Some(cursor) = self.meta()? {
            return Ok(cursor);
        }
        let epoch = new_epoch();
        let mut bindings = self.key();
        bindings.push(int(epoch)?);
        self.sql.exec(
            "INSERT INTO partyline_channels (name, id, epoch, head) VALUES (?, ?, ?, 0)",
            bindings,
        )?;
        Ok(Cursor::new(epoch, 0))
    }

    fn oldest(&self) -> worker::Result<Option<u64>> {
        scalar(
            &self.sql,
            "SELECT MIN(seq) FROM partyline_events WHERE name = ? AND id = ?",
            self.key(),
        )
    }

    fn append(&mut self, body: &[u8], now_ms: u64) -> worker::Result<u64> {
        let seq = self.head()?.seq + 1;
        // Insert first: if it fails, the head is unchanged.
        let mut bindings = self.key();
        bindings.extend([
            int(seq)?,
            int(now_ms)?,
            SqlStorageValue::Blob(body.to_vec()),
        ]);
        self.sql.exec(
            "INSERT INTO partyline_events (name, id, seq, ts, body) VALUES (?, ?, ?, ?, ?)",
            bindings,
        )?;
        let mut bindings = vec![int(seq)?];
        bindings.extend(self.key());
        self.sql.exec(
            "UPDATE partyline_channels SET head = ? WHERE name = ? AND id = ?",
            bindings,
        )?;
        Ok(seq)
    }

    fn range(&self, after: u64) -> worker::Result<Vec<(u64, Vec<u8>)>> {
        let mut bindings = self.key();
        bindings.push(int(after)?);
        let mut rows = Vec::new();
        for row in self
            .sql
            .exec(
                "SELECT seq, body FROM partyline_events WHERE name = ? AND id = ? AND seq > ? ORDER BY seq",
                bindings,
            )?
            .raw()
        {
            match row?.as_slice() {
                [seq, SqlStorageValue::Blob(body)] => rows.push((as_u64(seq)?, body.clone())),
                [seq, SqlStorageValue::String(body)] => {
                    rows.push((as_u64(seq)?, body.clone().into_bytes()))
                }
                other => {
                    return Err(worker::Error::RustError(format!(
                        "unexpected log row: {other:?}"
                    )));
                }
            }
        }
        Ok(rows)
    }

    fn trim(&mut self, policy: &Retention, now_ms: u64) -> worker::Result<()> {
        let head = self.head()?.seq;
        if let Some(cutoff) = policy.count_cutoff(head) {
            let mut bindings = self.key();
            bindings.push(int(cutoff)?);
            self.sql.exec(
                "DELETE FROM partyline_events WHERE name = ? AND id = ? AND seq <= ?",
                bindings,
            )?;
        }
        if let Some(cutoff) = policy.age_cutoff(now_ms) {
            // Remove every event up to the newest expired one, not only the expired ones,
            // so a clock that went backwards cannot leave a gap in the log.
            let mut bindings = self.key();
            bindings.extend(self.key());
            bindings.push(int(cutoff)?);
            self.sql.exec(
                "DELETE FROM partyline_events WHERE name = ? AND id = ? AND seq <= (
                    SELECT MAX(seq) FROM partyline_events WHERE name = ? AND id = ? AND ts < ?
                )",
                bindings,
            )?;
        }
        Ok(())
    }
}

/// The timestamp of the oldest retained event on any channel named `name`.
pub(crate) fn oldest_ts_of(sql: &SqlStorage, name: &str) -> worker::Result<Option<u64>> {
    scalar(
        sql,
        "SELECT MIN(ts) FROM partyline_events WHERE name = ?",
        vec![name.into()],
    )
}

/// The IDs of the channels named `name` that hold an event older than `cutoff`.
pub(crate) fn ids_with_events_before(
    sql: &SqlStorage,
    name: &str,
    cutoff: u64,
) -> worker::Result<Vec<String>> {
    let mut ids = Vec::new();
    for row in sql
        .exec(
            "SELECT DISTINCT id FROM partyline_events WHERE name = ? AND ts < ?",
            vec![name.into(), int(cutoff)?],
        )?
        .raw()
    {
        match row?.as_slice() {
            [SqlStorageValue::String(id)] => ids.push(id.clone()),
            other => {
                return Err(worker::Error::RustError(format!(
                    "unexpected channel row: {other:?}"
                )));
            }
        }
    }
    Ok(ids)
}

fn table_exists(sql: &SqlStorage, table: &str) -> worker::Result<bool> {
    Ok(scalar(
        sql,
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?",
        vec![table.into()],
    )?
    .is_some_and(|n| n > 0))
}

fn scalar(
    sql: &SqlStorage,
    query: &str,
    bindings: Vec<SqlStorageValue>,
) -> worker::Result<Option<u64>> {
    let row = sql.exec(query, bindings)?.raw().next().transpose()?;
    match row.as_deref() {
        None | Some([SqlStorageValue::Null, ..]) => Ok(None),
        Some([value, ..]) => as_u64(value).map(Some),
        Some([]) => Err(worker::Error::RustError("empty row".to_owned())),
    }
}

/// A random epoch in `[1, 2^53)`, so it survives the trip through a JavaScript number.
fn new_epoch() -> u64 {
    let random = worker::js_sys::Math::random();
    ((random * MAX_SAFE_INTEGER as f64) as u64).clamp(1, MAX_SAFE_INTEGER)
}

fn int(value: u64) -> worker::Result<SqlStorageValue> {
    if value > MAX_SAFE_INTEGER {
        return Err(worker::Error::RustError(format!(
            "{value} exceeds the JavaScript safe integer range"
        )));
    }
    Ok(SqlStorageValue::Integer(value as i64))
}

fn as_u64(value: &SqlStorageValue) -> worker::Result<u64> {
    match value {
        SqlStorageValue::Integer(i) if *i >= 0 => Ok(*i as u64),
        SqlStorageValue::Float(f) if *f >= 0.0 && f.fract() == 0.0 => Ok(*f as u64),
        other => Err(worker::Error::RustError(format!(
            "expected a non-negative integer, got {other:?}"
        ))),
    }
}
