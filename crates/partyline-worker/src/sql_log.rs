//! The [`Log`] implementation over Durable Object SQLite storage.

use partyline::Cursor;
use partyline::server::{Log, Retention};
use worker::{SqlStorage, SqlStorageValue};

/// The statements the hub runs in its constructor.
pub(crate) const SCHEMA: [&str; 2] = [
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

/// The largest integer that survives the trip through a JavaScript number.
const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;

/// A channel's event log in the Durable Object's SQLite storage.
///
/// Every method runs synchronous SQL statements only. A Durable Object commits all writes
/// made without an intervening `await` atomically, so an append and its head update land
/// together.
#[derive(Clone, Debug)]
pub struct SqlLog {
    sql: SqlStorage,
}

impl SqlLog {
    /// Wraps the storage. Call [`SqlLog::init`] once per Durable Object instance first.
    pub fn new(sql: SqlStorage) -> Self {
        Self { sql }
    }

    /// Creates the tables if they do not exist.
    pub fn init(sql: &SqlStorage) -> worker::Result<()> {
        for statement in SCHEMA {
            sql.exec(statement, None)?;
        }
        Ok(())
    }

    /// Wipes the log and starts a new epoch. Old cursors no longer match.
    pub fn reset(&self) -> worker::Result<Cursor> {
        self.sql.exec("DELETE FROM partyline_log", None)?;
        self.sql.exec("DELETE FROM partyline_meta", None)?;
        self.head()
    }

    /// The timestamp of the oldest retained event, in milliseconds since the Unix epoch.
    pub fn oldest_ts(&self) -> worker::Result<Option<u64>> {
        self.scalar("SELECT MIN(ts) FROM partyline_log", vec![])
    }

    fn scalar(&self, query: &str, bindings: Vec<SqlStorageValue>) -> worker::Result<Option<u64>> {
        let row = self.sql.exec(query, bindings)?.raw().next().transpose()?;
        match row.as_deref() {
            None | Some([SqlStorageValue::Null, ..]) => Ok(None),
            Some([value, ..]) => as_u64(value).map(Some),
            Some([]) => Err(worker::Error::RustError("empty row".to_owned())),
        }
    }

    fn meta(&self) -> worker::Result<Option<Cursor>> {
        let row = self
            .sql
            .exec("SELECT epoch, head FROM partyline_meta WHERE id = 1", None)?
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
        self.sql.exec(
            "INSERT INTO partyline_meta (id, epoch, head) VALUES (1, ?, 0)",
            vec![int(epoch)?],
        )?;
        Ok(Cursor::new(epoch, 0))
    }

    fn oldest(&self) -> worker::Result<Option<u64>> {
        self.scalar("SELECT MIN(seq) FROM partyline_log", vec![])
    }

    fn append(&mut self, body: &[u8], now_ms: u64) -> worker::Result<u64> {
        let seq = self.head()?.seq + 1;
        // Insert first: if it fails, the head is unchanged.
        self.sql.exec(
            "INSERT INTO partyline_log (seq, ts, body) VALUES (?, ?, ?)",
            vec![
                int(seq)?,
                int(now_ms)?,
                SqlStorageValue::Blob(body.to_vec()),
            ],
        )?;
        self.sql.exec(
            "UPDATE partyline_meta SET head = ? WHERE id = 1",
            vec![int(seq)?],
        )?;
        Ok(seq)
    }

    fn range(&self, after: u64) -> worker::Result<Vec<(u64, Vec<u8>)>> {
        let mut rows = Vec::new();
        for row in self
            .sql
            .exec(
                "SELECT seq, body FROM partyline_log WHERE seq > ? ORDER BY seq",
                vec![int(after)?],
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
            self.sql.exec(
                "DELETE FROM partyline_log WHERE seq <= ?",
                vec![int(cutoff)?],
            )?;
        }
        if let Some(cutoff) = policy.age_cutoff(now_ms) {
            self.sql
                .exec("DELETE FROM partyline_log WHERE ts < ?", vec![int(cutoff)?])?;
        }
        Ok(())
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
