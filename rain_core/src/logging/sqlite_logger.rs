use chrono::{DateTime, Utc};
use futures::Future;
use futures::Stream;
use futures::sync::{mpsc, oneshot};
use rusqlite::Connection;
use serde_json;
use std::path::PathBuf;

//use common::id::{SessionId, GovernorId, DataObjectId, TaskId, ClientId, SId};
use errors::{Error, Result};
use super::events;
use super::logger::{Logger, SearchCriteria, QueryEvents};
use types::SessionId;

#[derive(Clone, Debug)]
pub struct EventWrapper {
    pub event: events::Event,
    pub timestamp: DateTime<Utc>,
}

pub struct SQLiteLogger {
    events: Vec<EventWrapper>,
    queue: mpsc::UnboundedSender<LoggerMessage>,
    //    conn: Connection,
}

enum LoggerMessage {
    SaveEvents(Vec<EventWrapper>),
    LoadEvents(SearchCriteria, oneshot::Sender<QueryEvents>),
}

fn save_events(conn: &mut Connection, events: Vec<EventWrapper>) -> Result<()> {
    debug!("Saving {} events into log", events.len());
    let tx = conn.transaction()?;
    {
        let mut stmt = tx.prepare_cached(
            "INSERT INTO events (timestamp, event_type, session, event) VALUES (?, ?, ?, ?)",
        )?;

        for e in events.iter() {
            stmt.execute(&[
                &e.timestamp,
                &e.event.event_type(),
                &e.event.session_id(),
                &serde_json::to_string(&e.event)?,
            ])?;
        }
    }
    tx.commit()?;
    Ok(())
}

fn load_events(conn: &mut Connection, search_criteria: &SearchCriteria) -> Result<QueryEvents> {
    let mut args: Vec<&::rusqlite::types::ToSql> = Vec::new();
    let mut where_conds = Vec::new();

    if let Some(ref v) = search_criteria.id {
        where_conds.push(make_where_string("id", &v.mode)?);
        args.push(&v.value);
    }

    if let Some(ref v) = search_criteria.event_types {
        let conditions: Result<Vec<_>> = v
            .iter()
            .map(|e| make_where_string("event_type", &e.mode))
            .collect();
        where_conds.push(format!("({})", conditions?.join(" OR ")));

        for event in v {
            args.push(&event.value);
        }
    }

    if let Some(ref v) = search_criteria.session {
        where_conds.push(make_where_string("session", &v.mode)?);
        args.push(&v.value);
    }

    let query_str = if where_conds.is_empty() {
        "SELECT id, timestamp, event FROM events ORDER BY id".to_string()
    } else {
        format!(
            "SELECT id, timestamp, event FROM events WHERE {} ORDER BY id",
            where_conds.join(" AND ")
        )
    };

    debug!("Running query: {}", query_str);
    let mut query = conn.prepare_cached(&query_str)?;
    //query.execute(&[])?;
    let iter = query
        .query_map(&args, |row| (row.get(0), row.get(1), row.get(2)))?
        .map(|e| e.unwrap());
    let results: Vec<_> = iter.collect();
    debug!("Logger query response: {} rows", results.len());
    Ok(results)
}

impl SQLiteLogger {
    pub fn new(log_dir: &PathBuf) -> Result<(Self, SessionId)> {
        let mut conn = Connection::open(log_dir.join("events.db"))?;

        // There are basically two type of queries
        // (1) initial "big", where "id" is not involved
        // (2) "small" update, where we ask only for new updates, and ID is involved
        // Indexes are created for type (1) query; type (2) uses implicit "id" index
        // TOOD: This needs a benchmark

        conn.execute(
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                timestamp TEXT NOT NULL,
                event_type VARCHAR(14) NOT NULL,
                session INTEGER,
                event TEXT NOT NULL
             );
             CREATE INDEX IF NOT EXISTS idx_timestamp ON events(timestamp);
             CREATE INDEX IF NOT EXISTS idx_event_type ON events(event_type);
             CREATE INDEX IF NOT EXISTS idx_session ON events(session);
             ",
            &[],
        )?;

        let last_session: SessionId = {
            let mut q = conn.prepare("SELECT MAX(session) FROM events WHERE session IS NOT NULL;")?;
            let mut r = q
                .query_map(&[], |row| row.get_checked(0).unwrap_or(0))
                .unwrap();
            r.next().unwrap().unwrap()
        };

        let (sx, rx) = mpsc::unbounded();

        ::std::thread::spawn(move || {
            debug!("Logger thread started");
            let mut core = ::tokio_core::reactor::Core::new().unwrap();
            let future = rx.for_each(move |m| {
                match m {
                    LoggerMessage::SaveEvents(events) => {
                        save_events(&mut conn, events).unwrap();
                    }
                    LoggerMessage::LoadEvents(search_criteria, sender) => {
                        match load_events(&mut conn, &search_criteria) {
                            Ok(result) => sender.send(result).unwrap(),
                            Err(e) => info!("Event query error: {}", e.description()),
                        };
                    }
                }
                Ok(())
            });
            core.run(future).unwrap();
        });

        Ok((
            SQLiteLogger {
                events: Vec::new(),
                queue: sx,
            },
            last_session,
        ))
    }
}

fn make_where_string(column: &str, mode: &str) -> Result<String> {
    match mode {
        "=" | "<" | ">" | "<=" | ">=" => Ok(format!("{} {} ?", column, mode)),
        _ => bail!("Invalid search criteria"),
    }
}

impl Logger for SQLiteLogger {
    fn get_events(
        &self,
        search_criteria: SearchCriteria,
    ) -> Box<Future<Item = QueryEvents, Error = Error>> {
        let (sx, rx) = oneshot::channel();
        self.queue
            .unbounded_send(LoggerMessage::LoadEvents(search_criteria, sx))
            .unwrap();
        Box::new(rx.map_err(|_| "Invalid logger query".into()))
    }

    fn flush_events(&mut self) {
        debug!("Flushing {} events", self.events.len());
        self.queue
            .unbounded_send(LoggerMessage::SaveEvents(::std::mem::replace(
                &mut self.events,
                Vec::new(),
            )))
            .unwrap();
    }

    fn add_event_with_timestamp(&mut self, event: events::Event, timestamp: DateTime<Utc>) {
        self.events.push(EventWrapper { event, timestamp });
    }
}

#[cfg(test)]

mod tests {
    use super::*;
    use common::id::GovernorId;
    use std::net::{IpAddr, Ipv4Addr};

    fn create_test_governor_id() -> GovernorId {
        GovernorId::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9010)
    }

    fn create_logger() -> SQLiteLogger {
        SQLiteLogger::new(&PathBuf::from("/tmp")).unwrap().0
    }

    #[test]
    fn test_add_event() {
        let mut logger = create_logger();
        logger.add_dummy_event();
        assert_eq!(logger.events.len(), 1);
    }

    #[test]
    fn test_flush_events() {
        let mut logger = create_logger();
        logger.add_dummy_event();
        logger.add_dummy_event();
        assert_eq!(logger.events.len(), 2);
        logger.flush_events();
        assert_eq!(logger.events.len(), 0);
    }
}
