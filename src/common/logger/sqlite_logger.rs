use std::path::PathBuf;

use common::id::{SessionId, WorkerId, DataObjectId, TaskId, ClientId, SId};
use common::events;
use common::fs::LogDir;
use errors::Result;
use super::logger::{SearchCriteria, Logger};

use serde_json;
use rusqlite::Connection;

use chrono::{DateTime, Utc};


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EventWrapper {
    pub event: events::Event,
    pub timestamp: DateTime<Utc>,
}

pub struct SQLiteLogger {
    events: Vec<EventWrapper>,
    conn: Connection,
}

impl SQLiteLogger {
    pub fn new(log_dir: &PathBuf) -> Result<Self> {
        let conn = Connection::open(log_dir.join("events.sql"))?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
                timestamp TEXT NOT NULL,
                event_type VARCHAR(14) NOT NULL,
                session INTEGER,
                event TEXT NOT NULL
             )",
            &[],
        )?;

        Ok(SQLiteLogger {
            events: Vec::new(),
            conn: conn,
        })
    }

    fn save_events(&mut self) -> Result<()> {
        let tx = self.conn.transaction()?;
        {
            let mut stmt =
                tx.prepare_cached("INSERT INTO events (timestamp, event_type, session, event) VALUES (?, ?, ?, ?)")?;

            for e in self.events.iter() {
                stmt.execute(&[&e.timestamp,
                    &e.event.event_type(),
                    &e.event.session_id(),
                    &serde_json::to_string(&e.event)?])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}


fn make_where_string(column: &str, mode: &str) -> Result<String> {
    match mode {
        "=" | "<" | ">" | "<=" | ">=" => Ok(format!("{} {} ?", column, mode)),
        _ => bail!("Invalid search criteria")
    }
}


impl Logger for SQLiteLogger {

    fn get_events(&self, search_criteria: &SearchCriteria) -> Result<Vec<(events::EventId, DateTime<Utc>, String)>> {
        let mut args : Vec<&::rusqlite::types::ToSql> = Vec::new();
        let mut where_conds = Vec::new();


        if let Some(ref v) = search_criteria.id {
            where_conds.push(make_where_string("id", &v.mode)?);
            args.push(&v.value);
        }

        if let Some(ref v) = search_criteria.event_type {
            where_conds.push(make_where_string("event_type", &v.mode)?);
            args.push(&v.value);
        }

        if let Some(ref v) = search_criteria.session {
            where_conds.push(make_where_string("session", &v.mode)?);
            args.push(&v.value);
        }

        let query_str = if where_conds.is_empty() {
            "SELECT id, timestamp, event FROM events ORDER BY id".to_string()
        } else {
            format!("SELECT id, timestamp, event FROM events WHERE {} ORDER BY id", where_conds.join(" AND "))
        };

        debug!("Running query: {}", query_str);
        let mut query = self.conn.prepare_cached(&query_str)?;
        //query.execute(&[])?;
        let iter = query.query_map(&args, |row| {
            (row.get(0), row.get(1), row.get(2))
        })?.map(|e| e.unwrap());
        Ok(iter.collect())
    }

    fn flush_events(&mut self) {
        debug!("Flushing {} events", self.events.len());
        self.save_events().unwrap_or_else(|e| {
            error!("Writing events failed: {}", e);
        });
        self.events.clear();
    }

    fn add_event_with_timestamp(&mut self, event: events::Event, timestamp: DateTime<Utc>) {
        self.events.push(EventWrapper{event, timestamp});
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn create_test_worker_id() -> WorkerId {
        WorkerId::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9010)
    }

    fn create_test_client_id() -> ClientId {
        ClientId::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 9010)
    }

    fn create_test_task_ids() -> Vec<TaskId> {
        vec![TaskId::new(1, 1)]
    }

    fn create_test_task_id() -> TaskId {
        TaskId::new(1, 1)
    }

    fn create_test_dataobj_ids() -> Vec<DataObjectId> {
        vec![DataObjectId::new(1, 1)]
    }

    fn create_test_dataobj_id() -> DataObjectId {
        DataObjectId::new(1, 1)
    }

    fn create_logger() -> SQLiteLogger {
        SQLiteLogger::new(&PathBuf::from("/tmp")).unwrap()
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

    #[test]
    fn test_add_new_worker_event() {
        let mut logger = create_logger();
        let w = create_test_worker_id();
        logger.add_new_worker_event(w);
        let et = events::Event::WorkerNew(events::WorkerNewEvent {worker: w});
        assert!(logger.events[0].event == et);
    }
}
