use std::path::PathBuf;

use common::id::{SessionId, WorkerId, DataObjectId, TaskId, ClientId, SId};
use common::events::{Event, EventType, NewWorkerEvent, RemovedWorkerEvent, WorkerFailedEvent,
                     NewClientEvent, RemovedClientEvent, ClientSubmitEvent, ClientUnkeepEvent,
                     ClientInvalidRequestEvent, TaskStartedEvent, TaskFinishedEvent,
                     TaskFailedEvent, DataObjectFinishedEvent, DataObjectRemovedEvent,
                     WorkerMonitoringEvent};
use common::monitor::Frame;
use common::fs::LogDir;
use errors::Result;
use super::logger::Logger;

use serde_json;
use rusqlite::Connection;

use chrono::Utc;


pub struct SQLiteLogger {
    events: Vec<Event>,
    conn: Connection,
}

impl SQLiteLogger {
    pub fn new(log_dir: &PathBuf) -> Result<Self> {
        let conn = Connection::open(log_dir.join("events.sql"))?;
        conn.execute(
            "CREATE TABLE IF NOT EXISTS events (
                id SERIAL PRIMARY KEY,
                event TEXT NOT NULL,
                timestamp TEXT NOT NULL
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
                tx.prepare_cached("INSERT INTO events (timestamp, event) VALUES (?, ?)")?;

            for e in self.events.iter() {
                stmt.execute(&[&e.timestamp, &serde_json::to_string(&e.event)?])?;
            }
        }
        tx.commit()?;
        Ok(())
    }
}

impl Logger for SQLiteLogger {
    fn flush_events(&mut self) {
        debug!("Flushing {} events", self.events.len());
        self.save_events().unwrap_or_else(|e| {
            error!("Writing events failed: {}", e);
        });
        self.events.clear();
    }

    fn add_event(&mut self, event: Event) {
        self.events.push(event);
    }

    fn add_new_worker_event(&mut self, worker: WorkerId) {
        self.add_event(Event::new(
            EventType::WorkerNew(NewWorkerEvent::new(worker)),
            Utc::now(),
        ));
    }

    fn add_worker_removed_event(&mut self, worker: WorkerId, error_msg: String) {
        self.add_event(Event::new(
            EventType::WorkerRemoved(
                RemovedWorkerEvent::new(worker, error_msg),
            ),
            Utc::now(),
        ));
    }

    fn add_worker_failed_event(&mut self, worker: WorkerId, error_msg: String) {
        self.add_event(Event::new(
            EventType::WorkerFailed(
                WorkerFailedEvent::new(worker, error_msg),
            ),
            Utc::now(),
        ));
    }

    fn add_new_client_event(&mut self, client: ClientId) {
        self.add_event(Event::new(
            EventType::NewClient(NewClientEvent::new(client)),
            Utc::now(),
        ));
    }

    fn add_removed_client_event(&mut self, client: ClientId, error_msg: String) {
        self.add_event(Event::new(
            EventType::RemovedClient(
                RemovedClientEvent::new(client, error_msg),
            ),
            Utc::now(),
        ));
    }

    fn add_client_submit_event(&mut self, tasks: Vec<TaskId>, dataobjs: Vec<DataObjectId>) {
        self.add_event(Event::new(
            EventType::ClientSubmit(
                ClientSubmitEvent::new(tasks, dataobjs),
            ),
            Utc::now(),
        ));
    }

    fn add_client_invalid_request_event(&mut self, client: ClientId, error_msg: String) {
        self.add_event(Event::new(
            EventType::ClientInvalidRequest(
                ClientInvalidRequestEvent::new(client, error_msg),
            ),
            Utc::now(),
        ));
    }

    fn add_client_unkeep_event(&mut self, dataobjs: Vec<DataObjectId>) {
        self.add_event(Event::new(
            EventType::ClientUnkeep(ClientUnkeepEvent::new(dataobjs)),
            Utc::now(),
        ));
    }

    fn add_task_started_event(&mut self, task: TaskId, worker: WorkerId) {
        self.add_event(Event::new(
            EventType::TaskStarted(TaskStartedEvent::new(task, worker)),
            Utc::now(),
        ));
    }

    fn add_task_finished_event(&mut self, task: TaskId) {
        self.add_event(Event::new(
            EventType::TaskFinished(TaskFinishedEvent::new(task)),
            Utc::now(),
        ));
    }

    fn add_task_failed_event(&mut self, task: TaskId, worker: WorkerId, error_msg: String) {
        self.add_event(Event::new(
            EventType::TaskFailed(
                TaskFailedEvent::new(task, worker, error_msg),
            ),
            Utc::now(),
        ));
    }

    fn add_dataobject_finished_event(
        &mut self,
        dataobject: DataObjectId,
        worker: WorkerId,
        size: usize,
    ) {
        self.add_event(Event::new(
            EventType::DataObjectFinished(
                DataObjectFinishedEvent::new(dataobject, worker, size),
            ),
            Utc::now(),
        ));
    }

    fn add_dataobject_removed_event(&mut self, dataobject: DataObjectId, worker: WorkerId) {
        self.add_event(Event::new(
            EventType::DataObjectRemoved(
                DataObjectRemovedEvent::new(dataobject, worker),
            ),
            Utc::now(),
        ));
    }

    fn add_worker_monitoring_event(&mut self, frame: Frame, worker: WorkerId) {
        let timestamp = frame.timestamp.clone();
        self.add_event(Event::new(
            EventType::WorkerMonitoring(
                WorkerMonitoringEvent::new(frame, worker),
            ),
            timestamp,
        ));
    }

    fn add_dummy_event(&mut self) {
        self.add_event(Event::new(EventType::Dummy(), Utc::now()));
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

    fn create_test_frame() -> Frame {
        Frame {
            cpu_usage: vec![10, 10, 10, 10],
            mem_usage: 50,
            timestamp: Utc::now(),
            net_stat: [(String::from("net0"), vec![50])].iter().cloned().collect(),
        }
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
        let et = EventType::WorkerNew(NewWorkerEvent::new(w));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_worker_removed_event() {
        let mut logger = create_logger();
        let w = create_test_worker_id();
        let e = "error";
        logger.add_worker_removed_event(w, e.to_string());
        let et = EventType::WorkerRemoved(RemovedWorkerEvent::new(w, e.to_string()));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_worker_failed_event() {
        let mut logger = create_logger();
        let w = create_test_worker_id();
        let e = "error";
        logger.add_worker_failed_event(w, e.to_string());
        let et = EventType::WorkerFailed(WorkerFailedEvent::new(w, e.to_string()));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_new_client_event() {
        let mut logger = create_logger();
        let c = create_test_client_id();
        logger.add_new_client_event(c);
        let et = EventType::NewClient(NewClientEvent::new(c));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_removed_client_event() {
        let mut logger = create_logger();
        let c = create_test_client_id();
        let e = "error";
        logger.add_removed_client_event(c, e.to_string());
        let et = EventType::RemovedClient(RemovedClientEvent::new(c, e.to_string()));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_client_submit_event() {
        let mut logger = create_logger();
        let tasks = create_test_task_ids();
        let dataobjs = create_test_dataobj_ids();
        logger.add_client_submit_event(tasks.clone(), dataobjs.clone());
        let et = EventType::ClientSubmit(ClientSubmitEvent::new(tasks, dataobjs));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_client_invalid_request_event() {
        let mut logger = create_logger();
        let c = create_test_client_id();
        let e = "error";
        logger.add_client_invalid_request_event(c, e.to_string());
        let et = EventType::ClientInvalidRequest(ClientInvalidRequestEvent::new(c, e.to_string()));
        assert!(logger.events[0].event == et);
    }


    #[test]
    fn test_add_client_unkeep_event() {
        let mut logger = create_logger();
        let dataobjs = create_test_dataobj_ids();
        logger.add_client_unkeep_event(dataobjs.clone());
        let et = EventType::ClientUnkeep(ClientUnkeepEvent::new(dataobjs));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_task_started_event() {
        let mut logger = create_logger();
        let w = create_test_worker_id();
        let t = create_test_task_id();
        logger.add_task_started_event(t, w);
        let et = EventType::TaskStarted(TaskStartedEvent::new(t, w));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_task_finished_event() {
        let mut logger = create_logger();
        let t = create_test_task_id();
        logger.add_task_finished_event(t);
        let et = EventType::TaskFinished(TaskFinishedEvent::new(t));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_task_failed_event() {
        let mut logger = create_logger();
        let w = create_test_worker_id();
        let t = create_test_task_id();
        let e = "error";
        logger.add_task_failed_event(t, w, e.to_string());
        let et = EventType::TaskFailed(TaskFailedEvent::new(t, w, e.to_string()));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_dataobject_finished_event() {
        let mut logger = create_logger();
        let w = create_test_worker_id();
        let datobj = create_test_dataobj_id();
        let s: usize = 1024;
        logger.add_dataobject_finished_event(datobj, w, s);
        let et = EventType::DataObjectFinished(DataObjectFinishedEvent::new(datobj, w, s));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_dataobject_removed_event() {
        let mut logger = create_logger();
        let w = create_test_worker_id();
        let datobj = create_test_dataobj_id();
        logger.add_dataobject_removed_event(datobj, w);
        let et = EventType::DataObjectRemoved(DataObjectRemovedEvent::new(datobj, w));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_worker_monitoring_event() {
        let mut logger = create_logger();
        let frame = create_test_frame();
        let w = create_test_worker_id();
        logger.add_worker_monitoring_event(frame.clone(), w);
        let et = EventType::WorkerMonitoring(WorkerMonitoringEvent::new(frame, w));
        assert!(logger.events[0].event == et);
    }

    #[test]
    fn test_add_dummy_event() {
        let mut logger = create_logger();
        logger.add_dummy_event();
        let et = EventType::Dummy();
        assert!(logger.events[0].event == et);
    }
}
