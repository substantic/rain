use chrono::{DateTime, Utc};
use futures::Future;
use serde_derive::Deserialize;

use rain_core::errors::Error;
use rain_core::logging::{events, Event, EventId, DummyEvent};
use rain_core::types::{ClientId, DataObjectId, GovernorId, ObjectSpec, SessionId, TaskId, TaskSpec, SessionSpec, TaskInfo};


#[derive(Deserialize)]
pub struct SearchItemInt {
    pub value: i64,
    pub mode: String,
}

#[derive(Deserialize)]
pub struct SearchItemString {
    pub value: String,
    pub mode: String,
}

#[derive(Deserialize)]
pub struct SearchCriteria {
    pub id: Option<SearchItemInt>,
    pub event_types: Option<Vec<SearchItemString>>,
    pub session: Option<SearchItemInt>,
}

pub type QueryEvents = Vec<(EventId, DateTime<Utc>, String)>;

pub trait Logger {
    fn add_event(&mut self, event: Event) {
        self.add_event_with_timestamp(event, Utc::now());
    }

    fn add_event_with_timestamp(&mut self, event: Event, time: ::chrono::DateTime<::chrono::Utc>);

    fn flush_events(&mut self);

    fn add_new_governor_event(&mut self, governor: GovernorId) {
        self.add_event(Event::GovernorNew(events::GovernorNewEvent { governor }));
    }

    fn add_governor_removed_event(&mut self, governor: GovernorId, error_msg: String) {
        self.add_event(Event::GovernorRemoved(events::GovernorRemovedEvent {
            governor,
            error_msg,
        }));
    }

    fn add_governor_new_event(&mut self, governor: GovernorId) {
        self.add_event(Event::GovernorNew(events::GovernorNewEvent { governor }));
    }

    fn add_new_client_event(&mut self, client: ClientId) {
        self.add_event(Event::ClientNew(events::ClientNewEvent { client }));
    }

    fn add_removed_client_event(&mut self, client: ClientId, error_msg: String) {
        self.add_event(Event::ClientRemoved(events::ClientRemovedEvent {
            client,
            error_msg,
        }));
    }

    fn add_client_invalid_request_event(&mut self, client: ClientId, error_msg: String) {
        self.add_event(Event::ClientInvalidRequest(
            events::ClientInvalidRequestEvent { client, error_msg },
        ));
    }

    fn add_client_unkeep_event(&mut self, dataobjs: Vec<DataObjectId>) {
        self.add_event(Event::ClientUnkeep(events::ClientUnkeepEvent { dataobjs }));
    }

    fn add_task_started_event(&mut self, task: TaskId, info: TaskInfo) {
        self.add_event(Event::TaskStarted(events::TaskStartedEvent {
            task,
            info,
        }));
    }

    fn add_task_finished_event(&mut self, task: TaskId, info: TaskInfo) {
        self.add_event(Event::TaskFinished(events::TaskFinishedEvent { task, info }));
    }

    fn add_dataobject_finished_event(
        &mut self,
        dataobject: DataObjectId,
        governor: GovernorId,
        size: usize,
    ) {
        self.add_event(Event::DataObjectFinished(events::DataObjectFinishedEvent {
            dataobject,
            governor,
            size,
        }));
    }

    fn add_dummy_event(&mut self) {
        self.add_event(Event::Dummy(DummyEvent {dummy_value: 1}));
    }

    fn add_client_submit_event(&mut self, tasks: Vec<TaskSpec>, dataobjs: Vec<ObjectSpec>) {
        self.add_event(Event::ClientSubmit(events::ClientSubmitEvent {
            tasks,
            dataobjs,
        }));
    }

    fn add_new_session_event(&mut self, session: SessionId, client: ClientId, spec: SessionSpec) {
        self.add_event(Event::SessionNew(events::SessionNewEvent {
            session,
            client,
            spec,
        }));
    }

    fn add_closed_session_event(&mut self,
                                session: SessionId,
                                reason: events::SessionClosedReason,
                                message: String) {
        self.add_event(Event::SessionClosed(events::SessionClosedEvent { session, reason, message }));
    }

    fn get_events(
        &self,
        search_criteria: SearchCriteria,
    ) -> Box<Future<Item = QueryEvents, Error = Error>>;
}
