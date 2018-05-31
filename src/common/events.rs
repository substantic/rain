use super::id::{ClientId, DataObjectId, GovernorId, SessionId, TaskId};
use common::id::SId;
use common::{TaskSpec, ObjectSpec};
use server::graph::{DataObject, Task};

use std::collections::HashMap;

pub type EventId = i64;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GovernorNewEvent {
    pub governor: GovernorId, // TODO: Resources
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct GovernorRemovedEvent {
    pub governor: GovernorId,
    pub error_msg: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientNewEvent {
    pub client: ClientId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientRemovedEvent {
    pub client: ClientId,
    // If client is disconnected because of error, otherwise empty
    pub error_msg: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionNewEvent {
    pub session: SessionId,
    pub client: ClientId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct SessionCloseEvent {
    pub session: SessionId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientSubmitEvent {
    pub tasks: Vec<TaskSpec>,
    pub dataobjs: Vec<ObjectSpec>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientUnkeepEvent {
    pub dataobjs: Vec<DataObjectId>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskStartedEvent {
    pub task: TaskId,
    pub governor: GovernorId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskFinishedEvent {
    pub task: TaskId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataObjectFinishedEvent {
    pub dataobject: DataObjectId,
    pub governor: GovernorId,
    pub size: usize,
}

pub type CpuUsage = u8;
pub type MemUsage = u8;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MonitoringEvent {
    pub governor: GovernorId,
    pub cpu_usage: Vec<CpuUsage>,            // Cpu usage in percent
    pub mem_usage: MemUsage,                 // Memory usage in bytes
    pub net_stat: HashMap<String, Vec<u64>>, // Network IO
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskFailedEvent {
    pub task: TaskId,
    pub governor: GovernorId,
    pub error_msg: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientInvalidRequestEvent {
    pub client: ClientId,
    pub error_msg: String,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    GovernorNew(GovernorNewEvent),
    GovernorRemoved(GovernorRemovedEvent),

    ClientNew(ClientNewEvent),
    ClientRemoved(ClientRemovedEvent),

    SessionNew(SessionNewEvent),
    SessionClose(SessionCloseEvent),

    ClientSubmit(ClientSubmitEvent),
    ClientUnkeep(ClientUnkeepEvent),

    TaskStarted(TaskStartedEvent),
    TaskFinished(TaskFinishedEvent),
    DataObjectFinished(DataObjectFinishedEvent),

    Monitoring(MonitoringEvent),

    TaskFailed(TaskFailedEvent),
    ClientInvalidRequest(ClientInvalidRequestEvent),

    Dummy(i32),
}

impl Event {
    pub fn event_type(&self) -> &'static str {
        match self {
            &Event::GovernorNew(_) => "GovernorNew",
            &Event::GovernorRemoved(_) => "GovernorRemoved",
            &Event::ClientNew(_) => "ClientNew",
            &Event::ClientRemoved(_) => "ClientRemoved",
            &Event::SessionNew(_) => "SessionNew",
            &Event::SessionClose(_) => "SessionClose",
            &Event::ClientSubmit(_) => "ClientSubmit",
            &Event::ClientUnkeep(_) => "ClientUnkeep",
            &Event::TaskStarted(_) => "TaskStarted",
            &Event::TaskFinished(_) => "TaskFinished",
            &Event::TaskFailed(_) => "TaskFailed",
            &Event::DataObjectFinished(_) => "ObjectFinished",
            &Event::Monitoring(_) => "Monitoring",
            &Event::ClientInvalidRequest(_) => "InvalidRequest",
            &Event::Dummy(_) => "Dummy",
        }
    }

    pub fn session_id(&self) -> Option<SessionId> {
        match self {
            &Event::TaskFinished(ref e) => Some(e.task.get_session_id()),
            &Event::TaskStarted(ref e) => Some(e.task.get_session_id()),
            &Event::TaskFailed(ref e) => Some(e.task.get_session_id()),
            &Event::SessionNew(ref e) => Some(e.session),
            &Event::SessionClose(ref e) => Some(e.session),
            &Event::ClientSubmit(ref e) => {
                // TODO: Quick hack, we expect that submit contains only tasks/obj from one session
                e.tasks.get(0).map(|t| t.id.get_session_id())
            }
            _ => None,
        }
    }
}
