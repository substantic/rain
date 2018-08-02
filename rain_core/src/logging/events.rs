use std::collections::HashMap;

use types::{ClientId, DataObjectId, GovernorId, ObjectSpec, SId, SessionId, TaskId, TaskSpec, SessionSpec};

pub type EventId = i64;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernorNewEvent {
    pub governor: GovernorId, // TODO: Resources
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct GovernorRemovedEvent {
    pub governor: GovernorId,
    pub error_msg: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientNewEvent {
    pub client: ClientId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientRemovedEvent {
    pub client: ClientId,
    // If client is disconnected because of error, otherwise empty
    pub error_msg: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionNewEvent {
    pub session: SessionId,
    pub client: ClientId,
    pub spec: SessionSpec,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SessionClosedReason {
    /// Client closed the session, the is the normal termination
    ClientClose,
    /// Sess ends with error
    Error,
    /// When server starts and reuses existing log,
    /// it find open sessions and write this close reason
    /// This means that the real closing time
    /// of the session is not know and original server
    /// probably crashed
    ServerLost
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct SessionClosedEvent {
    pub session: SessionId,
    pub reason: SessionClosedReason,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientSubmitEvent {
    pub tasks: Vec<TaskSpec>,
    pub dataobjs: Vec<ObjectSpec>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientUnkeepEvent {
    pub dataobjs: Vec<DataObjectId>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskStartedEvent {
    pub task: TaskId,
    pub governor: GovernorId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskFinishedEvent {
    pub task: TaskId,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct DataObjectFinishedEvent {
    pub dataobject: DataObjectId,
    pub governor: GovernorId,
    pub size: usize,
}

pub type CpuUsage = u8;
pub type MemUsage = u8;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MonitoringEvent {
    pub governor: GovernorId,
    pub cpu_usage: Vec<CpuUsage>,            // Cpu usage in percent
    pub mem_usage: MemUsage,                 // Memory usage in bytes
    pub net_stat: HashMap<String, Vec<u64>>, // Network IO
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TaskFailedEvent {
    pub task: TaskId,
    pub governor: GovernorId,
    pub error_msg: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ClientInvalidRequestEvent {
    pub client: ClientId,
    pub error_msg: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum Event {
    GovernorNew(GovernorNewEvent),
    GovernorRemoved(GovernorRemovedEvent),

    ClientNew(ClientNewEvent),
    ClientRemoved(ClientRemovedEvent),

    SessionNew(SessionNewEvent),
    SessionClosed(SessionClosedEvent),

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
            &Event::SessionClosed(_) => "SessionClosed",
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
            &Event::SessionClosed(ref e) => Some(e.session),
            &Event::ClientSubmit(ref e) => {
                // TODO: Quick hack, we expect that submit contains only tasks/obj from one session
                e.tasks.get(0).map(|t| t.id.get_session_id())
            }
            _ => None,
        }
    }
}
