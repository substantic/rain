use super::id::{WorkerId, ClientId, DataObjectId, TaskId, SessionId};
use chrono::{DateTime, Utc};
use server::graph::{DataObject, Task};
use common::id::SId;

use std::collections::HashMap;


pub type EventId = i64;


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerNewEvent {
    pub worker: WorkerId, // TODO: Resources
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerRemovedEvent {
    pub worker: WorkerId,
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
pub struct ClientSubmitEvent {
    pub tasks: Vec<TaskDescriptor>,
    pub dataobjs: Vec<ObjectDescriptor>,
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct InputDescriptor {
    id: DataObjectId,
    label: String,
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskDescriptor {
    id: TaskId,
    inputs: Vec<InputDescriptor>,
    task_type: String,
    attributes: HashMap<String, String>,
}

impl TaskDescriptor {
    pub fn from(task: &Task) -> Self {
        TaskDescriptor {
            id: task.id(),
            inputs: task.inputs().iter().map(|i| {
                InputDescriptor {
                    id: i.object.get().id(),
                    label: i.label.clone(),
                }
            }).collect(),
            task_type: task.task_type().clone(),
            attributes: task.attributes().as_hashmap().clone(),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ObjectDescriptor {
    id: DataObjectId,
    producer: Option<TaskId>,
}

impl ObjectDescriptor {
    pub fn from(obj: &DataObject) -> Self {
        ObjectDescriptor {
            id: obj.id(),
            producer: obj.producer().as_ref().map(|t| t.get().id()),
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientUnkeepEvent {
    pub dataobjs: Vec<DataObjectId>,
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskStartedEvent {
    pub task: TaskId,
    pub worker: WorkerId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskFinishedEvent {
    pub task: TaskId,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataObjectFinishedEvent {
    pub dataobject: DataObjectId,
    pub worker: WorkerId,
    pub size: usize,
}

pub type CpuUsage = u8;
pub type MemUsage = u8;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct MonitoringEvent {
    pub cpu_usage: Vec<CpuUsage>,            // Cpu usage in percent
    pub mem_usage: MemUsage,                 // Memory usage in bytes
    pub net_stat: HashMap<String, Vec<u64>>, // Network IO
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskFailedEvent {
    pub task: TaskId,
    pub worker: WorkerId,
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
    WorkerNew(WorkerNewEvent),
    WorkerRemoved(WorkerRemovedEvent),

    ClientNew(ClientNewEvent),
    ClientRemoved(ClientRemovedEvent),

    SessionNew(SessionNewEvent),

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
            &Event::WorkerNew(_) => "WorkerNew",
            &Event::WorkerRemoved(_) => "WorkerRemoved",
            &Event::ClientNew(_) => "ClientNew",
            &Event::ClientRemoved(_) => "ClientRemoved",
            &Event::SessionNew(_) => "SessionNew",
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
            &Event::ClientSubmit(ref e) => {
                // TODO: Quick hack, we expect that submit contains only tasks/obj from one session
                e.tasks.get(0).map(|t| t.id.get_session_id())
            }
            _ => None,
        }
    }
}