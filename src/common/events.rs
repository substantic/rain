use super::id::{WorkerId, ClientId, DataObjectId, TaskId};
use super::monitor::Frame;
use chrono::{DateTime, Utc};


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewWorkerEvent {
    worker: WorkerId
    // TODO: Resources
}

impl NewWorkerEvent {
    pub fn new(worker: WorkerId) -> Self {
        NewWorkerEvent {
            worker: worker
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemovedWorkerEvent {
    worker: WorkerId,
    error_msg: String
}

impl RemovedWorkerEvent {
    pub fn new(worker: WorkerId, error_msg: String) -> Self {
        RemovedWorkerEvent {
            worker: worker,
            error_msg: error_msg
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct NewClientEvent {
    client: ClientId
}

impl NewClientEvent {
    pub fn new(client: ClientId) -> Self {
        NewClientEvent {
            client: client
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct RemovedClientEvent {
    client: ClientId,
    // If client is disconnected because of error, otherwise empty
    error_msg: String
}

impl RemovedClientEvent {
    pub fn new(client: ClientId, error_msg: String) -> Self {
        RemovedClientEvent {
            client: client,
            error_msg: error_msg
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientSubmitEvent {
    tasks: Vec<TaskId>,
    dataobjs: Vec<DataObjectId>
}

impl ClientSubmitEvent {
    pub fn new(tasks: Vec<TaskId>, dataobjs: Vec<DataObjectId>) -> Self {
        ClientSubmitEvent {
            tasks: tasks,
            dataobjs: dataobjs
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientUnkeepEvent {
    dataobjs: Vec<DataObjectId>
}

impl ClientUnkeepEvent {
    pub fn new(dataobjs: Vec<DataObjectId>) -> Self {
        ClientUnkeepEvent {
            dataobjs: dataobjs
        }
    }
}



#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskStartedEvent {
    task: TaskId,
    worker: WorkerId
}

impl TaskStartedEvent {
    pub fn new(task: TaskId, worker: WorkerId) -> Self {
        TaskStartedEvent {
            task: task,
            worker: worker
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskFinishedEvent {
    task: TaskId
}

impl TaskFinishedEvent {
    pub fn new(task: TaskId) -> Self {
        TaskFinishedEvent {
            task: task
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataObjectFinishedEvent {
    dataobject: DataObjectId,
    worker: WorkerId,
    size: usize
}

impl DataObjectFinishedEvent {
    pub fn new(dataobject: DataObjectId, worker: WorkerId, size: usize) -> Self {
        DataObjectFinishedEvent {
            dataobject: dataobject,
            worker: worker,
            size: size
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct DataObjectRemovedEvent {
    dataobject: DataObjectId,
    worker: WorkerId,
}

impl DataObjectRemovedEvent {
    pub fn new(dataobject: DataObjectId, worker: WorkerId) -> Self {
        DataObjectRemovedEvent {
            dataobject: dataobject,
            worker: worker
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerMonitoringEvent {
    frame: Frame,
    worker: WorkerId
}

impl WorkerMonitoringEvent {
    pub fn new(frame: Frame, worker: WorkerId) -> Self {
        WorkerMonitoringEvent {
            frame: frame,
            worker: worker
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct TaskFailedEvent {
    task: TaskId,
    worker: WorkerId,
    error_msg: String
}

impl TaskFailedEvent {
    pub fn new(task: TaskId, worker: WorkerId, error_msg: String) -> Self {
        TaskFailedEvent {
            task: task,
            worker: worker,
            error_msg: error_msg
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct WorkerFailedEvent {
    worker: WorkerId,
    error_msg: String
}

impl WorkerFailedEvent {
    pub fn new(worker: WorkerId, error_msg: String) -> Self {
        WorkerFailedEvent {
            worker: worker,
            error_msg: error_msg
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct ClientInvalidRequestEvent {
    client: ClientId,
    error_msg: String
}

impl ClientInvalidRequestEvent {
    pub fn new(client: ClientId, error_msg: String) -> Self {
        ClientInvalidRequestEvent {
            client: client,
            error_msg: error_msg
        }
    }
}


#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub enum EventType {

    WorkerNew(NewWorkerEvent),
    WorkerRemoved(RemovedWorkerEvent),

    NewClient(NewClientEvent),
    RemovedClient(RemovedClientEvent),

    ClientSubmit(ClientSubmitEvent),
    ClientUnkeep(ClientUnkeepEvent),

    TaskStarted(TaskStartedEvent),
    TaskFinished(TaskFinishedEvent),

    DataObjectFinished(DataObjectFinishedEvent),
    DataObjectRemoved(DataObjectRemovedEvent),

    WorkerMonitoring(WorkerMonitoringEvent),

    TaskFailed(TaskFailedEvent),
    WorkerFailed(WorkerFailedEvent),
    ClientInvalidRequest(ClientInvalidRequestEvent),

    Dummy()
}


#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub event: EventType,
    pub timestamp: DateTime<Utc>
}


impl Event {
    pub fn new(event: EventType, timestamp: DateTime<Utc>) -> Self {
        Event {
            event: event,
            timestamp: timestamp
        }
    }
}