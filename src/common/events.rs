
use super::id::{WorkerId, ClientId, DataObjectId, TaskId};

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewWorkerEvent {
    worker: WorkerId
    // TODO: Resources
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemovedWorkerEvent {
    worker: WorkerId,
    error_msg: String
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct NewClientEvent {
    client: ClientId
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RemovedClientEvent {
    client: ClientId,
    // If client is disconnected because of error, otherwise empty
    error_msg: String
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Input {
    task: TaskId,
    label: String,
    path: String
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Task {
    id: TaskId,
    inputs: Vec<Input>,
    task_type: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataObject {
    id: DataObjectId,
    label: String,
    produced: TaskId,
    keep: bool
    // TODO: DataType ...
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientSubmitEvent {
    tasks: Vec<Task>,
    objects: Vec<DataObject>
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientUnkeepEvent {
    dataobjs: Vec<DataObjectId>
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskStartedEvent {
    task: TaskId,
    worker: WorkerId
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskFinishedEvent {
    task: TaskId
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataObjectFinishedEvent {
    dataobject: DataObjectId,
    worker: WorkerId,
    size: usize
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct DataObjectRemovedEvent {
    dataobject: DataObjectId,
    worker: WorkerId,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerMonitoringEvent {
    worker: WorkerId
    // TODO cpu load, etc...
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TaskFailedEvent {
    task: DataObjectId,
    worker: WorkerId,
    error_msg: String
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerFailedEvent {
    worker: WorkerId,
    error_msg: String
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ClientInvalidRequestEvent {
    client: ClientId,
    error_msg: String
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Event {

    WorkerNew(NewWorkerEvent),
    RemovedWorkerEvent(RemovedWorkerEvent),

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
    ClientInvalidRequest(ClientInvalidRequestEvent)
}
