
use super::id::{WorkerId, ClientId, DataObjectId, TaskId};

struct NewWorkerEvent {
    worker: WorkerId
    // TODO: Resources
}

struct RemovedWorkerEvent {
    worker: WorkerId,
    error_msg: String
}

struct NewClientEvent {
    client: ClientId
}

struct RemovedClientEvent {
    client: ClientId,
    // If client is disconnected because of error, otherwise empty
    error_msg: String
}

struct Input {
    task: TaskId,
    label: String,
    path: String
}

struct Task {
    id: TaskId,
    inputs: Vec<Input>,
    task_type: String,
}

struct DataObject {
    id: DataObjectId,
    label: String,
    produced: TaskId,
    keep: bool
    // TODO: DataType ...
}

struct ClientSubmitEvent {
    tasks: Vec<Task>,
    objects: Vec<DataObject>
}

struct ClientUnkeepEvent {
    dataobjs: Vec<DataObjectId>
}

struct TaskStartedEvent {
    task: TaskId,
    worker: WorkerId
}

struct TaskFinishedEvent {
    task: TaskId
}

struct DataObjectFinishedEvent {
    dataobject: DataObjectId,
    worker: WorkerId,
    size: usize
}

struct DataObjectRemovedEvent {
    dataobject: DataObjectId,
    worker: WorkerId,
}

struct WorkerMonitoringEvent {
    worker: WorkerId
    // TODO cpu load, etc...
}


enum Event {

    WorkerNew(NewWorkerEvent),
    RemovedWorkerEvent(RemovedWorkerEvent),

    NewClient(NewClientEvent),
    RemovedClient(RemovedClientEvent),

    ClientSubmit(ClientSubmitEvent),
    ClientUnkeep(ClientUnkeepEvent),

    TaskStarted(TaskStartedEvent),
    TaskFinished(TaskFinishedEvent),

    DataObjectFinished(DataObjectFinishedEvent),
    DataObejctRemoved(DataObjectRemovedEvent),

    WorkerMonitoring(WorkerMonitoringEvent)
}