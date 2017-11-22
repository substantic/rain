use common::id::{SessionId, WorkerId, DataObjectId, TaskId, ClientId, SId};
use common::events::Event;
use common::monitor::Frame;

pub trait Logger {
    fn add_event(&mut self, event: Event);

    fn flush_events(&mut self);

    fn add_new_worker_event(&mut self, worker: WorkerId);

    fn add_worker_removed_event(&mut self, worker: WorkerId, error_msg: String);

    fn add_worker_failed_event(&mut self, worker: WorkerId, error_msg: String);

    fn add_new_client_event(&mut self, client: ClientId);

    fn add_removed_client_event(&mut self, client: ClientId, error_msg: String);

    fn add_client_submit_event(&mut self, tasks: Vec<TaskId>, dataobjs: Vec<DataObjectId>);

    fn add_client_invalid_request_event(&mut self, client_id: ClientId, error_msg: String);

    fn add_client_unkeep_event(&mut self, dataobjs: Vec<DataObjectId>);

    fn add_task_started_event(&mut self, task: TaskId, worker: WorkerId);

    fn add_task_finished_event(&mut self, task: TaskId);

    fn add_task_failed_event(&mut self, task: TaskId, worker: WorkerId, error_msg: String);

    fn add_dataobject_finished_event(
        &mut self,
        dataobject: DataObjectId,
        worker: WorkerId,
        size: usize,
    );

    fn add_dataobject_removed_event(&mut self, dataobject: DataObjectId, worker: WorkerId);

    fn add_worker_monitoring_event(&mut self, frame: Frame, worker: WorkerId);

    fn add_dummy_event(&mut self);
}
