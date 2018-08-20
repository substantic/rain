use serde::Serializer;
use types::{DataObjectId, GovernorId, Resources, SessionId, TaskId};

#[derive(Serialize, Deserialize)]
pub struct ClientToServerMessage {
    pub id: u32,
    pub data: RequestType,
}
#[derive(Serialize, Deserialize)]
pub struct ServerToClientMessage {
    pub id: u32,
    pub data: ResponseType,
}

#[derive(Serialize, Deserialize)]
pub enum RequestType {
    RegisterClient(RegisterClientRequest),
    NewSession(NewSessionRequest),
    CloseSession(CloseSessionRequest),
    GetServerInfo(GetServerInfoRequest),
    Submit(SubmitRequest),
    Fetch(FetchRequest),
    Unkeep(UnkeepRequest),
    Wait(WaitRequest),
    WaitSome(WaitSomeRequest),
    GetState(GetStateRequest),
    TerminateServer(TerminateServerRequest),
}
#[derive(Serialize, Deserialize)]
pub enum ResponseType {
    RegisterClient(RegisterClientResponse),
    NewSession(NewSessionResponse),
    CloseSession(CloseSessionResponse),
    GetServerInfo(GetServerInfoResponse),
    Submit(SubmitResponse),
    Fetch(FetchResponse),
    Unkeep(UnkeepResponse),
    Wait(WaitResponse),
    WaitSome(WaitSomeResponse),
    GetState(GetStateResponse),
    TerminateServer(TerminateServerResponse),
}

// common types
#[derive(Serialize, Deserialize)]
pub struct RpcError {
    pub message: String,
    pub debug: String,
    pub task: TaskId,
}
#[derive(Serialize, Deserialize)]
pub enum RpcResult {
    Ok,
    Error(RpcError),
}
#[derive(Serialize, Deserialize)]
pub struct Update {
    pub tasks: Vec<TaskUpdate>,
    pub objects: Vec<DataObjectUpdate>,
    pub status: RpcResult,
}
#[derive(Serialize, Deserialize)]
pub struct TaskUpdate {
    pub id: TaskId,
    pub state: TaskState,
    pub info: String,
}
#[derive(Serialize, Deserialize)]
pub struct DataObjectUpdate {
    pub id: DataObjectId,
    pub state: DataObjectState,
    pub info: String,
}
#[derive(Serialize, Deserialize)]
pub enum TaskState {
    NotAssigned,
    Ready,
    Assigned,
    Running,
    Finished,
    Failed,
}
#[derive(Serialize, Deserialize)]
pub enum DataObjectState {
    Unfinished,
    Finished,
    Removed,
}

// request/response types
#[derive(Serialize, Deserialize)]
pub struct RegisterClientRequest {
    pub version: u32,
}
#[derive(Serialize, Deserialize)]
pub struct RegisterClientResponse {}

#[derive(Serialize, Deserialize)]
pub struct NewSessionRequest {
    pub spec: String,
}
#[derive(Serialize, Deserialize)]
pub struct NewSessionResponse {
    pub session_id: SessionId,
}

#[derive(Serialize, Deserialize)]
pub struct CloseSessionRequest {
    pub session_id: SessionId,
}
#[derive(Serialize, Deserialize)]
pub struct CloseSessionResponse {}

#[derive(Serialize, Deserialize)]
pub struct GetServerInfoRequest {}
#[derive(Serialize, Deserialize)]
pub struct GetServerInfoResponse {
    pub governors: Vec<GovernorInfo>,
}
#[derive(Serialize, Deserialize)]
pub struct GovernorInfo {
    #[serde(serialize_with = "serialize_socket_addr")]
    pub governor_id: GovernorId,
    pub tasks: Vec<TaskId>,
    pub objects: Vec<DataObjectId>,
    pub objects_to_delete: Vec<DataObjectId>,
    pub resources: Resources,
}

fn serialize_socket_addr<S>(addr: &GovernorId, s: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    s.serialize_str(&format!("{}", addr))
}

#[derive(Serialize, Deserialize)]
pub struct SubmitRequest {
    pub tasks: Vec<Task>,
    pub objects: Vec<DataObject>,
}
#[derive(Serialize, Deserialize)]
pub struct SubmitResponse {}
#[derive(Serialize, Deserialize)]
pub struct Task {
    pub spec: String,
}
#[derive(Serialize, Deserialize)]
pub struct DataObject {
    pub spec: String,
    pub keep: bool,
    #[serde(with = "::serde_bytes")]
    pub data: Vec<u8>,
    pub has_data: bool,
}

#[derive(Serialize, Deserialize)]
pub struct FetchRequest {
    pub id: DataObjectId,
    pub include_info: bool,
    pub offset: u64,
    pub size: u64,
}
#[derive(Serialize, Deserialize)]
pub struct FetchResponse {
    pub status: FetchStatus,
    #[serde(with = "::serde_bytes")]
    pub data: Vec<u8>,
    pub info: String,
    pub transport_size: u64,
}

impl FetchResponse {
    pub fn error(error: RpcError) -> Self {
        FetchResponse {
            status: FetchStatus::Error(error),
            data: vec![],
            info: "".to_owned(),
            transport_size: 0,
        }
    }
}
#[derive(Serialize, Deserialize)]
pub enum FetchStatus {
    Ok,
    Redirect(GovernorId),
    NotHere,
    Removed,
    Error(RpcError),
    Ignored,
}

#[derive(Serialize, Deserialize)]
pub struct UnkeepRequest {
    pub object_ids: Vec<DataObjectId>,
}
#[derive(Serialize, Deserialize)]
pub struct UnkeepResponse {
    pub status: RpcResult,
}

#[derive(Serialize, Deserialize)]
pub struct WaitRequest {
    pub task_ids: Vec<TaskId>,
    pub object_ids: Vec<DataObjectId>,
}
#[derive(Serialize, Deserialize)]
pub struct WaitResponse {
    pub status: RpcResult,
}

#[derive(Serialize, Deserialize)]
pub struct WaitSomeRequest {
    pub task_ids: Vec<TaskId>,
    pub object_ids: Vec<DataObjectId>,
}
#[derive(Serialize, Deserialize)]
pub struct WaitSomeResponse {
    pub finished_tasks: Vec<TaskId>,
    pub finished_objects: Vec<DataObjectId>,
}

#[derive(Serialize, Deserialize)]
pub struct GetStateRequest {
    pub task_ids: Vec<TaskId>,
    pub object_ids: Vec<DataObjectId>,
}
#[derive(Serialize, Deserialize)]
pub struct GetStateResponse {
    pub update: Update,
}

#[derive(Serialize, Deserialize)]
pub struct TerminateServerRequest {}
#[derive(Serialize, Deserialize)]
pub struct TerminateServerResponse {}
