pub(crate) mod data_type;
pub mod id;
pub(crate) mod info;
pub(crate) mod resources;
pub(crate) mod spec;

pub type UserValue = ::serde_json::Value;
pub type UserAttrs = ::std::collections::HashMap<String, UserValue>;

pub use self::data_type::DataType;
pub use self::id::{ClientId, DataObjectId, ExecutorId, GovernorId, Id, SId, SessionId, TaskId};
pub use self::info::{ObjectInfo, TaskInfo};
pub use self::resources::Resources;
pub use self::spec::{ObjectSpec, TaskSpec, TaskSpecInput, SessionSpec};
