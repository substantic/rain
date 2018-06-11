pub(crate) mod data_type;
pub(crate) mod id;
pub(crate) mod info;
pub(crate) mod resources;
pub(crate) mod spec;

pub type UserValue = ::serde_json::Value;
pub type UserAttrs = ::std::collections::HashMap<String, UserValue>;

pub use self::data_type::DataType;
pub use self::id::{Id, SessionId, ExecutorId, GovernorId, ClientId, SId, TaskId, DataObjectId};
pub use self::info::{TaskInfo, ObjectInfo};
pub use self::spec::{TaskSpec, TaskSpecInput, ObjectSpec};
pub use self::resources::Resources;
