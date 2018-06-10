use common::id::{DataObjectId, ExecutorId, TaskId};
use common::{ObjectInfo, ObjectSpec, TaskInfo, TaskSpec};
use serde_bytes;
use std::path::PathBuf;

/// Message from executor to governor.
/// In JSON-equivalent serialized as `{"message": "register", "data": { ... }}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExecutorToGovernorMessage {
    Register(RegisterMsg),
    Result(ResultMsg),
}

/// Message from governor to executor.
/// In JSON-equivalent serialized as `{"message": "register", "data": { ... }}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum GovernorToExecutorMessage {
    Call(CallMsg),
    DropCached(DropCachedMsg),
}

/// First message sent from executor to verify the protocol version,
/// executor ID and executor type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegisterMsg {
    /// Executor protocol version
    pub protocol: String,
    /// SUbgovernor ID as assigned to the governor
    pub executor_id: ExecutorId,
    /// Executor task name prefix in task names
    pub executor_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallMsg {
    /// Task attributes
    pub spec: TaskSpec,
    /// Task input descriptions. In this context, all fields of `LocalObjectSpec` are valid.
    pub inputs: Vec<LocalObjectIn>,
    /// Task outputt descriptions. In this context,
    /// `LocalObjectSpec::location` should not be present (and ignored if present), all other
    /// fields are valid.
    pub outputs: Vec<LocalObjectIn>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ResultMsg {
    /// Task ID (must match `CallMsg::task`)
    pub task: TaskId,
    /// Task success. On `false`, attributes error must be set.
    pub success: bool,
    /// Resulting Task attributes.
    pub info: TaskInfo,
    /// Task outputt descriptions. In this context, `LocalObjectSpec::label` should not be present,
    /// `LocalObjectSpec::cache_hint` should be missing or false.
    /// The list must match `CallMsg::outputs` lengts and on `id`s.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub outputs: Vec<LocalObjectOut>,
    /// If any objects with `cache_hint` were sent, report which were newly cached
    /// (does not include objects previously cached and now reported with `DataLocation::Cached`).
    /// It is always allowed to cache no object and even omit this field (for simpler executors).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub cached_objects: Vec<DataObjectId>,
}

/// Data object information in `CallMsg` and `ResultMsg`. See the corresponding
/// fields there for precise semantics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalObjectIn {
    /// Compulsory object spec
    pub spec: ObjectSpec,
    /// Object info for inputs, None for outputs
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub info: Option<ObjectInfo>,
    /// Object data location
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub location: Option<DataLocation>,
    /// If able, the executor should cache this object, preferably in the
    /// unpacked form to save repeated unpacking (e.g. python cloudpickle).
    /// If the executor decides to cache the object, it must add it to
    /// `ResultMsg::cached_objects`.
    #[serde(skip_serializing_if = "::std::ops::Not::not")]
    #[serde(default)]
    pub cache_hint: bool,
}

/// Data object information in `CallMsg` and `ResultMsg`. See the corresponding
/// fields there for precise semantics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalObjectOut {
    pub info: ObjectInfo,
    /// Object data location
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub location: Option<DataLocation>,
    /// If able, the executor should cache this object, preferably in the
    /// unpacked form to save repeated unpacking (e.g. python cloudpickle).
    /// If the executor decides to cache the object, it must add it to
    /// `ResultMsg::cached_objects`.
    #[serde(skip_serializing_if = "::std::ops::Not::not")]
    #[serde(default)]
    pub cache_hint: bool,
}

/// Data location of inputs and outputs in `LocalObjectSpec::location`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DataLocation {
    /// The data is present in the given path that is relative to the executor working directory.
    Path(PathBuf),
    /// The data is directly contained in the message. Only reccomended for
    /// small objects (under cca 128kB).
    #[serde(with = "serde_bytes")]
    Memory(Vec<u8>),
    /// The data is identical to one of input objects.
    /// Only valid in `ResultMsg`.
    OtherObject(DataObjectId),
    /// The input data is already cached (and possibly unpacked) in the executor.
    /// Only valid in `CallMsg::inputs`.
    Cached,
}

/// Instruct the executor to drop the given cached objects.
/// It is an error to request dropping an object that is not cached.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DropCachedMsg {
    /// List of object ids to drop
    pub objects: Vec<DataObjectId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{de::DeserializeOwned, Serialize};
    use serde_cbor;
    use serde_json;
    use std::fmt::Debug;

    fn test_ser_de_eq<'a, T: Serialize + DeserializeOwned + Debug + PartialEq>(m: &T) {
        let json = serde_json::to_string(m).unwrap();
        println!("JSON: {} {}", json.len(), json);
        assert_eq!(m, &serde_json::from_str::<T>(&json).unwrap());

        let cb = serde_cbor::to_vec(m).unwrap();
        println!("CB: {} {}", cb.len(), String::from_utf8_lossy(&cb));
        assert_eq!(m, &serde_cbor::from_slice::<T>(&cb).unwrap());
    }

    #[test]
    fn test_register() {
        let s =
            r#"{"register": {"protocol": "swp1", "executor_id": 42, "executor_type": "dummy"}}"#;
        let m: ExecutorToGovernorMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_call() {
        let s = r#"{"call": {
             "spec":{
                    "id": [42, 48],
                    "task_type": "foo"
            },
            "inputs": [
                {"spec": {"id": [3,6], "label": "in1", "data_type": "blob"},
                 "location": {"memory": [0,0,0,0,0]}},
                {"spec": {"id": [3,7], "label": "in2", "data_type": "blob"},
                 "location": {"path": "in1.txt"}, "cache_hint": true},
                {"spec": {"id": [3,8], "data_type": "blob"},
                 "location": "cached"}
            ],
            "outputs": [
                {"spec": {"id": [3,11], "label": "out1", "data_type": "blob"},
                 "cache_hint": true},
                {"spec": {"id": [3,12], "data_type": "dir"}}
            ]
            }}"#;
        let m: GovernorToExecutorMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
        if let &GovernorToExecutorMessage::Call(ref c) = &m {
            assert_eq!(
                c.inputs[0].location,
                Some(DataLocation::Memory(vec![0u8; 5]))
            );
        } else {
            panic!()
        }
    }

    #[test]
    fn test_result() {
        let s = r#"{"result": {"task": [42, 48], "success": true,
            "info": {},
            "outputs": [
                {"info": {"debug": "log"}, "location": {"path": "in1.txt"}},
                {"info": {"size": 42}, "location": {"other_object": [3, 6]}, "cache_hint": true}
            ]
            }}"#;
        let m: ExecutorToGovernorMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_drop_cached() {
        let s = r#"{"drop_cached": {"objects": [[1,2], [4,5]]}}"#;
        let m: GovernorToExecutorMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

}
