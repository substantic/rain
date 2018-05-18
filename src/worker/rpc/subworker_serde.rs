use common::Attributes;
use common::id::{DataObjectId, SubworkerId, TaskId};
use serde_bytes;
use std::path::PathBuf;

/// Message from subworker to worker.
/// In JSON-equivalent serialized as `{"message": "register", "data": { ... }}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum SubworkerToWorkerMessage {
    Register(RegisterMsg),
    Result(ResultMsg),
}

/// Message from worker to subworker.
/// In JSON-equivalent serialized as `{"message": "register", "data": { ... }}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum WorkerToSubworkerMessage {
    Call(CallMsg),
    DropCached(DropCachedMsg),
}

/// First message sent from subworker to verify the protocol version,
/// subworker ID and subworker type.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct RegisterMsg {
    /// Subworker protocol version
    pub protocol: String,
    /// SUbworker ID as assigned to the worker
    pub subworker_id: SubworkerId,
    /// Subworker task name prefix in task names
    pub subworker_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct CallMsg {
    /// Task ID
    pub task: TaskId,
    /// Requested task type name (without `subworker_type` prefix)
    pub method: String,
    /// Task attributes
    pub attributes: Attributes,
    /// Task input descriptions. In this context, all fields of `DataObjectSpec` are valid.
    pub inputs: Vec<DataObjectSpec>,
    /// Task outputt descriptions. In this context,
    /// `DataObjectSpec::location` should not be present (and ignored if present), all other
    /// fields are valid.
    pub outputs: Vec<DataObjectSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ResultMsg {
    /// Task ID (must match `CallMsg::task`)
    pub task: TaskId,
    /// Task success. On `false`, attributes error must be set.
    pub success: bool,
    /// Resulting Task attributes. The `spec` and `user_spec` part is ignored.
    pub attributes: Attributes,
    /// Task outputt descriptions. In this context, `DataObjectSpec::label` should not be present,
    /// `DataObjectSpec::cache_hint` should be missing or false.
    /// In `DataObjectSpec::attributes`, `spec` and `user_spec` are ignored if present.
    /// The list must match `CallMsg::outputs` lengts and on `id`s.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub outputs: Vec<DataObjectSpec>,
    /// If any objects with `cache_hint` were sent, report which were newly cached
    /// (does not include objects previously cached and now reported with `DataLocation::Cached`).
    /// It is always allowed to cache no object and even omit this field (for simpler subworkers).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub cached_objects: Vec<DataObjectId>,
}

/// Data object information in `CallMsg` and `ResultMsg`. See the corresponding
/// fields there for precise semantics.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct DataObjectSpec {
    /// Data object ID
    pub id: DataObjectId,
    /// Object label within the task inputs or outputs
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub label: Option<String>,
    /// Object attributes
    pub attributes: Attributes,
    /// Object data location
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub location: Option<DataLocation>,
    /// If able, the subworker should cache this object, preferably in the
    /// unpacked form to save repeated unpacking (e.g. python cloudpickle).
    /// If the subworker decides to cache the object, it must add it to
    /// `ResultMsg::cached_objects`.
    #[serde(skip_serializing_if = "::std::ops::Not::not")]
    #[serde(default)]
    pub cache_hint: bool,
}

/// Data location of inputs and outputs in `DataObjectSpec::location`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DataLocation {
    /// The data is present in the given path that is relative to the subworker working directory.
    Path(PathBuf),
    /// The data is directly contained in the message. Only reccomended for
    /// small objects (under cca 128kB).
    #[serde(with = "serde_bytes")]
    Memory(Vec<u8>),
    /// The data is identical to one of input objects.
    /// Only valid in `ResultMsg`.
    OtherObject(DataObjectId),
    /// The input data is already cached (and possibly unpacked) in the subworker.
    /// Only valid in `CallMsg::inputs`.
    Cached,
}

/// Instruct the subworker to drop the given cached objects.
/// It is an error to request dropping an object that is not cached.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct DropCachedMsg {
    /// List of object ids to drop
    pub objects: Vec<DataObjectId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Serialize, de::DeserializeOwned};
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
            r#"{"register": {"protocol": "swp1", "subworkerId": 42, "subworkerType": "dummy"}}"#;
        let m: SubworkerToWorkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_call() {
        let s = r#"{"call": {"method": "foo", "task": [42, 48],
            "attributes": {},
            "inputs": [
                {"id": [3,6], "label": "in1", "attributes": {},
                 "location": {"memory": [0,0,0,0,0]}},
                {"id": [3,7], "label": "in2", "attributes": {},
                 "location": {"path": "in1.txt"}, "cacheHint": true},
                {"id": [3,8], "attributes": {}, "location": "cached"}
            ],
            "outputs": [
                {"id": [3,11], "label": "out1", "attributes": {}, "cacheHint": true},
                {"id": [3,12], "attributes": {}}
            ]
            }}"#;
        let m: WorkerToSubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
        if let &WorkerToSubworkerMessage::Call(ref c) = &m {
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
            "attributes": {},
            "outputs": [
                {"id": [3,11], "attributes": {}, "location": {"path": "in1.txt"}},
                {"id": [3,12], "attributes": {}, "location": {"otherObject": [3, 6]}}
            ]
            }}"#;
        let m: SubworkerToWorkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_drop_cached() {
        let s = r#"{"dropCached": {"objects": [[1,2], [4,5]]}}"#;
        let m: WorkerToSubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

}
