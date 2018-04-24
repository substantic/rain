use common::id::{TaskId, DataObjectId, SubworkerId};
use common::Attributes;

/// Subworker message, in JSON serialized as
/// `{"message": "register", "data": { ... }}`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "message", content = "data")]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub enum SubworkerMessage {
    Register(RegisterMsg),
    Call(CallMsg),
    Result(ResultMsg),
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
    pub outputs: Vec<DataObjectSpec>,
    /// If any objects with `cache_hint` were sent, report which were cached.
    /// It is always allowed to cache no object and even omit this field (for simpler subworkers).
    #[serde(skip_serializing_if="Vec::is_empty")]
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
    id: DataObjectId,
    /// Object label within the task inputs or outputs
    #[serde(skip_serializing_if="Option::is_none")]
    #[serde(default)]
    label: Option<String>,
    /// Object attributes
    attributes: Attributes,
    /// Object data location
    #[serde(skip_serializing_if="Option::is_none")]
    #[serde(default)]
    location: Option<DataLocation>,
    /// If able, the subworker should cache this object, preferably in the 
    /// unpacked form to save repeated unpacking (e.g. python cloudpickle).
    /// If the subworker decides to cache the object, it must add it to
    /// `ResultMsg::cached_objects`.
    #[serde(skip_serializing_if="::std::ops::Not::not")]
    #[serde(default)]
    cache_hint: bool,
}

/// Data location of inputs and outputs in `DataObjectSpec::location`.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DataLocation {
    /// The data is present in the given path that is relative to the subworker working directory.
    Path(String),
    /// The data is directly contained in the message. Only reccomended for
    /// small objects (under cca 128kB).
    Memory(Vec<u8>),
    /// The data is identical to one of input objects. 
    /// Only valid in `ResultMsg`.
    ObjectData(DataObjectId),
    /// The data should be cached (and possibly unpacked) in the subworker.
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
    drop: Vec<DataObjectId>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use rmp_serde;

    fn test_ser_de_eq(m: &SubworkerMessage) {
        let json = serde_json::to_string(m).unwrap();
        assert_eq!(m, &serde_json::from_str::<SubworkerMessage>(&json).unwrap());
        println!("JSON: {} {}", json.len(), json);
        let mpn = rmp_serde::to_vec_named(m).unwrap();
        assert_eq!(m, &rmp_serde::from_slice::<SubworkerMessage>(&mpn).unwrap());
        println!("MPN: {} {}", mpn.len(), String::from_utf8_lossy(&mpn));
        let mp = rmp_serde::to_vec(m).unwrap();
        assert_eq!(m, &rmp_serde::from_slice::<SubworkerMessage>(&mp).unwrap());
        println!("MP: {} {}", mp.len(), String::from_utf8_lossy(&mp));
    }

    #[test]
    fn test_print() {
        println!("JSON: {}", serde_json::to_string(&SubworkerMessage::DropCached(DropCachedMsg { drop: vec![] } )).unwrap());
    }

    #[test]
    fn test_register() {
        let s = r#"{"message": "register", "data": {"protocol": "swp1", "subworkerId": 42, "subworkerType": "dummy"}}"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

//                {"id": [3,6], "label": "in1", "attributes": {}, "location": {"memory": [0,0,0]}},
  //              {"id": [3,7], "label": "in2", "attributes": {}, "location": {"path": "in1.txt"}, "cacheHint": true}
    #[test]
    fn test_call() {
        let s = r#"{"message": "call", "data": {"method": "foo", "task": [42, 48],
            "attributes": {},
            "inputs": [
            ],
            "outputs": [
                {"id": [3,11], "label": "out1", "attributes": {}, "location": {"path": "tmp/"}, "cacheHint": true}
            ]
            }}"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_result() {
        let s = r#"{"message": "result", "data": {"task": [42, 48], "success": true,
            "attributes": {},
            "outputs": [
                {"id": [3,11], "attributes": {}}
            ]
            }}"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_drop_cached() {
        let s = r#"{"message": "dropCached", "data": {"drop": [[1,2], [4,5]]}}"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

}