use common::id::{TaskId, DataObjectId};
use common::Attributes;

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", content = "data")]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub enum SubworkerMessage {
    Register(RegisterMsg),
    Call(CallMsg),
    Result(ResultMsg),
    DropCached(DropCachedMsg),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct RegisterMsg {
    pub version: u32,
    pub subworker_id: u32,
    pub subworker_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct CallMsg {
    pub task: TaskId,
    pub method: String,
    pub attributes: Attributes,
    pub inputs: Vec<DataObjectSpec>,
    pub outputs: Vec<DataObjectSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ResultMsg {
    pub task: TaskId,
    pub success: bool,
    pub attributes: Attributes,
    #[serde(default)]
    pub outputs: Vec<DataObjectSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct DataObjectSpec {
    id: DataObjectId,
    #[serde(default)]
    label: Option<String>,
    attributes: Attributes,
    #[serde(default)]
    location: Option<DataLocation>,
    #[serde(default)]
    cache_hint: Option<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DataLocation {
    Path(String),
    Memory(Vec<u8>),
    ObjectData(DataObjectId),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct DropCachedMsg {
    drop: Vec<DataObjectId>,
}

/*
use std::fmt;
use serde::{Serialize, Deserialize, Serializer,
            Deserializer, de, ser::SerializeSeq};

impl Serialize for SubworkerMessage {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
        where S: Serializer
    {
        let mut seq = serializer.serialize_seq(Some(3))?;
        seq.serialize_element("not0")?;
        match *self {
            SubworkerMessage::Register(ref v) => {
                seq.serialize_element("register")?;
                seq.serialize_element(v)?;
            }
            SubworkerMessage::Call(ref v) => {
                seq.serialize_element("call")?;
                seq.serialize_element(v)?;
            }
            SubworkerMessage::Result(ref v) => {
                seq.serialize_element("result")?;
                seq.serialize_element(v)?;
            }
            SubworkerMessage::DropCached(ref v) => {
                seq.serialize_element("dropCached")?;
                seq.serialize_element(v)?;
            }
        }
        seq.end()
    }    
}

impl<'de> Deserialize<'de> for SubworkerMessage {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
        where D: Deserializer<'de>
    {
        struct SubworkerMessageVisitor;

        impl<'de> de::Visitor<'de> for SubworkerMessageVisitor {
            type Value = SubworkerMessage;
            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("tuple (version, msg_type, data)")
            }
            fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
                where A: de::SeqAccess<'de> {
                if seq.next_element()? != Some("not0") {
                    return Err(de::Error::custom("Missing or invalid version"));
                }
                let err = || de::Error::custom("Missing data field");
                match seq.next_element()? {
                    Some("register") => 
                        Ok(SubworkerMessage::Register(seq.next_element()?.ok_or_else(err)?)),
                    Some("call") => 
                        Ok(SubworkerMessage::Call(seq.next_element()?.ok_or_else(err)?)),
                    Some("result") => 
                        Ok(SubworkerMessage::Result(seq.next_element()?.ok_or_else(err)?)),
                    Some("dropCached") => 
                        Ok(SubworkerMessage::DropCached(seq.next_element()?.ok_or_else(err)?)),
                    Some(val) => Err(de::Error::custom(format!("Unknown message type {:?}", val))),
                    None => Err(de::Error::custom("Missing message type")),
                }
            }
        }

        deserializer.deserialize_seq(SubworkerMessageVisitor)
    }
}
*/
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
        let s = r#"{"type": "register", "data": {"version": 1, "subworkerId": 42, "subworkerType": "dummy"}}"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

//                {"id": [3,6], "label": "in1", "attributes": {}, "location": {"memory": [0,0,0]}, "cacheHint": 0.9},
  //              {"id": [3,7], "label": "in2", "attributes": {}, "location": {"path": "in1.txt"}, "cacheHint": 0.9}
    #[test]
    fn test_call() {
        let s = r#"{"type": "call", "data": {"method": "foo", "task": [42, 48],
            "attributes": {},
            "inputs": [
            ],
            "outputs": [
                {"id": [3,11], "label": "out1", "attributes": {}, "location": {"path": "tmp/"}, "cacheHint": 0.9}
            ]
            }}"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_result() {
        let s = r#"{"type": "result", "data": {"task": [42, 48], "success": true,
            "attributes": {},
            "outputs": [
                {"id": [3,11], "attributes": {}, "location": {"path": "tmp/out.txt"}}
            ]
            }}"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_drop_cached() {
        let s = r#"{"type": "dropCached", "data": {"drop": [[1,2], [4,5]]}}"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

}