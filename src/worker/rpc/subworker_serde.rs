use common::id::{TaskId, DataObjectId};
use serde::{Serialize, Deserialize, Serializer,
            Deserializer, de, ser::SerializeSeq};
use common::Attributes;
use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub enum SubworkerMessage {
    Register(RegisterMsg),
    Call(CallMsg),
    Result(ResultMsg),
    DropCached(DropCachedMsg),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct RegisterMsg {
    pub version: u32,
    pub subworker_id: u32,
    pub subworker_type: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CallMsg {
    pub task: TaskId,
    pub method: String,
    pub attributes: Attributes,
    pub inputs: Vec<DataObjectSpec>,
    pub outputs: Vec<DataObjectSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ResultMsg {
    pub task: TaskId,
    pub success: bool,
    pub attributes: Attributes,
    #[serde(default)]
    pub outputs: Vec<DataObjectSpec>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DataObjectSpec {
    id: DataObjectId,
    #[serde(default)]
    label: Option<String>,
    attributes: Attributes,
    #[serde(flatten)]
    location: DataLocation,
    #[serde(default)]
    cache_hint: Option<f32>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum DataLocation {
    Path(String),
    Memory(Vec<u8>),
    Object(DataObjectId),
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DropCachedMsg {
    drop: Vec<DataObjectId>,
}

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

        deserializer.deserialize_any(SubworkerMessageVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;
    use rmp_serde;

    fn test_ser_de_eq(m: &SubworkerMessage) {
        let json = serde_json::to_string(m).unwrap();
        assert_eq!(m, &serde_json::from_str::<SubworkerMessage>(&json).unwrap());
        let mp = rmp_serde::to_vec(m).unwrap();
        assert_eq!(m, &rmp_serde::from_slice::<SubworkerMessage>(&mp).unwrap());
        let mpn = rmp_serde::to_vec_named(m).unwrap();
        assert_eq!(m, &rmp_serde::from_slice::<SubworkerMessage>(&mpn).unwrap());
    }

    #[test]
    fn test_register() {
        let s = r#"["not0", "register", {"version": 1, "subworkerId": 42, "subworkerType": "dummy"}]"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_call() {
        // TODO: Add content
        let s = r#"["not0", "call", {"method": "foo", "task": [42, 48],
            "attributes": { "items": {} },
            "inputs": [],
            "outputs": []
            }]"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_result() {
        // TODO: Add content
        let s = r#"["not0", "result", {"task": [42, 48], "success": true,
            "attributes": { "items": {} },
            "outputs": []
            }]"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

    #[test]
    fn test_drop_cached() {
        let s = r#"["not0", "dropCached", { "drop": [[1,2], [4,5]]}]"#;
        let m: SubworkerMessage = serde_json::from_str(s).unwrap();
        test_ser_de_eq(&m);
    }

}