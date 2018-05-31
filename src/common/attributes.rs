use common::id::{TaskId, DataObjectId};
use common::DataType;
use common::Resources;
use errors::Result;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::HashMap;
use std::error::Error;

use serde_json;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct TaskSpecInput {
    pub id: DataObjectId,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct TaskSpec {
    pub id: TaskId,

    pub inputs: Vec<TaskSpecInput>,
    pub outputs: Vec<DataObjectId>,

    pub task_type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub config: Option<serde_json::Value>,

    pub resources: Resources,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub user: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct TaskInfo {

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub error: Option<String>,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub debug: String,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub governor: String,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub task_start: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub duration: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub user: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ObjectSpec {
    pub id: DataObjectId,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub label: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub content_type: Option<serde_json::Value>,

    pub data_type: DataType,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub user: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[serde(deny_unknown_fields)]
pub struct ObjectInfo {

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub size: Option<usize>,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub content_type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub error: Option<String>,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub debug: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub user: Option<serde_json::Value>,
}

#[derive(Default, Debug, Clone, PartialEq)]
pub struct Attributes {
    // TODO: Int & Float types
    items: HashMap<String, String>,
}

impl Serialize for Attributes {
    fn serialize<S>(&self, serializer: S) -> ::std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.items.serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Attributes {
    fn deserialize<D>(deserializer: D) -> ::std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        Ok(Attributes {
            items: Deserialize::deserialize(deserializer)?,
        })
    }
}

/*impl Attributes {
    pub fn new() -> Self {
        Default::default()
    }

    pub fn contains(&self, key: &str) -> bool {
        self.items.contains_key(key)
    }

    pub fn find<'a, D>(&'a self, key: &str) -> Result<Option<D>>
    where
        D: ::serde::de::Deserialize<'a>,
    {
        match self.items.get(key) {
            Some(ref value) => ::serde_json::from_str(value).map(|v| Some(v)).map_err(|e| {
                format!("Error in parsing attribute '{}': {}", key, e.description()).into()
            }),
            None => Ok(None),
        }
    }

    pub fn get<'a, D>(&'a self, key: &str) -> Result<D>
    where
        D: ::serde::de::Deserialize<'a>,
    {
        match self.items.get(key) {
            Some(ref value) => ::serde_json::from_str(value).map_err(|e| {
                format!(
                    "Error in parsing attribute '{}': {} (data {:?})",
                    key,
                    e.description(),
                    &value
                ).into()
            }),
            None => {
                bail!("Key {:?} not found in attributes", key);
            }
        }
    }

    pub fn set<S>(&mut self, key: &str, value: S) -> Result<()>
    where
        S: ::serde::ser::Serialize,
    {
        self.items
            .insert(key.to_string(), ::serde_json::to_string(&value)?);
        Ok(())
    }

    pub fn to_capnp(&self, builder: &mut ::common_capnp::attributes::Builder) {
        let mut items = builder.reborrow().init_items(self.items.len() as u32);
        for (i, (key, value)) in self.items.iter().enumerate() {
            let mut item = items.reborrow().get(i as u32);
            item.set_key(&key);
            item.set_value(&value);
        }
    }

    pub fn from_capnp(reader: &::common_capnp::attributes::Reader) -> Self {
        let mut attrs = Attributes::new();
        attrs.update_from_capnp(reader);
        attrs
    }

    pub fn update_from_capnp(&mut self, reader: &::common_capnp::attributes::Reader) {
        for item in reader.get_items().unwrap() {
            let key = item.get_key().unwrap().to_string();
            let value = item.get_value().unwrap().into();
            self.items.insert(key, value);
        }
    }

    pub fn update(&mut self, attributes: Attributes) {
        for (k, v) in attributes.items {
            self.items.insert(k, v);
        }
    }

    pub fn as_hashmap(&self) -> &HashMap<String, String> {
        &self.items
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }
}
*/