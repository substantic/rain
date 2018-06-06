use common::id::{TaskId, DataObjectId};
use common::DataType;
use common::Resources;
use errors::Result;
use std::collections::HashMap;
use std::error::Error;

use serde_json;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaskSpecInput {
    pub id: DataObjectId,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub label: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaskSpec {
    pub id: TaskId,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub inputs: Vec<TaskSpecInput>,

    #[serde(skip_serializing_if = "Vec::is_empty")]
    #[serde(default)]
    pub outputs: Vec<DataObjectId>,

    pub task_type: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub config: Option<serde_json::Value>,

    pub resources: Resources,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub user: HashMap<String, serde_json::Value>,
}

impl TaskSpec {

    pub fn parse_config<'a, D>(&'a self) -> Result<D> where for<'de> D: ::serde::de::Deserialize<'de>
    {
            match self.config {
                Some(ref c) => ::serde_json::from_value(c.clone()).map_err(|e| {
                    format!("Cannot parse task config: {}", e.description()).into()
                }),
                None => Err("Task config is empty, but non-empty config is expected".into())
            }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
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
    pub start_time: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub duration: Option<u32>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub user: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ObjectSpec {
    pub id: DataObjectId,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub label: String,

    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub content_type: Option<String>,

    pub data_type: DataType,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub user: HashMap<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub user: HashMap<String, serde_json::Value>,
}