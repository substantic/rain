use std::collections::HashMap;

use types::UserAttrs;

#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct TaskInfo {
    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub error: String,

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
    pub duration: Option<f32>,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub user: UserAttrs,
}

#[derive(Debug, Clone, Serialize, PartialEq, Deserialize, Default)]
#[serde(deny_unknown_fields)]
pub struct ObjectInfo {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(default)]
    pub size: Option<usize>,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub content_type: String,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub error: String,

    #[serde(skip_serializing_if = "String::is_empty")]
    #[serde(default)]
    pub debug: String,

    #[serde(skip_serializing_if = "HashMap::is_empty")]
    #[serde(default)]
    pub user: UserAttrs,
}
