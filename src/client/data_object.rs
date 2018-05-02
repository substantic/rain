use client::session::ObjectId;
use common::Attributes;

pub struct DataObject {
    pub id: ObjectId,
    pub label: String,
    pub keep: bool,
    pub data: Option<Vec<u8>>,
    pub attributes: Attributes,
}

impl DataObject {
    pub fn keep(&mut self) {
        self.keep = true;
    }
}
