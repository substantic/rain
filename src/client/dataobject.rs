use common::Attributes;
use common::id::DataObjectId;
use common::DataType;
use std::cell::Cell;

pub struct DataObject {
    pub id: DataObjectId,
    pub label: String,
    pub keep: Cell<bool>,
    pub data: Option<Vec<u8>>,
    pub attributes: Attributes,
    pub data_type: DataType,
}

impl DataObject {
    pub fn keep(&self) {
        self.keep.set(true);
    }
}
