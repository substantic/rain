use rain_core::types::{DataObjectId, ObjectSpec};
use std::cell::Cell;

pub struct DataObject {
    pub keep: Cell<bool>,
    pub data: Option<Vec<u8>>,
    pub spec: ObjectSpec,
}

impl DataObject {
    pub fn keep(&self) {
        self.keep.set(true);
    }
    pub fn id(&self) -> DataObjectId {
        self.spec.id
    }
}
