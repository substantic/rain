use common::Attributes;
use common::id::DataObjectId;

pub struct DataObject {
    pub id: DataObjectId,
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
