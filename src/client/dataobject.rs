use common::Attributes;
use common::id::DataObjectId;
use common::DataType;

pub struct DataObject {
    pub id: DataObjectId,
    pub label: String,
    pub keep: bool,
    pub data: Option<Vec<u8>>,
    pub attributes: Attributes,
    pub data_type: DataType,
}

impl DataObject {
    pub fn keep(&mut self) {
        self.keep = true;
    }
}
