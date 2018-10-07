use super::task::Task;
use client::dataobject::DataObject;
use rain_core::client_capnp;
use rain_core::utils::ToCapnp;
use serde_json;

macro_rules! to_capnp_list {
    ($builder:expr, $items:expr, $name:ident) => {{
        let mut builder = $builder.$name($items.len() as u32);
        for (i, obj) in $items.iter().enumerate() {
            obj.to_capnp(&mut builder.reborrow().get(i as u32));
        }
    }};
}
macro_rules! from_capnp_list {
    ($builder:expr, $items:ident, $obj:ident) => {{
        $builder
            .$items()?
            .iter()
            .map(|item| $obj::from_capnp(&item))
            .collect()
    }};
}

impl<'a> ToCapnp<'a> for Task {
    type Builder = client_capnp::task::Builder<'a>;

    fn to_capnp(&self, builder: &mut Self::Builder) {
        builder.set_spec(&serde_json::to_string(&self.spec).unwrap());
    }
}
impl<'a> ToCapnp<'a> for DataObject {
    type Builder = client_capnp::data_object::Builder<'a>;

    fn to_capnp(&self, builder: &mut Self::Builder) {
        builder.set_spec(&serde_json::to_string(&self.spec).unwrap());
        builder.set_keep(self.keep.get());

        if let &Some(ref data) = &self.data {
            builder.set_data(&data);
            builder.set_has_data(true);
        } else {
            builder.set_has_data(false);
        }
    }
}
