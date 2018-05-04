use super::task::Task;
use client::dataobject::DataObject;
use client::task::TaskInput;
use common::convert::ToCapnp;
use common::id::DataObjectId;

macro_rules! capnplist {
    ($builder:expr, $items:expr, $name:ident) => {
        {
            let mut builder = $builder.$name($items.len() as u32);
            for (i, obj) in $items.iter().enumerate() {
                obj.to_capnp(&mut builder.reborrow().get(i as u32));
            }
        }
    }
}

impl<'a> ToCapnp<'a> for TaskInput {
    type Builder = ::client_capnp::task::in_data_object::Builder<'a>;

    fn to_capnp(&self, builder: &mut Self::Builder) {
        self.data_object
            .id
            .to_capnp(&mut builder.reborrow().get_id().unwrap());

        if let &Some(ref label) = &self.label {
            builder.reborrow().set_label(label);
        }
    }
}

impl<'a> ToCapnp<'a> for Task {
    type Builder = ::client_capnp::task::Builder<'a>;

    fn to_capnp(&self, builder: &mut Self::Builder) {
        self.id.to_capnp(&mut builder.reborrow().get_id().unwrap());
        builder.set_task_type(&self.command);

        capnplist!(builder.reborrow(), self.inputs, init_inputs);
        capnplist!(
            builder.reborrow(),
            self.outputs
                .iter()
                .map(|o| o.id)
                .collect::<Vec<DataObjectId>>(),
            init_outputs
        );
        self.attributes
            .to_capnp(&mut builder.reborrow().get_attributes().unwrap());
    }
}
impl<'a> ToCapnp<'a> for DataObject {
    type Builder = ::client_capnp::data_object::Builder<'a>;

    fn to_capnp(&self, builder: &mut Self::Builder) {
        self.id.to_capnp(&mut builder.reborrow().get_id().unwrap());
        builder.set_keep(self.keep.get());
        builder.set_label(&self.label);
        builder.set_data_type(self.data_type.to_capnp());

        if let &Some(ref data) = &self.data {
            builder.set_data(&data);
            builder.set_has_data(true);
        } else {
            builder.set_has_data(false);
        }

        self.attributes
            .to_capnp(&mut builder.reborrow().get_attributes().unwrap());
    }
}
