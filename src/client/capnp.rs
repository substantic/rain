use super::task::Task;
use client::data_object::DataObject;
use client::task::TaskInput;
use common::convert::ToCapnp;

impl<'a> ToCapnp<'a> for TaskInput {
    type Builder = ::client_capnp::task::in_data_object::Builder<'a>;

    fn to_capnp(&self, builder: &mut Self::Builder) {
        self.data_object.get().id.to_capnp(&mut builder.reborrow().get_id().unwrap());

        if let &Some(ref label) = &self.label {
            builder.reborrow().set_label(label);
        }
    }
}

impl<'a> ToCapnp<'a> for Task {
    type Builder = ::client_capnp::task::Builder<'a>;

    fn to_capnp(&self, builder: &mut Self::Builder) {
        self.id.to_capnp(&mut builder.reborrow().get_id().unwrap());
        builder.set_task_type(self.command.get_task_type());

        {
            let mut inputs_builder = builder.reborrow().init_inputs(self.inputs.len() as u32);
            for (i, input) in self.inputs.iter().enumerate() {
                input.to_capnp(&mut inputs_builder.reborrow().get(i as u32));
            }
        }
        {
            let mut outputs_builder = builder.reborrow().init_outputs(self.outputs.len() as u32);
            for (i, output) in self.outputs.iter().enumerate() {
                output.get().id.to_capnp(&mut outputs_builder.reborrow().get(i as u32));
            }
        }
        self.attributes
            .to_capnp(&mut builder.reborrow().get_attributes().unwrap());
    }
}
impl<'a> ToCapnp<'a> for DataObject {
    type Builder = ::client_capnp::data_object::Builder<'a>;

    fn to_capnp(&self, builder: &mut Self::Builder) {
        self.id.to_capnp(&mut builder.reborrow().get_id().unwrap());
        builder.set_keep(self.keep);
        builder.set_label(&self.label);
        builder.set_data_type(::common_capnp::DataType::Blob); // TODO

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
