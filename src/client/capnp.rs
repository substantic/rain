use super::task::Task;
use super::session::ObjectId;
use capnp::Error;
use client::data_object::DataObject;
use client::task::TaskInput;
use common::Attributes;

pub trait Serializable<'a> {
    type Builder;

    fn serialize(&self, builder: &mut Self::Builder) -> Result<(), Box<Error>>;
}

pub struct TaskId(ObjectId);
pub struct DataObjectId(ObjectId);

impl From<ObjectId> for TaskId {
    fn from(id: ObjectId) -> Self {
        TaskId(id)
    }
}
impl From<ObjectId> for DataObjectId {
    fn from(id: ObjectId) -> Self {
        DataObjectId(id)
    }
}

impl<'a> Serializable<'a> for TaskId {
    type Builder = ::common_capnp::task_id::Builder<'a>;

    fn serialize(&self, builder: &mut Self::Builder) -> Result<(), Box<Error>> {
        builder.set_id(self.0.id);
        builder.set_session_id(self.0.session_id);

        Ok(())
    }
}
impl<'a> Serializable<'a> for DataObjectId {
    type Builder = ::common_capnp::data_object_id::Builder<'a>;

    fn serialize(&self, builder: &mut Self::Builder) -> Result<(), Box<Error>> {
        builder.set_id(self.0.id);
        builder.set_session_id(self.0.session_id);

        Ok(())
    }
}
impl<'a> Serializable<'a> for TaskInput {
    type Builder = ::client_capnp::task::in_data_object::Builder<'a>;

    fn serialize(&self, builder: &mut Self::Builder) -> Result<(), Box<Error>> {
        let id: DataObjectId = self.data_object.get().id.into();
        id.serialize(&mut builder.reborrow().get_id()?)?;

        if let &Some(ref label) = &self.label {
            builder.reborrow().set_label(label);
        }

        Ok(())
    }
}

impl<'a> Serializable<'a> for Attributes {
    type Builder = ::common_capnp::attributes::Builder<'a>;
    fn serialize(&self, builder: &mut Self::Builder) -> Result<(), Box<Error>> {
        Ok(self.to_capnp(builder))
    }
}

impl<'a> Serializable<'a> for Task {
    type Builder = ::client_capnp::task::Builder<'a>;

    fn serialize(&self, builder: &mut Self::Builder) -> Result<(), Box<Error>> {
        let task_id: TaskId = self.id.into();
        task_id.serialize(&mut builder.reborrow().get_id()?)?;
        builder.set_task_type(self.command.get_task_type());

        {
            let mut inputs_builder = builder.reborrow().init_inputs(self.inputs.len() as u32);
            for (i, input) in self.inputs.iter().enumerate() {
                input.serialize(&mut inputs_builder.reborrow().get(i as u32))?;
            }
        }
        {
            let mut outputs_builder = builder.reborrow().init_outputs(self.outputs.len() as u32);
            for (i, output) in self.outputs.iter().enumerate() {
                let id: DataObjectId = output.get().id.into();
                id.serialize(&mut outputs_builder.reborrow().get(i as u32))?;
            }
        }
        self.attributes
            .serialize(&mut builder.reborrow().get_attributes()?)?;

        Ok(())
    }
}
impl<'a> Serializable<'a> for DataObject {
    type Builder = ::client_capnp::data_object::Builder<'a>;

    fn serialize(&self, builder: &mut Self::Builder) -> Result<(), Box<Error>> {
        let data_obj_id: DataObjectId = self.id.into();
        data_obj_id.serialize(&mut builder.reborrow().get_id()?)?;
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
            .serialize(&mut builder.reborrow().get_attributes()?)?;

        Ok(())
    }
}
