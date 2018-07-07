use capnp_rpc::rpc_twoparty_capnp;
use futures::Future;
use rain_core::comm::new_rpc_system;
use std::error::Error;
use std::net::SocketAddr;
use tokio_core::net::TcpStream;
use tokio_core::reactor::Core;

use super::task::Task;
use client::dataobject::DataObject;
use rain_core::types::{DataObjectId, TaskId};
use rain_core::utils::{FromCapnp, ToCapnp};
use rain_core::{client_capnp, common_capnp, server_capnp};
use std::cell::{RefCell, RefMut};

pub struct Communicator {
    core: RefCell<Core>,
    service: client_capnp::client_service::Client,
}

impl Communicator {
    pub fn new(scheduler: SocketAddr, version: i32) -> Result<Self, Box<Error>> {
        let mut core = Core::new()?;
        let handle = core.handle();
        let stream = core.run(TcpStream::connect(&scheduler, &handle))?;
        stream.set_nodelay(true)?;

        debug!("Connection to server {} established", scheduler);

        let mut rpc = Box::new(new_rpc_system(stream, None));
        let bootstrap: server_capnp::server_bootstrap::Client =
            rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
        handle.spawn(rpc.map_err(|err| panic!("RPC error: {}", err)));

        let mut request = bootstrap.register_as_client_request();
        request.get().set_version(version);

        let service = core.run(request.send().promise)?.get()?.get_service()?;

        Ok(Self {
            core: RefCell::new(core),
            service,
        })
    }

    pub fn new_session(&self) -> Result<i32, Box<Error>> {
        let id: i32 = self.comm()
            .run(self.service.new_session_request().send().promise)?
            .get()?
            .get_session_id();

        Ok(id)
    }
    pub fn close_session(&self, id: i32) -> Result<(), Box<Error>> {
        self.comm().run({
            let mut req = self.service.close_session_request();
            req.get().set_session_id(id);
            req.send().promise
        })?;

        Ok(())
    }

    pub fn submit<T, D>(&self, tasks: &[T], data_objects: &[D]) -> Result<(), Box<Error>>
    where
        T: AsRef<Task>,
        D: AsRef<DataObject>,
    {
        let mut req = self.service.submit_request();

        to_capnp_list!(
            req.get(),
            tasks.iter().map(|t| t.as_ref()).collect::<Vec<&Task>>(),
            init_tasks
        );
        to_capnp_list!(
            req.get(),
            data_objects
                .iter()
                .map(|t| t.as_ref())
                .collect::<Vec<&DataObject>>(),
            init_objects
        );
        self.comm().run(req.send().promise)?;

        Ok(())
    }

    pub fn unkeep(&self, objects: &[DataObjectId]) -> Result<(), Box<Error>> {
        let mut req = self.service.unkeep_request();
        to_capnp_list!(req.get(), objects, init_object_ids);
        self.comm().run(req.send().promise)?;
        Ok(())
    }

    pub fn wait(&self, tasks: &[TaskId], objects: &[DataObjectId]) -> Result<(), Box<Error>> {
        let mut req = self.service.wait_request();
        to_capnp_list!(req.get(), tasks, init_task_ids);
        to_capnp_list!(req.get(), objects, init_object_ids);
        self.comm().run(req.send().promise)?;
        Ok(())
    }
    pub fn wait_some(
        &self,
        tasks: &[TaskId],
        objects: &[DataObjectId],
    ) -> Result<(Vec<TaskId>, Vec<DataObjectId>), Box<Error>> {
        let mut req = self.service.wait_some_request();
        to_capnp_list!(req.get(), tasks, init_task_ids);
        to_capnp_list!(req.get(), objects, init_object_ids);
        let res = self.comm().run(req.send().promise)?;

        Ok((
            from_capnp_list!(res.get()?, get_finished_tasks, TaskId),
            from_capnp_list!(res.get()?, get_finished_objects, DataObjectId),
        ))
    }

    pub fn fetch(&self, object_id: &DataObjectId) -> Result<Vec<u8>, Box<Error>> {
        let mut req = self.service.fetch_request();
        object_id.to_capnp(&mut req.get().get_id().unwrap());
        req.get().set_size(1024);

        let response = self.comm().run(req.send().promise)?;

        // TODO: handle error states
        let reader = response.get()?;
        match reader.get_status().which()? {
            common_capnp::fetch_result::status::Ok(()) => {
                let data = reader.get_data()?;
                Ok(Vec::from(data))
            }
            common_capnp::fetch_result::status::Removed(()) => {
                print!("Removed");
                Ok(vec![])
            }
            common_capnp::fetch_result::status::Error(err) => {
                print!("Error: {:?}", err.unwrap().get_message());
                Ok(vec![])
            }
            _ => bail!("Non-ok status"),
        }
    }

    pub fn terminate_server(&self) -> Result<(), Box<Error>> {
        self.comm()
            .run(self.service.terminate_server_request().send().promise)?;
        Ok(())
    }

    fn comm(&self) -> RefMut<Core> {
        self.core.borrow_mut()
    }
}
