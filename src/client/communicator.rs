use tokio_core::reactor::Core;
use std::net::SocketAddr;
use tokio_core::net::TcpStream;
use std::error::Error;
use common::rpc::new_rpc_system;
use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp;
use futures::Future;
use std::cell::Ref;

use super::task::Task;
use client::dataobject::DataObject;
use common::wrapped::WrappedRcRefCell;
use common::id::{DataObjectId, TaskId};
use common::convert::{FromCapnp, ToCapnp};

pub struct Communicator {
    core: Core,
    service: ::client_capnp::client_service::Client,
}

impl Communicator {
    pub fn new(scheduler: SocketAddr, version: i32) -> Result<Self, Box<Error>> {
        let mut core = Core::new()?;
        let handle = core.handle();
        let stream = core.run(TcpStream::connect(&scheduler, &handle))?;
        stream.set_nodelay(true)?;

        debug!("Connection to server {} established", scheduler);

        let mut rpc = Box::new(new_rpc_system(stream, None));
        let bootstrap: ::server_capnp::server_bootstrap::Client =
            rpc.bootstrap(rpc_twoparty_capnp::Side::Server);
        handle.spawn(rpc.map_err(|err| panic!("RPC error: {}", err)));

        let mut request = bootstrap.register_as_client_request();
        request.get().set_version(version);

        let service = core.run(
            request
                .send()
                .promise
                .and_then(|response| Promise::ok(pry!(response.get()).get_service())),
        )??;

        Ok(Self { core, service })
    }

    pub fn new_session(&mut self) -> Result<i32, Box<Error>> {
        let id: i32 = self.core.run(
            self.service
                .new_session_request()
                .send()
                .promise
                .and_then(|response| Promise::ok(pry!(response.get()).get_session_id())),
        )?;

        Ok(id)
    }
    pub fn close_session(&mut self, id: i32) -> Result<(), Box<Error>> {
        self.core.run({
            let mut req = self.service.close_session_request();
            req.get().set_session_id(id);
            req.send().promise
        })?;

        Ok(())
    }

    pub fn submit(
        &mut self,
        tasks: &[WrappedRcRefCell<Task>],
        data_objects: &[WrappedRcRefCell<DataObject>],
    ) -> Result<(), Box<Error>> {
        let mut req = self.service.submit_request();

        capnplist!(
            req.get(),
            tasks.iter().map(|t| t.get()).collect::<Vec<Ref<Task>>>(),
            init_tasks
        );
        capnplist!(
            req.get(),
            data_objects
                .iter()
                .map(|o| o.get())
                .collect::<Vec<Ref<DataObject>>>(),
            init_objects
        );

        self.core.run(req.send().promise)?;

        Ok(())
    }

    pub fn unkeep(&mut self, objects: &[WrappedRcRefCell<DataObject>]) -> Result<(), Box<Error>> {
        let mut req = self.service.unkeep_request();
        capnplist!(
            req.get(),
            objects
                .iter()
                .map(|o| o.get().id)
                .collect::<Vec<DataObjectId>>(),
            init_object_ids
        );
        self.core.run(req.send().promise)?;
        Ok(())
    }

    pub fn wait(
        &mut self,
        tasks: &[WrappedRcRefCell<Task>],
        objects: &[WrappedRcRefCell<DataObject>],
    ) -> Result<(), Box<Error>> {
        let mut req = self.service.wait_request();
        capnplist!(
            req.get(),
            tasks.iter().map(|t| t.get().id).collect::<Vec<TaskId>>(),
            init_task_ids
        );
        capnplist!(
            req.get(),
            objects
                .iter()
                .map(|o| o.get().id)
                .collect::<Vec<DataObjectId>>(),
            init_object_ids
        );
        self.core.run(req.send().promise)?;
        Ok(())
    }
    pub fn wait_some(
        &mut self,
        tasks: &[WrappedRcRefCell<Task>],
        objects: &[WrappedRcRefCell<DataObject>],
    ) -> Result<(Vec<TaskId>, Vec<DataObjectId>), Box<Error>> {
        let mut req = self.service.wait_some_request();
        capnplist!(
            req.get(),
            tasks.iter().map(|t| t.get().id).collect::<Vec<TaskId>>(),
            init_task_ids
        );
        capnplist!(
            req.get(),
            objects
                .iter()
                .map(|o| o.get().id)
                .collect::<Vec<DataObjectId>>(),
            init_object_ids
        );
        let res = self.core.run(req.send().promise)?;

        Ok((
           res.get()?.get_finished_tasks()?.iter().map(|id| TaskId::from_capnp(&id)).collect(),
           res.get()?.get_finished_objects()?.iter().map(|id| DataObjectId::from_capnp(&id)).collect(),
        ))
    }

    pub fn fetch(&mut self, object_id: DataObjectId) -> Result<Vec<u8>, Box<Error>> {
        let mut req = self.service.fetch_request();
        object_id.to_capnp(&mut req.get().get_id().unwrap());
        req.get().set_size(1024);

        let response = self.core.run(req.send().promise)?;

        let reader = response.get()?;
        match reader.get_status().which()? {
            ::common_capnp::fetch_result::status::Ok(()) => {
                let data = reader.get_data()?;
                Ok(Vec::from(data))
            }
            _ => bail!("Non-ok status"),
        }
    }

    pub fn terminate_server(&mut self) -> Result<(), Box<Error>> {
        self.core
            .run(self.service.terminate_server_request().send().promise)?;
        Ok(())
    }
}
