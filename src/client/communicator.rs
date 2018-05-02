use tokio_core::reactor::Core;
use std::net::SocketAddr;
use tokio_core::net::TcpStream;
use std::error::Error;
use common::rpc::new_rpc_system;
use capnp::capability::Promise;
use capnp_rpc::rpc_twoparty_capnp;
use futures::Future;

use super::task::Task;
use client::data_object::DataObject;
use client::capnp::Serializable;
use common::wrapped::WrappedRcRefCell;
use client::session::ObjectId;
use client::capnp::DataObjectId;

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
        {
            let mut tasks_builder = req.get().init_tasks(tasks.len() as u32);
            for (i, task) in tasks.iter().enumerate() {
                task.get()
                    .serialize(&mut tasks_builder.reborrow().get(i as u32))?;
            }
        }
        {
            let mut objects_builder = req.get().init_objects(data_objects.len() as u32);
            for (i, obj) in data_objects.iter().enumerate() {
                obj.get()
                    .serialize(&mut objects_builder.reborrow().get(i as u32))?;
            }
        }

        self.core.run(req.send().promise)?;

        Ok(())
    }

    pub fn fetch(&mut self, object: ObjectId) -> Result<Vec<u8>, Box<Error>> {
        let mut req = self.service.fetch_request();
        let obj_id: DataObjectId = object.into();
        obj_id.serialize(&mut req.get().get_id()?)?;
        req.get().set_size(1024);

        let response = self.core.run(
            req.send()
                .promise
                .and_then(|response| Promise::ok(response)),
        )?;

        let reader = response.get()?;
        match reader.get_status().which()? {
            ::common_capnp::fetch_result::status::Ok(()) => {
                println!("Status: ok");
            }
            _ => {
                println!("Status: not ok");
            }
        }
        let data = reader.get_data()?;
        Ok(Vec::from(data))
    }

    pub fn terminate_server(&mut self) -> Result<(), Box<Error>> {
        self.core
            .run(self.service.terminate_server_request().send().promise)?;
        Ok(())
    }
}
