use std::error::Error;
use std::net::SocketAddr;

use super::communicator::Communicator;
use super::session::Session;
use rain_core::CLIENT_PROTOCOL_VERSION;
use std::rc::Rc;

pub struct Client {
    comm: Rc<Communicator>,
}

impl Client {
    pub fn new(scheduler: SocketAddr) -> Result<Self, Box<Error>> {
        let comm = Rc::new(Communicator::new(scheduler, CLIENT_PROTOCOL_VERSION)?);

        Ok(Client { comm })
    }

    pub fn new_session(&self) -> Result<Session, Box<Error>> {
        let session_id = self.comm.new_session()?;
        Ok(Session::new(session_id, self.comm.clone()))
    }

    pub fn terminate_server(&self) -> Result<(), Box<Error>> {
        self.comm.terminate_server()
    }
}

#[cfg(test)]
mod tests {
    use super::super::localcluster::LocalCluster;
    use super::super::tasks::CommonTasks;
    use super::Client;
    use super::Session;
    use std::env;

    #[allow(dead_code)]
    struct TestContext {
        cluster: LocalCluster,
        client: Client,
        session: Session,
    }

    fn ctx() -> TestContext {
        let rain = env::var("RAIN_BINARY").unwrap();

        let cluster = LocalCluster::new(&rain).unwrap();
        let client = cluster.create_client().unwrap();
        let session = client.new_session().unwrap();

        TestContext {
            cluster,
            client,
            session,
        }
    }

    #[test]
    fn concat() {
        let mut ctx = ctx();
        let a = ctx.session.blob(vec![1, 2, 3]);
        let b = ctx.session.blob(vec![4, 5, 6]);
        let c = ctx.session.concat(&[a, b]);
        c.output().keep();
        ctx.session.submit().unwrap();
        ctx.session.wait(&[c.clone()], &[]).unwrap();
        assert_eq!(
            ctx.session.fetch(&c.output()).unwrap(),
            vec![1, 2, 3, 4, 5, 6]
        );
    }
}
