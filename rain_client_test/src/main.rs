extern crate rain_client;

use std::error::Error;
use rain_client::tasks::CommonTasks;
use rain_client::client::LocalCluster;

fn test() -> Result<(), Box<Error>> {
    let cluster = LocalCluster::new("/home/kobzol/projects/it4i/rain/target/debug/rain")?;

    let client = cluster.create_client()?;
    let mut s = client.new_session()?;

    let a = s.open("/tmp/asd.txt".to_owned());
    let b = s.open("/tmp/asd2.txt".to_owned());

    let c = s.concat(&[a.output(), b.output()]);
    c.output().keep();
    s.submit()?;
    let res = s.fetch(&c.output())?;
    println!("{}", String::from_utf8(res)?);

    Ok(())
}

fn main() {
    test().unwrap();
}
