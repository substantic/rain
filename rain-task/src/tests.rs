use super::*;
use std::thread::{spawn, JoinHandle};
use std::os::unix::net::{UnixStream, UnixListener};
use super::framing::SocketExt;
use serde_cbor;
use librain::worker::rpc::subworker_serde::*;
use std::env;
use std::fs;
use std::io::Read;

/// Start dummy worker RPC in another thread, waiting for registration and submitting given task calls.
/// Returns a list of received task relies (if there is an I/O error, the thread returns successfully
/// received so far).
fn dummy_worker(socket_path: &Path, id: SubworkerId, name: &str, requests: Vec<CallMsg>) -> JoinHandle<Vec<ResultMsg>> {
    env::set_current_dir(socket_path.parent().unwrap()).unwrap();
    let main_socket = UnixListener::bind(socket_path.file_name().unwrap()).unwrap();
    let name: String = name.into();
    spawn(move || {
        let (mut socket, addr) = main_socket.accept().unwrap();
        debug!("Dummy worker accepted connection from {:?}", addr);
        let data = socket.read_frame().unwrap();
        let msg = serde_cbor::from_slice::<SubworkerToWorkerMessage>(&data).unwrap();
        if let SubworkerToWorkerMessage::Register(ref reg) = msg {
            assert_eq!(reg.subworker_id, id);
            assert_eq!(reg.subworker_type, name);
            assert_eq!(reg.protocol, MSG_PROTOCOL);
        } else {
            panic!("expected Register msg");
        }
        let mut res = Vec::new();
        for r in requests {
            let data = serde_cbor::to_vec(&WorkerToSubworkerMessage::Call(r)).unwrap();
            let data = match socket.write_frame(&data).and_then(|_| socket.read_frame()) {
                Err(Error(ErrorKind::Io(ref e), _)) if (e.kind() == io::ErrorKind::UnexpectedEof) =>
                    break, // Immediatelly returns res
                Err(e) => Err(e).unwrap(),
                Ok(d) => d,
            };
            let msg = serde_cbor::from_slice::<SubworkerToWorkerMessage>(&data).unwrap();
            if let SubworkerToWorkerMessage::Result(result) = msg {
                res.push(result);
            } else {
                panic!("expected Result msg");
            }
        }
        res
    })
}

/// Setup helper to clean and create a test dir, setup a Subworker and create a dummy worker.
fn setup(name: &str, requests: Vec<CallMsg>) -> (Subworker, JoinHandle<Vec<ResultMsg>>) {
    // let _ = env_logger::try_init(); // Optional logging for beter debug (but normally too noisy)
    let p: PathBuf = env::current_dir().unwrap().join("testing").join(name);
    if p.exists() {
        fs::remove_dir_all(&p).unwrap();
    }
    fs::create_dir_all(&p).unwrap();
    let sock_path = p.join("subworker.socket");
    let s = Subworker::with_params(name, 42, &sock_path, &p);
    let handle = dummy_worker(&sock_path, 42, name, requests);
    (s, handle)
}

fn task1(_ctx: &mut Context, _inputs: &[DataInstance], _outputs: &mut [Output]) -> TaskResult<()>
{
    Ok(())
}

fn task1_fail(_ctx: &mut Context, _inputs: &[DataInstance], _outputs: &mut [Output]) -> TaskResult<()>
{
    bail!("expected failure")
}

#[allow(dead_code)]
fn task3(_ctx: &mut Context, _in1: &DataInstance, _in2: &DataInstance, _out: &mut Output) -> TaskResult<()> {
    Ok(())
}

/// A shortcut to create a DataObjectSpec.
fn data_spec(id: i32, label: &str, location: Option<DataLocation>) -> DataObjectSpec {
    DataObjectSpec {
        id: DataObjectId::new(1, id),
        label: Some(label.into()),
        attributes: Attributes::new(),
        location: location,
        cache_hint: false,
    }
}

#[test]
fn run_dummy_server() {
    let (mut s, handle) = setup("run_dummy_server", Vec::new());
    s.run();
    let res = handle.join().unwrap();
    assert_eq!(&res, &[]);
}

#[test]
fn run_unit_task() {
    let (mut s, handle) = setup("run_unit_task", vec![
        CallMsg {
            task: TaskId::new(1, 2),
            method: "task1".into(),
            attributes: Attributes::new(),
            inputs: vec![],
            outputs: vec![],
        }
    ]);
    s.add_task("task1", task1);
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_eq!(res[0].task, TaskId::new(1, 2));
}

#[test]
fn run_failing_task() {
    let (mut s, handle) = setup("run_failing_task", vec![
        CallMsg {
            task: TaskId::new(1, 2),
            method: "task1f".into(),
            attributes: Attributes::new(),
            inputs: vec![],
            outputs: vec![],
        }
    ]);
    s.add_task("task1f", task1_fail);
    s.run();
    let res = handle.join().unwrap();
    assert!(!res[0].success);
    assert_eq!(res[0].task, TaskId::new(1, 2));
}

#[test]
fn run_missing_task() {
    let (mut s, handle) = setup("run_missing_task", vec![
        CallMsg {
            task: TaskId::new(1, 2),
            method: "another_task".into(),
            attributes: Attributes::new(),
            inputs: vec![],
            outputs: vec![],
        }
    ]);
    s.run();
    let res = handle.join().unwrap();
    assert!(!res[0].success);
}

#[test]
#[should_panic(expected="already present")]
fn session_add_twice() {
    let p: PathBuf = "".into();
    let mut s = Subworker::with_params("", 1, &p, &p);
    s.add_task("task1", task1);
    s.add_task("task1", |_ctx, _ins, _outs| Ok(()));
    //s.add_task2("task1b", task1).unwrap();
    //add_task!(s, "task1a", task3, I I O).unwrap();
    //s.add_task2("task2b", |i: &[u8]| vec![1u8] ).unwrap();
    //s.run_task_test("task1").unwrap();
    //s.run_task_test("task2").unwrap();
}

#[test]
fn session_add() {
    let p: PathBuf = "".into();
    let mut s = Subworker::with_params("", 1, &p, &p);
    s.add_task("task1a", task1);
    s.add_task("task1b", task1);
    s.add_task("task2", |_ctx, _ins, _outs| Ok(()));
    //s.add_task2("task1b", task1).unwrap();
    //add_task!(s, "task1a", task3, I I O).unwrap();
    //s.add_task2("task2b", |i: &[u8]| vec![1u8] ).unwrap();
    //s.run_task_test("task1").unwrap();
    //s.run_task_test("task2").unwrap();
}

fn task_cat(_ctx: &mut Context, inputs: &[DataInstance], outputs: &mut [Output]) -> TaskResult<()>
{
    if outputs.len() != 1 {
        bail!("Expected exactly 1 output");
    }
    if inputs.len() == 1 {
        outputs[0].stage_input(&inputs[0])?;
    } else if inputs.len() >= 2 {
        for inp in inputs.iter() {
            outputs[0].write_all(inp.get_bytes()?).unwrap();
        }
    }
    Ok(())
}

#[test]
fn run_cat_task() {
    let (mut s, handle) = setup("run_cat_task", vec![
        CallMsg {
            task: TaskId::new(1, 2),
            method: "cat".into(),
            attributes: Attributes::new(),
            inputs: vec![
                data_spec(1, "in1", Some(DataLocation::Memory("Hello ".into()))),
                data_spec(2, "in2", Some(DataLocation::Memory("Rain!".into())))
                ],
            outputs: vec![data_spec(3, "out", None)],
        },
        CallMsg {
            task: TaskId::new(1, 2),
            method: "cat".into(),
            attributes: Attributes::new(),
            inputs: vec![
                data_spec(1, "", Some(DataLocation::Memory("Rain ".into()))),
                data_spec(2, "", Some(DataLocation::Memory("for ".into()))),
                data_spec(3, "", Some(DataLocation::Memory("everyone!".into())))
                ],
            outputs: vec![data_spec(10, "out", None)],
        },
    ]);
    s.add_task("cat", task_cat);
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_eq!(res[0].outputs[0].location, Some(DataLocation::Memory("Hello Rain!".into())));
    assert!(res[1].success);
    assert_eq!(res[1].outputs[0].location, Some(DataLocation::Memory("Rain for everyone!".into())));
}

#[test]
fn run_long_cat() {
    let (mut s, handle) = setup("run_long_cat", vec![
        CallMsg {
            task: TaskId::new(1, 2),
            method: "cat".into(),
            attributes: Attributes::new(),
            inputs: vec![
                data_spec(1, "in1", Some(DataLocation::Memory([0u8; MEM_BACKED_LIMIT - 1].as_ref().into()))),
                data_spec(2, "in2", Some(DataLocation::Memory([0u8; 2].as_ref().into())))
                ],
            outputs: vec![data_spec(3, "out", None)],
        },
    ]);
    s.add_task("cat", task_cat);
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    if let Some(DataLocation::Path(ref p)) = res[0].outputs[0].location {
        let mut d = Vec::new();
        fs::File::open(p).unwrap().read_to_end(&mut d).unwrap();
        assert_eq!(d.len(), MEM_BACKED_LIMIT + 1);
    } else {
        panic!("Expected output in a file");
    }
}

#[test]
fn run_empty_cat() {
    let (mut s, handle) = setup("run_empty_cat", vec![
        CallMsg {
            task: TaskId::new(1, 2),
            method: "cat".into(),
            attributes: Attributes::new(),
            inputs: vec![],
            outputs: vec![data_spec(3, "out", None)],
        },
    ]);
    s.add_task("cat", task_cat);
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_eq!(res[0].outputs[0].location, Some(DataLocation::Memory("".into())));
}

#[test]
fn run_pass_cat() {
    let (mut s, handle) = setup("run_pass_cat", vec![
        CallMsg {
            task: TaskId::new(1, 2),
            method: "cat".into(),
            attributes: Attributes::new(),
            inputs: vec![data_spec(1, "in", Some(DataLocation::Memory("drip".into())))],
            outputs: vec![data_spec(2, "out", None)],
        },
    ]);
    s.add_task("cat", task_cat);
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_eq!(res[0].outputs[0].location, Some(DataLocation::OtherObject(DataObjectId::new(1, 1))));
}

#[test]
fn test_make_file_backed() {
    let (mut s, handle) = setup("test_make_file_backed", vec![
        CallMsg {
            task: TaskId::new(1, 2),
            method: "mfb".into(),
            attributes: Attributes::new(),
            inputs: vec![
            ],
            outputs: vec![data_spec(3, "out", None)],
        },
    ]);
    s.add_task("mfb", |_ctx, _ins, outs| {
        write!(outs[0], "Rainfall")?;
        outs[0].make_file_backed()?;
        Ok(())
    });
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert!(matchvar!(res[0].outputs[0].location, Some(DataLocation::Path(_))));
}

#[test]
#[ignore]
fn test_get_path_writing() {
    let (mut s, handle) = setup("test_get_path_writing", vec![
        CallMsg {
            task: TaskId::new(1, 2),
            method: "gp".into(),
            attributes: Attributes::new(),
            inputs: vec![data_spec(1, "in", Some(DataLocation::Memory("drizzle".into())))],
            outputs: vec![],
        },
    ]);
    s.add_task("gp", |_ctx, ins, _outs| {
        let p = ins[0].get_path();
        let mut s = String::new();
        fs::File::open(&p)?.read_to_string(&mut s)?;
        assert_eq!(s, "drizzle");
        Ok(())
    });
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_eq!(res[0].outputs[0].location, Some(DataLocation::Memory("".into())));
}

#[test]
fn run_stage_file() {
    let (mut s, handle) = setup("run_stage_file", vec![
        CallMsg {
            task: TaskId::new(1, 2),
            method: "stage".into(),
            attributes: Attributes::new(),
            inputs: vec![],
            outputs: vec![data_spec(2, "out", None)],
        },
    ]);
    s.add_task("stage", |_ctx, _inp, outp| {
        let mut f = fs::File::create("testfile.txt").unwrap();
        f.write_all(b"Rainy day?").unwrap();
        outp[0].stage_file("testfile.txt")
        });
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    if let Some(DataLocation::Path(ref p)) = res[0].outputs[0].location {
        let mut d = Vec::new();
        fs::File::open(p).unwrap().read_to_end(&mut d).unwrap();
        assert_eq!(d, b"Rainy day?");
    } else {
        panic!("Expected output in a file");
    }
}
