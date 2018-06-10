use super::framing::SocketExt;
use super::*;
use rain_core::governor::rpc::executor_serde::*;
use rain_core::common::attributes::*;
use rain_core::common::datatype::DataType;

use serde_cbor;
use std::env;
use std::fs;
use std::io::Read;
use std::os::unix::net::{UnixListener, UnixStream};
use std::thread::{spawn, JoinHandle};

/// Start dummy governor RPC in another thread, waiting for registration and submitting given task calls.
/// Returns a list of received task relies (if there is an I/O error, the thread returns successfully
/// received so far).
fn dummy_governor(
    socket_path: &Path,
    id: ExecutorId,
    name: &str,
    requests: Vec<CallMsg>,
) -> JoinHandle<Vec<ResultMsg>> {
    env::set_current_dir(socket_path.parent().unwrap()).unwrap();
    let main_socket = UnixListener::bind(socket_path.file_name().unwrap()).unwrap();
    let name: String = name.into();
    spawn(move || {
        let (mut socket, addr) = main_socket.accept().unwrap();
        debug!("Dummy governor accepted connection from {:?}", addr);
        let data = socket.read_frame().unwrap();
        let msg = serde_cbor::from_slice::<ExecutorToGovernorMessage>(&data).unwrap();
        if let ExecutorToGovernorMessage::Register(ref reg) = msg {
            assert_eq!(reg.executor_id, id);
            assert_eq!(reg.executor_type, name);
            assert_eq!(reg.protocol, MSG_PROTOCOL);
        } else {
            panic!("expected Register msg");
        }
        let mut res = Vec::new();
        for r in requests {
            let data = serde_cbor::to_vec(&GovernorToExecutorMessage::Call(r)).unwrap();
            let data = match socket.write_frame(&data).and_then(|_| socket.read_frame()) {
                Err(Error(ErrorKind::Io(ref e), _))
                    if (e.kind() == io::ErrorKind::UnexpectedEof) =>
                {
                    break
                } // Immediatelly returns res
                Err(e) => Err(e).unwrap(),
                Ok(d) => d,
            };
            let msg = serde_cbor::from_slice::<ExecutorToGovernorMessage>(&data).unwrap();
            if let ExecutorToGovernorMessage::Result(result) = msg {
                res.push(result);
            } else {
                panic!("expected Result msg");
            }
        }
        res
    })
}

/// Setup helper to clean and create a test dir, setup a Executor and create a dummy governor.
fn setup(name: &str, requests: Vec<CallMsg>) -> (Executor, JoinHandle<Vec<ResultMsg>>) {
    // let _ = env_logger::try_init(); // Optional logging for beter debug (but normally too noisy)
    let p: PathBuf = env::current_dir().unwrap().join("testing").join(name);
    if p.exists() {
        fs::remove_dir_all(&p).unwrap();
    }
    fs::create_dir_all(&p).unwrap();
    let sock_path = p.join("executor.socket");
    let s = Executor::with_params(name, 42, &sock_path, &p);
    let handle = dummy_governor(&sock_path, 42, name, requests);
    (s, handle)
}

fn task1(_ctx: &mut Context, _inputs: &[DataInstance], _outputs: &mut [Output]) -> TaskResult<()> {
    Ok(())
}

fn task1_fail(
    _ctx: &mut Context,
    _inputs: &[DataInstance],
    _outputs: &mut [Output],
) -> TaskResult<()> {
    bail!("expected failure")
}

#[allow(dead_code)]
fn task3(
    _ctx: &mut Context,
    _in1: &DataInstance,
    _in2: &DataInstance,
    _out: &mut Output,
) -> TaskResult<()> {
    Ok(())
}

/// A shortcut to create a DataObjectSpec.
fn data_spec(id: i32, label: &str, location: Option<DataLocation>) -> LocalObjectIn {
    LocalObjectIn {
        spec: ObjectSpec {
            id: DataObjectId::new(1, id),
            label: label.into(),
            data_type: DataType::Blob,
            content_type: "".into(),
            user: HashMap::default(),
        },
        info: if location.is_some() { Some(ObjectInfo::default()) } else { None },
        location: location,
        cache_hint: false,
    }
}

fn task_in(id: i32) -> TaskSpecInput {
    TaskSpecInput {
        id: DataObjectId::new(1, id),
        label: "".into(),
    }
}

fn call_msg(id: i32, name: &str, inputs: Vec<LocalObjectIn>, outputs: Vec<LocalObjectIn>) -> CallMsg {
    CallMsg {
        spec: TaskSpec {
            id: TaskId::new(1, id),
            inputs: inputs.iter().map(|o| task_in(o.spec.id.get_id())).collect(),
            outputs: outputs.iter().map(|o| o.spec.id).collect(),
            task_type: name.into(),                
            .. TaskSpec::default()
        },
        inputs: inputs,
        outputs: outputs,
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
    let (mut s, handle) = setup(
        "run_unit_task",
        vec![call_msg(2, "run_unit_task/task1", vec![], vec![])]);
    s.register_task("task1", task1);
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_eq!(res[0].task, TaskId::new(1, 2));
}

#[test]
fn run_failing_task() {
    let (mut s, handle) = setup(
        "run_failing_task",
        vec![call_msg(2, "run_failing_task/task1f", vec![], vec![])]);
    s.register_task("task1f", task1_fail);
    s.run();
    let res = handle.join().unwrap();
    assert!(!res[0].success);
    assert_eq!(res[0].task, TaskId::new(1, 2));
}

#[test]
fn run_missing_task() {
    let (mut s, handle) = setup(
        "run_missing_task",
        vec![call_msg(1, "run_missing_task/missing_task", vec![], vec![])]);
    s.run();
    let res = handle.join().unwrap();
    assert!(!res[0].success);
}

#[test]
#[should_panic(expected = "already present")]
fn register_task_twice() {
    let p: PathBuf = "".into();
    let mut s = Executor::with_params("", 1, &p, &p);
    s.register_task("task1", task1);
    s.register_task("task1", |_ctx, _ins, _outs| Ok(()));
}

#[test]
fn register_task() {
    let p: PathBuf = "".into();
    let mut s = Executor::with_params("", 1, &p, &p);
    s.register_task("task1a", task1);
    s.register_task("task1b", task1);
    s.register_task("task2", |_ctx, _ins, _outs| Ok(()));
    register_task!(s, "task3", [I I O], task3);
    register_task!(s, "task4", [I I O O], |_ctx, _in1, _in2, _out1, _out2| Ok(()));
    register_task!(s, "task5", [Is Os], task1);
    register_task!(s, "task6", [I O Is Os],
        |_ctx, _i1: &DataInstance, _o1: &mut Output, _is: &[DataInstance], _os: &mut [Output]| Ok(()));
}

fn task_cat(_ctx: &mut Context, inputs: &[DataInstance], outputs: &mut [Output]) -> TaskResult<()> {
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
    let (mut s, handle) = setup(
        "run_cat_task",
        vec![
            call_msg(1, "run_cat_task/cat",
                vec![
                    data_spec(4, "in1", Some(DataLocation::Memory("Hello ".into()))),
                    data_spec(5, "in2", Some(DataLocation::Memory("Rain!".into()))),
                ],
                vec![data_spec(6, "out", None)]),
            call_msg(2, "run_cat_task/cat",
                vec![
                    data_spec(7, "", Some(DataLocation::Memory("Rain ".into()))),
                    data_spec(8, "", Some(DataLocation::Memory("for ".into()))),
                    data_spec(9, "", Some(DataLocation::Memory("everyone!".into()))),
                ],
                vec![data_spec(10, "out", None)])]);
    s.register_task("cat", task_cat);
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_eq!(
        res[0].outputs[0].location,
        Some(DataLocation::Memory("Hello Rain!".into()))
    );
    assert!(res[1].success);
    assert_eq!(
        res[1].outputs[0].location,
        Some(DataLocation::Memory("Rain for everyone!".into()))
    );
}

#[test]
fn run_long_cat() {
    let (mut s, handle) = setup(
        "run_long_cat",
        vec![call_msg(1, "run_long_cat/cat",
            vec![data_spec(1, "in1",
                    Some(DataLocation::Memory(
                        [0u8; MEM_BACKED_LIMIT - 1].as_ref().into(),
                    )),
                ),
                data_spec(2, "in2",
                    Some(DataLocation::Memory([0u8; 2].as_ref().into())),
                ),
            ],
            vec![data_spec(6, "out", None)]),
            ]);
    s.register_task("cat", task_cat);
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
    let (mut s, handle) = setup(
        "run_empty_cat",
        vec![
            call_msg(1, "run_empty_cat/cat", vec![], vec![data_spec(3, "out", None)])
            ],
    );
    s.register_task("cat", task_cat);
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_eq!(
        res[0].outputs[0].location,
        Some(DataLocation::Memory("".into()))
    );
}

#[test]
fn run_pass_cat() {
    let (mut s, handle) = setup(
        "run_pass_cat",
        vec![call_msg(2, "run_pass_cat/cat",
            vec![
                data_spec(1, "in", Some(DataLocation::Memory("drip".into())))
                ],
            vec![data_spec(2, "out", None)])]);
    s.register_task("cat", task_cat);
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_eq!(
        res[0].outputs[0].location,
        Some(DataLocation::OtherObject(DataObjectId::new(1, 1)))
    );
}

#[test]
fn test_make_file_backed() {
    let (mut s, handle) = setup(
        "test_make_file_backed",
        vec![call_msg(2, "test_make_file_backed/mfb", vec![], vec![data_spec(3, "out", None)])]);
    s.register_task("mfb", |_ctx, _ins, outs| {
        write!(outs[0], "Rainfall")?;
        outs[0].make_file_backed()?;
        Ok(())
    });
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert!(matchvar!(
        res[0].outputs[0].location,
        Some(DataLocation::Path(_))
    ));
}

#[test]
#[ignore]
fn test_get_path_writing() {
    let (mut s, handle) = setup(
        "test_get_path_writing",
        vec![call_msg(2, "test_get_path_writing/gp", vec![data_spec(
                1,
                "in",
                Some(DataLocation::Memory("drizzle".into())),
            )], vec![])]);
    s.register_task("gp", |_ctx, ins, _outs| {
        let p = ins[0].get_path();
        let mut s = String::new();
        fs::File::open(&p)?.read_to_string(&mut s)?;
        assert_eq!(s, "drizzle");
        Ok(())
    });
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_eq!(
        res[0].outputs[0].location,
        Some(DataLocation::Memory("".into()))
    );
}

#[test]
fn run_stage_file() {
    let (mut s, handle) = setup(
        "run_stage_file",
        vec![call_msg(2, "run_stage_file/stage", vec![], vec![data_spec(2, "out", None)])]);
    s.register_task("stage", |_ctx, _inp, outp| {
        let mut f = fs::File::create("testfile.txt").unwrap();
        f.write_all(b"Rainy day?").unwrap();
        drop(f);
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

fn dummy_callmsg(name: &str, ins: i32, outs: i32) -> CallMsg {
    call_msg(2, name,
        (0..ins)
            .map(|i| data_spec(i, "", Some(DataLocation::Memory(vec![]))))
            .collect(),
        (0..outs)
            .map(|i| data_spec(10 + i, "", None))
            .collect())
}

fn assert_res_error(res: &ResultMsg, err: &str) {
    assert!(!res.success);
    assert!(res.info.error.contains(err));
}

#[test]
fn register_task_fail_count() {
    let (mut s, handle) = setup(
        "register_task_fail_count1",
        vec![
            dummy_callmsg("register_task_fail_count1/foo", 1, 1),
            dummy_callmsg("register_task_fail_count1/foo", 0, 0),
            dummy_callmsg("register_task_fail_count1/foo", 1, 0),
            dummy_callmsg("register_task_fail_count1/foo", 2, 1),
            dummy_callmsg("register_task_fail_count1/foo", 1, 2),
        ],
    );
    register_task!(s, "foo", [I O], |_ctx, _inp: &DataInstance, _outp: &mut Output| Ok(()));
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_res_error(&res[1], "not enough inputs");
    assert_res_error(&res[2], "not enough outputs");
    assert_res_error(&res[3], "too many inputs");
    assert_res_error(&res[4], "too many outputs");
}

#[test]
fn register_task_fail_count_multi() {
    let (mut s, handle) = setup(
        "register_task_fail_count1",
        vec![
            dummy_callmsg("register_task_fail_count1/foo", 1, 1),
            dummy_callmsg("register_task_fail_count1/foo", 0, 6),
            dummy_callmsg("register_task_fail_count1/foo", 3, 0),
            dummy_callmsg("register_task_fail_count1/foo", 0, 0),
            dummy_callmsg("register_task_fail_count1/foo", 5, 3),
        ],
    );
    register_task!(s, "foo", [O Os I Is], |_ctx, _o: &mut Output, _os, _i: &DataInstance, _is| Ok(()));
    s.run();
    let res = handle.join().unwrap();
    assert!(res[0].success);
    assert_res_error(&res[1], "not enough inputs");
    assert_res_error(&res[2], "not enough outputs");
    assert_res_error(&res[3], "not enough outputs");
    assert!(res[4].success);
}

#[test]
fn read_set_content_type() {
    let mut call = call_msg(2, "read_set_content_type/foo",
        vec![data_spec(
            2,
            "out",
            Some(DataLocation::Memory((b"content!" as &[u8]).into())),
        )],
        vec![data_spec(3, "out", None)]);
    call.inputs[0].info = Some(ObjectInfo { content_type: "text".into(), .. ObjectInfo::default() });
    call.outputs[0].spec.content_type = "text".into();
    let (mut s, handle) = setup("read_set_content_type", vec![call]);
    register_task!(s, "foo", [I O], |_ctx, i: &DataInstance, o: &mut Output| {
        assert_eq!(i.get_content_type(), "text");
        assert_eq!(o.get_content_type(), "text");
        o.set_content_type("text").unwrap();
        Ok(())
        });
    s.run();
    let res = handle.join().unwrap();
    print!("{:?}", res[0]);
    assert!(res[0].success);
}
