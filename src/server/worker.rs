use std::cell::RefCell;
use std::rc::Rc;
use std::hash::{Hash, Hasher};

use common::id::WorkerId;

struct WorkerInner {
    id: WorkerId,

    control: ::worker_capnp::worker_control::Client,

    // Resources
    n_cpus: u32,
    free_n_cpus: u32,
}

#[derive(Clone)]
pub struct Worker {
    inner: Rc<RefCell<WorkerInner>>,
}

impl Hash for Worker {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ptr = &*self.inner as *const _;
        ptr.hash(state);
    }
}

impl PartialEq for Worker {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

impl Eq for Worker {}

impl Worker {
    pub fn new(worker_id: WorkerId,
               control: ::worker_capnp::worker_control::Client,
               n_cpus: u32) -> Self {
        Worker {
            inner: Rc::new(RefCell::new(WorkerInner {
                id: worker_id,
                control: control,
                n_cpus: n_cpus,
                free_n_cpus: n_cpus,
            }))
        }
    }

    #[inline]
    pub fn get_id(&self) -> WorkerId {
        self.inner.borrow().id
    }
}