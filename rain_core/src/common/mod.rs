pub mod asyncinit;
pub mod attributes;
pub mod comm;
pub mod convert;
pub mod datatype;
pub mod events;
pub mod id;
pub mod resources;
pub mod rpc;
pub mod sys;
pub mod wrapped;

use futures::unsync::oneshot;
use std::collections::HashSet;

pub type RcSet<T> = HashSet<T>;
pub use self::attributes::{ObjectInfo, ObjectSpec, TaskInfo, TaskSpec};
pub use self::resources::Resources;

pub mod fs;
pub mod logging;
pub mod monitor;

pub type FinishHook = oneshot::Sender<()>;

/// Common trait for objects with checkable consistency
pub trait ConsistencyCheck {
    fn check_consistency(&self) -> ::errors::Result<()>;

    /// Run check_consistency depending on DEBUG_CHECK_CONSISTENCY.
    fn check_consistency_opt(&self) -> ::errors::Result<()> {
        if ::DEBUG_CHECK_CONSISTENCY.load(::std::sync::atomic::Ordering::Relaxed) {
            self.check_consistency()
        } else {
            Ok(())
        }
    }
}

pub use self::datatype::DataType;
