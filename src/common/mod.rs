pub mod id;
pub mod keeppolicy;
pub mod convert;
pub mod rpc;
pub mod wrapped;
pub mod fs;
pub mod resources;
pub mod events;
pub mod asycinit;
pub mod additional;

use std::collections::HashSet;
use futures::unsync::oneshot;

pub type RcSet<T> = HashSet<T>;
pub use self::additional::Additionals;
pub use self::resources::Resources;

pub mod monitor;
pub mod logger;

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
