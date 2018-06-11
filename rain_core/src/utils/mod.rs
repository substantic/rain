pub(crate) mod asyncinit;
pub(crate) mod consistency;
pub(crate) mod convert;

use futures::unsync::oneshot;
use std::collections::HashSet;

pub type RcSet<T> = HashSet<T>;
pub type FinishHook = oneshot::Sender<()>;

pub use self::asyncinit::AsyncInitWrapper;
pub use self::consistency::{ConsistencyCheck, DEBUG_CHECK_CONSISTENCY};
pub use self::convert::{FromCapnp, ReadCapnp, ToCapnp, WriteCapnp};
