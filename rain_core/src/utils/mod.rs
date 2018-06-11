pub(crate) mod asyncinit;
pub(crate) mod convert;
pub(crate) mod consistency;
pub(crate) mod wrapped;

use futures::unsync::oneshot;
use std::collections::HashSet;

pub type RcSet<T> = HashSet<T>;
pub type FinishHook = oneshot::Sender<()>;

pub use self::asyncinit::AsyncInitWrapper;
pub use self::consistency::{DEBUG_CHECK_CONSISTENCY, ConsistencyCheck};
pub use self::convert::{FromCapnp, ToCapnp, ReadCapnp, WriteCapnp};
pub use self::wrapped::WrappedRcRefCell;