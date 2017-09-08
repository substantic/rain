pub mod id;
pub mod keeppolicy;
pub mod convert;
pub mod rpc;
pub mod wrapped;
pub mod fs;
pub mod resources;
pub mod events;

use std::collections::HashSet;

pub type RcSet<T> = HashSet<T>;

#[derive(Clone, Default, Debug)]
pub struct Additional {}
