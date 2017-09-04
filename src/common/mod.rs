pub mod id;
pub mod keeppolicy;
pub mod convert;
pub mod rpc;
pub mod wrapped;
pub mod fs;
pub mod resources;
pub mod events;
pub mod asycinit;

use std::collections::HashSet;

pub type RcSet<T> = HashSet<T>;

#[derive(Clone, Default, Debug)]
pub struct Additional {}
