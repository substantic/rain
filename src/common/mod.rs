pub mod id;
pub mod keeppolicy;

use capnp::{message, traits, serialize};
use std::io::{Write, Read};

trait FromCapnp<'a> where Self: Sized {
    type Reader: traits::FromPointerReader<'a>;
    fn from_capnp(read: &'a Self::Reader) -> Self;
}

trait FromCapnpOwned: for <'a> FromCapnp<'a> {
    fn read_capnp<R: Read>(r: &mut R) -> Self {
        let msg = serialize::read_message(r, Default::default()).unwrap();
        let read = msg.get_root::<Self::Reader>().unwrap();
        Self::from_capnp(&read)
    }
}

impl<T> FromCapnpOwned for T where T: for <'a> FromCapnp<'a> {}


trait ToCapnp<'a> {
    type Builder: traits::FromPointerBuilder<'a>;
    fn to_capnp(self: &Self, build: &mut Self::Builder);
}

trait ToCapnpOwned: for <'a> ToCapnp<'a> {
    fn write_capnp<W: Write>(self: &Self, w: &mut W) {
        let mut msg = message::Builder::new_default();
        self.to_capnp(&mut msg.get_root::<Self::Builder>().unwrap());
        serialize::write_message(w, &msg).unwrap();
    }
}

impl<T> ToCapnpOwned for T where T: for <'a> ToCapnp<'a> {}
