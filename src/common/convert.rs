use capnp::{message, traits, serialize};
use std::io::{Write, Read};

/// Generic trait for reading the value from a Capnp `Reader`.
/// All values are copied into `Self`.
pub trait FromCapnp<'a>
where
    Self: Sized,
{
    type Reader: traits::FromPointerReader<'a>;
    fn from_capnp(read: &'a Self::Reader) -> Self;
}

/// Generic trait for reading the value as a Capnp message from `Read`.
/// All values are copied into `Self`.
pub trait ReadCapnp {
    fn read_capnp<R: Read>(r: &mut R) -> Self;
}

/* NOTE: This general impl does not work (lifetime problems) :-(
impl<'a, T: FromCapnp<'a>> ReadCapnp for T {
    fn read_capnp<R: Read>(r: &mut R) -> Self {
        let msg = serialize::read_message(r, Default::default()).unwrap();
        let read = msg.get_root::<T::Reader>().unwrap();
        T::from_capnp(&read)
    }
}
*/

/// Generic trait for storing the value into a Capnp `Builder`.
pub trait ToCapnp<'a> {
    type Builder: traits::FromPointerBuilder<'a>;
    fn to_capnp(self: &Self, build: &mut Self::Builder);
}

/// Generic trait for writing the value into a `Write` as a Capnp message.
pub trait WriteCapnp {
    fn write_capnp<W: Write>(self: &Self, w: &mut W);
}

impl<T> WriteCapnp for T
where
    T: for<'a> ToCapnp<'a>,
{
    fn write_capnp<W: Write>(self: &Self, w: &mut W) {
        let mut msg = message::Builder::new_default();
        self.to_capnp(&mut msg.get_root::<T::Builder>().unwrap());
        serialize::write_message(w, &msg).unwrap();
    }
}
