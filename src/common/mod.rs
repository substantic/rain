pub mod id;
pub mod keeppolicy;

trait FromCapnp {
    type Reader;
    fn from_capnp(read: &Self::Reader) -> Self;
}

trait ToCapnp {
    type Builder;
    fn to_capnp(self: &Self, build: &mut Self::Builder);
}
