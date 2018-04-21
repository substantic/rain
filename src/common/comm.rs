
use tokio_io::AsyncWrite;
use tokio_io::AsyncRead;
use tokio_io::codec::length_delimited::{Framed, Builder};


pub fn create_protocol_stream<S>(stream: S) -> ::tokio_io::codec::length_delimited::Framed<S, Vec<u8>>
where
    S: AsyncRead + AsyncWrite + 'static
{
    Builder::new()
        .little_endian()
        .max_frame_length(128 * 1024 * 1024 /*128 MB*/)
        .new_framed(stream)
}