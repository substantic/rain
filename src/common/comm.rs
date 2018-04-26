
use tokio_io::AsyncWrite;
use tokio_io::AsyncRead;
use tokio_io::codec::length_delimited::{Framed, Builder};
use tokio_core::reactor::Handle;
use bytes::BytesMut;
use futures::{Future, Stream};
use std::collections::HashMap;


use errors::{Error, Result};

pub type SendType = Vec<u8>;

type CallId = i32;


#[derive(Debug)]
pub struct Sender {
    channel_sender: ::futures::unsync::mpsc::UnboundedSender<SendType>,
}

impl Sender {

    pub fn send(&self, data: SendType) {
        self.channel_sender.send(data).unwrap()
    }

}

pub fn create_protocol_stream<S>(stream: S) -> Framed<S, SendType>
where
    S: AsyncRead + AsyncWrite + 'static
{
    Builder::new()
        .little_endian()
        .max_frame_length(128 * 1024 * 1024 /*128 MB*/)
        .new_framed(stream)
}

pub struct Connection<S> where S: AsyncRead + AsyncWrite + 'static {
    stream: Framed<S, SendType>,
    channel_receiver: ::futures::unsync::mpsc::UnboundedReceiver<SendType>,
    channel_sender: ::futures::unsync::mpsc::UnboundedSender<SendType>,
}


impl<S> Connection<S> where S: AsyncRead + AsyncWrite + 'static {
    pub fn from(stream: Framed<S, SendType>) -> Self {
        let (channel_sender, channel_receiver) = ::futures::unsync::mpsc::unbounded();
        Connection {
            stream,
            channel_receiver,
            channel_sender
        }
    }

    pub fn sender(&self) -> Sender {
        Sender {
            channel_sender: self.channel_sender.clone()
        }
    }

    pub fn start<OnMessage, OnError>(self,
                 handle: &Handle,
                 on_message: OnMessage,
                 on_error: OnError) where OnMessage: Fn(BytesMut) -> Result<()> + 'static,
                                          OnError: FnOnce(Error) + 'static,
 {
        let Connection {
            stream: stream,
            channel_receiver: receiver,
            ..
        } = self;
        let (write, read) = stream.split();
        let send_future = receiver.map_err(|_| panic!("Send channel failed!")).forward(write).map(|_| ());
        let read_future = read.map_err(|e| e.into()).for_each(on_message);
        let future = read_future.select(send_future).map(|_| { panic!("Subworker connection closed") }).map_err(|(e, f)| on_error(e));
       // handle.spawn(send_future);
       // handle.spawn(read_future.then(|_| Ok(())));
        handle.spawn(future);
    }
}

/*
pub fn start_communication<S, OnMessage, OnError>(
                              handle: &Handle,
                              stream: Framed<S, SendType>,
                              on_message: OnMessage,
                              on_error: OnError) -> Sender
                              where OnMessage: Fn(BytesMut) -> Result<()> + 'static,
                                    OnError: FnOnce(Error) + 'static,

{
    let (write, read) = stream.split();
    let (write_sender, write_receiver) = ::futures::unsync::mpsc::unbounded();
    let send_future = write_receiver.map_err(|_| panic!("Send channel failed!")).forward(write).map(|_| ());
    let read_future = read.map_err(|e| e.into()).for_each(on_message);
    let future = read_future.select(send_future).map(|_| ()).map_err(|(e, f)| on_error(e));

    handle.spawn(future);

    Sender { channel_sender: write_sender }
}*/