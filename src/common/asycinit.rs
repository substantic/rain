use errors::Error;
use futures::{unsync, Future, IntoFuture};
use std::rc::Rc;

/// This code serves for "async" initialization Item may be in state "Initing"
/// that stores oneshots that are fired when the item is in ready state. The
/// object becomes ready when `set_value` is called

enum State<T> {
    // Object is still in initialization, vector contains callbacks when
    // object is ready
    Initing(Vec<unsync::oneshot::Sender<Rc<T>>>),

    // Value is ready
    Ready(Rc<T>),
}

pub struct AsyncInitWrapper<T> {
    state: State<T>,
}

impl<T: 'static> AsyncInitWrapper<T> {
    pub fn new() -> Self {
        Self {
            state: State::Initing(Vec::new()),
        }
    }

    pub fn is_ready(&self) -> bool {
        match self.state {
            State::Initing(_) => false,
            State::Ready(_) => true,
        }
    }

    /// Function that sets the value of the object when it is finally ready
    /// It triggers all waiting oneshots
    pub fn set_value(&mut self, value: Rc<T>) {
        match ::std::mem::replace(&mut self.state, State::Ready(value.clone())) {
            State::Initing(senders) => for sender in senders {
                // We do not care if send fails
                let _ = sender.send(value.clone());
            },
            State::Ready(_) => panic!("Element is already finished"),
        }
    }

    /// Returns future that is finished when object is ready,
    /// If object is already prepared than future is finished immediately
    pub fn wait(&mut self) -> Box<Future<Item = Rc<T>, Error = Error>> {
        match self.state {
            State::Ready(ref v) => Box::new(Ok(v.clone()).into_future()),
            State::Initing(ref mut senders) => {
                let (sender, receiver) = unsync::oneshot::channel();
                senders.push(sender);
                // TODO: Convert to testable error
                Box::new(receiver.map_err(|_| "Cancelled".into()))
            }
        }
    }
}
