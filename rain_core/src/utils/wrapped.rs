use std::cell::{Ref, RefCell, RefMut};
use std::clone::Clone;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::rc::Rc;

/// Wrapper struct containing a `Rc<RefCell<T>>`, implementing  several
/// helper functions and useful traits.
///
/// The traits implemented are `Clone` and `Debug` (if `T` is `Debug`).
/// The traits`PartialEq`, `Eq` and `Hash` are implemented on the *pointer value*.
/// This allows very fast collections of such wrapped structs when the contained
/// structs are all considered semantically distinct objects.
///
/// Note that you can add methods to the wrapper with
/// `impl WrappedRcRefCell<MyType> { fn foo(&self) {} }`
/// or even `type WrapType = WrappedRcRefCell<MyType>; impl WrapType { ... }`.
#[derive(Default)]
pub struct WrappedRcRefCell<T> {
    inner: Rc<RefCell<T>>,
}

impl<T> WrappedRcRefCell<T> {
    /// Create a new wrapped instance. This is not called `new` so that you may implement
    /// your own function `new`.
    pub(crate) fn wrap(t: T) -> Self {
        WrappedRcRefCell {
            inner: Rc::new(RefCell::new(t)),
        }
    }

    /// Return a immutable reference to contents. Panics whenever `RefCell::borrow()` would.
    pub(crate) fn get(&self) -> Ref<T> {
        self.inner.deref().borrow()
    }

    /// Return a mutable reference to contents. Panics whenever `RefCell::borrow_mut()` would.
    pub(crate) fn get_mut(&self) -> RefMut<T> {
        self.inner.deref().borrow_mut()
    }

    // Return the number of strong references to the contained Rc
    /* Not used now, feel free to uncomment this
    pub(crate) fn get_num_refs(&self) -> usize {
        Rc::strong_count(&self.inner)
    } */
}

impl<T> Clone for WrappedRcRefCell<T> {
    fn clone(&self) -> Self {
        WrappedRcRefCell {
            inner: self.inner.clone(),
        }
    }
}

impl<T> Hash for WrappedRcRefCell<T> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        let ptr = &*self.inner as *const RefCell<T>;
        ptr.hash(state);
    }
}

impl<T> PartialEq for WrappedRcRefCell<T> {
    fn eq(&self, other: &Self) -> bool {
        Rc::ptr_eq(&self.inner, &other.inner)
    }
}

impl<T> Eq for WrappedRcRefCell<T> {}

/*
impl<T: Debug> Debug for WrappedRcRefCell<T> {
    fn fmt(&self, f: &mut Formatter) -> FmtResult {
        f.write_str("RcRefCell( ")?;
        self.get().fmt(f)?;
        f.write_str(" )")
    }
}
*/
