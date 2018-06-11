use std::sync::atomic::AtomicBool;

lazy_static! {
    // Init debug mode TODO: depend on opts
    pub static ref DEBUG_CHECK_CONSISTENCY: AtomicBool = AtomicBool::new(false);
}

/// Common trait for objects with checkable consistency
pub trait ConsistencyCheck {
    fn check_consistency(&self) -> ::errors::Result<()>;

    /// Run check_consistency depending on DEBUG_CHECK_CONSISTENCY.
    fn check_consistency_opt(&self) -> ::errors::Result<()> {
        if DEBUG_CHECK_CONSISTENCY.load(::std::sync::atomic::Ordering::Relaxed) {
            self.check_consistency()
        } else {
            Ok(())
        }
    }
}
