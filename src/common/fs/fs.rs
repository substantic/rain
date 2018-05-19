use std::error::Error;
use std::io::Write;
use std::path::Path;
use std::process::exit;

/// Create "ready file", a file that is created when Rain is fully initialized
/// What it exactly means depends on type of execution (server/governor/...)
/// When creation failed, the program is terminated, since the outer waiter
/// cannot be informed about progress
pub fn create_ready_file(path: &Path) {
    match ::std::fs::File::create(path) {
        Ok(mut file) => {
            file.write_all(b"ready\n").unwrap();
            debug!("Ready file {:?} created", path);
        }
        Err(e) => {
            error!("Cannot create ready file: {}", e.description());
            exit(1);
        }
    }
}
