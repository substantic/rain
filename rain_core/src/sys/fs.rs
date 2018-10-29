use std::error::Error;
use std::fs::File;
use std::io::{Read, Seek, Write};
use std::path::Path;
use std::process::exit;

use errors::Result;

/// Create "ready file", a file that is created when Rain is fully initialized
/// What it exactly means depends on type of execution (server/governor/...)
/// When creation failed, the program is terminated, since the outer waiter
/// cannot be informed about progress
pub fn create_ready_file(path: &Path) {
    match ::std::fs::File::create(path) {
        Ok(mut file) => {
            file.write_all(b"ready\n").unwrap();
            log::debug!("Ready file {:?} created", path);
        }
        Err(e) => {
            log::error!("Cannot create ready file: {}", e.description());
            exit(1);
        }
    }
}

pub fn read_tail(filename: &Path, size: u64) -> Result<String> {
    let mut file = File::open(filename)?;
    let end = file.seek(::std::io::SeekFrom::End(0))?;
    file.seek(::std::io::SeekFrom::Start(if end > size {
        end - size
    } else {
        0
    }))?;
    let mut result = String::new();
    file.read_to_string(&mut result)?;
    Ok(result)
}
