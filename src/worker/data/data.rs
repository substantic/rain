use std::io::Read;
use std::path::{PathBuf, Path};
use std::os::unix::fs::PermissionsExt;

use errors::Result;

#[derive(Debug)]
pub struct DataOnFs {
    pub path: PathBuf,
    /// If data is directory than size is sum of sizes of all blobs in directory
    pub size: usize,
}

#[derive(Debug)]
pub enum Storage {
    Memory(Vec<u8>),
    Path(DataOnFs),
}

#[derive(Debug)]
pub struct Data {
    storage: Storage,
}


impl Data {
    /// Create Data from vector
    pub fn new(storage: Storage) -> Data {
        Data { storage }
    }

    pub fn new_from_path(path: PathBuf, size: usize) -> Data {
        Data {
            storage: Storage::Path(DataOnFs { path, size }),
        }
    }

    pub fn new_by_fs_move(
        source_path: &Path,
        target_path: PathBuf,
    ) -> ::std::result::Result<Self, ::std::io::Error> {
        ::std::fs::rename(source_path, &target_path)?;
        let metadata = ::std::fs::metadata(&target_path)?;
        metadata.permissions().set_mode(0o400);
        let size = metadata.len() as usize;
        Ok(Data::new_from_path(target_path, size))
    }

    pub fn new_by_fs_copy(
        source_path: &Path,
        target_path: PathBuf,
    ) -> ::std::result::Result<Self, ::std::io::Error> {
        ::std::fs::copy(source_path, &target_path)?;
        let metadata = ::std::fs::metadata(&target_path)?;
        metadata.permissions().set_mode(0o400);
        let size = metadata.len() as usize;
        Ok(Data::new_from_path(target_path, size))
    }


    pub fn storage(&self) -> &Storage {
        &self.storage
    }

    /// Return size of data in bytes
    /// If data is directory than size is sum of sizes of all blobs in directory
    pub fn size(&self) -> usize {
        match self.storage {
            Storage::Memory(ref data) => data.len(),
            Storage::Path(ref data) => data.size,
        }
    }

    /// Map data object on a given path
    /// Caller is responsible for deletion of the path
    /// It creates a symlink to real data or new file if data only in memory
    pub fn map_to_path(&self, path: &Path) -> Result<()> {
        use std::io::Write;
        use std::os::unix::fs::symlink;

        match self.storage {
            Storage::Memory(ref data) => {
                let mut file = ::std::fs::File::create(path)?;
                file.write_all(data)?;
            }
            Storage::Path(ref data) => {
                symlink(&data.path, path)?;
            }
        };
        Ok(())
    }

    /// Export data object on a given path
    pub fn export_to_path(&self, path: &Path) -> Result<()> {
        use std::io::Write;

        match self.storage {
            Storage::Memory(ref data) => {
                let mut file = ::std::fs::File::create(path)?;
                file.write_all(data)?;
            }
            Storage::Path(ref data) => {
                ::std::fs::copy(&data.path, path)?;
            }
        };
        Ok(())
    }

    #[inline]
    pub fn is_blob(&self) -> bool {
        // TODO: Directories        
        true
    }

    pub fn to_subworker_capnp(
        &self,
        builder: &mut ::subworker_capnp::local_data::Builder,
    ) {
        match self.storage {
            Storage::Memory(ref data) => builder.borrow().get_storage().set_memory(&data),
            Storage::Path(ref data) => {
                builder.borrow().get_storage().set_path(
                    data.path.to_str().unwrap(),
                )
            }
        };
    }
}

impl Drop for Data {
    fn drop(&mut self) {
        match self.storage {
            Storage::Path(ref data) => ::std::fs::remove_file(&data.path).unwrap(),
            Storage::Memory(_) => { /* Do nothing */ }
        }
    }
}
