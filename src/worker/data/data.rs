use std::path::{Path, PathBuf};
use std::os::unix::fs::PermissionsExt;

use common::DataType;

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
    data_type: DataType,
}

impl Data {
    /// Create Data from vector
    pub fn new(storage: Storage, data_type: DataType) -> Data {
        Data { storage, data_type }
    }

    pub fn new_from_path(path: PathBuf, size: usize, data_type: DataType) -> Data {
        Data {
            data_type,
            storage: Storage::Path(DataOnFs { path, size }),
        }
    }

    pub fn new_by_fs_move(
        source_path: &Path,
        target_path: PathBuf,
    ) -> ::std::result::Result<Self, ::std::io::Error> {
        ::std::fs::rename(source_path, &target_path)?;
        // TODO: If dir, set permissions recursively
        let metadata = ::std::fs::metadata(&target_path)?;
        metadata.permissions().set_mode(0o400);
        let size = metadata.len() as usize;
        let datatype = if metadata.is_dir() {
            DataType::Directory
        } else {
            DataType::Blob
        };
        Ok(Data::new_from_path(target_path, size, datatype))
    }

    pub fn new_by_fs_copy(
        source_path: &Path,
        target_path: PathBuf,
    ) -> ::std::result::Result<Self, ::std::io::Error> {
        ::std::fs::copy(source_path, &target_path)?;
        let metadata = ::std::fs::metadata(&target_path)?;
        metadata.permissions().set_mode(0o400);
        let size = metadata.len() as usize;
        let datatype = if metadata.is_dir() {
            DataType::Directory
        } else {
            DataType::Blob
        };
        Ok(Data::new_from_path(target_path, size, datatype))
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

    fn memory_to_fs(&self, data: &Vec<u8>, path: &Path) -> Result<()> {
        use std::io::Write;
        match self.data_type {
            DataType::Blob => {
                let mut file = ::std::fs::File::create(path)?;
                file.write_all(data)?;
                Ok(())
            }
            DataType::Directory => {
                let cursor = ::std::io::Cursor::new(data);
                ::tar::Archive::new(cursor).unpack(path)?;
                Ok(())
            }
        }
    }

    /// Map data object on a given path
    /// Caller is responsible for deletion of the path
    /// It creates a symlink to real data or new file if data only in memory
    pub fn map_to_path(&self, path: &Path) -> Result<()> {
        use std::os::unix::fs::symlink;

        match self.storage {
            Storage::Memory(ref data) => {
                self.memory_to_fs(data, path)
                //let mut file = ::std::fs::File::create(path)?;
                //file.write_all(data)?;
            }
            Storage::Path(ref data) => {
                symlink(&data.path, path)?;
                Ok(())
            }
        }
    }

    /// Export data object on a given path
    pub fn export_to_path(&self, path: &Path) -> Result<()> {
        match self.storage {
            Storage::Memory(ref data) => self.memory_to_fs(data, path),
            Storage::Path(ref data) => {
                ::std::fs::copy(&data.path, path)?;
                Ok(())
            }
        }
    }

    #[inline]
    pub fn is_blob(&self) -> bool {
        self.data_type == DataType::Blob
    }

    #[inline]
    pub fn is_directory(&self) -> bool {
        self.data_type == DataType::Directory
    }

    #[inline]
    pub fn data_type(&self) -> DataType {
        self.data_type
    }

    pub fn to_subworker_capnp(&self, builder: &mut ::subworker_capnp::local_data::Builder) {
        match self.storage {
            Storage::Memory(ref data) => builder.borrow().get_storage().set_memory(&data),
            Storage::Path(ref data) => builder
                .borrow()
                .get_storage()
                .set_path(data.path.to_str().unwrap()),
        };
        builder.borrow().set_data_type(self.data_type.to_capnp());
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
