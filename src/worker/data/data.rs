use std::path::{Path, PathBuf};

use common::DataType;
use worker::rpc::executor_serde::DataLocation;

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

fn isolate_symlink(path: &Path, prefix_path: &Path, metadata: &::std::fs::Metadata) {
    let link_target_path = ::std::fs::read_link(path).unwrap();
    if link_target_path.starts_with(prefix_path) {
        ::std::fs::remove_file(path).unwrap();
        debug!(
            "Expanding symlink to data dir {:?} to {:?}",
            link_target_path, path
        );
        if link_target_path.is_dir() {
            let mut flags = ::fs_extra::dir::CopyOptions::new();
            flags.copy_inside = true;
            ::fs_extra::dir::copy(&link_target_path, path, &flags).unwrap();
        } else {
            ::std::fs::copy(&link_target_path, path).unwrap();
        }
    } else {
        let mut perms = metadata.permissions();
        perms.set_readonly(true);
        ::std::fs::set_permissions(path, perms).unwrap();
    }
}

/** Replace all links to data with own copy &
    sets all file items as readonly */
fn isolate_directory(source_path: &Path, prefix_path: &Path) -> Result<()> {
    for entry in ::walkdir::WalkDir::new(source_path)
        .contents_first(true)
        .into_iter()
    {
        //let file_type = entry.file_type();
        let entry = entry.unwrap();
        let path = entry.path();
        let metadata = entry.metadata().unwrap();
        if !metadata.file_type().is_symlink() {
            let mut perms = metadata.permissions();
            perms.set_readonly(true);
            ::std::fs::set_permissions(path, perms)?;
        } else {
            isolate_symlink(path, prefix_path, &metadata);
        }
    }
    Ok(())
}

fn isolate_file(source_path: &Path, prefix_path: &Path, metadata: &::std::fs::Metadata) {
    if !metadata.file_type().is_symlink() {
        let mut perms = metadata.permissions();
        perms.set_readonly(true);
        ::std::fs::set_permissions(source_path, perms).unwrap();
    } else {
        isolate_symlink(source_path, prefix_path, metadata);
    }
}

fn set_readonly_dir(path: &Path, value: bool) {
    for entry in ::walkdir::WalkDir::new(path)
        .contents_first(true)
        .into_iter()
    {
        let entry = entry.unwrap();
        let metadata = entry.metadata().unwrap();
        let mut perms = metadata.permissions();
        perms.set_readonly(value);
        ::std::fs::set_permissions(entry.path(), perms).unwrap();
    }
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
        metadata: &::std::fs::Metadata,
        target_path: PathBuf,
        workdir_prefix: &Path,
    ) -> Result<Self> {
        let source_path = ::std::fs::canonicalize(source_path).unwrap();
        let datatype = if source_path.starts_with(workdir_prefix) {
            // Source path acutally points inside data dir
            // So we cannot move data, however
            // permissions & links are already resolved
            // so we need just bare copy
            if metadata.is_dir() {
                let mut flags = ::fs_extra::dir::CopyOptions::new();
                flags.copy_inside = true;
                ::fs_extra::dir::copy(source_path, &target_path, &flags).unwrap();
                DataType::Directory
            } else {
                ::std::fs::copy(source_path, &target_path)?;
                DataType::Blob
            }
        } else {
            ::std::fs::rename(source_path, &target_path)?;
            if metadata.is_dir() {
                isolate_directory(&target_path, workdir_prefix).unwrap();
                DataType::Directory
            } else {
                isolate_file(&target_path, workdir_prefix, &metadata);
                DataType::Blob
            }
        };
        let size = metadata.len() as usize;
        Ok(Data::new_from_path(target_path, size, datatype))
    }

    pub fn new_by_fs_copy(
        source_path: &Path,
        metadata: &::std::fs::Metadata,
        target_path: PathBuf,
        workdir_prefix: &Path,
    ) -> ::std::result::Result<Self, ::std::io::Error> {
        let size = metadata.len() as usize;
        let datatype = if metadata.is_dir() {
            let mut flags = ::fs_extra::dir::CopyOptions::new();
            flags.copy_inside = true;
            ::fs_extra::dir::copy(source_path, &target_path, &flags).unwrap();
            isolate_directory(&target_path, workdir_prefix).unwrap();
            DataType::Directory
        } else {
            ::std::fs::copy(source_path, &target_path)?;
            isolate_file(&target_path, workdir_prefix, metadata);
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
    pub fn link_to_path(&self, path: &Path) -> Result<()> {
        use std::os::unix::fs::symlink;

        match self.storage {
            Storage::Memory(ref data) => self.memory_to_fs(data, path),
            Storage::Path(ref data) => {
                symlink(&data.path, path)?;
                Ok(())
            }
        }
    }

    pub fn write_to_path(&self, path: &Path) -> Result<()> {
        match self.storage {
            Storage::Memory(ref data) => self.memory_to_fs(data, path),
            Storage::Path(ref data) => match self.data_type {
                DataType::Blob => {
                    ::std::fs::copy(&data.path, path)?;
                    let metadata = ::std::fs::metadata(path)?;
                    let mut perms = metadata.permissions();
                    perms.set_readonly(false);
                    ::std::fs::set_permissions(path, perms)?;
                    Ok(())
                }
                DataType::Directory => {
                    let mut flags = ::fs_extra::dir::CopyOptions::new();
                    flags.copy_inside = true;
                    ::fs_extra::dir::copy(&data.path, path, &flags).unwrap();
                    set_readonly_dir(path, false);
                    Ok(())
                }
            },
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

    pub fn create_location(&self) -> DataLocation {
        match self.storage {
            Storage::Memory(ref data) => DataLocation::Memory(data.clone()),
            Storage::Path(ref data) => DataLocation::Path(data.path.clone()),
        }
    }
}

impl Drop for Data {
    fn drop(&mut self) {
        match self.storage {
            Storage::Path(ref data) => match self.data_type {
                DataType::Blob => {
                    let mut perms = data.path.metadata().unwrap().permissions();
                    perms.set_readonly(false);
                    ::std::fs::set_permissions(&data.path, perms).unwrap();
                    ::std::fs::remove_file(&data.path).unwrap();
                }
                DataType::Directory => {
                    set_readonly_dir(&data.path, false);
                    ::std::fs::remove_dir_all(&data.path).unwrap();
                }
            },
            Storage::Memory(_) => { /* Do nothing */ }
        }
    }
}
